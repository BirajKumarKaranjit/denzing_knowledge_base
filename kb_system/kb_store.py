"""
kb_system/kb_store.py
---------------------
Handles all Postgres persistence for the KB system.

Database schema:
    kb_files — one row per markdown file, stores:
        - file_path: relative path within knowledge_base_files/
        - section: top-level folder (ddl, business_rules, etc.)
        - metadata: JSONB (all frontmatter fields — flexible, no migrations needed)
        - content: TEXT (raw markdown body — DDL, rules, guidelines)
        - is_entry_point: BOOLEAN (True for KB.md section index files)
        - embedding: vector(1536) — NULL for entry points, populated for table files

The pgvector extension enables cosine similarity search using the <=> operator
directly in SQL, which is orders of magnitude faster than computing similarity
in Python across thousands of vectors.

Connection pooling note: for production use, replace psycopg2 direct connections
with psycopg2.pool.ThreadedConnectionPool or SQLAlchemy connection pool.
"""

from __future__ import annotations

import json
from typing import Any, Optional

import psycopg2
import psycopg2.extras
from psycopg2.extensions import connection as PgConnection

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.config import POSTGRES_DSN, EMBEDDING_DIMENSION
from kb_system.kb_parser import ParsedKBFile


def get_connection() -> PgConnection:
    """
    Open and return a new Postgres connection using the DSN from config.

    Each call creates a fresh connection. For scripts that run many
    operations, pass the connection around explicitly rather than calling
    this repeatedly.

    Returns
    -------
    psycopg2.connection
        Open database connection. Caller is responsible for closing it
        (use as a context manager or call conn.close() explicitly).

    Raises
    ------
    psycopg2.OperationalError
        If the database is unreachable or credentials are wrong.
    """
    return psycopg2.connect(POSTGRES_DSN)


def init_schema(conn: PgConnection) -> None:
    """
    Create the kb_files table and required indexes if they don't exist.

    Installs the pgvector extension, creates the kb_files table with a
    JSONB metadata column (schema-flexible) and a vector embedding column,
    then creates appropriate indexes for both similarity search and metadata
    filtering.

    Safe to run multiple times — all statements use IF NOT EXISTS.

    Parameters
    ----------
    conn : psycopg2.connection
        Open database connection. Changes are committed inside this function.
    """
    with conn.cursor() as cur:
        # Enable pgvector — must be installed on the Postgres server first:
        # CREATE EXTENSION vector; (or via apt: postgresql-16-pgvector)
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")

        # Main KB files table
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS kb_files (
                id          SERIAL PRIMARY KEY,
                file_path   TEXT        NOT NULL,
                section     TEXT        NOT NULL,
                metadata    JSONB,
                content     TEXT,
                is_entry_point BOOLEAN  DEFAULT FALSE,
                -- embedding is NULL for KB.md entry points (not retrieval targets)
                -- populated for individual table files (players.md, games.md, etc.)
                embedding   vector({EMBEDDING_DIMENSION}),
                created_at  TIMESTAMP   DEFAULT NOW(),
                updated_at  TIMESTAMP   DEFAULT NOW(),
                CONSTRAINT uq_kb_file_path UNIQUE (file_path)
            );
        """)

        # IVFFlat index for fast approximate nearest-neighbor search.
        # Only indexes rows that HAVE an embedding (table files, not KB.md files).
        # lists=100 is a good default for up to ~1M rows; tune based on table count.
        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_kb_embedding
            ON kb_files
            USING ivfflat (embedding vector_cosine_ops)
            WITH (lists = 50)
            WHERE embedding IS NOT NULL;
        """)

        # GIN index on JSONB metadata for fast key/value queries
        # e.g., WHERE metadata->>'priority' = 'high'
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_kb_metadata
            ON kb_files
            USING GIN (metadata);
        """)

        # Index on section for fast section-scoped retrieval
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_kb_section
            ON kb_files (section);
        """)

    conn.commit()
    print("[kb_store] Schema initialized successfully.")


def upsert_kb_file(
    conn: PgConnection,
    parsed_file: ParsedKBFile,
    embedding: Optional[list[float]] = None,
) -> int:
    """
    Insert or update a single KB file record in the database.

    Uses INSERT ... ON CONFLICT DO UPDATE (upsert) so this function is
    idempotent — running it twice with the same file just updates the
    existing record. This is important for the KB rebuild workflow where
    you re-run kb_builder.py after editing markdown files.

    Parameters
    ----------
    conn : psycopg2.connection
        Open database connection.
    parsed_file : ParsedKBFile
        Fully parsed KB file from kb_parser.parse_markdown_file().
    embedding : list[float] | None
        Pre-computed embedding vector (1536 floats). Should be None for
        KB.md entry point files since they are not retrieval targets.

    Returns
    -------
    int
        The database row ID (id column) of the upserted record.
    """
    # Register psycopg2 adapters so Python lists serialize to Postgres vector
    psycopg2.extras.register_uuid()

    # Convert embedding list to the string format pgvector expects: '[0.1, 0.2, ...]'
    embedding_value = _format_embedding(embedding) if embedding else None

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO kb_files
                (file_path, section, metadata, content, is_entry_point, embedding, updated_at)
            VALUES
                (%s, %s, %s, %s, %s, %s::vector, NOW())
            ON CONFLICT (file_path)
            DO UPDATE SET
                section         = EXCLUDED.section,
                metadata        = EXCLUDED.metadata,
                content         = EXCLUDED.content,
                is_entry_point  = EXCLUDED.is_entry_point,
                embedding       = EXCLUDED.embedding,
                updated_at      = NOW()
            RETURNING id;
            """,
            (
                parsed_file.file_path,
                parsed_file.section,
                json.dumps(parsed_file.metadata),   # JSONB
                parsed_file.content,
                parsed_file.is_entry_point,
                embedding_value,                    # vector or NULL
            ),
        )
        row_id = cur.fetchone()[0]

    conn.commit()
    return row_id


def retrieve_similar_tables(
    conn: PgConnection,
    query_embedding: list[float],
    section: str = "ddl",
    top_k: int = 3,
    similarity_threshold: float = 0.50,
) -> list[dict[str, Any]]:
    """
    Find the top-K most relevant table files using cosine similarity.

    Executes a pgvector similarity search restricted to a specific section
    (e.g., "ddl") and only against non-entry-point files (individual table
    files like players.md). Returns results sorted by relevance score descending.

    Parameters
    ----------
    conn : psycopg2.connection
    query_embedding : list[float]
    section : str
    top_k : int
    similarity_threshold : float
    Returns
    -------
    list[dict]
        List of matching records, each containing:
            - file_path, section, metadata (dict), content, relevance_score
        Sorted by relevance_score descending (most relevant first).
    """
    embedding_str = _format_embedding(query_embedding)

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                file_path,
                section,
                metadata,
                content,
                -- Convert cosine DISTANCE to cosine SIMILARITY
                -- pgvector's <=> returns distance (0=identical, 2=opposite)
                -- so similarity = 1 - distance
                1 - (embedding <=> %s::vector) AS relevance_score
            FROM kb_files
            WHERE
                section         = %s
                AND is_entry_point = FALSE
                AND embedding   IS NOT NULL
                -- Pre-filter by threshold in SQL to avoid fetching irrelevant rows
                AND 1 - (embedding <=> %s::vector) >= %s
            ORDER BY embedding <=> %s::vector  -- ORDER BY distance ASC = similarity DESC
            LIMIT %s;
            """,
            (
                embedding_str,  # for SELECT similarity calculation
                section,
                embedding_str,  # for WHERE threshold filter
                similarity_threshold,
                embedding_str,  # for ORDER BY
                top_k,
            ),
        )
        rows = cur.fetchall()
    results = []
    for row in rows:
        record = dict(row)
        if isinstance(record.get("metadata"), str):
            record["metadata"] = json.loads(record["metadata"])
        results.append(record)

    return results


def get_entry_point(conn: PgConnection, section: str) -> Optional[dict[str, Any]]:
    """
    Fetch the KB.md index file for a given section.

    Entry points are injected alongside matched table files to give the
    LLM section-level context (e.g., what the DDL section covers overall).

    Parameters
    ----------
    conn : psycopg2.connection
        Open database connection.
    section : str
        Section name (e.g., "ddl", "sql_guidelines").

    Returns
    -------
    dict | None
        Record dict with file_path, metadata, content fields.
        None if no entry point exists for this section.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT file_path, section, metadata, content
            FROM kb_files
            WHERE section = %s AND is_entry_point = TRUE
            LIMIT 1;
            """,
            (section,),
        )
        row = cur.fetchone()

    if row is None:
        return None

    record = dict(row)
    if isinstance(record.get("metadata"), str):
        record["metadata"] = json.loads(record["metadata"])
    return record


def list_all_files(conn: PgConnection, section: Optional[str] = None) -> list[dict]:
    """
    List all KB files stored in the database, optionally filtered by section.

    Useful for debugging, auditing which files are loaded, and verifying
    that embeddings have been computed for all table files.

    Parameters
    ----------
    conn : psycopg2.connection
        Open database connection.
    section : str | None
        If provided, filter to this section only. If None, return all files.

    Returns
    -------
    list[dict]
        List of records with file_path, section, is_entry_point,
        has_embedding (bool), and metadata name field.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        if section:
            cur.execute(
                """
                SELECT
                    file_path,
                    section,
                    is_entry_point,
                    embedding IS NOT NULL AS has_embedding,
                    metadata->>'name' AS name
                FROM kb_files
                WHERE section = %s
                ORDER BY section, file_path;
                """,
                (section,),
            )
        else:
            cur.execute(
                """
                SELECT
                    file_path,
                    section,
                    is_entry_point,
                    embedding IS NOT NULL AS has_embedding,
                    metadata->>'name' AS name
                FROM kb_files
                ORDER BY section, file_path;
                """
            )
        return [dict(row) for row in cur.fetchall()]


def _format_embedding(embedding: list[float]) -> str:
    """
    Convert a Python float list to the string format pgvector expects.

    pgvector accepts embeddings as a string like '[0.123, 0.456, ...]'
    when cast with ::vector in SQL. This helper ensures consistent formatting.

    Parameters
    ----------
    embedding : list[float]
        Dense vector as a Python list of floats.

    Returns
    -------
    str
        Formatted string ready for use in %s::vector SQL parameter.
    """
    return "[" + ",".join(f"{v:.8f}" for v in embedding) + "]"
