"""
kb_system/kb_store.py
Handles all Postgres persistence for the KB system.
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
from utils.config import (
    POSTGRES_DSN,
    EMBEDDING_DIMENSION,
    RRF_K,
    MANDATORY_FILES,
    BM25_ENABLED,
    BM25_PER_QUERY_K,
)
from kb_system.kb_parser import ParsedKBFile


def get_connection(database_creds) -> PgConnection:
    """
    Open and return a new Postgres connection using the DSN from config.

    Returns
    psycopg2.connection

    Raises
    psycopg2.OperationalError
        If the database is unreachable or credentials are wrong.
    """
    return psycopg2.connect(database_creds)


def init_schema(conn: PgConnection) -> None:
    """
    Create the kb_files table and required indexes if they don't exist.

    Parameters
    ----------
    conn : psycopg2.connection
        Open database connection. Changes are committed inside this function.
    """
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")

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

        cur.execute("ALTER TABLE kb_files ADD COLUMN IF NOT EXISTS search_vector tsvector;")

        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_kb_embedding
            ON kb_files
            USING hnsw (embedding vector_cosine_ops)
            WITH (m = 16, ef_construction = 64)
            WHERE embedding IS NOT NULL;
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_kb_metadata
            ON kb_files
            USING GIN (metadata);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_kb_section
            ON kb_files (section);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_kb_search_vector
            ON kb_files
            USING GIN (search_vector);
        """)

        cur.execute(
            """
            CREATE OR REPLACE FUNCTION kb_files_update_search_vector()
            RETURNS trigger AS $$
            BEGIN
                NEW.search_vector := to_tsvector(
                    'english',
                    COALESCE(NEW.metadata->>'name', '') || ' ' ||
                    COALESCE(NEW.metadata->>'description', '') || ' ' ||
                    COALESCE(NEW.metadata->>'example_queries', '') || ' ' ||
                    COALESCE(NEW.metadata->>'tags', '') || ' ' ||
                    COALESCE(NEW.content, '')
                );
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            """
        )

        cur.execute("""
            DROP TRIGGER IF EXISTS trg_kb_files_search_vector ON kb_files;
            CREATE TRIGGER trg_kb_files_search_vector
            BEFORE INSERT OR UPDATE ON kb_files
            FOR EACH ROW EXECUTE FUNCTION kb_files_update_search_vector();
        """)

        cur.execute(
            """
            UPDATE kb_files
            SET search_vector = to_tsvector(
                'english',
                COALESCE(metadata->>'name', '') || ' ' ||
                COALESCE(metadata->>'description', '') || ' ' ||
                COALESCE(metadata->>'example_queries', '') || ' ' ||
                COALESCE(metadata->>'tags', '') || ' ' ||
                COALESCE(content, '')
            )
            WHERE search_vector IS NULL;
            """
        )

    conn.commit()
    print("[kb_store] Schema initialized successfully.")


def upsert_kb_file(
    conn: PgConnection,
    parsed_file: ParsedKBFile,
    embedding: Optional[list[float]] = None,
) -> int:
    """
    Insert or update a single KB file record in the database.

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
    psycopg2.extras.register_uuid()

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
                json.dumps(parsed_file.metadata),
                parsed_file.content,
                parsed_file.is_entry_point,
                embedding_value,
            ),
        )
        row_id = cur.fetchone()[0]

    conn.commit()
    return row_id


def _fetch_mandatory_files(
    conn: PgConnection,
    section: str,
    filenames: list[str],
) -> list[dict[str, Any]]:
    """Fetch specific files from the DB by their basename within a section.

    Parameters
    ----------
    conn : psycopg2.connection
    section : str
        Section to restrict the lookup to (e.g., ``"sql_guidelines"``).
    filenames : list[str]
        Bare filenames to look up (e.g., ``["joins.md", "filters.md"]``).

    Returns
    -------
    list[dict]
        One record per found file. Files not present in the DB are silently
        skipped so a missing file never raises an exception.
    """
    results: list[dict] = []
    for filename in filenames:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT file_path, section, metadata, content
                FROM kb_files
                WHERE section = %s
                  AND is_entry_point = FALSE
                  AND file_path LIKE %s
                LIMIT 1;
                """,
                (section, f"%/{filename}"),
            )
            row = cur.fetchone()

        if row is None:
            continue

        record = dict(row)
        if isinstance(record.get("metadata"), str):
            record["metadata"] = json.loads(record["metadata"])

        record["rrf_score"] = 0.0
        record["best_cosine_score"] = 0.0
        record["relevance_score"] = 0.0
        record["_mandatory"] = True
        results.append(record)

    return results


def retrieve_with_rrf(
    conn: PgConnection,
    query_embeddings: list[list[float]],
    query_text: str = "",
    section: str = "ddl",
    top_k: int = 3,
    per_query_k: int = 10,
    rrf_k: int = RRF_K,
) -> list[dict[str, Any]]:
    """Retrieve the top-K most relevant files using Reciprocal Rank Fusion (RRF).
    Parameters
    ----------
    conn : psycopg2.connection
    query_embeddings : list[list[float]]
    section : str
    top_k : int
    per_query_k : int
    rrf_k : int

    Returns
    -------
    list[dict]
    """
    all_ranked_lists: list[list[dict]] = []

    for emb in query_embeddings:
        embedding_str = _format_embedding(emb)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    file_path,
                    section,
                    metadata,
                    content,
                    1 - (embedding <=> %s::vector) AS cosine_score
                FROM kb_files
                WHERE
                    section        = %s
                    AND is_entry_point = FALSE
                    AND embedding  IS NOT NULL
                ORDER BY embedding <=> %s::vector
                LIMIT %s;
                """,
                (embedding_str, section, embedding_str, per_query_k),
            )
            rows = cur.fetchall()

        ranked: list[dict] = []
        for row in rows:
            record = dict(row)
            if isinstance(record.get("metadata"), str):
                record["metadata"] = json.loads(record["metadata"])
            ranked.append(record)

        all_ranked_lists.append(ranked)

    if BM25_ENABLED and query_text.strip():
        bm25_ranked = retrieve_with_bm25(
            conn=conn,
            query_text=query_text,
            section=section,
            top_k=max(BM25_PER_QUERY_K, per_query_k),
        )
        if bm25_ranked:
            all_ranked_lists.append(bm25_ranked)

    rrf_scores: dict[str, float] = {}
    best_cosine: dict[str, float] = {}
    record_cache: dict[str, dict] = {}

    for ranked_list in all_ranked_lists:
        for rank, record in enumerate(ranked_list, start=1):
            fp = record["file_path"]
            rrf_scores[fp] = rrf_scores.get(fp, 0.0) + 1.0 / (rrf_k + rank)
            cosine = record.get("cosine_score", 0.0)
            if cosine > best_cosine.get(fp, 0.0):
                best_cosine[fp] = cosine
            if fp not in record_cache:
                record_cache[fp] = record

    mandatory_filenames: list[str] = MANDATORY_FILES.get(section, [])

    mandatory_paths: set[str] = set()
    semantic_paths: list[str] = []

    all_sorted = sorted(rrf_scores, key=lambda fp: rrf_scores[fp], reverse=True)
    for fp in all_sorted:
        basename = fp.split("/")[-1]
        if basename in mandatory_filenames:
            mandatory_paths.add(fp)
        else:
            semantic_paths.append(fp)

    results: list[dict] = []
    for fp in semantic_paths[:top_k]:
        record = dict(record_cache[fp])
        record["rrf_score"] = round(rrf_scores[fp], 6)
        record["best_cosine_score"] = round(best_cosine.get(fp, 0.0), 4)
        record["relevance_score"] = record["best_cosine_score"]
        results.append(record)

    if not mandatory_filenames:
        return results

    already_present: set[str] = {r["file_path"] for r in results}
    already_present.update(mandatory_paths)

    for fp in mandatory_paths:
        record = dict(record_cache[fp])
        record["rrf_score"] = round(rrf_scores[fp], 6)
        record["best_cosine_score"] = round(best_cosine.get(fp, 0.0), 4)
        record["relevance_score"] = record["best_cosine_score"]
        record["_mandatory"] = True
        results.append(record)

    missing_filenames = [
        fn for fn in mandatory_filenames
        if not any(fp.split("/")[-1] == fn for fp in already_present)
    ]
    if missing_filenames:
        fetched = _fetch_mandatory_files(conn, section, missing_filenames)
        results.extend(fetched)

    return results


def retrieve_with_bm25(
    conn: PgConnection,
    query_text: str,
    section: str,
    top_k: int,
) -> list[dict[str, Any]]:
    """Retrieve ranked KB files using Postgres full-text search."""
    if not query_text.strip():
        return []

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                WITH q AS (
                    SELECT websearch_to_tsquery('english', %s) AS tsq
                )
                SELECT
                    file_path,
                    section,
                    metadata,
                    content,
                    ts_rank_cd(search_vector, q.tsq) AS bm25_score
                FROM kb_files, q
                WHERE
                    section = %s
                    AND is_entry_point = FALSE
                    AND search_vector @@ q.tsq
                ORDER BY bm25_score DESC
                LIMIT %s;
                """,
                (query_text, section, top_k),
            )
            rows = cur.fetchall()
    except psycopg2.Error:
        conn.rollback()
        return []

    ranked: list[dict] = []
    for row in rows:
        record = dict(row)
        if isinstance(record.get("metadata"), str):
            record["metadata"] = json.loads(record["metadata"])
        ranked.append(record)
    return ranked


def retrieve_similar_tables(
    conn: PgConnection,
    query_embedding: list[float],
    section: str = "ddl",
    top_k: int = 3,
    similarity_threshold: float = 0.50,
) -> list[dict[str, Any]]:
    """
    Find the top-K most relevant table files using cosine similarity.

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
                1 - (embedding <=> %s::vector) AS relevance_score
            FROM kb_files
            WHERE
                section         = %s
                AND is_entry_point = FALSE
                AND embedding   IS NOT NULL
                AND 1 - (embedding <=> %s::vector) >= %s
            ORDER BY embedding <=> %s::vector
            LIMIT %s;
            """,
            (
                embedding_str,
                section,
                embedding_str,
                similarity_threshold,
                embedding_str,
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


def get_section_sub_files(conn: PgConnection, section: str) -> list[dict[str, Any]]:
    """Fetch all non-entry-point files for a given section.

    Parameters
    ----------
    conn : psycopg2.connection
    section : str

    Returns
    -------
    list[dict]
        Records with file_path, section, metadata, content fields.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT file_path, section, metadata, content
            FROM kb_files
            WHERE section = %s AND is_entry_point = FALSE
            ORDER BY file_path;
            """,
            (section,),
        )
        rows = cur.fetchall()

    result = []
    for row in rows:
        record = dict(row)
        if isinstance(record.get("metadata"), str):
            record["metadata"] = json.loads(record["metadata"])
        result.append(record)
    return result


def get_entry_point(conn: PgConnection, section: str) -> Optional[dict[str, Any]]:
    """
    Fetch the KB.md index file for a given section.

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
