"""
kb_system/kb_retriever.py
--------------------------
Orchestrates the two-stage retrieval pipeline:

    Stage 1 — Section Classification (near-zero cost)
    ─────────────────────────────────────────────────
    Given a user query, decide which KB section(s) to search.
    For most NBA queries this is always "ddl" — but we also flag
    "business_rules" for metric/formula questions.

    This uses lightweight keyword matching + optional embedding
    similarity against section descriptions. It is O(n_sections)
    where n_sections ≈ 4, making it essentially free.

    Stage 2 — Vector Similarity Search (efficient)
    ───────────────────────────────────────────────
    Search only within the classified section(s), comparing the
    user query embedding against table file embeddings.
    O(n_tables_in_section) ≈ 8-15 for a typical NBA schema.

    This is the key efficiency win: instead of searching 50+ KB files
    globally, we search 8-15 table files in the relevant section only.

    The KB.md entry point files are NOT searched — they are fetched by
    section name and injected as context alongside the matched tables.
"""

from __future__ import annotations

from typing import Any

import psycopg2.extensions

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.config import (
    DEFAULT_TOP_K,
    SIMILARITY_THRESHOLD,
    ALWAYS_INJECT_SECTIONS,
    SECTION_KEYWORDS,
)
from kb_system.kb_embeddings import get_embedding
from kb_system.kb_store import retrieve_similar_tables, get_entry_point


def classify_sections(user_query: str) -> list[str]:
    """
    Determine which KB sections are relevant to the user's query.

    Uses keyword matching against SECTION_KEYWORDS from config.py.
    This is intentionally simple and fast — the vector search in Stage 2
    handles the nuanced semantic matching. Section classification just
    prevents searching completely irrelevant sections.

    For NBA queries, "ddl" will almost always be selected (since most
    questions involve tables). "business_rules" is added for metric
    questions. "sql_guidelines" and "response_guidelines" are always
    injected via ALWAYS_INJECT_SECTIONS in config.

    Parameters
    ----------
    user_query : str
        Raw natural language query from the user.
        e.g., "Who had the most assists per game last season?"

    Returns
    -------
    list[str]
        Ordered list of section names to search.
        Always includes at least ["ddl"] for NBA queries as a safe default.
    """
    query_lower = user_query.lower()
    matched_sections: set[str] = set()

    for section, keywords in SECTION_KEYWORDS.items():
        # Section is relevant if ANY keyword from its list appears in the query
        if any(kw in query_lower for kw in keywords):
            matched_sections.add(section)

    # Safety net: always include DDL for NBA analytics queries
    # since almost every SQL question requires schema knowledge
    matched_sections.add("ddl")

    # Put ddl first (most important), then others alphabetically
    ordered = ["ddl"]
    for section in sorted(matched_sections):
        if section != "ddl":
            ordered.append(section)

    return ordered


def retrieve_context_for_query(
    conn: psycopg2.extensions.connection,
    user_query: str,
    top_k: int = DEFAULT_TOP_K,
    similarity_threshold: float = SIMILARITY_THRESHOLD,
) -> dict[str, Any]:
    """
    Full two-stage retrieval pipeline: classify sections → vector search.

    Returns a structured context dict that prompt_builder.py uses to
    assemble the final SQL generation prompt. This is the main function
    called by the SQL pipeline at query time.

    Pipeline:
    1. Embed the user query (one API call)
    2. Classify which sections to search (keyword matching, free)
    3. For each relevant section, run pgvector similarity search
    4. Fetch the KB.md entry point for each section as context header
    5. Also fetch always-inject sections (sql_guidelines, response_guidelines)
    6. Return everything structured for prompt assembly

    Parameters
    ----------
    conn : psycopg2.connection
        Open Postgres connection with the KB tables populated.
    user_query : str
        Raw natural language query from the user.
    top_k : int
        Max number of table files to retrieve per section.
    similarity_threshold : float
        Minimum cosine similarity score for inclusion.

    Returns
    -------
    dict with keys:
        - "query_embedding": list[float] — the embedded query (cached for reuse)
        - "sections_searched": list[str] — which sections were searched
        - "matched_tables": list[dict] — top-K table files with content + scores
        - "section_entry_points": dict[str, dict] — KB.md files per section
        - "always_inject": dict[str, dict] — sql_guidelines + response_guidelines content
        - "retrieval_summary": str — human-readable summary for logging/debugging
    """
    print(f"\n[kb_retriever] Query: '{user_query[:80]}...' " if len(user_query) > 80 else f"\n[kb_retriever] Query: '{user_query}'")

    # ── Stage 1: Embed query + classify sections ──
    print("[kb_retriever] Stage 1: Embedding query and classifying sections...")
    query_embedding = get_embedding(user_query)
    target_sections = classify_sections(user_query)
    print(f"[kb_retriever] → Searching sections: {target_sections}")

    # ── Stage 2: Vector search within each classified section ──
    print(f"[kb_retriever] Stage 2: Searching within {len(target_sections)} section(s)...")
    all_matched_tables: list[dict] = []
    section_entry_points: dict[str, dict | None] = {}

    for section in target_sections:
        # Fetch the section's KB.md for context injection
        entry_point = get_entry_point(conn, section)
        section_entry_points[section] = entry_point

        # Run vector similarity search within this section
        tables = retrieve_similar_tables(
            conn=conn,
            query_embedding=query_embedding,
            section=section,
            top_k=top_k,
            similarity_threshold=similarity_threshold,
        )

        for t in tables:
            t["_source_section"] = section  # tag for debugging

        all_matched_tables.extend(tables)
        print(f"[kb_retriever] → {section}: {len(tables)} table(s) matched")

    # De-duplicate if the same file matched across multiple section searches
    seen_paths: set[str] = set()
    unique_tables: list[dict] = []
    for table in sorted(all_matched_tables, key=lambda x: x["relevance_score"], reverse=True):
        if table["file_path"] not in seen_paths:
            seen_paths.add(table["file_path"])
            unique_tables.append(table)

    # ── Fetch always-inject sections ──
    always_inject: dict[str, dict | None] = {}
    for section in ALWAYS_INJECT_SECTIONS:
        if section not in target_sections:  # Don't double-fetch
            always_inject[section] = get_entry_point(conn, section)

    # ── Build human-readable retrieval summary for logging ──
    summary_lines = [
        f"Sections searched: {', '.join(target_sections)}",
        f"Tables matched: {len(unique_tables)}",
    ]
    for t in unique_tables:
        name = t.get("metadata", {}).get("name", t["file_path"])
        score = t.get("relevance_score", 0)
        summary_lines.append(f"  • {name} (score: {score:.3f})")

    retrieval_summary = "\n".join(summary_lines)
    print(f"[kb_retriever] Retrieval summary:\n{retrieval_summary}\n")

    return {
        "query_embedding": query_embedding,
        "sections_searched": target_sections,
        "matched_tables": unique_tables,
        "section_entry_points": section_entry_points,
        "always_inject": always_inject,
        "retrieval_summary": retrieval_summary,
    }
