"""
kb_system/kb_retriever.py
--------------------------
Orchestrates the two-stage retrieval pipeline with production-grade robustness:

    Stage 1 — LLM-Based Section Classification

    Stage 2 — Multi-Query Expansion + Reciprocal Rank Fusion (RRF)
    Together MQE + RRF gives near-oracle retrieval quality for Text2SQL
    without needing to tune similarity thresholds or keyword lists.

    SQL Guidelines Sub-File Retrieval
"""

from __future__ import annotations

import json
from typing import Any

import openai
import psycopg2.extensions

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.config import (
    OPENAI_API_KEY,
    OPENAI_GENERATION_MODEL,
    DEFAULT_TOP_K,
    SIMILARITY_THRESHOLD,
    ALWAYS_INJECT_SECTIONS,
    MULTI_QUERY_EXPANSION_COUNT,
    RRF_K,
)
from kb_system.kb_embeddings import get_embeddings_batch
from kb_system.kb_store import retrieve_with_rrf, get_entry_point

_client = openai.OpenAI(api_key=OPENAI_API_KEY)


_SECTION_DESCRIPTIONS: dict[str, str] = {
    "ddl": (
        "Database table schemas, column definitions, data types, and semantic "
        "descriptions. Choose this for any query that involves player stats, team "
        "data, game results, box scores, awards, tracking, or any concrete data lookup."
    ),
    "business_rules": (
        " Domain specific metric definitions, KPI formulas, and business logic. Choose "
        "this when the query involves calculating advanced statistics, efficiency "
        "ratings, metrics, or any formula-based computation."
    ),
    "sql_guidelines": (
        "SQL query patterns, join conventions, aggregation rules, date handling, "
        "and Postgres-specific best practices. Choose this when the query requires "
        "joins, aggregations, date filtering, or performance considerations."
    ),
}

_FORCED_SECTIONS: set[str] = set(ALWAYS_INJECT_SECTIONS)


def classify_sections_with_llm(user_query: str) -> list[str]:
    """
    Use a fast LLM call to decide which KB sections are relevant to the query.
    Parameters
    ----------
    user_query : str

    Returns
    -------
    list[str]
    """
    section_list = "\n".join(
        f'- "{name}": {desc}' for name, desc in _SECTION_DESCRIPTIONS.items()
    )

    system_prompt = (
        "You are a routing assistant for a Text2SQL knowledge base system.\n"
        "Given a user's natural language question and a list of KB sections, "
        "return a JSON array of the section names that are relevant to answer the query.\n"
        "Return ONLY a valid JSON array of strings, nothing else.\n"
        "Example output: [\"ddl\", \"business_rules\"]"
    )

    user_prompt = (
        f"User query: {user_query}\n\n"
        f"Available sections:\n{section_list}\n\n"
        "Which sections should be searched? Return a JSON array."
    )

    try:
        response = _client.chat.completions.create(
            model=OPENAI_GENERATION_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.0,
            max_tokens=60,
        )
        raw = response.choices[0].message.content.strip()
        sections: list[str] = json.loads(raw)
        valid = set(_SECTION_DESCRIPTIONS.keys())
        sections = [s for s in sections if s in valid]
        if not sections:
            sections = ["ddl"]
        elif "ddl" not in sections:
            sections.insert(0, "ddl")
        return sections

    except (json.JSONDecodeError, openai.OpenAIError) as exc:
        print(f"[kb_retriever] Section classification failed ({exc}), defaulting to ['ddl']")
        return ["ddl"]


def expand_query_with_llm(user_query: str, n: int = MULTI_QUERY_EXPANSION_COUNT) -> list[str]:
    """
    Generate N semantically equivalent re-phrasings of the user query.
    Multi-Query Expansion (MQE) addresses the vocabulary mismatch problem.
    Parameters
    user_query : str
    n : int

    Returns
    list[str]
    """
    system_prompt = (
        "You are a query expansion assistant for a vector search retrieval system.\n"
        f"Generate {n} alternative phrasings of the user's question.\n"
        "Each phrasing should express the same information need from a different angle:\n"
        "  - Use different vocabulary conveying semantically same meaning\n"
        "Return ONLY a JSON array of strings with exactly {n} items. "
        "Do not include the original query."
    ).replace("{n}", str(n))

    user_prompt = f"Original query: {user_query}"

    try:
        response = _client.chat.completions.create(
            model=OPENAI_GENERATION_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.7,
            max_tokens=300,
        )
        raw = response.choices[0].message.content.strip()
        variants: list[str] = json.loads(raw)
        if not isinstance(variants, list):
            raise ValueError("LLM did not return a list")
        variants = [v for v in variants if isinstance(v, str) and v.strip()]
        return [user_query] + variants[:n]

    except (json.JSONDecodeError, ValueError, openai.OpenAIError) as exc:
        print(f"[kb_retriever] Query expansion failed ({exc}), using original query only.")
        return [user_query]


def retrieve_context_for_query(
    conn: psycopg2.extensions.connection,
    user_query: str,
    top_k: int = DEFAULT_TOP_K,
    similarity_threshold: float = SIMILARITY_THRESHOLD,
) -> dict[str, Any]:
    """
    Full retrieval pipeline: LLM classification → MQE → embed all variants → RRF.

    Parameters
    ----------
    conn : psycopg2.connection
    user_query : str
    top_k : int
    similarity_threshold : float

    Returns
    -------
    dict
    """
    display_query = user_query[:80] + "..." if len(user_query) > 80 else user_query
    print(f"\n[kb_retriever] Query: '{display_query}'")

    print("[kb_retriever] Step 1: LLM section classification...")
    target_sections = classify_sections_with_llm(user_query)
    print(f"[kb_retriever] → Sections selected: {target_sections}")

    print("[kb_retriever] Step 2: Expanding query into multiple variants...")
    all_variants = expand_query_with_llm(user_query)
    print(f"[kb_retriever] → {len(all_variants)} query variant(s):")
    for i, v in enumerate(all_variants):
        label = "(original)" if i == 0 else f"(variant {i})"
        print(f"     {label}: {v[:90]}")

    print("[kb_retriever] Step 3: Batch embedding all query variants...")
    all_embeddings = get_embeddings_batch(all_variants)
    query_embedding = all_embeddings[0]
    print(f"[kb_retriever] → {len(all_embeddings)} embedding(s) computed")

    print(f"[kb_retriever] Step 4: RRF retrieval across {len(target_sections)} section(s)...")
    all_matched_tables: list[dict] = []
    section_entry_points: dict[str, dict | None] = {}

    for section in target_sections:
        entry_point = get_entry_point(conn, section)
        section_entry_points[section] = entry_point

        tables = retrieve_with_rrf(
            conn=conn,
            query_embeddings=all_embeddings,
            section=section,
            top_k=top_k,
            per_query_k=max(top_k * 4, 10),
            rrf_k=RRF_K,
        )

        for t in tables:
            t["_source_section"] = section

        all_matched_tables.extend(tables)
        print(f"[kb_retriever] → [{section}]: {len(tables)} file(s) matched via RRF")
        for t in tables:
            name = t.get("metadata", {}).get("name", t["file_path"])
            print(f"       • {name}  rrf={t['rrf_score']:.5f}  cosine={t['best_cosine_score']:.3f}")

    seen_paths: set[str] = set()
    unique_tables: list[dict] = []
    for table in sorted(all_matched_tables, key=lambda x: x["rrf_score"], reverse=True):
        if table["file_path"] not in seen_paths:
            seen_paths.add(table["file_path"])
            unique_tables.append(table)

    print("[kb_retriever] Step 5: Retrieving relevant SQL guideline sub-files...")
    matched_sql_guidelines = retrieve_with_rrf(
        conn=conn,
        query_embeddings=all_embeddings,
        section="sql_guidelines",
        top_k=2,
        per_query_k=8,
        rrf_k=RRF_K,
    )
    print(f"[kb_retriever] → {len(matched_sql_guidelines)} SQL guideline sub-file(s) matched")
    for g in matched_sql_guidelines:
        name = g.get("metadata", {}).get("name", g["file_path"])
        print(f"       • {name}  rrf={g['rrf_score']:.5f}")

    sql_guidelines_entry = get_entry_point(conn, "sql_guidelines")

    always_inject: dict[str, dict | None] = {}
    for section in _FORCED_SECTIONS:
        if section not in target_sections:
            always_inject[section] = get_entry_point(conn, section)

    summary_lines = [
        f"Sections searched  : {', '.join(target_sections)}",
        f"Query variants     : {len(all_variants)}",
        f"Tables matched     : {len(unique_tables)}",
        f"SQL guidelines     : {len(matched_sql_guidelines)}",
    ]
    for t in unique_tables:
        name = t.get("metadata", {}).get("name", t["file_path"])
        summary_lines.append(
            f"  • {name}  (rrf={t['rrf_score']:.5f}, cosine={t['best_cosine_score']:.3f})"
        )

    retrieval_summary = "\n".join(summary_lines)
    print(f"[kb_retriever] Summary:\n{retrieval_summary}\n")

    return {
        "query_embedding": query_embedding,
        "all_query_variants": all_variants,
        "sections_searched": target_sections,
        "matched_tables": unique_tables,
        "matched_sql_guidelines": matched_sql_guidelines,
        "section_entry_points": section_entry_points,
        "sql_guidelines_entry": sql_guidelines_entry,
        "always_inject": always_inject,
        "retrieval_summary": retrieval_summary,
    }
