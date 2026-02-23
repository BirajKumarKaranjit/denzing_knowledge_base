"""
kb_system/kb_retriever.py
--------------------------
Orchestrates the two-stage retrieval pipeline with production-grade robustness:

    Stage 1 — LLM-Based Section Classification
    ──────────────────────────────────────────
    Given a user query, ask a fast LLM call to decide which KB sections are
    relevant. This is strictly better than keyword matching because:
      - It understands semantic intent, not surface words.
      - It generalises across domains: no hard-coded NBA keywords in code.
      - It handles paraphrase: "LeBron's scoring" → ddl (not just literal
        "player" keyword).
    The LLM returns a JSON list of section names. The call uses gpt-4o-mini
    (or equivalent cheap model) to keep latency low (typically < 300 ms).

    Stage 2 — Multi-Query Expansion + Reciprocal Rank Fusion (RRF)
    ───────────────────────────────────────────────────────────────
    A single embedding can miss relevant tables when the query phrasing
    doesn't closely match the table description (the classic vocabulary
    mismatch problem). We solve this with two complementary techniques:

    a) Multi-Query Expansion (MQE):
       Ask the LLM to rewrite the user query into N semantically equivalent
       variants. Each variant is independently embedded and searched.
       This covers different angles of the same information need.

       Example — "What is the score of LeBron James in 2022 regular season?"
       Expansion →
         1. "LeBron James points scored regular season 2022-23"
         2. "player box score scoring statistics for 2022 NBA season"
         3. "individual game performance stats LeBron 2022"
         4. "player total points game logs regular season"

    b) Reciprocal Rank Fusion (RRF):
       Merge the ranked result lists from all query variants into one
       consensus ranking using the formula:
           rrf_score(d) = Σ 1 / (k + rank_of_d_in_list_i)
       Tables that appear near the top of MULTIPLE lists score highest.
       This makes retrieval robust: if one phrasing misses a table,
       another phrasing catches it.

    Together MQE + RRF gives near-oracle retrieval quality for Text2SQL
    without needing to tune similarity thresholds or keyword lists.

    SQL Guidelines Sub-File Retrieval
    ──────────────────────────────────
    The sql_guidelines section now has embeddable sub-files (joins.md,
    aggregations.md, filters.md, comparisons.md, etc.) instead of one
    monolithic KB.md. At retrieval time we also run MQE + RRF over
    sql_guidelines sub-files so only the relevant guideline categories
    are injected (e.g., a simple filter query won't get aggregation rules).
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

# Module-level OpenAI client — shared across calls in a single process.
_client = openai.OpenAI(api_key=OPENAI_API_KEY)

# ── Section catalogue ─────────────────────────────────────────────────────────
# Descriptions are passed to the LLM so it understands what each section covers.
# Updating these descriptions (without touching code) changes classification
# behaviour — e.g., add a new section here when you add a new KB folder.
_SECTION_DESCRIPTIONS: dict[str, str] = {
    "ddl": (
        "Database table schemas, column definitions, data types, and semantic "
        "descriptions. Choose this for any query that involves player stats, team "
        "data, game results, box scores, awards, tracking, or any concrete data lookup."
    ),
    "business_rules": (
        "NBA domain metric definitions, KPI formulas, and business logic. Choose "
        "this when the query involves calculating advanced statistics, efficiency "
        "ratings, per-game metrics, or any formula-based computation."
    ),
    "sql_guidelines": (
        "SQL query patterns, join conventions, aggregation rules, date handling, "
        "and Postgres-specific best practices. Choose this when the query requires "
        "joins, aggregations, date filtering, or performance considerations."
    ),
}

# Sections whose KB.md is always injected into the prompt regardless of query.
# response_guidelines is always included to enforce output formatting.
_FORCED_SECTIONS: set[str] = set(ALWAYS_INJECT_SECTIONS)


def classify_sections_with_llm(user_query: str) -> list[str]:
    """
    Use a fast LLM call to decide which KB sections are relevant to the query.

    This replaces hard-coded keyword matching with genuine NLU. The LLM
    receives descriptions of each available section and returns a JSON list
    of the relevant section names. A fallback to ["ddl"] is applied if the
    LLM returns an unparseable response.

    Why not keyword matching?
    ─────────────────────────
    Keyword matching is fragile and domain-specific:
      - "What did LeBron score?" has no keywords like "player", "points"
        unless we enumerate every possible phrasing.
      - New domains (football, finance) would require new keyword lists.
    LLM classification understands intent and generalises with zero config.

    Latency note:
        Uses gpt-4o with max_tokens=60. This is fast (~200-400 ms) and cheap.
        The response is a tiny JSON array — no risk of long output delays.

    Parameters
    ----------
    user_query : str
        Raw natural language query from the user.

    Returns
    -------
    list[str]
        Ordered list of section names to search. Always includes "ddl" as a
        safe default. "sql_guidelines" is added when joins/aggregations are
        detected, "business_rules" for metric/formula questions.
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
            temperature=0.0,   # Deterministic — classification should be consistent
            max_tokens=60,     # A JSON array of 3 short strings is well under 60 tokens
        )
        raw = response.choices[0].message.content.strip()
        sections: list[str] = json.loads(raw)
        # Filter to only known section names and guarantee "ddl" is always present
        valid = set(_SECTION_DESCRIPTIONS.keys())
        sections = [s for s in sections if s in valid]
        if not sections:
            sections = ["ddl"]
        elif "ddl" not in sections:
            sections.insert(0, "ddl")
        return sections

    except (json.JSONDecodeError, openai.OpenAIError) as exc:
        # Graceful degradation: if LLM call fails, fall back to searching ddl only
        print(f"[kb_retriever] Section classification failed ({exc}), defaulting to ['ddl']")
        return ["ddl"]


def expand_query_with_llm(user_query: str, n: int = MULTI_QUERY_EXPANSION_COUNT) -> list[str]:
    """
    Generate N semantically equivalent re-phrasings of the user query.

    Multi-Query Expansion (MQE) addresses the vocabulary mismatch problem:
    the user's phrasing may not closely match the description stored in the
    KB. By generating multiple alternative phrasings and embedding each one,
    we search the vector space from multiple angles, dramatically improving
    recall for the same tables.

    The original query is always included as the first element so that the
    precision of the exact phrasing is preserved in the RRF fusion.

    Example
    -------
    Input:  "What is the score of LeBron James in 2022 regular season?"
    Output: [
        "What is the score of LeBron James in 2022 regular season?",   # original
        "LeBron James points scored regular season 2022-23",
        "player box score stats LeBron James 2022 season",
        "individual game performance scoring statistics 2022 NBA",
        "player points per game regular season 2022 box score data",
    ]

    Parameters
    ----------
    user_query : str
        The original natural language question.
    n : int
        Number of ADDITIONAL query variants to generate (excluding original).
        Total embeddings computed = n + 1. Default: MULTI_QUERY_EXPANSION_COUNT.

    Returns
    -------
    list[str]
        List of query strings: [original] + [n generated variants].
        Falls back to [original] only if the LLM call fails.
    """
    system_prompt = (
        "You are a query expansion assistant for a vector search retrieval system.\n"
        f"Generate {n} alternative phrasings of the user's question.\n"
        "Each phrasing should express the same information need from a different angle:\n"
        "  - Use different vocabulary (e.g., 'scoring' vs 'points')\n"
        "  - Vary specificity (specific player name vs generic 'player stats')\n"
        "  - Use database/technical language (e.g., 'box score statistics')\n"
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
            temperature=0.7,   # Some creativity to diversify phrasings
            max_tokens=300,    # n short strings, comfortably fits in 300 tokens
        )
        raw = response.choices[0].message.content.strip()
        variants: list[str] = json.loads(raw)
        # Sanitise: ensure we got a list of strings
        if not isinstance(variants, list):
            raise ValueError("LLM did not return a list")
        variants = [v for v in variants if isinstance(v, str) and v.strip()]
        # Combine: original first (for precision), variants after (for recall)
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

    This is the main function called by the SQL pipeline at query time.
    It replaces the previous keyword + single-embedding approach with a
    production-grade retrieval stack that handles vocabulary mismatch,
    ambiguous queries, and multi-section needs robustly.

    Complete Pipeline
    ─────────────────
    1. LLM section classification  — decide which sections to search
       (one fast LLM call, ~200-400 ms, deterministic temperature=0)
    2. Multi-query expansion       — generate N query variants
       (one LLM call, ~300-500 ms, creative temperature=0.7)
    3. Batch embed all variants    — one OpenAI embeddings API call
       (batches original + all variants together for efficiency)
    4. RRF retrieval per section   — for each target section, run
       per-query vector searches then fuse rankings via RRF
    5. SQL guidelines sub-retrieval — also search sql_guidelines sub-files
       with RRF to inject only relevant guideline categories
    6. Always-inject sections      — fetch KB.md for response_guidelines
    7. Return structured context   — ready for prompt_builder.py

    Parameters
    ----------
    conn : psycopg2.connection
        Open Postgres connection with KB tables populated.
    user_query : str
        Raw natural language question from the user.
    top_k : int
        Max number of table/guideline files to return per section.
    similarity_threshold : float
        Kept for API compatibility; RRF mode does not use this threshold
        (RRF is always-on and self-calibrating via rank position).

    Returns
    -------
    dict with keys:
        - "query_embedding"       : list[float] — the original query embedding
        - "all_query_variants"    : list[str]   — original + expanded queries
        - "sections_searched"     : list[str]   — which sections were searched
        - "matched_tables"        : list[dict]  — top-K DDL/rule files + RRF scores
        - "matched_sql_guidelines": list[dict]  — top-K sql_guidelines sub-files
        - "section_entry_points"  : dict[str, dict|None] — KB.md per searched section
        - "always_inject"         : dict[str, dict|None] — forced sections (response_guidelines)
        - "retrieval_summary"     : str         — human-readable log for debugging
    """
    display_query = user_query[:80] + "..." if len(user_query) > 80 else user_query
    print(f"\n[kb_retriever] Query: '{display_query}'")

    # ── Step 1: LLM-based section classification ──────────────────────────────
    print("[kb_retriever] Step 1: LLM section classification...")
    target_sections = classify_sections_with_llm(user_query)
    print(f"[kb_retriever] → Sections selected: {target_sections}")

    # ── Step 2: Multi-query expansion ─────────────────────────────────────────
    print("[kb_retriever] Step 2: Expanding query into multiple variants...")
    all_variants = expand_query_with_llm(user_query)
    print(f"[kb_retriever] → {len(all_variants)} query variant(s):")
    for i, v in enumerate(all_variants):
        label = "(original)" if i == 0 else f"(variant {i})"
        print(f"     {label}: {v[:90]}")

    # ── Step 3: Batch embed all variants in one API call ──────────────────────
    print("[kb_retriever] Step 3: Batch embedding all query variants...")
    all_embeddings = get_embeddings_batch(all_variants)
    # Keep original query embedding for caching / downstream reuse
    query_embedding = all_embeddings[0]
    print(f"[kb_retriever] → {len(all_embeddings)} embedding(s) computed")

    # ── Step 4: RRF retrieval for each target section ─────────────────────────
    print(f"[kb_retriever] Step 4: RRF retrieval across {len(target_sections)} section(s)...")
    all_matched_tables: list[dict] = []
    section_entry_points: dict[str, dict | None] = {}

    for section in target_sections:
        # Fetch the KB.md entry point for this section (context header for the LLM)
        entry_point = get_entry_point(conn, section)
        section_entry_points[section] = entry_point

        # Run RRF retrieval using all query variants
        tables = retrieve_with_rrf(
            conn=conn,
            query_embeddings=all_embeddings,
            section=section,
            top_k=top_k,
            per_query_k=max(top_k * 4, 10),  # Fetch 4× candidates per query for good fusion
            rrf_k=RRF_K,
        )

        for t in tables:
            t["_source_section"] = section  # Tag for debugging

        all_matched_tables.extend(tables)
        print(f"[kb_retriever] → [{section}]: {len(tables)} file(s) matched via RRF")
        for t in tables:
            name = t.get("metadata", {}).get("name", t["file_path"])
            print(f"       • {name}  rrf={t['rrf_score']:.5f}  cosine={t['best_cosine_score']:.3f}")

    # De-duplicate across sections (same file can't match twice)
    seen_paths: set[str] = set()
    unique_tables: list[dict] = []
    for table in sorted(all_matched_tables, key=lambda x: x["rrf_score"], reverse=True):
        if table["file_path"] not in seen_paths:
            seen_paths.add(table["file_path"])
            unique_tables.append(table)

    # ── Step 5: RRF retrieval for SQL guidelines sub-files ────────────────────
    # sql_guidelines now has sub-files (joins.md, aggregations.md, etc.) that are
    # embedded — we retrieve only the relevant ones instead of injecting all.
    print("[kb_retriever] Step 5: Retrieving relevant SQL guideline sub-files...")
    matched_sql_guidelines = retrieve_with_rrf(
        conn=conn,
        query_embeddings=all_embeddings,
        section="sql_guidelines",
        top_k=2,               # Max 2 guideline categories per query
        per_query_k=8,         # There are only ~6 sub-files total; fetch all
        rrf_k=RRF_K,
    )
    print(f"[kb_retriever] → {len(matched_sql_guidelines)} SQL guideline sub-file(s) matched")
    for g in matched_sql_guidelines:
        name = g.get("metadata", {}).get("name", g["file_path"])
        print(f"       • {name}  rrf={g['rrf_score']:.5f}")

    # Fetch sql_guidelines KB.md entry point (section overview header)
    sql_guidelines_entry = get_entry_point(conn, "sql_guidelines")

    # ── Step 6: Always-inject sections ───────────────────────────────────────
    always_inject: dict[str, dict | None] = {}
    for section in _FORCED_SECTIONS:
        if section not in target_sections:
            always_inject[section] = get_entry_point(conn, section)

    # ── Step 7: Build retrieval summary for logging / debugging ──────────────
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
