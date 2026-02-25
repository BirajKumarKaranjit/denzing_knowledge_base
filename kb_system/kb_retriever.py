"""
kb_system/kb_retriever.py

Orchestrates the full retrieval pipeline:

    Stage 1  — LLM-based section classification
    Stage 2  — Multi-query expansion + Reciprocal Rank Fusion (RRF)
    Stage 3  — Cross-encoder LLM re-ranking with FK expansion and adaptive cutoff
    Stage 4  — SQL guidelines sub-file retrieval
"""

from __future__ import annotations

import json
import re
from typing import Any

import openai
import psycopg2.extensions
from openai.types.chat import ChatCompletionSystemMessageParam, ChatCompletionUserMessageParam

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
from utils.prompts.kb_generation_prompts import (
    CROSS_ENCODER_SYSTEM_PROMPT,
    cross_encoder_user_prompt,
)

_client = openai.OpenAI(api_key=OPENAI_API_KEY)

# How many RRF candidates to feed into the cross-encoder.
_CROSS_ENCODER_CANDIDATE_K: int = 10

# Elbow detection: drop candidates if the score gap exceeds this fraction.
_ELBOW_DROP_THRESHOLD: float = 0.50

# Top-N tables for FK expansion after re-ranking.
_FK_EXPANSION_TOP_N: int = 3

_SECTION_DESCRIPTIONS: dict[str, str] = {
    "ddl": (
        "Database table schemas, column definitions, data types, and semantic "
        "descriptions. Choose this for any query that involves concrete data lookup, "
        "entity attributes, or any table/column reference."
    ),
    "business_rules": (
        "Domain-specific metric definitions, KPI formulas, and business logic. Choose "
        "this when the query involves calculating derived statistics, efficiency "
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
    """Use an LLM call to decide which KB sections are relevant to the query.

    Parameters
    ----------
    user_query:
        Raw user question.

    Returns
    -------
    list[str]
        Ordered list of section names to search. Always includes "ddl".
    """
    section_list = "\n".join(
        f'- "{name}": {desc}' for name, desc in _SECTION_DESCRIPTIONS.items()
    )
    system_prompt = (
        "You are a routing assistant for a Text2SQL knowledge base system.\n"
        "Given a user's natural language question and a list of KB sections, "
        "return a JSON array of the section names relevant to answer the query.\n"
        "Return ONLY a valid JSON array of strings, nothing else.\n"
        'Example output: ["ddl", "business_rules"]'
    )
    user_prompt = (
        f"User query: {user_query}\n\n"
        f"Available sections:\n{section_list}\n\n"
        "Which sections should be searched? Return a JSON array."
    )
    try:
        _msgs_classify: list[
            ChatCompletionSystemMessageParam | ChatCompletionUserMessageParam
        ] = [
            ChatCompletionSystemMessageParam(role="system", content=system_prompt),
            ChatCompletionUserMessageParam(role="user", content=user_prompt),
        ]
        response = _client.chat.completions.create(
            model=OPENAI_GENERATION_MODEL,
            messages=_msgs_classify,
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
    """Generate N semantically equivalent re-phrasings of the user query.

    Multi-Query Expansion addresses the vocabulary mismatch problem between
    how users phrase questions and how table descriptions are written.

    Parameters
    ----------
    user_query:
        Original question.
    n:
        Number of variants to generate (excluding the original).

    Returns
    -------
    list[str]
        Original query prepended to the generated variants.
    """
    system_prompt = (
        "You are a query expansion assistant for a vector search retrieval system.\n"
        f"Generate {n} alternative phrasings of the user's question.\n"
        "Each phrasing should express the same information need from a different angle "
        "using different vocabulary.\n"
        f"Return ONLY a JSON array of strings with exactly {n} items. "
        "Do not include the original query."
    )
    try:
        _msgs_expand: list[
            ChatCompletionSystemMessageParam | ChatCompletionUserMessageParam
        ] = [
            ChatCompletionSystemMessageParam(role="system", content=system_prompt),
            ChatCompletionUserMessageParam(
                role="user", content=f"Original query: {user_query}"
            ),
        ]
        response = _client.chat.completions.create(
            model=OPENAI_GENERATION_MODEL,
            messages=_msgs_expand,
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


def _score_candidate_with_llm(
    user_query: str,
    table_name: str,
    table_description: str,
) -> float:
    """Score a single (query, table) pair using a lightweight LLM call.

    This is the cross-encoder step: unlike bi-encoder similarity which scores
    query and document independently, this call sees both together, enabling
    term-level interaction matching (e.g. "scoring" → "points" column).

    Parameters
    ----------
    user_query:
        The original user question.
    table_name:
        Name of the candidate table.
    table_description:
        The table's frontmatter description text.

    Returns
    -------
    float
        Relevance score in [0.0, 1.0]. Returns 0.0 on parse failure.
    """
    try:
        _msgs_score: list[
            ChatCompletionSystemMessageParam | ChatCompletionUserMessageParam
        ] = [
            ChatCompletionSystemMessageParam(role="system", content=CROSS_ENCODER_SYSTEM_PROMPT),
            ChatCompletionUserMessageParam(
                role="user",
                content=cross_encoder_user_prompt(user_query, table_name, table_description),
            ),
        ]
        response = _client.chat.completions.create(
            model=OPENAI_GENERATION_MODEL,
            messages=_msgs_score,
            temperature=0.0,
            max_tokens=30,
        )
        raw = response.choices[0].message.content.strip()
        # Extract JSON even if the model adds minor surrounding text
        match = re.search(r'\{[^}]+}', raw)
        if match:
            data = json.loads(match.group())
            score = float(data.get("score", 0.0))
            return max(0.0, min(1.0, score))
    except (json.JSONDecodeError, ValueError, openai.OpenAIError, KeyError):
        pass
    return 0.0


def _extract_foreign_key_refs(ddl_content: str) -> list[str]:
    """Parse REFERENCES clauses from a DDL string to find FK-linked tables.

    Parameters
    ----------
    ddl_content:
        Raw markdown content of a table KB file (contains CREATE TABLE SQL).

    Returns
    -------
    list[str]
        Table names found in REFERENCES clauses.
    """
    return re.findall(
        r'REFERENCES\s+["\'`]?(\w+)["\'`]?\s*\(',
        ddl_content,
        re.IGNORECASE,
    )


def _apply_cross_encoder_reranking(
    user_query: str,
    candidates: list[dict],
    conn: psycopg2.extensions.connection,
) -> list[dict]:
    """Re-rank RRF candidates using a cross-encoder LLM call, then apply FK expansion.

    Steps:
        1. Score each candidate with a joint (query, description) LLM call.
        2. Re-sort by cross-encoder score descending.
        3. Apply elbow detection: drop everything below a 30% score gap.
        4. For the top-3 tables, parse their DDL for REFERENCES clauses and
           pull in any referenced tables not already in the result set.

    Parameters
    ----------
    user_query:
        Original user question.
    candidates:
        Up to _CROSS_ENCODER_CANDIDATE_K records from RRF retrieval.
    conn:
        Open Postgres connection for FK expansion lookups.

    Returns
    -------
    list[dict]
        Re-ranked and FK-expanded list of table records.
    """
    if not candidates:
        return candidates

    # --- Step 1 & 2: Score and sort ---
    print(f"[kb_retriever] Cross-encoder: scoring {len(candidates)} candidate(s)...")
    for record in candidates:
        metadata = record.get("metadata") or {}
        description = metadata.get("description", "")
        table_name = metadata.get("name", record.get("file_path", ""))
        score = _score_candidate_with_llm(user_query, table_name, description)
        record["cross_encoder_score"] = score
        print(f"       • {table_name}: cross={score:.3f}  rrf={record.get('rrf_score', 0):.5f}")

    reranked = sorted(candidates, key=lambda r: r["cross_encoder_score"], reverse=True)

    # --- Step 3: Elbow detection ---
    scores = [r["cross_encoder_score"] for r in reranked]
    cutoff_idx = len(reranked)
    for i in range(1, len(scores)):
        if scores[i - 1] > 0 and (scores[i - 1] - scores[i]) / scores[i - 1] > _ELBOW_DROP_THRESHOLD:
            cutoff_idx = i
            break

    if cutoff_idx < len(reranked):
        dropped = len(reranked) - cutoff_idx
        print(
            f"[kb_retriever] Elbow cutoff at index {cutoff_idx} "
            f"(gap {scores[cutoff_idx - 1]:.3f} → {scores[cutoff_idx]:.3f}): "
            f"dropping {dropped} low-relevance candidate(s)."
        )
    reranked = reranked[:cutoff_idx]

    # Update relevance_score to reflect the cross-encoder result
    for r in reranked:
        r["relevance_score"] = r["cross_encoder_score"]

    # --- Step 4: FK expansion for top-N tables ---
    import psycopg2.extras

    existing_paths = {r["file_path"] for r in reranked}
    top_tables = reranked[:_FK_EXPANSION_TOP_N]

    for table_record in top_tables:
        content = table_record.get("content", "")
        fk_refs = _extract_foreign_key_refs(content)

        for ref_table in fk_refs:
            # Check if this table already appears in results
            already_present = any(
                r.get("metadata", {}).get("name", "") == ref_table
                or ref_table in r.get("file_path", "")
                for r in reranked
            )
            if already_present:
                continue

            # Look it up in the database
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT file_path, section, metadata, content
                    FROM kb_files
                    WHERE metadata->>'name' = %s AND is_entry_point = FALSE
                    LIMIT 1;
                    """,
                    (ref_table,),
                )
                row = cur.fetchone()

            if row and row["file_path"] not in existing_paths:
                import json as _json
                fk_record = dict(row)
                if isinstance(fk_record.get("metadata"), str):
                    fk_record["metadata"] = _json.loads(fk_record["metadata"])
                fk_record["cross_encoder_score"] = 0.0
                fk_record["rrf_score"] = 0.0
                fk_record["best_cosine_score"] = 0.0
                fk_record["relevance_score"] = 0.0
                fk_record["_fk_expanded"] = True
                reranked.append(fk_record)
                existing_paths.add(fk_record["file_path"])
                print(
                    f"[kb_retriever] FK expansion: added '{ref_table}' "
                    f"(referenced by {table_record.get('metadata', {}).get('name', '')})"
                )

    return reranked


def retrieve_context_for_query(
    conn: psycopg2.extensions.connection,
    user_query: str,
    top_k: int = DEFAULT_TOP_K,
    similarity_threshold: float = SIMILARITY_THRESHOLD,
) -> dict[str, Any]:
    """Full retrieval pipeline: classify → expand → embed → RRF → cross-encoder → FK expand.

    Parameters
    ----------
    conn:
        Open Postgres connection.
    user_query:
        Natural language question to convert to SQL.
    top_k:
        Number of tables to retrieve per section before re-ranking.
    similarity_threshold:
        Minimum cosine similarity (used by fallback retrieval path).

    Returns
    -------
    dict
        Keys: matched_tables, matched_sql_guidelines, section_entry_points,
        sql_guidelines_entry, always_inject, retrieval_summary,
        query_embedding, all_query_variants, sections_searched.
    """
    display_query = user_query[:80] + "..." if len(user_query) > 80 else user_query
    print(f"\n[kb_retriever] Query: '{display_query}'")

    print("[kb_retriever] Step 1: LLM section classification...")
    target_sections = classify_sections_with_llm(user_query)
    print(f"[kb_retriever] Sections selected: {target_sections}")

    print("\n[kb_retriever] Step 2: Expanding query into multiple variants...")
    all_variants = expand_query_with_llm(user_query)
    print(f"[kb_retriever] {len(all_variants)} query variant(s):")
    for i, v in enumerate(all_variants):
        label = "(original)" if i == 0 else f"(variant {i})"
        print(f"     {label}: {v[:90]}")

    print("\n[kb_retriever] Step 3: Batch embedding all query variants...")
    all_embeddings = get_embeddings_batch(all_variants)
    query_embedding = all_embeddings[0]
    print(f"[kb_retriever] {len(all_embeddings)} embedding(s) computed")

    print(f"\n[kb_retriever] Step 4: RRF retrieval across {len(target_sections)} section(s)...")
    all_matched_tables: list[dict] = []
    section_entry_points: dict[str, dict | None] = {}

    for section in target_sections:
        section_entry_points[section] = get_entry_point(conn, section)

        # Fetch more candidates than top_k so the cross-encoder has enough to work with
        candidate_k = min(_CROSS_ENCODER_CANDIDATE_K, max(top_k * 4, 10))
        tables = retrieve_with_rrf(
            conn=conn,
            query_embeddings=all_embeddings,
            section=section,
            top_k=candidate_k,
            per_query_k=max(candidate_k * 2, 10),
            rrf_k=RRF_K,
        )
        for t in tables:
            t["_source_section"] = section

        all_matched_tables.extend(tables)
        print(f"[kb_retriever] [{section}]: {len(tables)} candidate(s) from RRF")

    # Deduplicate across sections
    seen_paths: set[str] = set()
    unique_candidates: list[dict] = []
    for table in sorted(all_matched_tables, key=lambda x: x["rrf_score"], reverse=True):
        if table["file_path"] not in seen_paths:
            seen_paths.add(table["file_path"])
            unique_candidates.append(table)

    # Cap at _CROSS_ENCODER_CANDIDATE_K before re-ranking
    unique_candidates = unique_candidates[:_CROSS_ENCODER_CANDIDATE_K]

    print(f"\n[kb_retriever] Step 5: Cross-encoder re-ranking {len(unique_candidates)} candidate(s)...")
    reranked_tables = _apply_cross_encoder_reranking(user_query, unique_candidates, conn)

    # After re-ranking, respect top_k
    final_tables = reranked_tables[:top_k + _FK_EXPANSION_TOP_N]

    print("\n[kb_retriever] Step 6: SQL guideline sub-file retrieval...")
    matched_sql_guidelines = retrieve_with_rrf(
        conn=conn,
        query_embeddings=all_embeddings,
        section="sql_guidelines",
        top_k=2,
        per_query_k=8,
        rrf_k=RRF_K,
    )
    print(f"[kb_retriever] {len(matched_sql_guidelines)} SQL guideline sub-file(s) matched")

    sql_guidelines_entry = get_entry_point(conn, "sql_guidelines")

    always_inject: dict[str, dict | None] = {}
    for section in _FORCED_SECTIONS:
        if section not in target_sections:
            always_inject[section] = get_entry_point(conn, section)

    summary_lines = [
        f"Sections searched  : {', '.join(target_sections)}",
        f"Query variants     : {len(all_variants)}",
        f"Tables matched     : {len(final_tables)}",
        f"SQL guidelines     : {len(matched_sql_guidelines)}",
    ]
    for t in final_tables:
        name = t.get("metadata", {}).get("name", t["file_path"])
        ce_score = t.get("cross_encoder_score", 0.0)
        fk_tag = " [FK]" if t.get("_fk_expanded") else ""
        summary_lines.append(
            f"  • {name}{fk_tag}  "
            f"(ce={ce_score:.3f}, rrf={t.get('rrf_score', 0):.5f}, "
            f"cosine={t.get('best_cosine_score', 0):.3f})"
        )

    retrieval_summary = "\n".join(summary_lines)
    print(f"[kb_retriever] Summary:\n{retrieval_summary}\n")

    return {
        "query_embedding": query_embedding,
        "all_query_variants": all_variants,
        "sections_searched": target_sections,
        "matched_tables": final_tables,
        "matched_sql_guidelines": matched_sql_guidelines,
        "section_entry_points": section_entry_points,
        "sql_guidelines_entry": sql_guidelines_entry,
        "always_inject": always_inject,
        "retrieval_summary": retrieval_summary,
    }
