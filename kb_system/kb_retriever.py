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
from kb_system.kb_store import retrieve_with_rrf, get_entry_point, get_section_sub_files
from utils.prompts.kb_generation_prompts import (
    CROSS_ENCODER_SYSTEM_PROMPT,
    cross_encoder_user_prompt,
)

_client = openai.OpenAI(api_key=OPENAI_API_KEY)
_CROSS_ENCODER_CANDIDATE_K: int = 10
_ELBOW_DROP_THRESHOLD: float = 0.50
_FK_EXPANSION_TOP_N: int = 2

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
    user_query
    Returns
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

    Parameters
    user_query
    n:Number of variants to generate (excluding the original).

    Returns
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


def _score_candidates_batched(
    user_query: str,
    candidates: list[dict],
) -> list[float]:
    """Score all candidates in a single LLM call (batched cross-encoder).
    Parameters
    user_query
    candidates
    Returns
    -------
    list[float]
        Relevance scores in [0.0, 1.0], one per candidate in the same
        order as the input. Falls back to 0.0 for any unparseable entry.
    """
    payload = [
        {
            "name": c.get("metadata", {}).get("name", c.get("file_path", f"table_{i}")),
            "description": c.get("metadata", {}).get("description", ""),
        }
        for i, c in enumerate(candidates)
    ]

    fallback = [0.0] * len(candidates)

    try:
        _msgs: list[
            ChatCompletionSystemMessageParam | ChatCompletionUserMessageParam
        ] = [
            ChatCompletionSystemMessageParam(
                role="system", content=CROSS_ENCODER_SYSTEM_PROMPT
            ),
            ChatCompletionUserMessageParam(
                role="user",
                content=cross_encoder_user_prompt(user_query, payload),
            ),
        ]
        response = _client.chat.completions.create(
            model=OPENAI_GENERATION_MODEL,
            messages=_msgs,
            temperature=0.0,
            max_tokens=256,
        )
        raw = response.choices[0].message.content.strip()

        # Extract JSON array even if the model wraps it in markdown fences
        match = re.search(r'\[.*]', raw, re.DOTALL)
        if not match:
            return fallback

        scored: list[dict] = json.loads(match.group())
        if not isinstance(scored, list) or len(scored) != len(candidates):
            return fallback

        scores: list[float] = []
        for entry in scored:
            try:
                scores.append(max(0.0, min(1.0, float(entry.get("score", 0.0)))))
            except (TypeError, ValueError):
                scores.append(0.0)
        return scores

    except (json.JSONDecodeError, openai.OpenAIError, KeyError):
        return fallback



def _extract_foreign_key_refs(ddl_content: str) -> list[str]:
    """Parse REFERENCES clauses from a DDL body string.

    Parameters
    ----------
    ddl_content:
        Raw markdown body of a table KB file (the CREATE TABLE SQL block).

    Returns
    -------
    list[str]
        Table names found in SQL REFERENCES clauses. Empty when the DDL
        was generated without explicit FK constraint declarations.
    """
    return re.findall(
        r'REFERENCES\s+["\'`]?(\w+)["\'`]?\s*\(',
        ddl_content,
        re.IGNORECASE,
    )


def _extract_fk_refs_from_metadata(metadata: dict) -> list[str]:
    """Extract referenced table names from the frontmatter ``fk_to`` field.
    is structured as::

        fk_to:
          - column: team_id
            ref_table: dwh_d_teams
            ref_column: team_id

    Parameters
    ----------
    metadata:
        Parsed YAML frontmatter dict from a KB table file.

    Returns
    -------
    list[str]
        Unique list of ``ref_table`` values from ``fk_to`` entries, plus any
        names listed under ``related_tables``.
    """
    refs: list[str] = []

    fk_to = metadata.get("fk_to") or []
    for entry in fk_to:
        if isinstance(entry, dict):
            ref_table = entry.get("ref_table", "")
            if ref_table:
                refs.append(ref_table)

    related = metadata.get("related_tables") or []
    for name in related:
        if isinstance(name, str) and name:
            refs.append(name)

    # Deduplicate while preserving order
    seen: set[str] = set()
    unique: list[str] = []
    for r in refs:
        if r not in seen:
            seen.add(r)
            unique.append(r)
    return unique


def _apply_cross_encoder_reranking(
    user_query: str,
    candidates: list[dict],
    conn: psycopg2.extensions.connection,
) -> list[dict]:
    """Re-rank RRF candidates: cross-encoder → FK expansion → elbow cutoff.

    Pipeline order:
        1. Score + sort by cross-encoder (joint query/description LLM call).
        2. FK expansion for top-N tables — BEFORE elbow cutoff so that
           joinable dimension tables are never pruned before they are added.
        3. Elbow detection — FK-expanded tables are marked protected and
           are never dropped regardless of score gap.

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
        Re-ranked, FK-expanded, and elbow-pruned list of table records.
    """
    if not candidates:
        return candidates

    import json as _json
    import psycopg2.extras

    print(f"[kb_retriever] Cross-encoder: scoring {len(candidates)} candidate(s) in one call...")
    scores = _score_candidates_batched(user_query, candidates)
    for record, score in zip(candidates, scores):
        record["cross_encoder_score"] = score
        record["relevance_score"] = score
        table_name = record.get("metadata", {}).get("name", record.get("file_path", ""))
        print(f"       • {table_name}: cross={score:.3f}  rrf={record.get('rrf_score', 0):.5f}")

    reranked = sorted(candidates, key=lambda r: r["cross_encoder_score"], reverse=True)

    existing_paths = {r["file_path"] for r in reranked}
    top_tables = reranked[:_FK_EXPANSION_TOP_N]

    for table_record in top_tables:
        content = table_record.get("content", "")
        record_metadata = table_record.get("metadata") or {}

        fk_refs = _extract_fk_refs_from_metadata(record_metadata)

        fk_refs += _extract_foreign_key_refs(content)

        seen_refs: set[str] = set()
        unique_fk_refs: list[str] = []
        for ref in fk_refs:
            if ref not in seen_refs:
                seen_refs.add(ref)
                unique_fk_refs.append(ref)

        for ref_table in unique_fk_refs:
            in_place_protected = False
            for r in reranked:
                if (
                    r.get("metadata", {}).get("name", "") == ref_table
                    or ref_table in r.get("file_path", "")
                ):
                    r["_fk_expanded"] = True
                    in_place_protected = True
                    print(
                        f"[kb_retriever] FK protection: preserved '{ref_table}' "
                        f"(referenced by {table_record.get('metadata', {}).get('name', '')})"
                    )
                    break

            if in_place_protected:
                continue

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

    scored = [r for r in reranked if not r.get("_fk_expanded")]
    fk_protected = [r for r in reranked if r.get("_fk_expanded")]

    scores = [r["cross_encoder_score"] for r in scored]
    cutoff_idx = len(scored)
    for i in range(1, len(scores)):
        if scores[i - 1] > 0 and (scores[i - 1] - scores[i]) / scores[i - 1] > _ELBOW_DROP_THRESHOLD:
            cutoff_idx = i
            break

    if cutoff_idx < len(scored):
        dropped = len(scored) - cutoff_idx
        print(
            f"[kb_retriever] Elbow cutoff at index {cutoff_idx} "
            f"(gap {scores[cutoff_idx - 1]:.3f} → {scores[cutoff_idx]:.3f}): "
            f"dropping {dropped} low-relevance candidate(s)."
        )

    survivors = scored[:cutoff_idx]

    if fk_protected:
        print(
            f"[kb_retriever] Preserving {len(fk_protected)} FK-protected table(s) "
            "past elbow cutoff."
        )

    return survivors + fk_protected


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

    seen_paths: set[str] = set()
    unique_candidates: list[dict] = []
    for table in sorted(all_matched_tables, key=lambda x: x["rrf_score"], reverse=True):
        if table["file_path"] not in seen_paths:
            seen_paths.add(table["file_path"])
            unique_candidates.append(table)

    unique_candidates = unique_candidates[:_CROSS_ENCODER_CANDIDATE_K]

    print(f"\n[kb_retriever] Step 5: Cross-encoder re-ranking {len(unique_candidates)} candidate(s)...")
    reranked_tables = _apply_cross_encoder_reranking(user_query, unique_candidates, conn)

    semantic_tables = [t for t in reranked_tables if not t.get("_fk_expanded")]
    fk_tables = [t for t in reranked_tables if t.get("_fk_expanded")]
    final_tables = semantic_tables[:top_k] + fk_tables

    print("\n[kb_retriever] Step 6: SQL guideline sub-file retrieval...")
    matched_sql_guidelines = retrieve_with_rrf(
        conn=conn,
        query_embeddings=all_embeddings,
        section="sql_guidelines",
        top_k=1,
        per_query_k=8,
        rrf_k=RRF_K,
    )
    print(f"[kb_retriever] {len(matched_sql_guidelines)} SQL guideline sub-file(s) matched")

    sql_guidelines_entry = get_entry_point(conn, "sql_guidelines")

    always_inject: dict[str, dict] = {}
    for section in _FORCED_SECTIONS:
        if section not in target_sections:
            entry = get_entry_point(conn, section)
            sub_files = get_section_sub_files(conn, section)
            always_inject[section] = {"entry": entry, "sub_files": sub_files}

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
