"""
Assembles the final SQL generation prompt from retrieved KB context
and reports estimated token usage.
"""

from __future__ import annotations

from typing import Any

from utils.citation_builder import (
    build_citations,
    format_citations_as_xml,
    format_citations_for_user,
    TableCitation,
)
from utils.config import SQL_DIALECT
from utils.prompts.kb_generation_prompts import get_dialect_instruction

_CHARS_PER_TOKEN: int = 4


def _estimate_tokens(text: str) -> int:
    """Return a rough token estimate for a string (chars / 4)."""
    return max(1, len(text) // _CHARS_PER_TOKEN)


def build_sql_prompt(
    user_query: str,
    retrieval_result: dict[str, Any],
    agent_backstory: str = "",
    dialect: str = SQL_DIALECT,
) -> tuple[str, str]:
    """Assemble the SQL generation prompt from KB retrieval results.

    Prompt section order:
        [1] System header (role + hard constraint: only use listed tables)
        [2] Dialect instruction block
        [3] <kb_retrieval_citations> XML  — anti-hallucination manifest
        [4] Always-inject sections (response_guidelines)
        [5] SQL Guidelines entry point overview (KB.md)
        [6] Matched SQL guideline sub-files (joins.md, aggregations.md, etc.)
        [7] Section entry point overviews (DDL KB.md files)
        [8] Matched table schemas with rank-numbered headers
        [9] User question
    """
    matched_tables: list[dict] = retrieval_result.get("matched_tables", [])
    always_inject: dict = retrieval_result.get("always_inject", {})
    section_entry_points: dict = retrieval_result.get("section_entry_points", {})
    matched_sql_guidelines: list[dict] = retrieval_result.get("matched_sql_guidelines", [])
    sql_guidelines_entry: dict | None = retrieval_result.get("sql_guidelines_entry")

    citations: list[TableCitation] = build_citations(matched_tables)
    citation_xml: str = format_citations_as_xml(citations)
    citation_md: str = format_citations_for_user(citations)

    sections: list[str] = []

    system_header = (
        "You are an expert SQL analyst.\n"
        "Generate a single, valid, efficient SQL query to answer the user's question.\n\n"
        "HARD CONSTRAINTS — never violate these:\n"
        "1. Produce exactly ONE SQL statement. Never split the answer into two separate queries.\n"
        "   For compound questions (e.g. 'who leads X, and what about Y?'), combine both parts\n"
        "   into a single query using shared CTEs and UNION ALL in the final SELECT.\n"
        "2. All SELECT branches in a UNION ALL must have the same number of columns with compatible\n"
        "   types. Never use SELECT * when joining UNION ALL branches from different CTEs — always\n"
        "   name columns explicitly so column counts match.\n"
        "3. ORDER BY and row-limiting clauses cannot appear inside an individual SELECT branch of a\n"
        "   UNION ALL. Wrap the branch in a subquery if per-branch limiting is needed:\n"
        "     SELECT col1, col2 FROM (SELECT col1, col2 FROM cte ORDER BY col2 DESC LIMIT 1) sub\n"
        "     UNION ALL\n"
        "     SELECT col1, col2 FROM cte WHERE col1 ILIKE '%name%';\n"
        "4. Use ONLY the tables listed in the <kb_retrieval_citations> block below.\n"
        "   Do not reference any table or column not present in the provided schemas.\n"
        "5. Every column reference MUST be qualified with its table alias (e.g. <<table_alias>>.<<col_name>>,\n"
        "  ). Never write a bare unqualified column name. This is\n"
        "   mandatory even when only one table is in scope."
    )
    if agent_backstory:
        system_header = f"{agent_backstory}\n\n{system_header}"

    sections.append(system_header)
    sections.append(f"## SQL DIALECT INSTRUCTIONS\n\n{get_dialect_instruction(dialect)}")
    sections.append(citation_xml)

    for section_name, section_data in always_inject.items():
        label = section_name.replace("_", " ").upper()
        entry = section_data.get("entry") if isinstance(section_data, dict) else section_data
        sub_files = section_data.get("sub_files", []) if isinstance(section_data, dict) else []

        if entry and entry.get("content"):
            sections.append(f"## {label}\n\n{entry['content']}")

        for sub_file in sub_files:
            content = sub_file.get("content", "").strip()
            name = sub_file.get("metadata", {}).get("name", sub_file.get("file_path", ""))
            if content:
                sub_label = name.replace("_", " ").title()
                sections.append(f"## {label} — {sub_label}\n\n{content}")

    if sql_guidelines_entry and sql_guidelines_entry.get("content"):
        sections.append(f"## SQL GUIDELINES OVERVIEW\n\n{sql_guidelines_entry['content']}")

    if matched_sql_guidelines:
        guideline_blocks: list[str] = []
        for guideline in matched_sql_guidelines:
            content = guideline.get("content", "").strip()
            name = guideline.get("metadata", {}).get("name", guideline["file_path"])
            if content:
                guideline_blocks.append(f"### {name.replace('_', ' ').title()}\n\n{content}")
        if guideline_blocks:
            sections.append(
                "## RELEVANT SQL GUIDELINES\n\n" + "\n\n---\n\n".join(guideline_blocks)
            )

    for section_name, entry in section_entry_points.items():
        if entry and entry.get("content"):
            label = f"{section_name.upper()} SECTION OVERVIEW"
            sections.append(f"## {label}\n\n{entry['content']}")

    if matched_tables:
        table_blocks: list[str] = []
        for table, citation in zip(matched_tables, citations):
            content = table.get("content", "").strip()
            fk_tag = " [FK-expanded]" if table.get("_fk_expanded") else ""
            header = (
                f"### [{citation.rank}] Table: `{citation.table_name}`{fk_tag}  "
                f"(relevance: {citation.relevance_score} — {citation.confidence_label} confidence)"
            )
            table_blocks.append(f"{header}\n\n{content}")
        sections.append(
            "## RELEVANT TABLE SCHEMAS\n\n" + "\n\n---\n\n".join(table_blocks)
        )
    else:
        sections.append(
            "## RELEVANT TABLE SCHEMAS\n\n"
            "No tables were retrieved from the Knowledge Base for this query.\n"
            "Do NOT invent table names or column names. "
            "Return an empty SQL block if valid SQL cannot be generated."
        )

    sections.append(f"## USER QUESTION\n\n{user_query}")

    prompt_str = "\n\n".join(sections)

    estimated_tokens = _estimate_tokens(prompt_str)
    print(
        f"[prompt_builder] Assembled prompt: {len(prompt_str):,} chars "
        f"(~{estimated_tokens:,} tokens estimated)"
    )

    return prompt_str, citation_md

