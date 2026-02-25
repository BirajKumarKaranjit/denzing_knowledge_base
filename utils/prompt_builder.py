"""utils/prompt_builder.py

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

# Approximate token count: 1 token ≈ 4 characters for English text.
_CHARS_PER_TOKEN: int = 4


def _estimate_tokens(text: str) -> int:
    """Return a rough token estimate for a string (chars / 4)."""
    return max(1, len(text) // _CHARS_PER_TOKEN)


def build_sql_prompt(
    user_query: str,
    retrieval_result: dict[str, Any],
    agent_backstory: str = "",
) -> tuple[str, str]:
    """Assemble the SQL generation prompt from KB retrieval results.

    Prompt section order:
        [1] System header (role + hard constraint: only use listed tables)
        [2] <kb_retrieval_citations> XML  — anti-hallucination manifest
        [3] Always-inject sections (response_guidelines)
        [4] SQL Guidelines entry point overview (KB.md)
        [5] Matched SQL guideline sub-files (joins.md, aggregations.md, etc.)
        [6] Section entry point overviews (DDL KB.md files)
        [7] Matched table schemas with rank-numbered headers
        [8] User question

    Parameters
    ----------
    user_query:
        The original natural language question.
    retrieval_result:
        Output from kb_retriever.retrieve_context_for_query().
    agent_backstory:
        Optional agent persona prepended to the system header.

    Returns
    -------
    tuple[str, str]
        (prompt_str, citation_md) — prompt for the LLM and markdown citations for the UI.
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

    # [1] System header
    system_header = (
        "You are an expert SQL analyst.\n"
        "Generate a valid, efficient SQL query to answer the user's question.\n"
        "IMPORTANT CONSTRAINT: Use ONLY the tables listed in the "
        "<kb_retrieval_citations> block below. "
        "Do not reference any table or column not explicitly listed there "
        "and present in the schemas provided."
    )
    if agent_backstory:
        system_header = f"{agent_backstory}\n\n{system_header}"
    sections.append(system_header)

    # [2] Citation manifest
    sections.append(citation_xml)

    # [3] Always-inject sections
    for section_name, entry in always_inject.items():
        if entry and entry.get("content"):
            label = section_name.replace("_", " ").upper()
            sections.append(f"## {label}\n\n{entry['content']}")

    # [4] SQL guidelines entry point overview
    if sql_guidelines_entry and sql_guidelines_entry.get("content"):
        sections.append(f"## SQL GUIDELINES OVERVIEW\n\n{sql_guidelines_entry['content']}")

    # [5] Matched SQL guideline sub-files
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

    # [6] Section entry point overviews
    for section_name, entry in section_entry_points.items():
        if entry and entry.get("content"):
            label = f"{section_name.upper()} SECTION OVERVIEW"
            sections.append(f"## {label}\n\n{entry['content']}")

    # [7] Matched table schemas
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

    # [8] User question
    sections.append(f"## USER QUESTION\n\n{user_query}")

    prompt_str = "\n\n".join(sections)

    # Token usage estimate
    estimated_tokens = _estimate_tokens(prompt_str)
    print(
        f"[prompt_builder] Assembled prompt: {len(prompt_str):,} chars "
        f"(~{estimated_tokens:,} tokens estimated)"
    )

    return prompt_str, citation_md

