"""
kb_system/prompt_builder.py
----------------------------
Assembles the final SQL generation prompt from retrieved KB context.

Takes the structured output from kb_retriever.retrieve_context_for_query()
and formats it into a prompt string ready to send to the LLM.

Prompt structure:
    1. System instruction
    2. Always-inject sections (sql_guidelines, response_guidelines)
    3. Section entry points (KB.md context for each searched section)
    4. Matched table DDL (the actual CREATE TABLE statements + column docs)
    5. User question

The matched table content (DDL) is the most important part — it gives the
LLM the schema it needs to write correct SQL without hallucinating columns.
"""

from __future__ import annotations
from typing import Any
from utils.citation_builder import (
    build_citations,
    format_citations_as_xml,
    format_citations_for_user,
    TableCitation,
)


def build_sql_prompt(
    user_query: str,
    retrieval_result: dict[str, Any],
    agent_backstory: str = "",
) -> tuple[str, str]:
    """
    Assemble the SQL generation prompt from KB retrieval results.

    Integrates the citation layer at two levels:
        1. XML citation block → injected into the LLM prompt before schemas.
        2. Markdown citation string → returned to the caller for display to the end user

    Prompt section order:
        [1] System header (role + hard constraint: only use listed tables)
        [2] <kb_retrieval_citations> XML  ← anti-hallucination manifest
        [3] Always-inject sections (sql_guidelines, response_guidelines)
        [4] Section entry point overviews (KB.md files)
        [5] Matched table schemas with rank-numbered headers
        [6] User question

    Parameters
    ----------
    user_query : str
        The original natural language question from the user.
    retrieval_result : dict
        Output from kb_retriever.retrieve_context_for_query().
        Expected keys: matched_tables, section_entry_points, always_inject.
    agent_backstory : str
        Optional agent persona to prepend to the system header.
        Pass "" if not needed.

    Returns
    -------
    tuple[str, str]
        prompt_str   : Fully assembled LLM prompt. Pass this to generate_sql().
        citation_md  : Markdown citation text. Show this to the end user in the
                       UI or API response alongside the generated SQL.
    """
    matched_tables: list[dict] = retrieval_result.get("matched_tables", [])
    always_inject: dict = retrieval_result.get("always_inject", {})
    section_entry_points: dict = retrieval_result.get("section_entry_points", {})

    citations: list[TableCitation] = build_citations(matched_tables)
    citation_xml: str = format_citations_as_xml(citations)
    citation_md: str = format_citations_for_user(citations)

    sections: list[str] = []

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

    sections.append(citation_xml)

    for section_name, entry in always_inject.items():
        if entry and entry.get("content"):
            label = section_name.replace("_", " ").upper()
            sections.append(f"## {label}\n\n{entry['content']}")

    for section_name, entry in section_entry_points.items():
        if entry and entry.get("content"):
            label = f"{section_name.upper()} SECTION OVERVIEW"
            sections.append(f"## {label}\n\n{entry['content']}")

    if matched_tables:
        table_blocks: list[str] = []

        for table, citation in zip(matched_tables, citations):
            content = table.get("content", "").strip()
            header = (
                f"### [{citation.rank}] Table: `{citation.table_name}`  "
                f"(relevance: {citation.relevance_score} — {citation.confidence_label} confidence)"
            )
            table_blocks.append(f"{header}\n\n{content}")

        sections.append(
            "## RELEVANT TABLE SCHEMAS\n\n"
            + "\n\n---\n\n".join(table_blocks)
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
    return prompt_str, citation_md