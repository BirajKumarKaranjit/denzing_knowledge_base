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


def build_sql_prompt(
    user_query: str,
    retrieval_result: dict[str, Any],
    agent_backstory: str = "",
) -> str:
    """
    Assemble the complete SQL generation prompt from KB retrieval results.

    This function is the bridge between the KB retrieval system and the
    SQL generation LLM call. It formats all retrieved context into a
    structured prompt that gives the model exactly the schema information
    it needs — no more, no less.

    Parameters
    ----------
    user_query : str
        The original natural language question from the user.
    retrieval_result : dict
        Output from kb_retriever.retrieve_context_for_query().
        Contains matched_tables, section_entry_points, always_inject.
    agent_backstory : str
        Optional agent persona/backstory to prepend to the system prompt.
        E.g., "You are an NBA analytics assistant..."

    Returns
    -------
    str
        Fully assembled prompt string ready to send to the LLM.
    """
    sections: list[str] = []

    # ── 1. System header ──
    system_header = (
        "You are an expert SQL analyst. Generate a valid, efficient SQL query "
        "to answer the user's question using ONLY the tables and columns described below. "
        "Do not reference any table or column not present in the schema provided."
    )
    if agent_backstory:
        system_header = f"{agent_backstory}\n\n{system_header}"
    sections.append(system_header)

    # ── 2. Always-inject sections (sql_guidelines, response_guidelines) ──
    always_inject = retrieval_result.get("always_inject", {})
    for section_name, entry in always_inject.items():
        if entry and entry.get("content"):
            label = section_name.replace("_", " ").upper()
            sections.append(f"## {label}\n\n{entry['content']}")

    # ── 3. Section entry points as context headers ──
    section_entry_points = retrieval_result.get("section_entry_points", {})
    for section_name, entry in section_entry_points.items():
        if entry and entry.get("content"):
            label = f"{section_name.upper()} SECTION OVERVIEW"
            sections.append(f"## {label}\n\n{entry['content']}")

    # ── 4. Matched table schemas (the actual DDL + column descriptions) ──
    matched_tables = retrieval_result.get("matched_tables", [])

    if matched_tables:
        table_blocks: list[str] = []
        for table in matched_tables:
            name = table.get("metadata", {}).get("name", table["file_path"])
            score = table.get("relevance_score", 0)
            content = table.get("content", "").strip()
            # Each table block shows the file name, relevance score, and full DDL content
            table_blocks.append(
                f"### Table: {name}  (relevance: {score:.2f})\n\n{content}"
            )
        sections.append("## RELEVANT TABLE SCHEMAS\n\n" + "\n\n---\n\n".join(table_blocks))
    else:
        sections.append(
            "## RELEVANT TABLE SCHEMAS\n\n"
            "⚠ No tables matched the query. Consider lowering SIMILARITY_THRESHOLD in config.py."
        )

    # ── 5. User question ──
    sections.append(f"## USER QUESTION\n\n{user_query}")

    return "\n\n".join(sections)
