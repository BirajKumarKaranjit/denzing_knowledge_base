"""utils/prompts/kb_generation_prompts.py

System and user prompt templates used by kb_generator to produce KB markdown files.
All prompts are domain-agnostic; domain-specific details are injected at call time.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# System prompts
# ---------------------------------------------------------------------------

TABLE_FILE_SYSTEM_PROMPT = (
    "You are a technical documentation expert specialising in analytics databases.\n"
    "Your job is to generate structured knowledge base files in a specific markdown format.\n\n"
    "These files are used by an AI SQL-generation system. Each file has two parts:\n"
    "1. YAML frontmatter (the 'metadata field'): used for semantic routing via embedding similarity.\n"
    "2. Markdown body (the 'data field'): the actual DDL + column semantics injected into SQL prompts.\n\n"
    "RULES:\n"
    "- The frontmatter 'description' field is CRITICAL. Write it as:\n"
    "  'Use when the query involves X, Y, Z.' and make it rich and specific (3-5 sentences).\n"
    "  This description is embedded and compared against user queries at retrieval time.\n"
    "- 'fk_to': list every foreign key in this table as a YAML list of objects with keys:\n"
    "  column, ref_table, ref_column. Leave as empty list [] if the table has no FKs.\n"
    "  These are used for automatic JOIN expansion — accuracy is critical here.\n"
    "- 'related_tables': list any other tables frequently joined with this one beyond FK columns.\n"
    "  Use an empty list [] if none.\n"
    "- In the body, explain what each column means in domain context, common value ranges,\n"
    "  business rules, and join patterns.\n"
    "- Tags: 2-5 lowercase keywords relevant to the table.\n"
    "- Priority: high = core fact tables, medium = dimension tables, low = lookup/reference tables.\n"
    "- Return ONLY the markdown content. No explanations, no code fences around the whole output."
)

SECTION_KB_SYSTEM_PROMPT = (
    "You are a technical documentation expert. Generate a KB.md index file for a knowledge base section.\n"
    "This file describes what the section covers and is used by humans navigating the KB.\n"
    "It is NOT embedded for vector retrieval — it is injected as context alongside retrieved table files.\n\n"
    "Return ONLY the markdown content. No explanations outside the format."
)

ROOT_KB_SYSTEM_PROMPT = (
    "You are a technical documentation expert. Generate the root KB.md index file for an analytics\n"
    "knowledge base. This file gives an overview of all sections.\n\n"
    "Return ONLY the markdown content. No explanations outside the format."
)

SQL_GUIDELINES_SUB_SYSTEM_PROMPT = (
    "You are a SQL expert specialising in Postgres analytics databases.\n"
    "Generate a knowledge base markdown file for a specific SQL guideline category.\n\n"
    "The file has two parts:\n"
    "1. YAML frontmatter: contains name, description, tags, priority.\n"
    "   The 'description' field MUST start with 'Use when the query involves...'\n"
    "   and be rich and specific so that a vector search can match it against user queries\n"
    "   that need this type of SQL guidance.\n"
    "2. Markdown body: practical SQL patterns, templates, and gotchas for this guideline category.\n"
    "   Include concrete SQL code blocks that reference the ACTUAL column names from the provided DDL.\n\n"
    "IMPORTANT: Use ONLY the column names and table names that appear in the provided DDL.\n"
    "Do NOT invent or guess column names. If a column does not exist in the DDL, do not reference it.\n\n"
    "Return ONLY the markdown content. No explanations, no code fences around the whole output."
)

# ---------------------------------------------------------------------------
# User prompt templates (use .format() or f-strings to fill placeholders)
# ---------------------------------------------------------------------------


def table_file_user_prompt(
    table_name: str,
    ddl_sql: str,
    domain: str,
) -> str:
    """Return the user prompt for generating a single table KB file."""
    return (
        f"Generate a knowledge base markdown file for the following database table.\n\n"
        f"Domain: {domain}\n"
        f"Table name: {table_name}\n\n"
        f"DDL:\n{ddl_sql}\n\n"
        f"Required format:\n"
        f"---\n"
        f"name: {table_name}\n"
        f'description: "WRITE A RICH DESCRIPTION HERE. Start with: Use when the query involves..."\n'
        f"  Include all {domain}-relevant terminology users might use when asking about this data.\n"
        f"  This is the most important field — make it comprehensive (3-5 sentences).\n"
        f"tags: [tag1, tag2, tag3]\n"
        f"priority: high | medium | low\n"
        f"fk_to:\n"
        f"  - column: <fk_column_in_this_table>\n"
        f"    ref_table: <referenced_table_name>\n"
        f"    ref_column: <referenced_column_name>\n"
        f"related_tables: [<other_tables_often_joined_with_this_one>]\n"
        f"---\n\n"
        f"IMPORTANT: fk_to must list EVERY column in this table that is a foreign key,\n"
        f"inferred from the column naming convention (e.g. team_id → dwh_d_teams.team_id,\n"
        f"player_id → dwh_d_players.player_id, game_id → dwh_d_games.game_id).\n"
        f"Use an empty list [] if there are no FK columns.\n\n"
        f"# DDL\n\n"
        f"```sql\n{ddl_sql}\n```\n\n"
        f"## Column Semantics\n"
        f"For each column in the DDL, explain:\n"
        f"- Business meaning in {domain} context\n"
        f"- Value ranges or example values if inferrable from the DDL\n"
        f"- Whether it is typically used in WHERE, GROUP BY, or SELECT\n"
        f"- Any gotchas (nullable, approximate values, etc.)\n\n"
        f"## Common Query Patterns\n"
        f"2-4 bullet points showing how this table is typically used.\n"
        f"Include example WHERE conditions or JOIN patterns using ONLY columns from the DDL above.\n\n"
        f"## Join Relationships\n"
        f"How this table relates to other tables (foreign keys, typical join conditions).\n"
        f"Reference only columns that appear in the DDL."
    )


def section_kb_user_prompt(
    section_name: str,
    table_names: list[str],
    section_description: str,
) -> str:
    """Return the user prompt for generating a section KB.md index file."""
    file_list = "\n".join(f"- {name}" for name in table_names)
    return (
        f"Generate a KB.md index file for the '{section_name}' section of an analytics knowledge base.\n\n"
        f"Section description: {section_description}\n\n"
        f"Files in this section:\n{file_list}\n\n"
        f"Required format:\n"
        f"---\n"
        f"name: {section_name}_index\n"
        f'description: "Brief description of what this section covers and when to use it."\n'
        f"---\n\n"
        f"# {section_name.replace('_', ' ').title()} Section\n\n"
        f"[2-3 sentence overview of what this section contains]\n\n"
        f"## Contents\n\n"
        f"[List each file with a one-line description of what it covers]"
    )


def root_kb_user_prompt(sections: dict[str, str], domain: str) -> str:
    """Return the user prompt for generating the root KB.md file."""
    section_list = "\n".join(f"- {name}: {desc}" for name, desc in sections.items())
    return (
        f"Generate the root KB.md for the {domain} knowledge base.\n\n"
        f"Sections:\n{section_list}\n\n"
        f"Required format:\n"
        f"---\n"
        f"name: root_knowledge_base\n"
        f'description: "Root index of the {domain} knowledge base."\n'
        f"---\n\n"
        f"# {domain} Knowledge Base\n\n"
        f"[2-3 sentence overview of the entire KB]\n\n"
        f"## Sections\n"
        f"[List each section with description]"
    )


def sql_guideline_sub_file_user_prompt(
    sub_section: str,
    hint: str,
    ddl_summary: str,
) -> str:
    """Return the user prompt for generating a SQL guideline sub-file."""
    return (
        f"Generate a knowledge base markdown file for the '{sub_section}' SQL guideline category.\n\n"
        f"Category focus: {hint}\n\n"
        f"The following DDL defines the EXACT tables and columns available in this database.\n"
        f"Use ONLY these table names and column names in all SQL examples. Do not invent columns.\n\n"
        f"--- DDL START ---\n"
        f"{ddl_summary}\n"
        f"--- DDL END ---\n\n"
        f"Required frontmatter:\n"
        f"---\n"
        f"name: {sub_section}\n"
        f'description: "Use when the query involves [SPECIFIC SCENARIOS FOR THIS CATEGORY].\n'
        f"  Be detailed (3-4 sentences) — this text is embedded for vector retrieval.\"\n"
        f"tags: [tag1, tag2, tag3, tag4]\n"
        f"priority: high | medium | low\n"
        f"---\n\n"
        f"Then write the markdown body with:\n"
        f"- At least 4 practical SQL code blocks referencing ONLY the tables/columns from the DDL above\n"
        f"- A clear structure with ## headers for each sub-topic\n"
        f"- Specific gotchas and anti-patterns to avoid\n"
        f"- At least one complete multi-table query example"
    )


# ---------------------------------------------------------------------------
# Query relevance check prompts
# ---------------------------------------------------------------------------

RELEVANCE_CHECK_SYSTEM_PROMPT = (
    "You are a query routing assistant for a Text2SQL system.\n"
    "Your job is to determine whether a user's question can be answered by querying\n"
    "the database described in the schema context.\n\n"
    "Respond with a JSON object:\n"
    '  {"is_relevant": true/false, "reason": "one sentence explanation"}\n\n'
    "Return is_relevant=true only if the question asks for data that could plausibly\n"
    "exist in the provided database schema. Return false for general knowledge questions,\n"
    "questions about topics entirely unrelated to the domain, or impossible queries."
)


def relevance_check_user_prompt(user_query: str, schema_summary: str) -> str:
    """Return the user prompt for the query relevance gate."""
    return (
        f"Database domain summary:\n{schema_summary}\n\n"
        f"User question: {user_query}\n\n"
        "Is this question answerable by querying this database? "
        "Also, suggest four relevant questions if the user query is not suitable for this domain to help the user understand what types of questions they can ask.\n\n"
        "Strictly check the schema summary and relevancy of the user query to the database domain. Do not make assumptions or guesses about what data might be in the database beyond what is stated in the schema summary.\n\n"
        'Return JSON: {"is_relevant": true/false, "reason": "...", "suggested_questions": ["...", "...", "...", "..."]}'
    )


# ---------------------------------------------------------------------------
# Cross-encoder re-ranking prompts
# ---------------------------------------------------------------------------

CROSS_ENCODER_SYSTEM_PROMPT = (
    "You are a relevance scoring assistant for a Text2SQL retrieval system.\n"
    "Given a user query and a database table description, score how relevant the table is\n"
    "to answering the query on a scale from 0.0 to 1.0.\n\n"
    "Scoring guide:\n"
    "  1.0 — The table directly contains the data needed to answer the query.\n"
    "  0.7 — The table is likely needed as part of a join to answer the query.\n"
    "  0.4 — The table might provide supporting context.\n"
    "  0.1 — The table is probably not needed.\n"
    "  0.0 — The table is completely unrelated.\n\n"
    'Return ONLY a JSON object: {"score": <float between 0.0 and 1.0>}'
)


def cross_encoder_user_prompt(user_query: str, table_name: str, table_description: str) -> str:
    """Return the user prompt for scoring one (query, table) pair."""
    return (
        f"User query: {user_query}\n\n"
        f"Table name: {table_name}\n"
        f"Table description: {table_description}\n\n"
        "How relevant is this table to answering the query? "
        'Return JSON: {"score": <float>}'
    )

