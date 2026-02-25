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
# Query relevance gate — single LLM call for garbage, greeting, and domain check
# ---------------------------------------------------------------------------

RELEVANCE_CHECK_SYSTEM_PROMPT = (
    "You are a query classification gate for a Text2SQL analytics system.\n"
    "You will receive a user query and a schema context (table names and columns)\n"
    "derived from the actual database. Classify the query into exactly one category.\n\n"

    "### Categories (evaluate in this order)\n\n"

    "1. GARBAGE\n"
    "   Random characters, keyboard mash, or completely unintelligible text with no\n"
    "   discernible intent.\n"
    '   Examples: "asldkjf", "xyzzy 123", "@@##$$"\n\n'

    "2. GREETING\n"
    "   Purely conversational — no data retrieval intent whatsoever.\n"
    '   Examples: "Hello", "How are you?", "Thanks", "Good job", "Who created you?"\n'
    "   IMPORTANT: if the message contains BOTH a greeting AND any data question,\n"
    '   classify as SQL_RELEVANT (e.g. "Hi, show me top results" → SQL_RELEVANT).\n\n'

    "3. OUT_OF_DOMAIN\n"
    "   The query asks about a topic that provably cannot map to any table, column,\n"
    "   or entity type in the provided schema.\n\n"
    "   ### CRITICAL RULE FOR NAMES AND ENTITIES ###\n"
    "   Any human name, organisation name, product name, or proper noun must be\n"
    "   treated as SQL_RELEVANT if the schema contains columns that could store such\n"
    "   values (e.g. name, full_name, player_name, team_name, title, label, etc.).\n"
    "   You CANNOT know in advance which names exist in the database. Therefore:\n"
    "   - 'Who is <any person>?' → SQL_RELEVANT (name may exist in a name column)\n"
    "   - 'Tell me about <any entity>?' → SQL_RELEVANT\n"
    "   Only classify as OUT_OF_DOMAIN when the query is about a concept that\n"
    "   fundamentally cannot exist in any column — e.g. scientific phenomena,\n"
    "   recipes, geography unrelated to the domain, or programming questions.\n\n"

    "4. SQL_RELEVANT (DEFAULT)\n"
    "   Anything that could plausibly require a database query. This is the default\n"
    "   category. Includes:\n"
    "   - Data questions: stats, comparisons, rankings, aggregations, filters\n"
    "   - Any proper noun (person, place, organisation) — may exist in a name column\n"
    "   - Analytics vocabulary: top, count, list, compare, show, total, average\n"
    "   - Ambiguous or underspecified queries\n"
    "   - Follow-up phrases implying prior data context\n\n"

    "### Core principle\n"
    "Rejecting a valid query is far more costly than passing an irrelevant one.\n"
    "When uncertain, ALWAYS classify as SQL_RELEVANT.\n"
    "You must be 100% certain a query is unrelated before returning anything other\n"
    "than SQL_RELEVANT.\n\n"

    "### Output format — return ONLY this raw JSON, no extra text\n"
    "{\n"
    '  "category": "GARBAGE" | "GREETING" | "OUT_OF_DOMAIN" | "SQL_RELEVANT",\n'
    '  "is_relevant": true if SQL_RELEVANT else false,\n'
    '  "reason": "<one sentence>",\n'
    '  "response": "<user-facing message if not relevant, else empty string>",\n'
    '  "suggested_questions": ["<q1>", "<q2>", "<q3>", "<q4>"] if not relevant else []\n'
    "}"
)


def relevance_check_user_prompt(user_query: str, schema_context: str) -> str:
    """Build the user prompt for the unified relevance gate.

    Parameters
    ----------
    user_query:
        Raw user input to classify.
    schema_context:
        Table names and column names derived from the actual DDL. Used by the
        LLM to infer the domain and decide whether a name or entity could
        plausibly appear in the data. Generated dynamically — not hardcoded.

    Returns
    -------
    str
        Formatted prompt string.
    """
    return (
        f"Schema context (table and column names from the database):\n"
        f"{schema_context}\n\n"
        f"User query: {user_query}\n\n"
        "Classify this query. Remember: any proper noun could be a value in a name "
        "column — default to SQL_RELEVANT when uncertain.\n"
        "Return JSON only."
    )


# ---------------------------------------------------------------------------
# Cross-encoder re-ranking prompts  (batched — one LLM call for all candidates)
# ---------------------------------------------------------------------------

CROSS_ENCODER_SYSTEM_PROMPT = (
    "You are a relevance scoring assistant for a Text2SQL retrieval system.\n"
    "You will be given a user query and a numbered list of database tables.\n"
    "Score each table's relevance to answering the query on a scale from 0.0 to 1.0.\n\n"
    "Scoring guide:\n"
    "  1.0 — The table directly contains the data needed to answer the query.\n"
    "  0.7 — The table is likely needed as part of a JOIN to answer the query.\n"
    "  0.4 — The table might provide supporting context.\n"
    "  0.1 — The table is probably not needed.\n"
    "  0.0 — The table is completely unrelated.\n\n"
    "Return ONLY a JSON array of objects, one per table, in the same order as the input.\n"
    'Each object must have exactly two keys: "table" (the table name) and "score" (float).\n'
    'Example for 3 tables: [{"table": "orders", "score": 1.0}, {"table": "products", "score": 0.7}, {"table": "logs", "score": 0.0}]'
)


def cross_encoder_user_prompt(user_query: str, candidates: list[dict]) -> str:
    """Return the user prompt for scoring all (query, candidate) pairs in one call.

    Parameters
    ----------
    user_query:
        The original user question.
    candidates:
        List of dicts with keys ``name`` and ``description``.

    Returns
    -------
    str
        Prompt that instructs the LLM to return a JSON array of scores.
    """
    table_lines = "\n".join(
        f"{i + 1}. Table: {c['name']}\n   Description: {c['description']}"
        for i, c in enumerate(candidates)
    )
    return (
        f"User query: {user_query}\n\n"
        f"Tables to score:\n{table_lines}\n\n"
        "Return a JSON array with one score object per table, in the same order. "
        'Format: [{"table": "<name>", "score": <float>}, ...]'
    )



