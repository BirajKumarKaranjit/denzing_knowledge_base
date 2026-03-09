"""utils/prompts/kb_generation_prompts.py

Prompt templates for KB markdown file generation and query classification.
All generation prompts are domain-agnostic; domain context is injected at call time.
"""

from __future__ import annotations

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

SECTION_SUB_FILE_SYSTEM_PROMPT = (
    "You are a knowledge base authoring expert.\n"
    "Generate a structured markdown knowledge base sub-file for the given section and topic.\n\n"
    "The file has two parts:\n"
    "1. YAML frontmatter: name, description, tags, priority.\n"
    "   The 'description' field MUST start with 'Use when the query involves...'\n"
    "   or 'Reference when users ask about...' and must be rich enough (3-4 sentences)\n"
    "   for a vector search to correctly route queries to this file.\n"
    "   When DDL is provided, use ONLY the column and table names from it.\n"
    "2. Markdown body: comprehensive, well-structured content about the topic.\n"
    "   Use ## headers, bullet lists, tables, and SQL code blocks where appropriate.\n\n"
    "Return ONLY the markdown content. No explanations, no code fences around the whole output."
)


def table_file_user_prompt(table_name: str, ddl_sql: str, domain: str) -> str:
    """User prompt for generating a single table KB file."""
    return (
        f"Generate a knowledge base markdown file for the following database table.\n\n"
        f"Domain: {domain}\n"
        f"Table name: {table_name}\n\n"
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
        f"inferred from column naming conventions (e.g. team_id → dwh_d_teams.team_id).\n"
        f"Use an empty list [] if there are no FK columns.\n\n"
        f"# DDL\n\n"
        f"```sql\n{ddl_sql}\n```\n\n"
        f"## Column Semantics\n"
        f"For each column explain: business meaning, value ranges, typical usage (WHERE/GROUP BY/SELECT), gotchas.\n\n"
        f"## Common Query Patterns\n"
        f"2-4 bullet points. Use ONLY columns from the DDL above.\n\n"
        f"## Join Relationships\n"
        f"Foreign keys and typical join conditions. Reference only columns in the DDL."
    )


def section_kb_user_prompt(
    section_name: str,
    table_names: list[str],
    section_description: str,
) -> str:
    """User prompt for generating a section KB.md index file."""
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
        f"[2-3 sentence overview]\n\n"
        f"## Contents\n\n"
        f"[List each file with a one-line description]"
    )


def root_kb_user_prompt(sections: dict[str, str], domain: str) -> str:
    """User prompt for generating the root KB.md file."""
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
        f"[2-3 sentence overview]\n\n"
        f"## Sections\n"
        f"[List each section with description]"
    )


def section_sub_file_user_prompt(
    section: str,
    sub_section: str,
    hint: str,
    ddl_summary: str | None = None,
) -> str:
    """User prompt for generating any section sub-file.

    Pass ``ddl_summary`` for schema-grounded sections (e.g. sql_guidelines).
    Pass ``None`` for content-only sections (e.g. business_rules).
    """
    ddl_block = (
        f"Use ONLY the table and column names from the DDL below — do not invent any.\n\n"
        f"--- DDL START ---\n{ddl_summary}\n--- DDL END ---\n\n"
        if ddl_summary
        else ""
    )
    return (
        f"Generate a knowledge base markdown file for the '{sub_section}' sub-topic "
        f"inside the '{section}' section.\n\n"
        f"Topic focus: {hint}\n\n"
        f"{ddl_block}"
        f"Required frontmatter:\n"
        f"---\n"
        f"name: {sub_section}\n"
        f'description: "Reference when users ask about [SPECIFIC SCENARIOS].\n'
        f"  Be detailed (3-4 sentences) — this text is embedded for vector retrieval.\"\n"
        f"tags: [tag1, tag2, tag3]\n"
        f"priority: high | medium | low\n"
        f"---\n\n"
        f"Write a comprehensive markdown body with:\n"
        f"- ## headers for each major sub-topic\n"
        f"- Tables, bullet lists, or SQL code blocks where appropriate\n"
        f"- Concrete, specific information — avoid vague generalities"
    )


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
    '   Examples: "Hello", "How are you?", "Thanks", "Good job"\n'
    "   IMPORTANT: if the message contains BOTH a greeting AND any data question,\n"
    '   classify as SQL_RELEVANT (e.g. "Hi, show me top results" → SQL_RELEVANT).\n\n'

    "3. META_QUERY\n"
    "   Questions about the system, platform, product, agent, or knowledge base itself —\n"
    "   not about data in the database tables.\n"
    "   These are answered from project/product documentation, not SQL.\n"
    "   Examples:\n"
    '   - "What is this system?", "What does this agent do?"\n'
    '   - "What is Denzing?", "Who built this?", "What data do you have access to?"\n'
    '   - "What kinds of questions can I ask?", "How does this work?"\n'
    '   - "What is this project about?", "Tell me about this platform"\n'
    "   IMPORTANT: classify as META_QUERY only when the question is clearly about the\n"
    "   system/product/platform itself, not about the underlying data.\n\n"

    "4. OUT_OF_DOMAIN\n"
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

    "5. SQL_RELEVANT (DEFAULT)\n"
    "   Anything that could plausibly require a database query. This is the default\n"
    "   category. Includes:\n"
    "   - Data questions: stats, comparisons, rankings, aggregations, filters\n"
    "   - Any proper noun (person, place, organisation) — may exist in a name column\n"
    "   - Analytics vocabulary: top, count, list, compare, show, total, average\n"
    "   - Ambiguous or underspecified queries\n"
    "   - Follow-up phrases implying prior data context\n\n"

    "### Core principle\n"
    "Rejecting a valid SQL query is far more costly than passing an irrelevant one.\n"
    "When uncertain between SQL_RELEVANT and META_QUERY, choose SQL_RELEVANT.\n"
    "When uncertain between SQL_RELEVANT and OUT_OF_DOMAIN, choose SQL_RELEVANT.\n"
    "You must be 100% certain a query is unrelated before returning OUT_OF_DOMAIN.\n\n"

    "### Output format — return ONLY this raw JSON, no extra text\n"
    "{\n"
    '  "category": "GARBAGE" | "GREETING" | "META_QUERY" | "OUT_OF_DOMAIN" | "SQL_RELEVANT",\n'
    '  "is_relevant": true if SQL_RELEVANT or META_QUERY else false,\n'
    '  "reason": "<one sentence>",\n'
    '  "response": "<user-facing message if GARBAGE/GREETING/OUT_OF_DOMAIN, else empty string>",\n'
    '  "suggested_questions": ["<q1>", "<q2>", "<q3>", "<q4>"] if OUT_OF_DOMAIN else []\n'
    "}"
)


def relevance_check_user_prompt(user_query: str, schema_context: str) -> str:
    """User prompt for the unified relevance gate."""
    return (
        f"Schema context (table and column names from the database):\n"
        f"{schema_context}\n\n"
        f"User query: {user_query}\n\n"
        "Classify this query. Remember: any proper noun could be a value in a name "
        "column — default to SQL_RELEVANT when uncertain.\n"
        "Return JSON only."
    )


META_QUERY_SYSTEM_PROMPT = (
    "You are a knowledgeable assistant for an AI analytics platform.\n"
    "You will be given a user's question about the system, platform, or project,\n"
    "along with relevant documentation excerpts from the knowledge base.\n\n"
    "Answer the user's question clearly and concisely using only the provided documentation.\n"
    "If the documentation does not cover the question, say so honestly.\n"
    "Do not invent capabilities or features not mentioned in the documentation.\n"
    "Keep the response focused — 2-5 sentences unless the question requires more detail.\n"
    "Do not mention that you are reading from a knowledge base or documentation."
)


def meta_query_user_prompt(user_query: str, kb_context: str) -> str:
    """User prompt for answering meta/project questions from KB documentation."""
    return (
        f"User question: {user_query}\n\n"
        f"Relevant documentation:\n{kb_context}\n\n"
        "Answer the question using the documentation above."
    )


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
    'Example: [{"table": "orders", "score": 1.0}, {"table": "products", "score": 0.7}]'
)


def cross_encoder_user_prompt(user_query: str, candidates: list[dict]) -> str:
    """User prompt for batch cross-encoder scoring."""
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


# ---------------------------------------------------------------------------
# SQL DIALECT INSTRUCTION BLOCKS
# ---------------------------------------------------------------------------
# Each block is injected verbatim into the SQL generation prompt so the LLM
# produces engine-correct SQL without being told the dialect inline.

_DIALECT_INSTRUCTIONS: dict[str, str] = {
    "postgresql": (
        "TARGET DATABASE: PostgreSQL\n"
        "- Use standard ANSI SQL with PostgreSQL extensions where helpful.\n"
        "- String matching: use ILIKE for case-insensitive patterns.\n"
        "- Date truncation: DATE_TRUNC('month'|'year'|'week', col).\n"
        "- Safe division: NULLIF(denominator, 0).\n"
        "- Window functions: fully supported (RANK, DENSE_RANK, ROW_NUMBER, LAG, LEAD).\n"
        "- CTEs: fully supported; columns in the WITH clause are accessible in SELECT.\n"
        "- String concatenation: use || operator.\n"
        "- Identifier quoting: use double quotes for reserved words (e.g. \"order\").\n"
        "- Do NOT use TOP — use LIMIT instead.\n"
        "- Do NOT use GETDATE() — use NOW() or CURRENT_TIMESTAMP."
    ),
    "snowflake": (
        "TARGET DATABASE: Snowflake\n"
        "- String matching: use ILIKE for case-insensitive patterns.\n"
        "- Date truncation: DATE_TRUNC('MONTH'|'YEAR'|'WEEK', col).\n"
        "- Safe division: NULLIF(denominator, 0) or DIV0(numerator, denominator).\n"
        "- Window functions: fully supported.\n"
        "- CTEs: fully supported.\n"
        "- String concatenation: use || or CONCAT().\n"
        "- Identifier quoting: use double quotes; identifiers are case-insensitive by default.\n"
        "- QUALIFY clause can replace a subquery when filtering window function results.\n"
        "- Do NOT use ILIKE with ESCAPE on the same column twice in one predicate.\n"
        "- Use CURRENT_TIMESTAMP() (with parentheses) for the current time."
    ),
    "bigquery": (
        "TARGET DATABASE: Google BigQuery\n"
        "- String matching: use LIKE (case-sensitive) or LOWER(col) LIKE LOWER(pattern).\n"
        "- Date truncation: DATE_TRUNC(col, MONTH|YEAR|WEEK).\n"
        "- Safe division: SAFE_DIVIDE(numerator, denominator).\n"
        "- Window functions: fully supported.\n"
        "- CTEs: fully supported.\n"
        "- String concatenation: use || or CONCAT().\n"
        "- Identifier quoting: use backticks for reserved words or project.dataset.table paths.\n"
        "- Use CURRENT_TIMESTAMP() for the current timestamp.\n"
        "- Arrays: UNNEST() to flatten array columns.\n"
        "- Do NOT use ILIKE — it is not supported in BigQuery."
    ),
    "mysql": (
        "TARGET DATABASE: MySQL\n"
        "- String matching: LIKE is case-insensitive for utf8 collations by default.\n"
        "- Date truncation: DATE_FORMAT(col, '%Y-%m-01') or DATE_TRUNC is unavailable — "
        "use DATE_FORMAT instead.\n"
        "- Safe division: NULLIF(denominator, 0).\n"
        "- Window functions: supported in MySQL 8.0+.\n"
        "- CTEs: supported in MySQL 8.0+.\n"
        "- String concatenation: use CONCAT(a, b) — do NOT use || (treated as OR).\n"
        "- Identifier quoting: use backticks.\n"
        "- Do NOT use ILIKE — it is not supported.\n"
        "- Use NOW() for the current timestamp."
    ),
    "mssql": (
        "TARGET DATABASE: Microsoft SQL Server (T-SQL)\n"
        "- String matching: LIKE is case-insensitive for default collations.\n"
        "- Date truncation: DATETRUNC(month|year|week, col) (SQL Server 2022+) or "
        "DATEADD(month, DATEDIFF(month, 0, col), 0) for earlier versions.\n"
        "- Safe division: NULLIF(denominator, 0).\n"
        "- Window functions: fully supported.\n"
        "- CTEs: fully supported.\n"
        "- String concatenation: use + operator or CONCAT().\n"
        "- Identifier quoting: use square brackets [column_name].\n"
        "- Use TOP N instead of LIMIT.\n"
        "- Do NOT use ILIKE — use LIKE with appropriate collation.\n"
        "- Use GETDATE() or CURRENT_TIMESTAMP for the current timestamp."
    ),
}

_DIALECT_FALLBACK = (
    "TARGET DATABASE: Standard SQL\n"
    "- Write ANSI-compatible SQL that avoids engine-specific extensions.\n"
    "- Use LIMIT for row restriction, LIKE for string matching, and standard window functions."
)


def get_dialect_instruction(dialect: str) -> str:
    """Return the dialect-specific SQL instruction block for the given engine name."""
    return _DIALECT_INSTRUCTIONS.get(dialect.lower(), _DIALECT_FALLBACK)


# ---------------------------------------------------------------------------
# PEER — ENTITY EXTRACTION PROMPTS
# ---------------------------------------------------------------------------

PEER_ENTITY_EXTRACTION_SYSTEM_PROMPT = (
    "You are a SQL analysis assistant for a Text2SQL system.\n"
    "Inspect a SQL query and extract every filter value in a WHERE clause\n"
    "that represents a real-world named entity — a person name, organisation name,\n"
    "category label, status value, or any similar domain concept.\n\n"
    "Rules:\n"
    "- DO extract: string literals used in equality (=), ILIKE, LIKE, or IN filters.\n"
    "- DO NOT extract: numeric thresholds, date literals, computed expressions, or column-to-column comparisons.\n"
    "- If the SQL has no entity filters, return an empty JSON array [].\n"
    "- Resolve table aliases to full physical table names where possible.\n"
    "- IMPORTANT: if the filter appears inside a CTE body, resolve 'table' to the physical\n"
    "  source table that the column originates from (e.g. if CTE 'td' selects from\n"
    "  'dwh_d_players', use 'dwh_d_players' as the table). If you cannot determine the\n"
    "  physical table, use the CTE name as-is — the system will skip probing for it.\n\n"
    "Return ONLY a valid JSON array. Each element must have exactly these keys:\n"
    '  "column"   — the column being filtered (string)\n'
    '  "table"    — the physical table name (string, or CTE name if unresolvable)\n'
    '  "value"    — the raw string value as it appears in the SQL (string)\n'
    '  "operator" — the SQL operator: =, ILIKE, LIKE, or IN (string)\n\n'
    "Example output:\n"
    '[\n'
    '  {"column": "full_name", "table": "users", "value": "jon smth", "operator": "ILIKE"},\n'
    '  {"column": "status", "table": "orders", "value": "active", "operator": "="}\n'
    ']'
)


def peer_entity_extraction_user_prompt(sql: str) -> str:
    """User prompt to extract entity filter values from the generated SQL."""
    return (
        f"SQL query to analyse:\n\n```sql\n{sql}\n```\n\n"
        "Extract all named-entity filter values from WHERE clauses. "
        "Return a JSON array only — no extra text."
    )


PEER_LLM_PATCH_SYSTEM_PROMPT = (
    "You are a SQL rewriting assistant.\n"
    "Apply ONLY the specified substitutions to the SQL query — change nothing else.\n"
    "Preserve all formatting, whitespace, comments, and SQL structure.\n"
    "Return ONLY the corrected SQL — no markdown fences, no explanations."
)


def peer_llm_patch_user_prompt(original_sql: str, substitutions: list[dict]) -> str:
    """User prompt to apply entity substitutions to SQL via LLM rewrite."""
    sub_lines = "\n".join(
        f'  - Replace "{s["original"]}" with "{s["corrected"]}" '
        f'(column: {s["column"]}, table: {s["table"]})'
        for s in substitutions
    )
    return (
        f"Original SQL:\n\n```sql\n{original_sql}\n```\n\n"
        f"Apply these substitutions exactly:\n{sub_lines}\n\n"
        "Return the corrected SQL only — no markdown fencing."
    )

