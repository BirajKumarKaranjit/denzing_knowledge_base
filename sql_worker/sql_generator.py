from __future__ import annotations

import json
import re
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import openai
from openai.types.chat import ChatCompletionSystemMessageParam, ChatCompletionUserMessageParam

from utils.config import OPENAI_API_KEY, OPENAI_SQL_MODEL
from utils.prompts.kb_generation_prompts import (
    META_QUERY_SYSTEM_PROMPT,
    RELEVANCE_CHECK_SYSTEM_PROMPT,
    meta_query_user_prompt,
    relevance_check_user_prompt,
)

_client = openai.OpenAI(api_key=OPENAI_API_KEY)


def is_query_relevant(
    user_query: str, schema_context: str
) -> tuple[bool, str, str, list[str]]:
    """Unified relevance gate: garbage, greeting, out-of-domain, or SQL-relevant.

    Parameters
    user_query
    schema_context

    Returns
    tuple[bool, str, str, list[str]]
        (is_relevant, category, response_message, suggested_questions).
        If is_relevant is True, response_message and suggested_questions are empty.
    """
    try:
        response = _client.chat.completions.create(
            model=OPENAI_SQL_MODEL,
            messages=[
                ChatCompletionSystemMessageParam(
                    role="system", content=RELEVANCE_CHECK_SYSTEM_PROMPT
                ),
                ChatCompletionUserMessageParam(
                    role="user",
                    content=relevance_check_user_prompt(user_query, schema_context),
                ),
            ],
            temperature=0.0,
            max_tokens=300,
        )
        raw = response.choices[0].message.content.strip()
        match = re.search(r'\{.*}', raw, re.DOTALL)
        if match:
            data = json.loads(match.group())
            is_relevant = bool(data.get("is_relevant", True))
            category = str(data.get("category", "SQL_RELEVANT"))
            msg = str(data.get("response", ""))
            suggestions = data.get("suggested_questions", [])
            if not isinstance(suggestions, list):
                suggestions = []
            return is_relevant, category, msg, suggestions
    except (json.JSONDecodeError, openai.OpenAIError, KeyError):
        pass
    return True, "SQL_RELEVANT", "", []


def answer_meta_query(user_query: str, kb_context: str) -> str:
    """Answer a META_QUERY using knowledge base documentation instead of SQL.

    Parameters
    user_query
    kb_context

    Returns
    -------
    str
        Natural language answer derived from the KB documentation.
    """
    messages: list[ChatCompletionSystemMessageParam | ChatCompletionUserMessageParam] = [
        ChatCompletionSystemMessageParam(
            role="system", content=META_QUERY_SYSTEM_PROMPT
        ),
        ChatCompletionUserMessageParam(
            role="user",
            content=meta_query_user_prompt(user_query, kb_context),
        ),
    ]
    response = _client.chat.completions.create(
        model=OPENAI_SQL_MODEL,
        messages=messages,
        temperature=0.2,
        max_tokens=512,
    )
    return (response.choices[0].message.content or "").strip()


_SQL_GENERATION_SYSTEM_PROMPT = (
    "You are an expert SQL analyst. Generate a single, optimized SQL query. "
    "Follow every rule below without exception.\n\n"

    "## OUTPUT RULES\n"
    "- Return ONLY executable SQL inside a markdown ```sql ... ``` block. No text outside it.\n"
    "- Return exactly ONE SQL statement. Never produce two separate SELECT statements.\n"
    "- If valid SQL cannot be generated (missing tables/columns), return an empty code block:\n"
    "  ```sql\n"
    "  ```\n"
    "- Never return message-only queries like: SELECT 'explanation' AS message;\n\n"

    "## COMPOUND QUESTIONS\n"
    "When the user asks two related things in one question (e.g. 'who leads X, and what about Y?'),\n"
    "combine both answers into ONE query using shared CTEs and a UNION ALL in the final SELECT.\n"
    "Never split the answer into two standalone queries.\n\n"

    "## UNION ALL RULES (applies to all SQL dialects)\n"
    "- All SELECT branches in a UNION ALL must return the same number of columns with compatible types.\n"
    "  Explicitly name every column — never use SELECT * when joining branches with different source CTEs.\n"
    "- ORDER BY and LIMIT/TOP cannot appear inside an individual SELECT that is part of a UNION ALL.\n"
    "  Wrap the branch that needs ordering/limiting in a subquery:\n"
    "    CORRECT:\n"
    "      SELECT col1, col2 FROM (SELECT col1, col2 FROM cte ORDER BY col2 DESC LIMIT 1) top_result\n"
    "      UNION ALL\n"
    "      SELECT col1, col2 FROM cte WHERE col1 ILIKE '%name%';\n"
    "    WRONG:\n"
    "      SELECT col1, col2 FROM cte ORDER BY col2 DESC LIMIT 1\n"
    "      UNION ALL\n"
    "      SELECT col1, col2 FROM cte WHERE col1 ILIKE '%name%';\n\n"

    "## CTE RULES\n"
    "- A CTE defined in a WITH clause is accessible to all branches of a UNION ALL in the same query.\n"
    "- Use one WITH block at the top. Do not redefine the same CTE twice.\n"
    "- Do NOT place LIMIT inside an analytical CTE body — apply LIMIT only on the final SELECT.\n\n"

    "## SCHEMA CORRECTNESS\n"
    "- Use ONLY the tables and columns listed in the RELEVANT TABLE SCHEMAS section of the prompt.\n"
    "- Verify every table in FROM/JOIN exists in the provided schema.\n"
    "- Verify every column in SELECT, WHERE, GROUP BY, ORDER BY exists in the provided schema.\n"
    "- Never invent table names, column names, or aliases not present in the schema.\n"
    "- Use internal ID columns only for JOINs or filters — never expose raw ID columns in SELECT output.\n\n"

    "## COLUMN OWNERSHIP — CRITICAL\n"
    "Columns belong to specific tables. Before writing any filter or subquery, confirm the column\n"
    "exists on the table you are querying.\n\n"

    "## GROUP BY RULE\n"
    "- Every non-aggregated column in a SELECT clause must appear in the GROUP BY clause.\n"
    "- Whenever aggregation functions (SUM, COUNT, AVG, MAX, MIN) are used, apply GROUP BY correctly.\n\n"
    
    "## OUTPUT COMPLETENESS\n"
    "- Every value computed in a CTE that scopes the query MUST appear in the final SELECT.\n"
    "-If a CTE computes a boundary value (a minimum, maximum, threshold, or derived period) to filter the main query, expose that value in the output so the user understands what time period, scope, or condition the result covers.\n"
    "- Always include the dimension columns that explain an aggregate: the time period it covers, the entity it belongs to, and the sample size it is based on.\n"
    "- Never return a bare aggregate (SUM, AVG, COUNT, MAX, MIN) as the sole output column. Every aggregate must be accompanied by the columns that answer: aggregate of what, over what scope, for how many records?\n"
    
    "## NULL AND DIVISION SAFETY\n"
    "- Handle divide-by-zero with NULLIF(denominator, 0).\n"
    "- Use COALESCE(col, 0) for NULL numeric values when a zero default is appropriate.\n"
    "- Use IS NOT NULL / IS NULL for NULL checks — never use = NULL or != NULL.\n\n"

    "## ENTITY MATCHING\n"
    "- Apply case-insensitive comparison for all text filters (names, labels, categories, etc.).\n"
    "- For string columns, use dialect-appropriate case-insensitive matching as directed in the\n"
    "  SQL DIALECT INSTRUCTIONS section. Never use strict = for human names or categorical labels.\n\n"

    "## RECENCY AND DATES\n"
    "- For 'latest', 'most recent', or 'last' record: dynamically select the MAX of the date column.\n"
    "  Never hardcode a date value or use CURRENT_DATE unless the schema or question requires it.\n\n"

    "## VAGUE QUERIES\n"
    "- For underspecified questions: select key columns from the most relevant table, apply LIMIT 10.\n"
    "- If the question cannot be answered due to missing tables/columns, return an empty SQL block.\n\n"

    "## DIALECT\n"
    "- Strictly follow the SQL DIALECT INSTRUCTIONS block in the prompt for engine-specific syntax.\n\n"
    
    "## MANDATORY SELF-CHECK BEFORE OUTPUT\n"
    "Before writing the final SQL, verify:\n"
    "  1. Every table referenced exists in the provided schema.\n"
    "  2. Every column referenced exists in its table in the provided schema.\n"
    "  3. All UNION ALL branches have the same number and type of columns.\n"
    "  4. No ORDER BY / LIMIT appears directly inside a UNION ALL branch (wrap in subquery if needed).\n"
    "  5. GROUP BY is complete — every non-aggregated SELECT column is listed.\n"
    "  6. Exactly one SQL statement is produced."
    "  7. Whenever AND and OR appear in the same WHERE clause, always add parentheses to make the intended logic explicit to avoid precedence errors."
)


def generate_sql(prompt: str, temperature: float = 0.0) -> str:
    """Send the assembled prompt to the LLM and return the generated SQL.

    Parameters
    prompt:
    temperature:

    Returns
    str
        Raw LLM response text containing a SQL code block.
    """
    messages: list[ChatCompletionSystemMessageParam | ChatCompletionUserMessageParam] = [
        ChatCompletionSystemMessageParam(
            role="system",
            content=_SQL_GENERATION_SYSTEM_PROMPT,
        ),
        ChatCompletionUserMessageParam(role="user", content=prompt),
    ]
    response = _client.chat.completions.create(
        model=OPENAI_SQL_MODEL,
        messages=messages,
        temperature=temperature,
    )
    return response.choices[0].message.content or ""


def extract_sql_from_response(llm_response: str) -> str:
    """Extract the raw SQL string from an LLM response containing a code block.

    Parameters
    llm_response:
        Raw string returned by generate_sql().

    Returns
    -------
    str
        Clean SQL string with no markdown fencing.
    """
    match = re.search(r"```(?:sql)?\s*\n?(.*?)```", llm_response, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return llm_response.strip()


def build_retry_prompt(original_prompt: str, failed_sql: str, error_message: str) -> str:
    """Append an error-feedback block to *original_prompt* for a single retry.

    Parameters
    original_prompt
    failed_sql
    error_message

    Returns
    str
        Extended prompt ready for a second generate_sql() call.
    """
    error_block = (
        "\n\n## PREVIOUS SQL ATTEMPT FAILED\n\n"
        "The SQL below was generated but failed during execution.\n"
        "Fix only what caused the error. Do not change the query logic or structure.\n\n"
        f"Failed SQL:\n```sql\n{failed_sql}\n```\n\n"
        f"Execution error:\n{error_message}"
    )
    return original_prompt + error_block

