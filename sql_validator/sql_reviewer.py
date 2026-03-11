"""sql_validator/sql_reviewer.py

LLM-based SQL quality review stage.

After structural verification passes, the reviewer checks whether the SQL
correctly and completely answers the user's question. It may return a revised
SQL. Any parsing failure is treated as approval — the reviewer never blocks
execution.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

import openai
from openai.types.chat import (
    ChatCompletionSystemMessageParam,
    ChatCompletionUserMessageParam,
)

_log = logging.getLogger(__name__)

_REVIEWER_SYSTEM_PROMPT = (
    "You are a SQL quality reviewer. You will be given a user question, a generated SQL\n"
    "query, the DDL of the relevant tables, and SQL writing guidelines.\n\n"
    "Your job is to verify whether the SQL correctly and completely answers the user's question.\n\n"
    "STRICT RULES:\n"
    "- Use ONLY table names and column names present in the provided DDL. Never add a column\n"
    "  or table that does not explicitly exist in the DDL.\n"
    "- Do not change the fundamental query approach if it is logically correct.\n"
    "- Do not add complexity that is not needed to answer the question.\n\n"
    "REVIEW CHECKLIST — flag an issue only if clearly wrong:\n"
    "1. Does the SQL answer what the user actually asked?\n"
    "2. If a CTE computed a value to scope the query, does that value appear in the final SELECT?\n"
    "3. Is the aggregation logic applied at the correct level (per-row vs per-group)?\n"
    "4. Are the contextual dimension columns included alongside aggregates\n"
    "   (time period, entity name, sample size)?\n"
    "5. Does any JOIN pattern produce duplicate rows before a LIMIT or ORDER BY?\n"
    "6. Do all UNION ALL branches return the same number of columns?\n\n"
    "RESPONSE FORMAT — return exactly one of these two formats, nothing else:\n\n"
    "If SQL is correct and complete:\n"
    "APPROVED\n\n"
    "If SQL has issues:\n"
    "REVISED\n"
    "```sql\n"
    "<corrected SQL here>\n"
    "```\n"
    "CHANGES:\n"
    "- <specific change 1 and why>\n"
    "- <specific change 2 and why>"
)


@dataclass
class ReviewResult:
    """Outcome of a single review_sql() call."""

    approved: bool
    revised_sql: str | None = None
    changes: list[str] = field(default_factory=list)


def review_sql(
    user_query: str,
    generated_sql: str,
    ddl_context: str,
    guidelines_context: str,
    client: openai.OpenAI,
    model: str,
) -> ReviewResult:
    """Submit *generated_sql* to the LLM reviewer and return a ReviewResult.

    Parameters
    ----------
    user_query:
        The original natural language question.
    generated_sql:
        The SQL to review (post-PEER, post-verification).
    ddl_context:
        Concatenated DDL of the tables used in the query.
    guidelines_context:
        Relevant SQL guidelines injected into the prompt.
    client:
        Initialised OpenAI client.
    model:
        Model name to use for the review call.

    Returns
    -------
    ReviewResult
        ``approved=True`` when no changes are needed or when the response
        cannot be parsed. ``approved=False`` with ``revised_sql`` and
        ``changes`` populated when the reviewer suggests a correction.
    """
    user_prompt = _build_user_prompt(user_query, generated_sql, ddl_context, guidelines_context)

    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                ChatCompletionSystemMessageParam(
                    role="system", content=_REVIEWER_SYSTEM_PROMPT
                ),
                ChatCompletionUserMessageParam(role="user", content=user_prompt),
            ],
            temperature=0.0,
        )
        raw = (response.choices[0].message.content or "").strip()
    except openai.OpenAIError as exc:
        _log.warning("[sql_reviewer] LLM call failed: %s — treating as approved.", exc)
        return ReviewResult(approved=True)

    return _parse_response(raw)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _build_user_prompt(
    user_query: str,
    generated_sql: str,
    ddl_context: str,
    guidelines_context: str,
) -> str:
    """Assemble the user-facing review prompt."""
    parts: list[str] = [
        f"## USER QUESTION\n\n{user_query}",
        f"## GENERATED SQL\n\n```sql\n{generated_sql}\n```",
    ]
    if ddl_context.strip():
        parts.append(f"## RELEVANT TABLE DDL\n\n{ddl_context}")
    if guidelines_context.strip():
        parts.append(f"## SQL GUIDELINES\n\n{guidelines_context}")
    return "\n\n".join(parts)


def _parse_response(raw: str) -> ReviewResult:
    """Parse the raw LLM response into a ReviewResult.

    Returns ``approved=True`` on any parsing failure so the reviewer never
    blocks execution.
    """
    first_token = raw.split()[0].upper() if raw.split() else ""

    if first_token == "APPROVED":
        return ReviewResult(approved=True)

    if first_token == "REVISED":
        sql_match = re.search(r"```(?:sql)?\s*\n?(.*?)```", raw, re.DOTALL | re.IGNORECASE)
        if not sql_match:
            _log.warning(
                "[sql_reviewer] REVISED response had no SQL block — treating as approved."
            )
            return ReviewResult(approved=True)

        revised_sql = sql_match.group(1).strip()

        changes: list[str] = []
        changes_match = re.search(r"CHANGES:\s*\n((?:\s*-[^\n]+\n?)+)", raw, re.IGNORECASE)
        if changes_match:
            for line in changes_match.group(1).splitlines():
                line = line.strip().lstrip("- ").strip()
                if line:
                    changes.append(line)

        return ReviewResult(approved=False, revised_sql=revised_sql, changes=changes)

    _log.warning(
        "[sql_reviewer] Unparseable response (first token: %r) — treating as approved.",
        first_token,
    )
    return ReviewResult(approved=True)

