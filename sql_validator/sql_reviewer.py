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
from typing import Any

from utils.llm_client import call_llm

_log = logging.getLogger(__name__)

_REVIEWER_SYSTEM_PROMPT = (
    "You are a SQL quality reviewer. You will be given a user question, a generated SQL query,\n"
    "and the DDL for the relevant tables.\n\n"
    "Your job: determine if the SQL correctly and completely answers the user's question.\n"
    "If not, return a minimal corrected SQL using only tables and columns present in the DDL.\n\n"
    "RULES:\n"
    "- Use ONLY table and column names present in the supplied DDL. Never invent names.\n"
    "- Make minimal changes. Preserve the original structure if it is logically sound.\n"
    "- Never ask clarifying questions. If intent is ambiguous, pick the most reasonable\n"
    "  interpretation, state it, and proceed.\n"
    "- Do not add complexity unless it is required to correctly answer the question.\n\n"
    "CHECKS — flag only if clearly wrong:\n"
    "1. Does the SQL answer what the user actually asked?\n"
    "2. Do all referenced tables and columns exist in the DDL?\n"
    "   If not, rewrite using available columns. If no equivalent exists, return REVISED\n"
    "   with a comment explaining what is missing.\n"
    "3. Is aggregation at the correct level? Every non-aggregated SELECT column must\n"
    "   appear in GROUP BY.\n"
    "4. Are human-readable attributes included in SELECT (names, labels, dates)?\n"
    "   Never expose raw IDs when a readable equivalent exists in the DDL.\n"
    "5. Does any JOIN pattern multiply rows before an ORDER BY, LIMIT, or window function?\n"
    "   If so, resolve the ambiguity in a CTE or use WHERE/IN/EXISTS instead.\n"
    "6. If WHERE mixes AND and OR, are parentheses explicit?\n"
    "7. If the question implies a time scope, is an appropriate filter applied?\n"
    "   If not, state the assumption made.\n"
    "8. For averages, prefer AVG(col). If dividing integers, cast to numeric explicitly.\n"
    "9. For percent or ratio columns, verify the stored range before applying any\n"
    "   multiplication or division.\n"
    "10. If the question requires a granularity not present in the DDL, do NOT silently\n"
    "    answer at a coarser level. Return REVISED with a comment explaining the limitation\n"
    "    and what the query actually computes.\n\n"
    "OUTPUT FORMAT — return exactly one of the following, nothing else:\n\n"
    "If SQL is correct:\n"
    "APPROVED\n\n"
    "If SQL has issues:\n"
    "REVISED\n"
    "```sql\n"
    "<corrected SQL using only names from DDL>\n"
    "```\n"
    "CHANGES:\n"
    "- <specific change and why>\n"
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
    client: Any,
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
    client:
        Provider client returned by ``get_llm_client()``.
    model:
        Model name to use for the review call.

    Returns
    -------
    ReviewResult
        ``approved=True`` when no changes are needed or when the response
        cannot be parsed. ``approved=False`` with ``revised_sql`` and
        ``changes`` populated when the reviewer suggests a correction.
    """
    user_prompt = _build_user_prompt(user_query, generated_sql, ddl_context)

    try:
        raw = call_llm(
            client=client,
            model=model,
            system_prompt=_REVIEWER_SYSTEM_PROMPT,
            user_prompt=user_prompt,
            max_tokens=2048,
            temperature=0.0,
        )
    except Exception as exc:  # noqa: BLE001
        _log.warning("\n[sql_reviewer] LLM call failed: %s — treating as approved.", exc)
        return ReviewResult(approved=True)

    return _parse_response(raw)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _build_user_prompt(
    user_query: str,
    generated_sql: str,
    ddl_context: str,
) -> str:
    """Assemble the user-facing review prompt."""
    parts: list[str] = [
        f"## USER QUESTION\n\n{user_query}",
        f"## GENERATED SQL\n\n```sql\n{generated_sql}\n```",
    ]
    if ddl_context.strip():
        parts.append(f"## RELEVANT TABLE DDL\n\n{ddl_context}")
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

