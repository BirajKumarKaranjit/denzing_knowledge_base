"""sql_worker/sql_reviewer.py

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
    "and DDL context.\n\n"
    "Your job: determine whether the SQL answers the question correctly and completely.\n"
    "If not, return a minimal corrected SQL.\n\n"
    "RULES:\n"
    "- The SQL has already passed schema verification upstream. Do not remove, rename, or\n"
    "  question any table or column because it appears absent from DDL context.\n"
    "  Schema existence is not your concern — focus on logic and result quality only.\n"
    "- Use only names present in the supplied SQL and DDL context. Never invent new names.\n"
    "- Make minimal edits. Preserve the original structure when it is logically sound.\n"
    "- Do not add complexity unless required to correctly answer the question.\n"
    "- Always prefix every column reference with its table alias (e.g. pb.points, g.game_date).\n"
    "  This applies to SELECT, WHERE, JOIN ON, GROUP BY, HAVING, and ORDER BY.\n"
    "  Never use bare column names.\n\n"
    "CHECKS — flag only if clearly wrong:\n"
    "1.  Does the SQL answer what the user actually asked?\n"
    "2.  Is aggregation at the correct level? Every non-aggregated SELECT column must\n"
    "    appear in GROUP BY.\n"
    "3.  Are contextual dimension columns included alongside aggregates?\n"
    "    If the query is about a specific named entity (player, team, product, customer),\n"
    "    that entity's human-readable name MUST appear in SELECT — even when the query\n"
    "    is already filtered to that entity. Also include time period and sample size.\n"
    "4.  Does any JOIN pattern multiply rows before an ORDER BY, LIMIT, or window function?\n"
    "    Resolve in a CTE or use WHERE/IN/EXISTS instead.\n"
    "5.  If WHERE mixes AND and OR, are parentheses explicit?\n"
    "6.  If the question implies a time scope, is an explicit filter applied or assumption stated?\n"
    "7.  For averages, prefer AVG(col). If dividing integers, cast to numeric explicitly.\n"
    "8.  For percent or ratio columns, verify the stored range (0–1 vs 0–100) before\n"
    "    applying any multiplication or division.\n"
    "9.  If the question requires a data granularity not present in the DDL, do NOT silently\n"
    "    answer at a coarser level. Return REVISED with a SQL comment explaining the\n"
    "    limitation and what the query actually computes.\n"
    "10. Do all UNION ALL branches return the same number of columns?\n"
    "11. If the question contains superlative intent ('best', 'most', 'highest', 'lowest',\n"
    "    'worst', 'top'), use LIMIT 10 not LIMIT 1, unless the user explicitly asked\n"
    "    for a single result.\n\n"
    "OUTPUT FORMAT — return exactly one of the following. First line must be APPROVED or REVISED:\n\n"
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
    stripped = raw.strip()
    if not stripped:
        _log.warning("[sql_reviewer] Empty response — treating as approved.")
        return ReviewResult(approved=True)

    first_line = stripped.splitlines()[0].strip().upper()
    normalized = stripped.upper()

    if first_line.startswith("APPROVED"):
        return ReviewResult(approved=True)

    if first_line.startswith("REVISED") or "REVISED" in normalized:
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

    if "APPROVED" in normalized and "REVISED" not in normalized:
        return ReviewResult(approved=True)

    _log.warning("[sql_reviewer] Unparseable response (first line: '%s') — treating as approved.", first_line)
    return ReviewResult(approved=True)
