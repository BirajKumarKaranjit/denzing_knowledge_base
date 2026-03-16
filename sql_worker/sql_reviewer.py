"""sql_worker/sql_reviewer.py

LLM-based SQL quality review stage.

After structural verification passes, the reviewer checks whether the SQL
correctly and completely answers the user's question. It may return a revised
SQL. Any parsing failure is treated as approval — the reviewer never blocks
execution.
"""

from __future__ import annotations

import logging
import json
import re
from dataclasses import dataclass, field
from typing import Any

from utils.llm_client import call_llm
from utils.prompts.kb_generation_prompts import get_dialect_instruction

_log = logging.getLogger(__name__)

_REVIEWER_SYSTEM_PROMPT = (
    "You are a SQL quality reviewer. You will be given a user question, generated SQL,\n"
    "relevant DDL, and dialect instructions.\n\n"
    "Goal: judge whether the SQL is logically correct and complete for the question.\n"
    "If incorrect, return a minimal corrected SQL.\n\n"
    "RULES:\n"
    "- Schema existence validation is handled upstream. Do not remove or question any\n"
    "  table or column. Focus on logic and answer quality only.\n"
    "- If the provided SQL is empty or comment-only, return approved=true immediately.\n"
    "  Never construct SQL from scratch - only review and minimally correct.\n"
    "- Do not invent table or column names. Use only names present in the SQL or DDL.\n"
    "- Make minimal edits. Preserve the original structure when logically sound.\n"
    "- Follow dialect instructions exactly.\n"
    "- Always prefix every column reference with its table alias in any SQL you write.\n"
    "  Never use bare column names in SELECT, WHERE, JOIN ON, GROUP BY, HAVING, ORDER BY.\n"
    "- Never filter a window-function alias at the same SELECT level where it is computed.\n"
    "  Wrap in an outer CTE first: WITH ranked AS (...) SELECT * FROM ranked WHERE rank = 1.\n"
    "- Keep revised SQL executable as a single statement with no prose or markdown.\n\n"
    "CHECKLIST - flag only if clearly wrong:\n"
    "1) Intent: does the SQL answer exactly what the user asked?\n"
    "2) Aggregation grain: grouping and calculation level are correct.\n"
    "   Every non-aggregated SELECT column must appear in GROUP BY.\n"
    "3) Named entity in SELECT: if the question is about a specific entity\n"
    "   (player, team, product, customer), its human-readable name must appear\n"
    "   in SELECT even when the query is already filtered to that entity.\n"
    "4) Scope: time and entity filters match the question intent.\n"
    "   If implied scope is missing, apply a sensible default and note it in changes.\n"
    "5) Join correctness: no row multiplication before ORDER BY, LIMIT, or window functions.\n"
    "   Resolve fan-out joins in a CTE or use WHERE/IN/EXISTS.\n"
    "6) Superlative intent: if question contains 'best', 'most', 'highest', 'lowest',\n"
    "   'worst', or 'top', use LIMIT 10 not LIMIT 1 unless user asked for a single result.\n"
    "   When using RANK(), filter in an outer CTE - never WHERE rank=1 inside the same CTE.\n"
    "7) UNION compatibility: all branches return the same number of columns.\n"
    "8) AND/OR predicates: parentheses are explicit when AND and OR are mixed.\n"
    "9) Averages: prefer AVG(col) over SUM/COUNT. Cast integers to numeric before division.\n"
    "10) Percent/ratio columns: verify stored range (0-1 vs 0-100) before any transformation.\n"
    "11) Granularity: if the question requires detail not in the DDL, do not silently answer\n"
    "    at a coarser level. Set approved=false, include a SQL comment explaining the\n"
    "    limitation, and compute the closest available approximation.\n"
    "12) Output usefulness: include human-readable dimensions (names, dates, sample size)\n"
    "    alongside aggregates. Never return bare aggregates without context columns.\n"
    "- Only flag genuine issues. Approve confidently when SQL is correct.\n\n"
    "OUTPUT - strictly a JSON object, no markdown:\n"
    "{\n"
    "  \"approved\": true | false,\n"
    "  \"revised_sql\": string | null,\n"
    "  \"changes\": [string, ...]\n"
    "}\n"
    "If approved=true: revised_sql must be null, changes must be [], no exceptions.\n"
    "If approved=false: revised_sql must be a complete executable SQL statement.\n"
    "Return only the JSON object; any extra text will be discarded.\n"
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
    dialect: str = "",
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
    user_prompt = _build_user_prompt(
        user_query=user_query,
        generated_sql=generated_sql,
        ddl_context=ddl_context,
        dialect=dialect,
    )

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
    dialect: str,
) -> str:
    """Assemble the user-facing review prompt."""
    parts: list[str] = [
        f"## USER QUESTION\n\n{user_query}",
        f"## GENERATED SQL\n\n```sql\n{generated_sql}\n```",
    ]

    if dialect.strip():
        parts.append(
            "## SQL DIALECT INSTRUCTIONS\n\n"
            f"Target dialect: {dialect}\n\n"
            f"{get_dialect_instruction(dialect)}"
        )

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

    json_result = _parse_json_response(stripped)
    if json_result is not None:
        return json_result

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


def _parse_json_response(raw: str) -> ReviewResult | None:
    """Parse JSON-formatted reviewer response. Returns None when not JSON-like."""
    payload = raw
    if payload.startswith("```"):
        payload = re.sub(r"^```(?:json)?\s*", "", payload, flags=re.IGNORECASE)
        payload = re.sub(r"\s*```$", "", payload)

    if not payload.startswith("{"):
        return None

    try:
        obj = json.loads(payload)
    except Exception:  # noqa: BLE001
        return None

    approved = bool(obj.get("approved", True))
    revised_sql = obj.get("revised_sql")
    changes_raw = obj.get("changes", [])
    changes = [str(c).strip() for c in changes_raw if str(c).strip()]

    if approved:
        return ReviewResult(approved=True)

    if not isinstance(revised_sql, str) or not revised_sql.strip():
        _log.warning(
            "[sql_reviewer] JSON response had approved=false but empty revised_sql — treating as approved."
        )
        return ReviewResult(approved=True)

    return ReviewResult(approved=False, revised_sql=revised_sql.strip(), changes=changes)

