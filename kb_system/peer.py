"""kb_system/peer.py

Pre-Execution Entity Resolution (PEER) layer.

After SQL is generated, PEER validates named-entity filter values against
the actual database using fuzzy matching and patches the SQL if needed.
PEER is non-fatal: any failure returns the original SQL unchanged.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import List
import re

import openai
import psycopg2
import psycopg2.extras
import sqlparse
from sqlparse.sql import Comparison, Identifier, Function, Parenthesis
from sqlparse import tokens as T
from openai.types.chat import (
    ChatCompletionSystemMessageParam,
    ChatCompletionUserMessageParam,
)
from rapidfuzz import fuzz

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.config import (
    OPENAI_API_KEY,
    OPENAI_SQL_MODEL,
    PEER_AUTO_THRESHOLD,
    PEER_ENABLED,
    PEER_EXACT_THRESHOLD,
    PEER_FLAG_THRESHOLD,
    PEER_PATCH_METHOD,
    PEER_PROBE_PREFIX_LEN,
    PEER_PROBE_ROW_LIMIT,
    nba_db_config,
)
from utils.prompts.kb_generation_prompts import (
    PEER_ENTITY_EXTRACTION_SYSTEM_PROMPT,
    PEER_LLM_PATCH_SYSTEM_PROMPT,
    peer_entity_extraction_user_prompt,
    peer_llm_patch_user_prompt,
)

_log = logging.getLogger(__name__)
_client = openai.OpenAI(api_key=OPENAI_API_KEY)

# Valid SQL comparison operators that can precede a string literal entity filter.
_COMPARISON_OPERATORS = {"=", "like", "ilike"}


@dataclass
class _EntityMatch:
    """Internal per-entity resolution result."""

    column: str
    table: str
    value: str
    operator: str
    corrected: str = ""
    score: int = 0
    action: str = "skip"  # exact | auto_sub | flag_sub | no_match | skip


@dataclass
class PEERResult:
    """Result returned by the PEER orchestrator."""

    sql: str
    patched: bool = False
    messages: list[str] = field(default_factory=list)
    unvalidatable: list[str] = field(default_factory=list)
    error: str = ""


def _extract_entities(sql: str) -> list[_EntityMatch]:
    """Call the LLM to extract named-entity filter values from the SQL.

    Returns an empty list if no entity filters exist or if parsing fails.
    """
    try:
        response = _client.chat.completions.create(
            model=OPENAI_SQL_MODEL,
            messages=[
                ChatCompletionSystemMessageParam(
                    role="system",
                    content=PEER_ENTITY_EXTRACTION_SYSTEM_PROMPT,
                ),
                ChatCompletionUserMessageParam(
                    role="user",
                    content=peer_entity_extraction_user_prompt(sql),
                ),
            ],
            temperature=0.0,
            max_tokens=512,
        )
        raw = (response.choices[0].message.content or "").strip()
        match = re.search(r"\[.*]", raw, re.DOTALL)
        if not match:
            return []
        items: list[dict] = json.loads(match.group())
        if not isinstance(items, list):
            return []
        entities: list[_EntityMatch] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            value = str(item.get("value", "")).strip()
            table = str(item.get("table", "")).strip()
            column = str(item.get("column", "")).strip()
            if not value or not table or not column:
                continue
            entities.append(
                _EntityMatch(
                    column=column,
                    table=table,
                    value=value,
                    operator=str(item.get("operator", "=")),
                )
            )
        return entities
    except (json.JSONDecodeError, openai.OpenAIError, KeyError, ValueError) as exc:
        _log.warning("PEER entity extraction failed: %s", exc)
        return []


def _strip_sql_wildcards(value: str) -> str:
    """Remove ILIKE/LIKE wildcards and surrounding quotes from a raw SQL filter value.

    Handles values like ``'%LeBron James%'``, ``"%regular%"``, ``LeBron James```.
    Returns the clean literal string for fuzzy matching and prefix probing.
    """
    cleaned = value.strip()
    cleaned = cleaned.strip("'\"")
    cleaned = cleaned.strip("%")
    return cleaned.strip()


def _probe_candidates(
    conn: psycopg2.extensions.connection,
    table: str,
    column: str,
    value: str,
    db_schema: str = "",
) -> list[str]:
    """Run a prefix-filtered DISTINCT probe query and return candidate values.

    Strips ILIKE wildcards from ``value`` before building the prefix.
    Qualifies the table name with ``db_schema`` when provided.
    Returns an empty list on any database error.
    """
    clean_value = _strip_sql_wildcards(value)
    prefix = clean_value.split()[0][:PEER_PROBE_PREFIX_LEN] if clean_value else ""

    qualified_table = f"{db_schema}.{table}" if db_schema else table

    try:
        with conn.cursor() as cur:
            if prefix:
                cur.execute(
                    f"SELECT DISTINCT {column} FROM {qualified_table} "
                    f"WHERE {column} ILIKE %s LIMIT %s;",
                    (f"{prefix}%", PEER_PROBE_ROW_LIMIT),
                )
            else:
                cur.execute(
                    f"SELECT DISTINCT {column} FROM {qualified_table} LIMIT %s;",
                    (PEER_PROBE_ROW_LIMIT,),
                )
            rows = cur.fetchall()
        return [str(r[0]) for r in rows if r[0] is not None]
    except psycopg2.Error as exc:
        _log.warning("PEER probe query failed (%s.%s): %s", qualified_table, column, exc)
        conn.rollback()
        return []


def _fuzzy_match(value: str, candidates: list[str]) -> tuple[str, int]:
    """Return the best-matching candidate and its token_sort_ratio score."""
    best_candidate = ""
    best_score = 0
    for candidate in candidates:
        score = fuzz.token_sort_ratio(value.lower(), candidate.lower())
        if score > best_score:
            best_score = score
            best_candidate = candidate
    return best_candidate, best_score


def _determine_action(score: int) -> str:
    """Map a fuzzy score to a PEER action string."""
    if score >= PEER_EXACT_THRESHOLD:
        return "exact"
    if score >= PEER_AUTO_THRESHOLD:
        return "auto_sub"
    if score >= PEER_FLAG_THRESHOLD:
        return "flag_sub"
    return "no_match"


def _sql_unescape(raw: str) -> str:
    """Strip outer quotes and unescape doubled single-quotes from a SQL string literal.

    ``'O''Neal'``  →  ``O'Neal``
    ``'regular'``  →  ``regular``
    """
    inner = raw.strip("'\"")
    return inner.replace("''", "'")


def _sql_escape(value: str) -> str:
    """Escape a plain Python string for safe embedding inside a SQL single-quoted literal.

    ``O'Neal``  →  ``O''Neal``
    """
    return value.replace("'", "''")


def _get_column_name(left_token: sqlparse.sql.Token) -> str:
    """Extract the bare column name from a Comparison left-hand side token.

    Handles:
    - Plain ``Identifier``:          ``game_type``        → ``game_type``
    - Qualified ``Identifier``:      ``p.full_name``      → ``full_name``
    - Function-wrapped plain:        ``LOWER(game_type)`` → ``game_type``
    - Function-wrapped qualified:    ``TRIM(p.full_name)``→ ``full_name``
    - Bare ``Token.Name``:           ``name``             → ``name``

    Returns lowercase column name, or empty string when unrecognisable.
    """
    if isinstance(left_token, Identifier):
        name = left_token.get_name()
        return name.lower() if name else ""

    if left_token.ttype == T.Name:
        return left_token.normalized.lower()

    if isinstance(left_token, Function):
        # The argument list is a Parenthesis token; the real column lives inside it.
        for tok in left_token.tokens:
            if isinstance(tok, Parenthesis):
                for inner in tok.tokens:
                    if isinstance(inner, Identifier):
                        name = inner.get_name()
                        return name.lower() if name else ""
                    if inner.ttype == T.Name:
                        return inner.normalized.lower()

    # Generic fallback: walk all sub-tokens and return the last Name found.
    if getattr(left_token, "tokens", None):
        for tok in reversed(left_token.tokens):
            if isinstance(tok, Identifier):
                name = tok.get_name()
                if name:
                    return name.lower()
            if tok.ttype == T.Name:
                return tok.normalized.lower()

    return ""


def _get_qualifier(left_token: sqlparse.sql.Token) -> str:
    """Return the table alias or prefix from a qualified column reference.

    ``p.full_name``  → ``p``
    ``game_type``    → ``""``
    ``LOWER(p.full_name)`` → ``p``

    Used for table disambiguation when ``sub.table`` is populated.
    """
    target: sqlparse.sql.Token | None = None

    if isinstance(left_token, Identifier):
        target = left_token
    elif isinstance(left_token, Function):
        for tok in left_token.tokens:
            if isinstance(tok, Parenthesis):
                for inner in tok.tokens:
                    if isinstance(inner, Identifier):
                        target = inner
                        break

    if target is None:
        return ""

    try:
        parent = target.get_parent_name()
        return parent.lower() if parent else ""
    except AttributeError:
        parts = target.value.split(".")
        return parts[0].strip().lower() if len(parts) > 1 else ""


def _find_rhs_string_token(
    comparison: Comparison,
) -> sqlparse.sql.Token | None:
    """Locate the first string-literal token on the right-hand side of a Comparison.

    Handles both a bare ``Token.Literal.String.Single`` and a ``Function``/
    ``Parenthesis`` that wraps a string (e.g. ``CAST('x' AS TEXT)``).
    Returns ``None`` when no string literal is found — the caller skips the patch.

    Dollar-quoted strings (``$$...$$``) and ``E'...'`` escape-prefix forms are
    not detected as string literals by sqlparse; they surface here as non-string
    tokens and are therefore safely skipped with a debug-level log entry.
    """
    found_op = False
    for tok in comparison.tokens:
        if tok.ttype in (T.Text.Whitespace, T.Text.Whitespace.Newline, T.Newline):
            continue
        if not found_op:
            if tok.value.strip().lower() in _COMPARISON_OPERATORS:
                found_op = True
            continue
        # Direct string literal (the common case).
        if tok.ttype == T.Literal.String.Single:
            return tok
        # TokenList on RHS (e.g. CAST expression): search via flatten().
        if getattr(tok, "tokens", None):
            for flat in tok.flatten():
                if flat.ttype == T.Literal.String.Single:
                    return flat
        # Non-string token after operator (function result, column, etc.) — skip.
        if tok.ttype is not None:
            _log.debug(
                "PEER: RHS of comparison is not a string literal (ttype=%s, val=%r) — skipping.",
                tok.ttype, tok.value[:40],
            )
            return None
    return None


def _build_new_literal(original_raw: str, corrected_escaped: str) -> str:
    """Reconstruct a SQL string literal preserving quote style and ``%`` wildcards.

    ``corrected_escaped`` must already have single-quotes doubled
    (use ``_sql_escape`` before calling this).

    Examples::

        _build_new_literal("'%Jimmy Butler%'", "Jimmy Butler III")
            → "'%Jimmy Butler III%'"

        _build_new_literal("'O''Neal'", "O''Brien")
            → "'O''Brien'"
    """
    if not original_raw:
        return original_raw

    quote = original_raw[0] if original_raw[0] in ("'", '"') else "'"
    inner = original_raw.strip("'\"")

    lead_pct = "%" if inner.startswith("%") else ""
    trail_pct = "%" if inner.endswith("%") else ""

    return f"{quote}{lead_pct}{corrected_escaped}{trail_pct}{quote}"


def _collect_comparisons(token: sqlparse.sql.Token, out: list[Comparison]) -> None:
    """Recursively collect every Comparison node from the token tree.

    Covers WHERE clauses, ON conditions, CTEs, and nested subqueries.
    Comment tokens (``Token.Comment.*``) are leaf nodes and are never
    ``Comparison`` instances — they are inherently skipped.
    """
    if isinstance(token, Comparison):
        out.append(token)
    if token.is_group:
        for sub in token.tokens:
            _collect_comparisons(sub, out)


def _patch_comparison(comparison: Comparison, sub: _EntityMatch) -> bool:
    """Attempt to patch a single Comparison token in-place.

    Guards applied in order:
    1. Column name must match ``sub.column`` (case-insensitive).
    2. If ``sub.table`` is set, the qualifier (alias/table prefix) must match
       or be absent — prevents patching the wrong table when the same column
       name appears in multiple tables.
    3. Operator must be ``=``, ``LIKE``, or ``ILIKE``.
    4. RHS must contain a SQL string literal.
    5. Unescaped inner value must differ from ``sub.corrected`` (idempotence).

    On success, patches ``tok.value`` in-place and logs the change.
    Returns ``True`` if patched, ``False`` otherwise.
    """
    left = comparison.left

    col_name = _get_column_name(left)
    if col_name != sub.column.lower():
        return False

    # Table/alias qualifier guard.
    # Only enforce when both qualifier and sub.table are non-empty AND
    # the qualifier looks like a full table name (not a short alias).
    # When the SQL uses an alias (e.g. "p") but sub.table is the full
    # table name ("dwh_d_players"), we cannot resolve the alias without
    # parsing the FROM clause, so we allow the patch through.
    # We do enforce strictly when both look like aliases (length <= 4,
    # no underscores) — this covers the common disambiguation case of
    # "a.col" vs "b.col" where sub.table is literally "a" or "b".
    qualifier = _get_qualifier(left)
    if sub.table and qualifier:
        tbl_is_alias = len(sub.table) <= 4 and "_" not in sub.table
        qual_is_alias = len(qualifier) <= 4 and "_" not in qualifier
        if tbl_is_alias and qual_is_alias and qualifier != sub.table.lower():
            return False

    # Robust operator detection: check normalised value against allowed set,
    # accepting both T.Operator.Comparison and T.Keyword (dialect variations).
    op_found = False
    for tok in comparison.tokens:
        if tok.ttype in (T.Text.Whitespace, T.Text.Whitespace.Newline, T.Newline):
            continue
        if tok is left:
            continue
        if tok.value.strip().lower() in _COMPARISON_OPERATORS:
            op_found = True
            break

    if not op_found:
        return False

    rhs_token = _find_rhs_string_token(comparison)
    if rhs_token is None:
        return False

    original_raw = rhs_token.value

    # Unescape the inner literal for an accurate idempotence comparison.
    inner_unescaped = _sql_unescape(original_raw).strip("%")
    if inner_unescaped == sub.corrected:
        return False

    corrected_escaped = _sql_escape(sub.corrected)
    new_literal = _build_new_literal(original_raw, corrected_escaped)
    rhs_token.value = new_literal

    _log.info(
        "PEER patched %s.%s: %r -> %r",
        sub.table, sub.column, original_raw, new_literal,
    )
    return True


def _patch_python(sql: str, substitutions: List[_EntityMatch]) -> str:
    """Patch SQL using sqlparse token-level replacement (no regex).

    Properties:
    - **Comment-safe**: comment tokens are never Comparison nodes.
    - **Idempotent**: unescaped inner value is compared to ``sub.corrected``
      so a second run on already-patched SQL is a no-op.
    - **Escaped-quote-safe**: doubled single-quotes (``O''Neal``) are
      correctly unescaped for comparison and re-escaped on write.
    - **Wildcard-preserving**: leading/trailing ``%`` and quote style kept.
    - **Function-wrapped LHS**: ``LOWER(col)``, ``TRIM(p.col)`` supported.
    - **Robust operator detection**: value-based check, not ttype-only.
    - **Robust RHS detection**: searches inside TokenList/Function via flatten.
    - **Table-qualifier guard**: avoids patching the wrong table when the
      same column name appears in multiple tables.
    - **Nested-query-aware**: recursive walk covers CTEs and subqueries.
    """
    if not substitutions:
        return sql

    statements = sqlparse.parse(sql)
    if not statements:
        return sql

    for statement in statements:
        comparisons: list[Comparison] = []
        _collect_comparisons(statement, comparisons)
        for sub in substitutions:
            for comp in comparisons:
                _patch_comparison(comp, sub)

    return "".join(str(s) for s in statements)




def _patch_llm(sql: str, substitutions: list[_EntityMatch]) -> str:
    """Patch SQL by sending explicit substitution instructions to the LLM."""
    sub_dicts = [
        {"original": s.value, "corrected": s.corrected, "column": s.column, "table": s.table}
        for s in substitutions
    ]
    try:
        response = _client.chat.completions.create(
            model=OPENAI_SQL_MODEL,
            messages=[
                ChatCompletionSystemMessageParam(
                    role="system", content=PEER_LLM_PATCH_SYSTEM_PROMPT
                ),
                ChatCompletionUserMessageParam(
                    role="user",
                    content=peer_llm_patch_user_prompt(sql, sub_dicts),
                ),
            ],
            temperature=0.0,
            max_tokens=1024,
        )
        raw = (response.choices[0].message.content or "").strip()
        clean = re.sub(r"^```(?:sql)?\s*", "", raw, flags=re.IGNORECASE)
        clean = re.sub(r"\s*```$", "", clean).strip()
        return clean if clean else sql
    except openai.OpenAIError as exc:
        _log.warning("PEER LLM patch failed: %s", exc)
        return sql


def run_peer(
    sql: str,
    conn: psycopg2.extensions.connection,
) -> PEERResult:
    """Execute the full PEER pipeline against the generated SQL.

    Extracts entity filters, probes the DB for candidates, fuzzy-matches,
    and patches the SQL. Non-fatal: any exception returns the original SQL.

    Parameters
    ----------
    sql : str
        Generated SQL to validate and patch.
    conn : psycopg2.extensions.connection
        Open Postgres connection used for probe queries.

    Returns
    -------
    PEERResult
    """
    if not PEER_ENABLED:
        return PEERResult(sql=sql)

    try:
        return _run_peer_internal(sql, conn)
    except Exception as exc:  # noqa: BLE001
        _log.error("PEER encountered an unexpected error: %s", exc)
        return PEERResult(sql=sql, error=str(exc))


def _run_peer_internal(
    sql: str,
    conn: psycopg2.extensions.connection,
) -> PEERResult:
    print("[peer] Running Pre-Execution Entity Resolution...")

    entities = _extract_entities(sql)
    if not entities:
        print("[peer] No named-entity filters found — passing SQL through unchanged.")
        return PEERResult(sql=sql)

    for e in entities:
        e.value = _strip_sql_wildcards(e.value)

    print(f"[peer] Extracted {len(entities)} entity filter(s):")
    for e in entities:
        print(f"       • {e.table}.{e.column} = '{e.value}'")

    db_schema: str = nba_db_config.get("schema", "")

    messages: list[str] = []
    unvalidatable: list[str] = []
    to_substitute: list[_EntityMatch] = []

    for entity in entities:
        candidates = _probe_candidates(conn, entity.table, entity.column, entity.value, db_schema)
        if not candidates:
            _log.debug(
                "PEER: probe returned no candidates for '%s' in %s.%s",
                entity.value, entity.table, entity.column,
            )
            unvalidatable.append(
                f"{entity.table}.{entity.column}='{entity.value}' (no candidates)"
            )
            continue

        if len(candidates) == 1:
            entity.corrected = candidates[0]
            entity.score = 100
            entity.action = "exact"
            if entity.corrected.lower() != entity.value.lower():
                to_substitute.append(entity)
                messages.append(
                    f"Single candidate: '{entity.value}' replaced with '{entity.corrected}'."
                )
            print(
                f"[peer]   '{entity.value}' → '{entity.corrected}' "
                f"(single candidate, action={entity.action})"
            )
        else:
            best, score = _fuzzy_match(entity.value, candidates)
            action = _determine_action(score)
            entity.corrected = best
            entity.score = score
            entity.action = action

            print(f"[peer]   '{entity.value}' → '{best}' (score={score}, action={action})")

            if action == "auto_sub":
                to_substitute.append(entity)
                messages.append(
                    f"Note: '{entity.value}' was interpreted as '{best}' "
                    f"(auto-corrected, similarity {score}%)."
                )
            elif action == "flag_sub":
                to_substitute.append(entity)
                messages.append(
                    f"Assumed '{entity.value}' refers to '{best}' "
                    f"(similarity {score}%). Please verify the result."
                )
            elif action == "no_match":
                messages.append(
                    f"Could not find '{entity.value}' in {entity.table}.{entity.column} "
                    f"(best match: '{best}', similarity {score}%). "
                    "The query may return no results."
                )

    if not to_substitute:
        print("[peer] No substitutions required — SQL is unchanged.")
        return PEERResult(sql=sql, messages=messages, unvalidatable=unvalidatable)

    print(f"[peer] Patching SQL using method='{PEER_PATCH_METHOD}'...")
    patched_sql = (
        _patch_llm(sql, to_substitute)
        if PEER_PATCH_METHOD == "llm"
        else _patch_python(sql, to_substitute)
    )
    print("[peer] SQL patched successfully.")

    return PEERResult(
        sql=patched_sql,
        patched=True,
        messages=messages,
        unvalidatable=unvalidatable,
    )
