"""kb_system/peer.py
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
from rapidfuzz import fuzz, utils as fuzz_utils

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
    PEER_TRGM_SIMILARITY_THRESHOLD,
    PEER_USE_TRIGRAM,
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


def _extract_cte_names(sql: str) -> set[str]:
    """Return the set of CTE names defined in the WITH clause of *sql* (lower-cased).
    """
    return {m.lower() for m in re.findall(r"\b(\w+)\s+AS\s*\(", sql, re.IGNORECASE)}


def _extract_entities(sql: str) -> list[_EntityMatch]:
    """Call the LLM to extract named-entity filter values from the SQL.
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
    """
    cleaned = value.strip()
    cleaned = cleaned.strip("'\"")
    cleaned = cleaned.strip("%")
    return cleaned.strip()


def _build_word_prefixes(value: str) -> list[str]:
    """Extract PEER_PROBE_PREFIX_LEN-char prefixes from every word in value.
    """
    seen: set[str] = set()
    result: list[str] = []
    for word in value.split():
        prefix = word[:PEER_PROBE_PREFIX_LEN].lower()
        if prefix and prefix not in seen:
            seen.add(prefix)
            result.append(prefix)
    return result


_trgm_cache: dict[int, bool] = {}


def _trgm_available(conn: psycopg2.extensions.connection) -> bool:
    """Return True if pg_trgm similarity() is callable on this connection.
    """
    if not PEER_USE_TRIGRAM:
        return False
    conn_id = id(conn)
    if conn_id in _trgm_cache:
        return _trgm_cache[conn_id]
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT similarity('a', 'a');")
            cur.fetchone()
        _trgm_cache[conn_id] = True
        _log.debug("PEER: pg_trgm is available on this connection.")
    except psycopg2.Error:
        conn.rollback()
        _trgm_cache[conn_id] = False
        _log.debug("PEER: pg_trgm not available — will use ILIKE fallback.")
    return _trgm_cache[conn_id]

def _probe_trigram(
    conn: psycopg2.extensions.connection,
    qualified_table: str,
    column: str,
    value: str,
) -> list[str]:
    """Resolve candidates using the three-step trigram pipeline.

    Step A — exact match  (cheapest, returns immediately if found)
    Step B — prefix match (ILIKE 'value%', catches clean prefix hits)
    Step C — trigram similarity (pg_trgm % operator + similarity() scoring)

    Each step returns immediately when it finds results, so the expensive
    trigram scan is only reached when exact and prefix both miss.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT {column} FROM {qualified_table} WHERE {column} = %s LIMIT 1;",
                (value,),
            )
            row = cur.fetchone()
        if row:
            _log.debug("PEER trigram Step A (exact) hit for %r in %s.%s", value, qualified_table, column)
            return [str(row[0])]
    except psycopg2.Error as exc:
        _log.warning("PEER trigram Step A failed (%s.%s): %s", qualified_table, column, exc)
        conn.rollback()

    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT DISTINCT {column} FROM {qualified_table} "
                f"WHERE {column} ILIKE %s LIMIT 5;",
                (f"{value}%",),
            )
            rows = cur.fetchall()
        if rows:
            _log.debug("PEER trigram Step B (prefix) hit for %r in %s.%s", value, qualified_table, column)
            return [str(r[0]) for r in rows if r[0] is not None]
    except psycopg2.Error as exc:
        _log.warning("PEER trigram Step B failed (%s.%s): %s", qualified_table, column, exc)
        conn.rollback()

    try:
        with conn.cursor() as cur:
            cur.execute(f"SET pg_trgm.similarity_threshold = {PEER_TRGM_SIMILARITY_THRESHOLD};")
            cur.execute(
                f"SELECT DISTINCT {column}, similarity({column}, %s) AS score "
                f"FROM {qualified_table} "
                f"WHERE {column} %% %s "
                f"ORDER BY score DESC "
                f"LIMIT %s;",
                (value, value, PEER_PROBE_ROW_LIMIT),
            )
            rows = cur.fetchall()
        if rows:
            _log.debug(
                "PEER trigram Step C hit for %r in %s.%s: %d candidate(s)",
                value, qualified_table, column, len(rows),
            )
        return [str(r[0]) for r in rows if r[0] is not None]
    except psycopg2.Error as exc:
        _log.warning("PEER trigram Step C failed (%s.%s): %s", qualified_table, column, exc)
        conn.rollback()
        return []


def _probe_ilike_fallback(
    conn: psycopg2.extensions.connection,
    qualified_table: str,
    column: str,
    value: str,
) -> list[str]:
    """Resolve candidates using the multi-prefix ILIKE strategy.

    Used when pg_trgm is not available. Applies three layers in order:
    1. Multi-word AND intersection (most selective)
    2. First-word anchored prefix only (when AND returns nothing)
    3. All-word contains patterns (when the entity word is not the first word
       in the stored value, e.g. "Timberwolves" vs "Minnesota Timberwolves")
    """
    prefixes = _build_word_prefixes(value)

    def _run(where_clause: str, params: tuple) -> list[str]:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT DISTINCT {column} FROM {qualified_table} "
                    f"WHERE {where_clause} LIMIT %s;",
                    (*params, PEER_PROBE_ROW_LIMIT),
                )
                rows = cur.fetchall()
            return [str(r[0]) for r in rows if r[0] is not None]
        except psycopg2.Error as exc:
            _log.warning("PEER ILIKE probe failed (%s.%s): %s", qualified_table, column, exc)
            conn.rollback()
            return []

    if not prefixes:
        return _run("TRUE", ())

    conditions = " AND ".join(
        [f"{column} ILIKE %s"] + [f"{column} ILIKE %s" for _ in prefixes[1:]]
    )
    params_list = [f"{prefixes[0]}%"] + [f"%{p}%" for p in prefixes[1:]]
    candidates = _run(conditions, tuple(params_list))

    _log.debug(
        "PEER ILIKE layer-1 %s.%s prefixes=%s → %d candidate(s)",
        qualified_table, column, prefixes, len(candidates),
    )

    if not candidates and len(prefixes) > 1:
        candidates = _run(f"{column} ILIKE %s", (f"{prefixes[0]}%",))
        _log.debug(
            "PEER ILIKE layer-2 fallback %s.%s → %d candidate(s)",
            qualified_table, column, len(candidates),
        )

    if not candidates:
        contains_conditions = " AND ".join(f"{column} ILIKE %s" for _ in prefixes)
        contains_params = tuple(f"%{p}%" for p in prefixes)
        candidates = _run(contains_conditions, contains_params)
        _log.debug(
            "PEER ILIKE layer-3 contains fallback %s.%s → %d candidate(s)",
            qualified_table, column, len(candidates),
        )

    return candidates


def _probe_candidates(
    conn: psycopg2.extensions.connection,
    table: str,
    column: str,
    value: str,
    db_schema: str = "",
) -> list[str]:
    """Return candidate column values for *value* using the best available strategy.
    """
    clean_value = _strip_sql_wildcards(value)
    if not clean_value:
        return []

    qualified_table = f"{db_schema}.{table}" if db_schema else table

    if _trgm_available(conn):
        _log.debug("PEER: using trigram resolver for %s.%s", qualified_table, column)
        return _probe_trigram(conn, qualified_table, column, clean_value)

    _log.debug("PEER: using ILIKE fallback for %s.%s", qualified_table, column)
    return _probe_ilike_fallback(conn, qualified_table, column, clean_value)


def _fuzzy_match(value: str, candidates: list[str]) -> tuple[str, int]:
    """Return the best candidate and its score using both token scorers.
    """
    best_candidate = ""
    best_score = 0
    val_proc = fuzz_utils.default_process(value)
    for candidate in candidates:
        cand_proc = fuzz_utils.default_process(candidate)
        score = max(
            fuzz.token_sort_ratio(val_proc, cand_proc, processor=None),
            fuzz.token_set_ratio(val_proc, cand_proc, processor=None),
        )
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
    """
    inner = raw.strip("'\"")
    return inner.replace("''", "'")


def _sql_escape(value: str) -> str:
    """Escape a plain Python string for safe embedding inside a SQL single-quoted literal.
    """
    return value.replace("'", "''")


def _get_column_name(left_token: sqlparse.sql.Token) -> str:
    """Extract the bare column name from a Comparison left-hand side token.
    """
    if isinstance(left_token, Identifier):
        name = left_token.get_name()
        return name.lower() if name else ""

    if left_token.ttype == T.Name:
        return left_token.normalized.lower()

    if isinstance(left_token, Function):
        for tok in left_token.tokens:
            if isinstance(tok, Parenthesis):
                for inner in tok.tokens:
                    if isinstance(inner, Identifier):
                        name = inner.get_name()
                        return name.lower() if name else ""
                    if inner.ttype == T.Name:
                        return inner.normalized.lower()

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
    """
    found_op = False
    for tok in comparison.tokens:
        if tok.ttype in (T.Text.Whitespace, T.Text.Whitespace.Newline, T.Newline):
            continue
        if not found_op:
            if tok.value.strip().lower() in _COMPARISON_OPERATORS:
                found_op = True
            continue
        if tok.ttype == T.Literal.String.Single:
            return tok
        if getattr(tok, "tokens", None):
            for flat in tok.flatten():
                if flat.ttype == T.Literal.String.Single:
                    return flat
        if tok.ttype is not None:
            _log.debug(
                "PEER: RHS of comparison is not a string literal (ttype=%s, val=%r) — skipping.",
                tok.ttype, tok.value[:40],
            )
            return None
    return None


def _build_new_literal(original_raw: str, corrected_escaped: str) -> str:
    """Reconstruct a SQL string literal preserving quote style and ``%`` wildcards.
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
    """
    if isinstance(token, Comparison):
        out.append(token)
    if token.is_group:
        for sub in token.tokens:
            _collect_comparisons(sub, out)


def _patch_comparison(comparison: Comparison, sub: _EntityMatch) -> bool:
    """Attempt to patch a single Comparison token in-place.
    """
    left = comparison.left

    col_name = _get_column_name(left)
    if col_name != sub.column.lower():
        return False

    qualifier = _get_qualifier(left)
    if sub.table and qualifier:
        tbl_is_alias = len(sub.table) <= 4 and "_" not in sub.table
        qual_is_alias = len(qualifier) <= 4 and "_" not in qualifier
        if tbl_is_alias and qual_is_alias and qualifier != sub.table.lower():
            return False

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
    inner_unescaped = _sql_unescape(original_raw).strip("%")
    if inner_unescaped.lower() != sub.value.lower():
        return False

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
    print("\n[peer] Running Pre-Execution Entity Resolution...")

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
    cte_names: set[str] = _extract_cte_names(sql)

    messages: list[str] = []
    unvalidatable: list[str] = []
    to_substitute: list[_EntityMatch] = []

    for entity in entities:
        if entity.table.lower() in cte_names:
            _log.debug(
                "PEER: '%s' is a CTE name — skipping probe for %s.%s",
                entity.table, entity.table, entity.column,
            )
            print(
                f"[peer]   '{entity.table}.{entity.column}' — CTE alias, "
                "cannot probe DB directly; value passes through unchanged."
            )
            continue

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

            if action == "exact":
                if best.lower() != entity.value.lower():
                    to_substitute.append(entity)
                    messages.append(
                        f"Single candidate: '{entity.value}' replaced with '{best}'."
                    )
            elif action == "auto_sub":
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
