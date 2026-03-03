"""kb_system/peer.py

Pre-Execution Entity Resolution (PEER) layer.

After SQL is generated, PEER validates named-entity filter values against
the actual database using fuzzy matching and patches the SQL if needed.
PEER is non-fatal: any failure returns the original SQL unchanged.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field

import openai
import psycopg2
import psycopg2.extras
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

    Handles values like ``'%LeBron James%'``, ``"%regular%"``, ``LeBron James``.
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


def _patch_python(sql: str, substitutions: list[_EntityMatch]) -> str:
    """Patch SQL using regex substitution (Python method).

    Handles both bare values (``= 'LeBron James'``) and ILIKE wildcard
    patterns (``ILIKE '%LeBron James%'``) by treating surrounding quotes
    and percent signs as optional captured groups that are preserved.
    """
    patched = sql
    for sub in substitutions:
        original_escaped = re.escape(sub.value)
        # Quote may precede or follow the percent wildcard, so capture both
        # orderings: plain quote, quote+percent, or percent alone.
        pattern = rf"(['\"]?%?){original_escaped}(%?['\"]?)"
        replacement = rf"\g<1>{sub.corrected}\g<2>"
        patched = re.sub(pattern, replacement, patched, flags=re.IGNORECASE)
    return patched


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

    # Clean the raw extracted values (strip ILIKE wildcards / quotes) so that
    # probe prefix and fuzzy matching work on the actual literal text.
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
            unvalidatable.append(f"{entity.table}.{entity.column}='{entity.value}' (no candidates)")
            continue

        if len(candidates) == 1:
            # Only one candidate, no fuzzy matching needed
            entity.corrected = candidates[0]
            entity.score = 100
            entity.action = "exact"
            print(f"[peer]   '{entity.value}' → '{candidates[0]}' (only candidate, auto-exact)")
        else:
            best, score = _fuzzy_match(entity.value, candidates)
            action = _determine_action(score)
            entity.corrected = best
            entity.score = score
            entity.action = action
            print(f"[peer]   '{entity.value}' → '{best}' (score={score}, action={action})")

            if action in ("auto_sub", "flag_sub"):
                to_substitute.append(entity)
                messages.append(
                    f"'{entity.value}' interpreted as '{best}' (similarity {score}%)."
                    if action == "auto_sub" else
                    f"Assumed '{entity.value}' refers to '{best}' (similarity {score}%). Please verify."
                )

        if entity.action == "no_match":
            messages.append(
                f"Could not find '{entity.value}' in {entity.table}.{entity.column} "
                f"(best match: '{entity.corrected}', similarity {entity.score}%)."
            )

    if not to_substitute:
        print("[peer] No substitutions required — SQL is unchanged.")
        return PEERResult(sql=sql, messages=messages, unvalidatable=unvalidatable)

    print(f"[peer] Patching SQL using method='{PEER_PATCH_METHOD}'...")
    patched_sql = _patch_llm(sql, to_substitute) if PEER_PATCH_METHOD == "llm" else _patch_python(sql, to_substitute)
    print("[peer] SQL patched successfully.")

    return PEERResult(
        sql=patched_sql,
        patched=True,
        messages=messages,
        unvalidatable=unvalidatable,
    )
