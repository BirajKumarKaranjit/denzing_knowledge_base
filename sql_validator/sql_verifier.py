"""sql_validator/sql_verifier.py

Structural SQL verification using sqlglot AST analysis.

Validates generated SQL against a DDL column registry before execution.
No LLM calls. No semantic matching. Exact structural checks only.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import sqlglot
import sqlglot.expressions as exp

_log = logging.getLogger(__name__)

# SQL aggregate function names — used for GROUP BY completeness check.
_AGGREGATE_FUNCS: frozenset[str] = frozenset(
    {"sum", "avg", "count", "max", "min", "stddev", "variance",
     "array_agg", "string_agg", "json_agg", "jsonb_agg", "listagg"}
)

# SQL window/scalar functions that should never be flagged as column references.
_BUILTIN_FUNCS: frozenset[str] = frozenset(
    {"now", "current_date", "current_timestamp", "current_time",
     "extract", "date_trunc", "date_part", "coalesce", "nullif",
     "cast", "convert", "lower", "upper", "trim", "length",
     "substr", "substring", "replace", "concat", "row_number",
     "rank", "dense_rank", "lag", "lead", "ntile", "percent_rank",
     "cume_dist", "first_value", "last_value", "generate_series",
     "unnest", "greatest", "least", "abs", "ceil", "floor", "round",
     "to_char", "to_date", "to_timestamp", "regexp_replace",
     "regexp_match", "regexp_matches", "split_part", "string_to_array"}
)


@dataclass
class VerificationError:
    """A fatal validation error that should block SQL execution (or trigger a retry)."""

    error_type: str
    """One of: column_not_in_ddl | wrong_table_for_column |
    union_column_mismatch | order_by_in_union_branch"""
    message: str
    column: str | None = None
    table: str | None = None
    suggestion: str | None = None


@dataclass
class VerificationResult:
    """Outcome of a single verify_sql() call."""

    is_valid: bool
    errors: list[VerificationError] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def verify_sql(sql: str, registry: dict[str, list[str]]) -> VerificationResult:
    """Validate *sql* against *registry* using sqlglot AST analysis.

    Parameters
    ----------
    sql:
        The generated SQL string to verify.
    registry:
        ``{table_name: [col1, col2, ...]}`` built by ``build_column_registry()``.
        All names must be lowercase.

    Returns
    -------
    VerificationResult
        ``is_valid=False`` when at least one hard error is found.
        ``warnings`` are non-fatal and do not block execution.
    """
    if not sql or not sql.strip():
        return VerificationResult(is_valid=True)

    errors: list[VerificationError] = []
    warnings: list[str] = []

    try:
        statements = sqlglot.parse(sql, error_level=sqlglot.ErrorLevel.IGNORE)
    except Exception as exc:  # noqa: BLE001
        _log.debug("sqlglot parse error (non-fatal): %s", exc)
        return VerificationResult(is_valid=True, warnings=[f"Parse warning: {exc}"])

    for statement in statements:
        if statement is None:
            continue

        # --- Step 1: build CTE output set for this statement ---
        cte_columns = _extract_cte_output_columns(statement)

        # --- Step 2: alias → table map ---
        alias_map = _build_alias_map(statement)

        # --- Step 3: column validation ---
        col_errors, col_warnings = _validate_columns(
            statement, registry, alias_map, cte_columns
        )
        errors.extend(col_errors)
        warnings.extend(col_warnings)

        # --- Priority 2: structural compliance ---
        errors.extend(_check_union_column_parity(statement))
        errors.extend(_check_order_by_in_union_branch(statement))
        warnings.extend(_check_group_by_completeness(statement))

    is_valid = len(errors) == 0
    return VerificationResult(is_valid=is_valid, errors=errors, warnings=warnings)


# ---------------------------------------------------------------------------
# CTE output column extraction
# ---------------------------------------------------------------------------

def _extract_cte_output_columns(statement: exp.Expression) -> set[str]:
    """Return all column aliases exported by every CTE in *statement*."""
    cte_columns: set[str] = set()

    for cte in statement.find_all(exp.CTE):
        # The CTE query is the second arg (alias is first)
        cte_query = cte.this
        if not isinstance(cte_query, exp.Select):
            continue
        for sel_expr in cte_query.expressions:
            alias = _get_alias(sel_expr)
            if alias:
                cte_columns.add(alias.lower())
            else:
                # No alias — try to get the bare column name
                col_name = _bare_column_name(sel_expr)
                if col_name:
                    cte_columns.add(col_name.lower())

    return cte_columns


def _get_alias(expr: exp.Expression) -> str | None:
    """Return the alias string of an expression if present."""
    if isinstance(expr, exp.Alias):
        return str(expr.alias)
    return None


def _bare_column_name(expr: exp.Expression) -> str | None:
    """Return the column name from a bare column reference (no alias)."""
    if isinstance(expr, exp.Column):
        return expr.name
    return None


# ---------------------------------------------------------------------------
# Alias → table map
# ---------------------------------------------------------------------------

def _build_alias_map(statement: exp.Expression) -> dict[str, str]:
    """Map all table aliases (and bare table names) to their table names.

    Returns
    -------
    dict[str, str]
        ``{alias_or_table: canonical_table_name}`` — all lowercase.
    """
    alias_map: dict[str, str] = {}

    for table in statement.find_all(exp.Table):
        table_name = (table.name or "").lower()
        alias = (table.alias or "").lower()
        if not table_name:
            continue
        # The table name itself is always a valid reference
        alias_map[table_name] = table_name
        if alias and alias != table_name:
            alias_map[alias] = table_name

    return alias_map


# ---------------------------------------------------------------------------
# Column validation
# ---------------------------------------------------------------------------

def _validate_columns(
    statement: exp.Expression,
    registry: dict[str, list[str]],
    alias_map: dict[str, str],
    cte_columns: set[str],
) -> tuple[list[VerificationError], list[str]]:
    """Walk all column references in *statement* and validate against *registry*."""
    errors: list[VerificationError] = []
    warnings: list[str] = []

    # Pre-compute a reverse map: column_name → set of tables that own it
    col_to_tables: dict[str, set[str]] = {}
    for tbl, cols in registry.items():
        for c in cols:
            col_to_tables.setdefault(c.lower(), set()).add(tbl)

    # Collect all SELECT-level aliases so we don't flag them in ORDER BY / HAVING
    select_aliases = _collect_select_aliases(statement)

    for col_ref in statement.find_all(exp.Column):
        col_name = (col_ref.name or "").lower()
        qualifier = _resolve_qualifier(col_ref, alias_map)

        if not col_name or col_name == "*":
            continue
        if col_name in _BUILTIN_FUNCS:
            continue
        # Skip aliases defined within the query itself
        if col_name in select_aliases or col_name in cte_columns:
            continue

        if qualifier:
            # Qualified reference: alias.column or table.column
            _check_qualified_column(
                col_name, qualifier, registry, col_to_tables, errors
            )
        else:
            # Bare column reference — no table qualifier
            _check_bare_column(col_name, col_to_tables, cte_columns, warnings, errors)

    return errors, warnings


def _collect_select_aliases(statement: exp.Expression) -> set[str]:
    """Collect all column aliases defined in SELECT clauses across the statement."""
    aliases: set[str] = set()
    for sel in statement.find_all(exp.Select):
        for expr in sel.expressions:
            if isinstance(expr, exp.Alias):
                aliases.add(str(expr.alias).lower())
    return aliases


def _resolve_qualifier(col_ref: exp.Column, alias_map: dict[str, str]) -> str | None:
    """Resolve the table qualifier of a column reference to a canonical table name."""
    table_part = col_ref.table
    if not table_part:
        return None
    table_str = str(table_part).lower()
    # Try alias map first; fall back to the raw qualifier (may be a CTE name)
    return alias_map.get(table_str, table_str)


def _check_qualified_column(
    col_name: str,
    table_name: str,
    registry: dict[str, list[str]],
    col_to_tables: dict[str, set[str]],
    errors: list[VerificationError],
) -> None:
    """Validate a qualified column reference (table.column)."""
    if table_name not in registry:
        # Table not in registry — could be a CTE name or external table; skip silently
        return

    table_cols = [c.lower() for c in registry[table_name]]
    if col_name in table_cols:
        return  # All good

    # Column not found on this table — check if it lives elsewhere
    other_tables = col_to_tables.get(col_name, set()) - {table_name}
    if other_tables:
        errors.append(
            VerificationError(
                error_type="wrong_table_for_column",
                message=(
                    f"Column '{col_name}' does not exist on '{table_name}'. "
                    f"It exists on: {sorted(other_tables)}. "
                    f"Check your JOIN and alias."
                ),
                column=col_name,
                table=table_name,
                suggestion=f"Use the correct table alias for '{col_name}'.",
            )
        )
    else:
        errors.append(
            VerificationError(
                error_type="column_not_in_ddl",
                message=(
                    f"Column '{col_name}' does not exist on '{table_name}' "
                    f"or any other table in the schema."
                ),
                column=col_name,
                table=table_name,
            )
        )


def _check_bare_column(
    col_name: str,
    col_to_tables: dict[str, set[str]],
    cte_columns: set[str],
    warnings: list[str],
    errors: list[VerificationError],
) -> None:
    """Validate a bare (unqualified) column reference."""
    if col_name in cte_columns:
        return  # CTE-derived, always valid

    owning_tables = col_to_tables.get(col_name, set())
    if owning_tables:
        if len(owning_tables) > 1:
            warnings.append(
                f"Bare column '{col_name}' is ambiguous — exists on: "
                f"{sorted(owning_tables)}. Use a table alias to be explicit."
            )
        # Single-table ownership → valid, no warning needed
    else:
        # Column found nowhere — hard error only if it's clearly invented
        errors.append(
            VerificationError(
                error_type="column_not_in_ddl",
                message=(
                    f"Column '{col_name}' does not exist in any table in the schema."
                ),
                column=col_name,
            )
        )


# ---------------------------------------------------------------------------
# Priority 2 — Structural compliance checks
# ---------------------------------------------------------------------------

def _check_union_column_parity(statement: exp.Expression) -> list[VerificationError]:
    """Verify all UNION / UNION ALL branches return the same number of columns."""
    errors: list[VerificationError] = []

    for union in statement.find_all(exp.Union):
        # Collect all leaf SELECT expressions in the union tree
        branches = _flatten_union_branches(union)
        if len(branches) < 2:
            continue

        counts = [len(b.expressions) for b in branches]
        if len(set(counts)) > 1:
            errors.append(
                VerificationError(
                    error_type="union_column_mismatch",
                    message=(
                        f"UNION ALL branches have different column counts: "
                        f"{counts}. All branches must return the same number "
                        f"of columns with compatible types."
                    ),
                )
            )

    return errors


def _flatten_union_branches(union: exp.Union) -> list[exp.Select]:
    """Recursively collect all SELECT leaves of a UNION tree."""
    branches: list[exp.Select] = []
    for side in (union.left, union.right):
        if isinstance(side, exp.Union):
            branches.extend(_flatten_union_branches(side))
        elif isinstance(side, exp.Select):
            branches.append(side)
    return branches


def _check_order_by_in_union_branch(statement: exp.Expression) -> list[VerificationError]:
    """Detect ORDER BY or LIMIT placed directly inside a UNION branch (not in a subquery)."""
    errors: list[VerificationError] = []

    for union in statement.find_all(exp.Union):
        for side in (union.left, union.right):
            if not isinstance(side, exp.Select):
                continue
            # Only flag ORDER BY / LIMIT that are *direct* children of the SELECT,
            # not ones nested inside a Subquery expression within the branch.
            for child in side.args.values():
                if child is None:
                    continue
                children = child if isinstance(child, list) else [child]
                for node in children:
                    if isinstance(node, (exp.Order, exp.Limit)):
                        errors.append(
                            VerificationError(
                                error_type="order_by_in_union_branch",
                                message=(
                                    "ORDER BY or LIMIT cannot appear directly inside a "
                                    "UNION ALL branch. Wrap the branch in a subquery:\n"
                                    "  SELECT ... FROM (SELECT ... ORDER BY col LIMIT n) sub\n"
                                    "  UNION ALL\n"
                                    "  SELECT ...;"
                                ),
                            )
                        )

    return errors


def _check_group_by_completeness(statement: exp.Expression) -> list[str]:
    """Warn when a SELECT with aggregates has non-aggregated columns not in GROUP BY."""
    warnings: list[str] = []

    for select in statement.find_all(exp.Select):
        if not _has_aggregates(select):
            continue

        group_by = select.find(exp.Group)
        grouped_cols: set[str] = set()
        if group_by:
            for g_expr in group_by.expressions:
                if isinstance(g_expr, exp.Column):
                    grouped_cols.add(g_expr.name.lower())
                elif isinstance(g_expr, exp.Literal):
                    # positional GROUP BY (e.g., GROUP BY 1, 2) — skip
                    pass

        for sel_expr in select.expressions:
            if _is_aggregate(sel_expr):
                continue
            # Strip the alias wrapper to get the inner expression
            inner = sel_expr.this if isinstance(sel_expr, exp.Alias) else sel_expr
            if isinstance(inner, exp.Column):
                col_name = inner.name.lower()
                if grouped_cols and col_name not in grouped_cols:
                    warnings.append(
                        f"GROUP BY may be incomplete: non-aggregated column "
                        f"'{col_name}' is not in GROUP BY."
                    )

    return warnings


def _has_aggregates(select: exp.Select) -> bool:
    """Return True if the SELECT clause contains any aggregate function."""
    for expr in select.expressions:
        if _is_aggregate(expr):
            return True
    return False


def _is_aggregate(expr: exp.Expression) -> bool:
    """Return True if *expr* is or wraps an aggregate function call."""
    for func in expr.find_all(exp.AggFunc):
        return True  # any AggFunc subclass means this expression is an aggregate
    for func in expr.find_all(exp.Anonymous):
        fn_name = (func.name or "").lower()
        if fn_name in _AGGREGATE_FUNCS:
            return True
    # Check concrete aggregate types present in this sqlglot version.
    _concrete_agg_types = [exp.Sum, exp.Avg, exp.Count, exp.Max, exp.Min]
    for _agg_name in ("StddevSamp", "StddevPop", "Stddev", "Variance", "VariancePop"):
        _cls = getattr(exp, _agg_name, None)
        if _cls is not None:
            _concrete_agg_types.append(_cls)
    for agg_type in _concrete_agg_types:
        if expr.find(agg_type):
            return True
    return False

