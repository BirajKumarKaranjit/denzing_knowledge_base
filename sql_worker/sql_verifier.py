"""sql_worker/sql_verifier.py

Hybrid SQL verifier.

This module performs registry-aware semantic checks using ``sqlglot`` (column
existence, alias resolution, scope and GROUP BY checks) and delegates structural
and style rules to external tools via adapters:

- ``sqlfluff`` (style and structural lints)
- ``sqllineage`` (CTE/table lineage and unused-CTE detection)

Keep DDL-aware checks (column validation, filter/scope projection, having alias
rules, union parity) implemented via sqlglot to ensure correctness against the
project's JSONB-based DDL registry; offload generic style/pattern checks to the
adapters to reduce custom code and maintenance burden.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import sqlglot
import sqlglot.expressions as exp

_log = logging.getLogger(__name__)

_AGGREGATE_FUNCS: frozenset[str] = frozenset(
    {"sum", "avg", "count", "max", "min", "stddev", "variance",
     "array_agg", "string_agg", "json_agg", "jsonb_agg", "listagg"}
)

_LITERAL_COMPARISON_TYPES = (
    exp.EQ,
    exp.NEQ,
    exp.ILike,
    exp.Like,
    exp.In,
    exp.GT,
    exp.GTE,
    exp.LT,
    exp.LTE,
)


@dataclass
class VerificationError:
    """A fatal validation error that should block SQL execution (or trigger a retry)."""

    error_type: str
    """One of: column_not_in_ddl | wrong_table_for_column |
    union_column_mismatch | order_by_in_union_branch | scope_filter_not_projected |
    filter_context_not_projected | having_alias_reference |
    window_function_in_where | limit_inside_cte | self_join_without_alias |
    missing_nullif_in_division"""
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

def verify_sql(
    sql: str,
    registry: dict[str, list[str]],
    dialect: str = "",
) -> VerificationResult:
    """Validate *sql* against *registry*.

    Implementation notes
    - Use ``sqlglot`` for registry-aware semantic checks (columns/tables/aliases,
      GROUP BY completeness, HAVING alias usage, UNION parity).
    - Call adapters (``sqlfluff`` and ``sqllineage``) for style and lineage
      checks; adapter results are merged into the returned ``VerificationResult``.

    Parameters
    ----------
    sql:
    registry:
        ``{table_name: [col1, col2, ...]}`` built by ``build_column_registry()``.
        All names must be lowercase.
    dialect:
        SQL dialect name used for sqlglot parsing (for example: postgresql,
        snowflake, bigquery).

    Returns
    -------
    VerificationResult
        ``is_valid=False`` when at least one hard error is found. ``warnings``
        are non-fatal and do not block execution.
    """
    if not sql or not sql.strip():
        return VerificationResult(is_valid=True)

    errors: list[VerificationError] = []
    warnings: list[str] = []

    read_dialect = _normalize_sqlglot_dialect(dialect=dialect)

    try:
        statements = sqlglot.parse(
            sql,
            read=read_dialect,
            error_level=sqlglot.ErrorLevel.IGNORE,
        )
    except Exception as exc:  # noqa: BLE001
        _log.debug("sqlglot parse error (non-fatal): %s", exc)
        return VerificationResult(is_valid=True, warnings=[f"Parse warning: {exc}"])

    for statement in statements:
        if statement is None:
            continue

        cte_columns = _extract_cte_output_columns(statement)
        alias_map = _build_alias_map(statement)
        col_errors, col_warnings = _validate_columns(
            statement, registry, alias_map, cte_columns
        )
        errors.extend(col_errors)
        warnings.extend(col_warnings)

        errors.extend(_check_union_column_parity(statement))
        errors.extend(_check_order_by_in_union_branch(statement))
        errors.extend(_check_limit_inside_cte(statement))
        # New checks
        warnings.extend(_check_order_by_without_limit(statement))
        errors.extend(_check_window_function_in_where(statement))
        errors.extend(_check_self_join_without_alias(statement))
        # cross/join-without-condition detection is delegated to sqlfluff via the adapter
        warnings.extend(_check_bare_aggregate_in_final_select(statement))
        # SELECT * detection is handled by sqlfluff adapter; removed hand-rolled check.
        warnings.extend(_check_non_sargable_function_on_filter_column(statement))
        errors.extend(_check_duplicate_cte_name(statement))
        warnings.extend(_check_unused_cte(statement))
        errors.extend(_check_missing_nullif_in_division(statement))
        errors.extend(_check_scope_filter_projection(statement))
        errors.extend(_check_filter_context_projection(statement))
        warnings.extend(_check_group_by_completeness(statement))
        errors.extend(_check_having_alias_reference(statement, dialect=read_dialect))

        # Run external adapters (sqlfluff + sqllineage) to catch style/lineage issues.
        try:
            from .sql_rule_adapters import run_sqlfluff_checks, run_sqllineage_checks

            fluff_errors, fluff_warnings = run_sqlfluff_checks(sql, dialect)
            errors.extend(fluff_errors)
            warnings.extend(fluff_warnings)

            lineage_warnings = run_sqllineage_checks(sql)
            warnings.extend(lineage_warnings)
        except Exception as exc:  # pragma: no cover - adapter may error in some envs
            _log.debug("rule adapter error (non-fatal): %s", exc)

    is_valid = len(errors) == 0
    return VerificationResult(is_valid=is_valid, errors=errors, warnings=warnings)


def _extract_cte_output_columns(statement: exp.Expression) -> set[str]:
    """Return all column aliases exported by every CTE in *statement*."""
    cte_columns: set[str] = set()

    for cte in statement.find_all(exp.CTE):
        cte_query = cte.this
        if not isinstance(cte_query, exp.Select):
            continue
        for sel_expr in cte_query.expressions:
            alias = _get_alias(sel_expr)
            if alias:
                cte_columns.add(alias.lower())
            else:
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
        alias_map[table_name] = table_name
        if alias and alias != table_name:
            alias_map[alias] = table_name

    return alias_map


def _validate_columns(
    statement: exp.Expression,
    registry: dict[str, list[str]],
    alias_map: dict[str, str],
    cte_columns: set[str],
) -> tuple[list[VerificationError], list[str]]:
    """Walk all column references in *statement* and validate against *registry*."""
    errors: list[VerificationError] = []
    warnings: list[str] = []

    col_to_tables: dict[str, set[str]] = {}
    for tbl, cols in registry.items():
        for c in cols:
            col_to_tables.setdefault(c.lower(), set()).add(tbl)

    select_aliases = _collect_select_aliases(statement)

    for col_ref in statement.find_all(exp.Column):
        col_name = (col_ref.name or "").lower()
        qualifier = _resolve_qualifier(col_ref, alias_map)

        if not col_name or col_name == "*":
            continue
        if _is_same_select_having_alias_reference(col_ref):
            continue

        if qualifier:
            _check_qualified_column(
                col_name, qualifier, registry, col_to_tables, errors
            )
        else:
            if col_name in cte_columns:
                continue
            if col_name in select_aliases and _is_order_by_reference(col_ref):
                continue
            tables_in_query = set(alias_map.values())
            _check_bare_column(col_name, col_to_tables, cte_columns, tables_in_query, warnings, errors)

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
    return alias_map.get(table_str, table_str)


def _is_order_by_reference(col_ref: exp.Column) -> bool:
    """Return True when the column is referenced inside ORDER BY."""
    return col_ref.find_ancestor(exp.Order) is not None


def _normalize_sqlglot_dialect(dialect: str) -> str:
    """Map configured dialect names to sqlglot parser dialect names."""
    normalized = (dialect or "").strip().lower()
    if not normalized:
        return "postgres"

    mapping = {
        "postgresql": "postgres",
        "postgres": "postgres",
        "snowflake": "snowflake",
        "bigquery": "bigquery",
        "mysql": "mysql",
        "mariadb": "mysql",
        "sqlserver": "tsql",
        "mssql": "tsql",
        "tsql": "tsql",
        "sqlite": "sqlite",
        "oracle": "oracle",
        "redshift": "redshift",
        "duckdb": "duckdb",
        "trino": "trino",
        "presto": "presto",
        "databricks": "databricks",
    }
    return mapping.get(normalized, normalized)


def _check_qualified_column(
    col_name: str,
    table_name: str,
    registry: dict[str, list[str]],
    col_to_tables: dict[str, set[str]],
    errors: list[VerificationError],
) -> None:
    """Validate a qualified column reference (table.column)."""
    if table_name not in registry:
        return

    table_cols = [c.lower() for c in registry[table_name]]
    if col_name in table_cols:
        return
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
    tables_in_query: set[str],
    warnings: list[str],
    errors: list[VerificationError],
) -> None:
    """Validate a bare (unqualified) column reference."""
    if col_name in cte_columns:
        return

    owning_tables = col_to_tables.get(col_name, set())
    if owning_tables:
        tables_in_scope = owning_tables & tables_in_query
        if not tables_in_scope:
            errors.append(
                VerificationError(
                    error_type="column_not_in_scope",
                    message=(
                        f"Column '{col_name}' exists on tables {sorted(owning_tables)} "
                        "but none of those tables are present in this query scope."
                    ),
                    column=col_name,
                    suggestion=(
                        f"Join the table containing '{col_name}' or use the correct alias."
                    ),
                )
            )
            return
        if len(tables_in_scope) > 1:
            errors.append(
                VerificationError(
                    error_type="ambiguous_bare_column",
                    message=(
                        f"Column '{col_name}' is unqualified and exists on multiple "
                        f"joined tables: {sorted(tables_in_scope)}. "
                        f"Prefix it with the correct table alias (e.g. alias.{col_name})."
                    ),
                    column=col_name,
                    suggestion=(
                        f"Add a table alias prefix to '{col_name}' to remove the ambiguity."
                    ),
                )
            )
    else:
        errors.append(
            VerificationError(
                error_type="column_not_in_ddl",
                message=(
                    f"Column '{col_name}' does not exist in any table in the schema."
                ),
                column=col_name,
            )
        )

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
    """Detect ORDER BY or LIMIT placed directly inside a UNION branch (not in a subquery).

    NOTE: This check is intentionally implemented here (hand-rolled) and kept
    permanently. sqlfluff is unable to reliably parse some UNION/UNION ALL
    constructs and may raise parse errors rather than producing a lint that we
    can depend on. Therefore we keep this sqlglot-based check to ensure
    deterministic behavior for ORDER BY / LIMIT inside UNION branches.
    """
    errors: list[VerificationError] = []

    for union in statement.find_all(exp.Union):
        for side in (union.left, union.right):
            if not isinstance(side, exp.Select):
                continue
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
                    pass

        for sel_expr in select.expressions:
            if _is_aggregate(sel_expr):
                continue
            inner = sel_expr.this if isinstance(sel_expr, exp.Alias) else sel_expr
            if isinstance(inner, exp.Column):
                col_name = inner.name.lower()
                if grouped_cols and col_name not in grouped_cols:
                    warnings.append(
                        f"GROUP BY may be incomplete: non-aggregated column "
                        f"'{col_name}' is not in GROUP BY."
                    )

    return warnings


def _check_having_alias_reference(
    statement: exp.Expression,
    dialect: str = "",
) -> list[VerificationError]:
    """Flag same-SELECT HAVING references to SELECT aliases in strict dialects."""
    if (dialect or "").strip().lower() in {"snowflake", "bigquery", "duckdb"}:
        return []

    errors: list[VerificationError] = []

    for select in statement.find_all(exp.Select):
        aliases = {
            str(sel_expr.alias).lower()
            for sel_expr in select.expressions
            if isinstance(sel_expr, exp.Alias)
        }
        if not aliases:
            continue

        having_node = select.args.get("having")
        if not isinstance(having_node, exp.Having):
            continue

        flagged_aliases: set[str] = set()
        for col in having_node.find_all(exp.Column):
            alias_name = (col.name or "").lower()
            if not alias_name or alias_name not in aliases:
                continue
            if not _is_direct_filter_column(col, having_node):
                continue
            flagged_aliases.add(alias_name)

        for alias_name in sorted(flagged_aliases):
            errors.append(
                VerificationError(
                    error_type="having_alias_reference",
                    message=(
                        f"HAVING references SELECT alias '{alias_name}' in the same SELECT. "
                        "The alias may not be resolved at HAVING evaluation time in this dialect."
                    ),
                    column=alias_name,
                    suggestion=(
                        "Replace the alias in HAVING with its full expression, or wrap the SELECT "
                        "in an outer CTE/subquery and filter on the alias there."
                    ),
                )
            )

    return errors


def _is_same_select_having_alias_reference(col_ref: exp.Column) -> bool:
    """Return True when a HAVING column matches a SELECT alias in the same SELECT."""
    having_node = col_ref.find_ancestor(exp.Having)
    if not isinstance(having_node, exp.Having):
        return False

    select_node = having_node.find_ancestor(exp.Select)
    if not isinstance(select_node, exp.Select):
        return False

    alias_name = (col_ref.name or "").lower()
    if not alias_name:
        return False

    aliases = {
        str(sel_expr.alias).lower()
        for sel_expr in select_node.expressions
        if isinstance(sel_expr, exp.Alias)
    }
    if alias_name not in aliases:
        return False

    return _is_direct_filter_column(col_ref, having_node)


def _has_aggregates(select: exp.Select) -> bool:
    """Return True if the SELECT clause contains any aggregate function."""
    for expr in select.expressions:
        if _is_aggregate(expr):
            return True
    return False


def _is_aggregate(expr: exp.Expression) -> bool:
    """Return True if *expr* is or wraps an aggregate function call."""
    for func in expr.find_all(exp.AggFunc):
        return True
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


def _check_scope_filter_projection(statement: exp.Expression) -> list[VerificationError]:
    """Require columns used to restrict query scope to be visible in SELECT output."""
    top_select = _top_level_select(statement)
    if top_select is None:
        return []

    filtered_cols = _collect_scope_filtered_columns(statement)

    cte_nodes = {cte.alias_or_name.lower(): cte for cte in statement.find_all(exp.CTE) if cte.alias_or_name}
    discovered_cte_names = _collect_cte_dependency_names(top_select, cte_nodes, max_hops=2)

    cte_scope_cols: set[str] = set()
    cte_literal_cols: set[str] = set()
    cte_wildcard_like_cols: set[str] = set()
    for cte_name in discovered_cte_names:
        cte_node = cte_nodes.get(cte_name)
        if cte_node is None:
            continue
        cte_scope_cols.update(_collect_cte_scope_filtered_columns(cte_node))
        cte_literal_cols.update(_collect_cte_literal_filter_columns(cte_node))
        cte_wildcard_like_cols.update(_collect_cte_wildcard_like_filter_columns(cte_node))

    filtered_cols |= cte_scope_cols
    if not filtered_cols:
        return []

    join_key_cols = _collect_join_key_columns(statement)
    literal_filter_cols = _collect_literal_filter_columns(statement) | cte_literal_cols
    wildcard_like_cols = _collect_wildcard_like_filter_columns(statement) | cte_wildcard_like_cols
    id_filter_cols = _collect_id_filter_columns(filtered_cols)
    scope_cols = filtered_cols - join_key_cols - literal_filter_cols - wildcard_like_cols - id_filter_cols
    if not scope_cols:
        return []

    projected = _collect_projected_field_names(top_select)
    if "*" in projected:
        return []

    missing = sorted(col for col in scope_cols if col not in projected)
    if not missing:
        return []

    return [
        VerificationError(
            error_type="scope_filter_not_projected",
            message=(
                "Columns used to restrict query scope are not in SELECT output: "
                f"{missing}. Include them so the result is self-descriptive."
            ),
            suggestion="Add the missing scope columns to the SELECT clause.",
        )
    ]


def _check_filter_context_projection(statement: exp.Expression) -> list[VerificationError]:
    """Require literal-filter context columns to be visible in SELECT output."""
    top_select = _top_level_select(statement)
    if top_select is None:
        return []

    top_level_literal_cols = _collect_literal_filter_columns(statement)

    cte_nodes = {cte.alias_or_name.lower(): cte for cte in statement.find_all(exp.CTE) if cte.alias_or_name}
    discovered_cte_names = _collect_cte_dependency_names(top_select, cte_nodes, max_hops=2)

    cte_literal_cols: set[str] = set()
    for cte_name in discovered_cte_names:
        cte_node = cte_nodes.get(cte_name)
        if cte_node is None:
            continue
        cte_literal_cols.update(_collect_cte_literal_filter_columns(cte_node))

    literal_filter_cols = top_level_literal_cols | cte_literal_cols
    if not literal_filter_cols:
        return []

    id_filter_cols = _collect_id_filter_columns(literal_filter_cols)
    context_cols = literal_filter_cols - id_filter_cols
    if not context_cols:
        return []

    projected = _collect_projected_field_names(top_select)
    if "*" in projected:
        return []

    missing = sorted(col for col in context_cols if col not in projected)
    if not missing:
        return []

    return [
        VerificationError(
            error_type="filter_context_not_projected",
            message=(
                "Columns used in literal WHERE/HAVING filters are not present "
                f"in SELECT output: {missing}."
            ),
            suggestion=(
                "Add the missing filtered context columns to SELECT so the result "
                "confirms the applied filter context."
            ),
        )
    ]


def _top_level_select(statement: exp.Expression) -> exp.Select | None:
    """Return the root SELECT node when available."""
    if isinstance(statement, exp.With):
        statement = statement.this

    if isinstance(statement, exp.Union):
        node: exp.Expression = statement
        while isinstance(node, exp.Union):
            node = node.right
        if isinstance(node, exp.Select):
            return node
        return None

    if isinstance(statement, exp.Select):
        return statement

    if isinstance(statement, exp.Subquery):
        inner = statement.this
        if isinstance(inner, exp.With):
            inner = inner.this
        if isinstance(inner, exp.Select):
            return inner

    return None


def _collect_scope_filtered_columns(statement: exp.Expression) -> set[str]:
    """Collect all columns used in the top-level WHERE/HAVING predicates."""
    top_select = _top_level_select(statement)
    if top_select is None:
        return set()

    def _belongs_to_top_select(node: exp.Expression) -> bool:
        owner = node.find_ancestor(exp.Select)
        return owner is top_select

    cols: set[str] = set()
    for filter_node in (top_select.find(exp.Where), top_select.find(exp.Having)):
        if filter_node is None:
            continue
        for col in filter_node.find_all(exp.Column):
            if not _belongs_to_top_select(col):
                continue
            if not _is_direct_filter_column(col, filter_node):
                continue
            col_name = (col.name or "").lower()
            if not col_name:
                continue
            cols.add(col_name)
    return cols


def _collect_scope_filtered_columns_from_select(select: exp.Select) -> set[str]:
    """Collect WHERE/HAVING filter columns that directly belong to one SELECT."""
    def _belongs_to_select(node: exp.Expression) -> bool:
        owner = node.find_ancestor(exp.Select)
        return owner is select

    cols: set[str] = set()
    for filter_node in (select.find(exp.Where), select.find(exp.Having)):
        if filter_node is None:
            continue
        for col in filter_node.find_all(exp.Column):
            if not _belongs_to_select(col):
                continue
            if not _is_direct_filter_column(col, filter_node):
                continue
            col_name = (col.name or "").lower()
            if not col_name:
                continue
            cols.add(col_name)
    return cols


def _collect_cte_scope_filtered_columns(cte: exp.CTE) -> set[str]:
    """Collect scope-filtered columns from one CTE body."""
    cols: set[str] = set()
    for select in _collect_select_nodes(cte.this):
        cols.update(_collect_scope_filtered_columns_from_select(select))
    return cols


def _is_direct_filter_column(col: exp.Column, filter_node: exp.Expression) -> bool:
    """Return True when *col* is in the direct WHERE/HAVING path, not a nested subquery."""
    cursor: exp.Expression | None = col
    while cursor is not None:
        if isinstance(cursor, exp.Subquery):
            return False
        if cursor is filter_node or isinstance(cursor, (exp.Where, exp.Having)):
            return True
        cursor = cursor.parent
    return False


def _collect_join_key_columns(statement: exp.Expression) -> set[str]:
    """Collect column names used in JOIN ON clauses."""
    cols: set[str] = set()
    for join in statement.find_all(exp.Join):
        on_clause = join.args.get("on")
        if on_clause is None:
            continue
        for col in on_clause.find_all(exp.Column):
            if col.name:
                cols.add(col.name.lower())
    return cols


def _collect_literal_filter_columns(statement: exp.Expression) -> set[str]:
    """Collect filtered columns compared directly against literal-like values."""
    cols: set[str] = set()
    top_select = _top_level_select(statement)
    if top_select is None:
        return cols

    return _collect_literal_filter_columns_from_select(top_select)


def _collect_cte_literal_filter_columns(cte: exp.CTE) -> set[str]:
    """Collect literal-filtered columns from a single CTE body."""
    cols: set[str] = set()
    for select in _collect_select_nodes(cte.this):
        cols.update(_collect_literal_filter_columns_from_select(select))
    return cols


def _collect_wildcard_like_filter_columns(statement: exp.Expression) -> set[str]:
    """Collect columns filtered by wildcard LIKE/ILIKE in top-level WHERE/HAVING."""
    top_select = _top_level_select(statement)
    if top_select is None:
        return set()
    return _collect_wildcard_like_filter_columns_from_select(top_select)


def _collect_cte_wildcard_like_filter_columns(cte: exp.CTE) -> set[str]:
    """Collect columns filtered by wildcard LIKE/ILIKE inside a CTE body."""
    cols: set[str] = set()
    for select in _collect_select_nodes(cte.this):
        cols.update(_collect_wildcard_like_filter_columns_from_select(select))
    return cols


def _collect_literal_filter_columns_from_select(select: exp.Select) -> set[str]:
    """Collect literal-filtered columns from one SELECT's WHERE/HAVING."""
    cols: set[str] = set()

    def _has_literal_like_value(node: exp.Expression) -> bool:
        for child in node.args.values():
            if child is None:
                continue
            if isinstance(child, list):
                if any(isinstance(item, (exp.Literal, exp.Tuple)) for item in child):
                    return True
                continue
            if isinstance(child, (exp.Literal, exp.Tuple)):
                return True
        return False

    def _belongs_to_top_select(node: exp.Expression) -> bool:
        owner = node.find_ancestor(exp.Select)
        return owner is select

    for filter_node in (select.find(exp.Where), select.find(exp.Having)):
        if filter_node is None:
            continue
        for comparison in filter_node.find_all(
            _LITERAL_COMPARISON_TYPES
        ):
            if not _belongs_to_top_select(comparison):
                continue
            if _is_wildcard_like_lookup(comparison):
                continue
            has_literal = _has_literal_like_value(comparison)
            if not has_literal:
                continue
            for col in comparison.find_all(exp.Column):
                if col.name and _is_direct_filter_column(col, filter_node):
                    cols.add(col.name.lower())

        for not_node in filter_node.find_all(exp.Not):
            if not _belongs_to_top_select(not_node):
                continue
            inner = not_node.this
            if not isinstance(inner, exp.In):
                continue

            has_literal = _has_literal_like_value(inner)
            if not has_literal:
                continue

            for col in inner.find_all(exp.Column):
                if col.name and _is_direct_filter_column(col, filter_node):
                    cols.add(col.name.lower())

    return cols


def _collect_wildcard_like_filter_columns_from_select(select: exp.Select) -> set[str]:
    """Collect columns used in wildcard LIKE/ILIKE filters for one SELECT."""
    cols: set[str] = set()

    def _belongs_to_top_select(node: exp.Expression) -> bool:
        owner = node.find_ancestor(exp.Select)
        return owner is select

    for filter_node in (select.find(exp.Where), select.find(exp.Having)):
        if filter_node is None:
            continue
        for comparison in filter_node.find_all((exp.ILike, exp.Like)):
            if not _belongs_to_top_select(comparison):
                continue
            if not _is_wildcard_like_lookup(comparison):
                continue
            for col in comparison.find_all(exp.Column):
                if col.name and _is_direct_filter_column(col, filter_node):
                    cols.add(col.name.lower())

    return cols


def _is_wildcard_like_lookup(node: exp.Expression) -> bool:
    """Return True when a LIKE/ILIKE predicate compares against a % wildcard literal."""
    if not isinstance(node, (exp.ILike, exp.Like)):
        return False
    for child in node.args.values():
        if child is None:
            continue
        children = child if isinstance(child, list) else [child]
        for item in children:
            if isinstance(item, exp.Literal) and "%" in str(item.this or ""):
                return True
    return False


def _collect_select_nodes(node: exp.Expression) -> list[exp.Select]:
    """Return SELECT leaves from an expression (supports UNION trees)."""
    if isinstance(node, exp.Select):
        return [node]
    if isinstance(node, exp.Subquery):
        inner = node.this
        if isinstance(inner, exp.Select):
            return [inner]
        if isinstance(inner, exp.Union):
            return _collect_select_nodes(inner)
        return []
    if isinstance(node, exp.Union):
        return _collect_select_nodes(node.left) + _collect_select_nodes(node.right)
    return []


def _collect_direct_cte_names(top_select: exp.Select, all_cte_names: set[str]) -> set[str]:
    """Collect CTE names directly read by top_select FROM/JOIN relations."""
    direct_names: set[str] = set()

    from_clause = top_select.args.get("from")
    if from_clause is None:
        # sqlglot versions differ: some expose FROM as "from_" in args.
        from_clause = top_select.args.get("from_")
    if from_clause is not None:
        relations = []
        if from_clause.this is not None:
            relations.append(from_clause.this)
        relations.extend(from_clause.expressions or [])
        for relation in relations:
            if isinstance(relation, exp.Table) and relation.name:
                name = relation.name.lower()
                if name in all_cte_names:
                    direct_names.add(name)

    for join in top_select.args.get("joins") or []:
        relation = join.this
        if isinstance(relation, exp.Table) and relation.name:
            name = relation.name.lower()
            if name in all_cte_names:
                direct_names.add(name)

    return direct_names


def _collect_cte_dependency_names(
    top_select: exp.Select,
    cte_nodes: dict[str, exp.CTE],
    *,
    max_hops: int,
) -> set[str]:
    """Collect direct and bounded dependent CTE names reachable from top_select."""
    cte_names = set(cte_nodes)
    discovered: set[str] = set(_collect_direct_cte_names(top_select, cte_names))
    frontier: set[str] = set(discovered)

    for _ in range(max_hops):
        next_frontier: set[str] = set()
        for cte_name in frontier:
            cte_node = cte_nodes.get(cte_name)
            if cte_node is None:
                continue
            for select in _collect_select_nodes(cte_node.this):
                deps = _collect_direct_cte_names(select, cte_names)
                next_frontier.update(deps - discovered)
        if not next_frontier:
            break
        discovered.update(next_frontier)
        frontier = next_frontier

    return discovered


def _collect_id_filter_columns(filtered_cols: set[str]) -> set[str]:
    """Collect identifier-style columns that should not be required in output."""
    return {c for c in filtered_cols if c == "id" or c.endswith("_id")}


def _collect_projected_field_names(select: exp.Select) -> set[str]:
    """Return column/alias names visible in the top-level SELECT output."""
    projected: set[str] = set()

    for sel_expr in select.expressions:
        if isinstance(sel_expr, exp.Star):
            return {"*"}

        if isinstance(sel_expr, exp.Column) and (sel_expr.name or "").strip() == "*":
            return {"*"}

        if isinstance(sel_expr, exp.Alias):
            alias = str(sel_expr.alias).lower()
            projected.add(alias)
        for col in sel_expr.find_all(exp.Column):
            if col.name:
                projected.add(col.name.lower())


    return projected


def _check_limit_inside_cte(statement: exp.Expression) -> list[VerificationError]:
    """Flag LIMIT or TOP inside a CTE body as an error.

    CTEs are not ordered sets in many SQL dialects; placing a LIMIT/TOP inside a
    CTE can lead to non-deterministic or engine-dependent behaviour.
    """
    errors: list[VerificationError] = []

    for cte in statement.find_all(exp.CTE):
        # Walk select nodes inside the CTE body
        for select in _collect_select_nodes(cte.this):
            # If the select contains a Limit or an Order without being wrapped by
            # a surrounding subquery with a limit, flag it. sqlglot represents
            # TOP as a Select.args.get("limit") in some dialects; checking for
            # exp.Limit is sufficient for most cases.
            if select.find(exp.Limit) is not None:
                errors.append(
                    VerificationError(
                        error_type="limit_inside_cte",
                        message=(
                            "LIMIT found inside a CTE body. LIMIT/TOP inside CTEs is "
                            "not portable and may produce non-deterministic results."
                        ),
                    )
                )
            # Also check for presence of TOP via Select.args (sqlglot may parse TOP
            # by placing a Value in the 'limit' arg depending on dialect)
            limit_arg = select.args.get("limit")
            if limit_arg is not None and not isinstance(limit_arg, exp.Limit):
                # If it's a raw expression representing TOP, flag it
                errors.append(
                    VerificationError(
                        error_type="limit_inside_cte",
                        message=(
                            "TOP/LIMIT found inside a CTE body. LIMIT/TOP inside CTEs is "
                            "not portable and may produce non-deterministic results."
                        ),
                    )
                )
    return errors


def _check_order_by_without_limit(statement: exp.Expression) -> list[str]:
    """Warn when a non-top-level SELECT (CTE or subquery) contains ORDER BY but no LIMIT.

    We only warn for ORDER BY inside CTEs or subqueries because top-level ORDER BY
    is often intentional alongside LIMIT; subquery/CTE ORDER BY without LIMIT is
    often a sign of a misplaced ordering.
    """
    warnings: list[str] = []
    top = _top_level_select(statement)
    for select in statement.find_all(exp.Select):
        # skip the top-level final select
        if top is not None and select is top:
            continue
        has_order = select.find(exp.Order) is not None
        has_limit = select.find(exp.Limit) is not None or select.args.get("limit") is not None
        if has_order and not has_limit:
            warnings.append(
                "ORDER BY without LIMIT detected inside a subquery/CTE; consider adding LIMIT or removing ORDER BY."
            )
    return warnings


def _check_window_function_in_where(statement: exp.Expression) -> list[VerificationError]:
    """Flag usage of window functions inside WHERE clauses (not allowed).

    Detect exp.Window nodes appearing under a WHERE where the Window is not
    enclosed in a subquery. Window functions cannot be filtered by WHERE and
    should be computed in an outer query or a CTE.
    """
    errors: list[VerificationError] = []
    # Walk WHERE nodes and look for Window descendants that are not inside a subquery
    for where in statement.find_all(exp.Where):
        # If the WHERE itself is inside a subquery, ignore; we only flag top-level
        # WHERE nodes of the main query (or CTE bodies) that contain window funcs.
        if where.find_ancestor(exp.Subquery) is not None:
            continue
        for window in where.find_all(exp.Window):
            # If the window is inside a subquery (ancestor Subquery between window and where), ignore
            anc = window.parent
            inside_subquery = False
            while anc is not None and anc is not where:
                if isinstance(anc, exp.Subquery):
                    inside_subquery = True
                    break
                anc = anc.parent
            if inside_subquery:
                continue
            errors.append(
                VerificationError(
                    error_type="window_function_in_where",
                    message=(
                        "Window function used inside WHERE clause. Window functions are "
                        "not allowed in WHERE; move the expression to a subquery or use HAVING."
                    ),
                )
            )
    return errors


def _check_self_join_without_alias(statement: exp.Expression) -> list[VerificationError]:
    """Detect self-joins where the same table appears multiple times without proper aliases."""
    errors: list[VerificationError] = []
    for select in statement.find_all(exp.Select):
        # collect table occurrences in this select
        name_to_occurrences: dict[str, list[exp.Table]] = {}
        from_clause = select.args.get("from") or select.args.get("from_")
        relations: list[exp.Expression] = []
        if from_clause is not None:
            if getattr(from_clause, "this", None) is not None:
                relations.append(from_clause.this)
            relations.extend(from_clause.expressions or [])
        for join in select.args.get("joins") or []:
            relations.append(join.this)

        for rel in relations:
            if isinstance(rel, exp.Table) and rel.name:
                name = rel.name.lower()
                name_to_occurrences.setdefault(name, []).append(rel)

        for name, occ in name_to_occurrences.items():
            if len(occ) > 1:
                # If any occurrence lacks an alias or uses the bare table name as alias, flag it
                for table_node in occ:
                    alias = (table_node.alias or "").lower()
                    if not alias or alias == name:
                        errors.append(
                            VerificationError(
                                error_type="self_join_without_alias",
                                message=(
                                    f"Table '{name}' is joined multiple times but one occurrence has no alias. "
                                    "Provide distinct aliases for self-joins to avoid ambiguity."
                                ),
                                table=name,
                            )
                        )
                        break
    return errors


def _check_bare_aggregate_in_final_select(statement: exp.Expression) -> list[str]:
    """Warn when the final/top-level SELECT returns only aggregates with no GROUP BY or context.

    This can be intentional, but often a user expects row-level context alongside aggregates.
    """
    warnings: list[str] = []
    top_select = _top_level_select(statement)
    if top_select is None:
        return warnings

    if _has_aggregates(top_select):
        group_by = top_select.find(exp.Group)
        non_agg_columns = []
        for sel_expr in top_select.expressions:
            if not _is_aggregate(sel_expr):
                inner = sel_expr.this if isinstance(sel_expr, exp.Alias) else sel_expr
                if isinstance(inner, exp.Column):
                    non_agg_columns.append(inner.name.lower())
        if not non_agg_columns and not group_by:
            warnings.append(
                "Top-level SELECT returns only aggregate expressions with no GROUP BY; verify this is intended."
            )
    return warnings


def _check_non_sargable_function_on_filter_column(statement: exp.Expression) -> list[str]:
    """Warn when WHERE/HAVING predicates apply a non-sargable function to a column (e.g. LOWER(col) = 'x')."""
    warnings: list[str] = []

    def _is_function_wrapping_column(node: exp.Expression) -> bool:
        # If node is a function and contains a Column child, consider it non-sargable
        if isinstance(node, exp.Func) or isinstance(node, exp.Anonymous):
            for c in node.find_all(exp.Column):
                return True
        return False

    for select in statement.find_all(exp.Select):
        for filter_node in (select.find(exp.Where), select.find(exp.Having)):
            if filter_node is None:
                continue
            for comparison in filter_node.find_all(_LITERAL_COMPARISON_TYPES):
                # check left and right sides
                for side in comparison.args.values():
                    if side is None:
                        continue
                    nodes = side if isinstance(side, list) else [side]
                    for node in nodes:
                        if _is_function_wrapping_column(node):
                            warnings.append(
                                "Potential non-sargable predicate detected (function applied to column). "
                                "Consider rewriting to make use of indexes (e.g., LOWER(col) -> col ILIKE ...)."
                            )
    return warnings


def _check_duplicate_cte_name(statement: exp.Expression) -> list[VerificationError]:
    """Error when the same CTE name is declared more than once."""
    errors: list[VerificationError] = []
    names: dict[str, int] = {}
    for cte in statement.find_all(exp.CTE):
        name = (cte.alias_or_name or "").lower()
        if not name:
            continue
        names[name] = names.get(name, 0) + 1
    for name, count in names.items():
        if count > 1:
            errors.append(
                VerificationError(
                    error_type="duplicate_cte_name",
                    message=(f"CTE name '{name}' declared {count} times. Duplicate CTE names are not allowed."),
                    suggestion="Rename the duplicate CTEs so each has a unique alias.",
                )
            )
    return errors


def _check_unused_cte(statement: exp.Expression) -> list[str]:
    """Warn when a CTE is declared but never referenced by the top-level query or other CTEs."""
    warnings: list[str] = []
    cte_nodes = {cte.alias_or_name.lower(): cte for cte in statement.find_all(exp.CTE) if cte.alias_or_name}
    if not cte_nodes:
        return warnings
    top_select = _top_level_select(statement)
    if top_select is None:
        return warnings
    referenced = _collect_cte_dependency_names(top_select, cte_nodes, max_hops=10)
    for name in sorted(cte_nodes.keys()):
        if name not in referenced:
            warnings.append(f"CTE '{name}' is declared but never used in the final query.")
    return warnings


def _check_missing_nullif_in_division(statement: exp.Expression) -> list[VerificationError]:
    """Error when division operations do not guard denominator with NULLIF to avoid divide-by-zero.

    This flags simple binary division operators where the right-hand side is not
    wrapped in NULLIF(..., 0). Only divisions that appear in SELECT expressions
    or top-level WHERE predicates are considered.
    """
    errors: list[VerificationError] = []
    # sqlglot may represent division with exp.Div or exp.Divide depending on version
    div_types = [getattr(exp, "Div", None), getattr(exp, "Divide", None)]
    div_types = [t for t in div_types if t is not None]

    def _is_nullif(node: exp.Expression) -> bool:
        # sqlglot versions may represent NULLIF as a concrete class or as a
        # function-like Anonymous/Func node named 'nullif'. Detect both.
        if node is None:
            return False
        if getattr(exp, "NullIf", None) is not None and isinstance(node, getattr(exp, "NullIf")):
            return True
        if isinstance(node, (exp.Anonymous, exp.Func)):
            name = (getattr(node, "name", None) or "").lower()
            # For Func node, name may be in node.this
            if not name and hasattr(node, "this") and getattr(node.this, "name", None):
                name = getattr(node.this, "name").lower()
            return name == "nullif"
        return False

    for node in statement.walk():
        # Check any node that is one of the division types
        if type(node) in div_types:
            # Determine if this division is inside a SELECT expression or a WHERE
            in_select = node.find_ancestor(exp.Select) is not None
            in_where = node.find_ancestor(exp.Where) is not None
            if not (in_select or in_where):
                continue

            right = getattr(node, "right", None) or node.args.get("right") or node.args.get("this")
            if right is None:
                continue
            # If the right side is a NULLIF, it's fine
            if _is_nullif(right):
                continue
            # If right contains a Literal zero, treat as guarded
            has_zero = False
            for lit in right.find_all(exp.Literal):
                try:
                    if str(lit.this) == "0":
                        has_zero = True
                        break
                except Exception:
                    pass
            if has_zero:
                continue

            errors.append(
                VerificationError(
                    error_type="missing_nullif_in_division",
                    message=(
                        "Division operation without NULLIF guard on denominator detected. "
                        "Wrap the denominator with NULLIF(denom, 0) to avoid divide-by-zero errors."
                    ),
                )
            )
    return errors

