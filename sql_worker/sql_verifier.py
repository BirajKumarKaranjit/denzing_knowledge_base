"""sql_worker/sql_verifier.py

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

_AGGREGATE_FUNCS: frozenset[str] = frozenset(
    {"sum", "avg", "count", "max", "min", "stddev", "variance",
     "array_agg", "string_agg", "json_agg", "jsonb_agg", "listagg"}
)


@dataclass
class VerificationError:
    """A fatal validation error that should block SQL execution (or trigger a retry)."""

    error_type: str
    """One of: column_not_in_ddl | wrong_table_for_column |
    union_column_mismatch | order_by_in_union_branch | scope_filter_not_projected |
    filter_context_not_projected"""
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
    """Validate *sql* against *registry* using sqlglot AST analysis.

    Parameters
    sql:
    registry:
        ``{table_name: [col1, col2, ...]}`` built by ``build_column_registry()``.
        All names must be lowercase.
    dialect:
        SQL dialect name used for sqlglot parsing (for example: postgresql,
        snowflake, bigquery).
    Returns
    VerificationResult
        ``is_valid=False`` when at least one hard error is found.
        ``warnings`` are non-fatal and do not block execution.
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
        errors.extend(_check_scope_filter_projection(statement))
        errors.extend(_check_filter_context_projection(statement))
        warnings.extend(_check_group_by_completeness(statement))

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
    """Detect ORDER BY or LIMIT placed directly inside a UNION branch (not in a subquery)."""
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
    if not filtered_cols:
        return []

    join_key_cols = _collect_join_key_columns(statement)
    literal_filter_cols = _collect_literal_filter_columns(statement)
    id_filter_cols = _collect_id_filter_columns(filtered_cols)
    scope_cols = filtered_cols - join_key_cols - literal_filter_cols - id_filter_cols
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

    literal_filter_cols = _collect_literal_filter_columns(statement)
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
            col_name = (col.name or "").lower()
            if not col_name:
                continue
            cols.add(col_name)
    return cols


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
        return owner is top_select

    for filter_node in (top_select.find(exp.Where), top_select.find(exp.Having)):
        if filter_node is None:
            continue
        for comparison in filter_node.find_all(
            (exp.EQ, exp.NEQ, exp.ILike, exp.Like, exp.In, exp.GT, exp.GTE, exp.LT, exp.LTE)
        ):
            if not _belongs_to_top_select(comparison):
                continue
            has_literal = _has_literal_like_value(comparison)
            if not has_literal:
                continue
            for col in comparison.find_all(exp.Column):
                if col.name:
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
                if col.name:
                    cols.add(col.name.lower())

    return cols


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


