"""sql_validator/schema_linker.py

Builds a column registry from a DDL dict for use by the SQL verifier.
"""

from __future__ import annotations

import sqlglot
import sqlglot.expressions as exp

_DDL_KEYWORDS: frozenset[str] = frozenset(
    {
        "constraint", "primary", "foreign", "unique", "check",
        "index", "key", "references", "like", "exclude",
    }
)


def build_column_registry(ddl_dict: dict[str, str]) -> dict[str, list[str]]:
    """Parse a DDL dict and return a mapping of table name → column names.

    Parameters
    ----------
    ddl_dict:
        Mapping of ``table_name -> CREATE TABLE SQL string``.
        Supports both single-line and multi-line DDL formats.

    Returns
    -------
    dict[str, list[str]]
        ``{table_name: [col1, col2, ...]}`` — all names lowercased.
    """
    registry: dict[str, list[str]] = {}
    for table_name, ddl_sql in ddl_dict.items():
        registry[table_name.lower()] = _extract_columns(ddl_sql)
    return registry


def _extract_columns(ddl_sql: str) -> list[str]:
    """Extract column names from a CREATE TABLE statement via sqlglot, with regex fallback."""
    try:
        statements = sqlglot.parse(ddl_sql, error_level=sqlglot.ErrorLevel.IGNORE)
    except Exception:  # noqa: BLE001
        return _regex_fallback(ddl_sql)

    for stmt in statements:
        if stmt is None:
            continue
        if isinstance(stmt, exp.Create):
            columns = [
                col_def.name.lower()
                for col_def in stmt.find_all(exp.ColumnDef)
                if col_def.name and col_def.name.lower() not in _DDL_KEYWORDS
            ]
            if columns:
                return columns

    return _regex_fallback(ddl_sql)


def _regex_fallback(ddl_sql: str) -> list[str]:
    """Comma-split fallback for DDL formats sqlglot cannot parse."""
    import re

    body_match = re.search(r"\((.+)\)", ddl_sql, re.DOTALL)
    if not body_match:
        return []

    columns: list[str] = []
    for part in body_match.group(1).split(","):
        first = part.strip().split()
        if first:
            name = first[0].lower().strip("()")
            if name and name not in _DDL_KEYWORDS:
                columns.append(name)
    return columns

