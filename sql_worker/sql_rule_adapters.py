"""Adapters that run external SQL tooling and normalize results for the verifier.

This module provides thin adapters over sqlfluff and sqllineage. The functions
return data in the same formats used by the verifier: a list of
VerificationError objects and a list of warning strings.
"""
from __future__ import annotations

from typing import List, Tuple

from sqlglot.expressions import Expression

from sqlglot import exp
from sqlglot.errors import ParseError

try:
    from sqlfluff.core import Linter
except Exception:  # pragma: no cover - developer env may not have installed yet
    Linter = None  # type: ignore

try:
    from sqllineage.runner import LineageRunner
except Exception:  # pragma: no cover
    LineageRunner = None  # type: ignore

from .sql_verifier import VerificationError


def run_sqlfluff_checks(sql: str, dialect: str = "postgres") -> Tuple[List[VerificationError], List[str]]:
    """Run sqlfluff linting and map important violations to verifier types.

    Returns (errors, warnings).
    """
    errors: List[VerificationError] = []
    warnings: List[str] = []

    if Linter is None:
        warnings.append("sqlfluff not installed; skipping sqlfluff checks")
        return errors, warnings

    # sqlfluff expects dialect names like 'postgres' or 'snowflake'
    linter = Linter(dialect=dialect)
    try:
        result = linter.lint_string(sql)
    except Exception as exc:
        warnings.append(f"sqlfluff parse/lint error: {exc}")
        return errors, warnings

    # Only report sqlfluff rule codes we explicitly opt into. By default,
    # allow SELECT * related rules (ST*). This prevents noise from style/format
    # rules such as capitalization, whitespace, and line-breaks.
    # Allow SELECT star (ST*) and join-related AM* rules that detect implicit
    # cross-joins and join qualification issues.
    ALLOWED_SQLFLUFF_PREFIXES = {"ST", "AM"}

    # Build a map of detailed violations (rule_code -> description) where available
    violations_map = {}
    try:
        for v in result.get_violations():
            # sqlfluff's SQLLintError exposes rule_code as a callable method in some versions
            code_obj = getattr(v, "rule_code", None)
            try:
                code = code_obj() if callable(code_obj) else code_obj
            except Exception:
                code = code_obj
            desc = getattr(v, "description", None) or getattr(v, "description_long", None) or ""
            if code:
                violations_map[str(code).upper()] = str(desc)
    except Exception:
        # some sqlfluff versions may not expose get_violations; ignore
        pass

    # Map sqlfluff tuples (rule_code, line_no, pos) into our warnings/errors
    try:
        tuples = list(result.check_tuples())
    except Exception as exc:
        # sqlfluff may raise a SQLParseError when it cannot parse complex constructs.
        # Treat this as adapter non-coverage for the pattern: return a warning and no errors.
        warnings.append(f"sqlfluff parse error: {exc}")
        return errors, warnings

    for t in tuples:
        if not t:
            continue
        rule_code = str(t[0]).upper()
        # Only proceed if code matches an allowed prefix
        if not any(rule_code.startswith(p) for p in ALLOWED_SQLFLUFF_PREFIXES):
            continue
        line_no = t[1] if len(t) > 1 else None
        desc = violations_map.get(rule_code, "")
        desc_l = (desc or "").lower()

        # SELECT * detection (sqlfluff uses ST* codes for star warnings)
        if rule_code.startswith("ST") or "select *" in desc_l or "select star" in desc_l:
            warnings.append("SELECT * detected; prefer explicit columns (sqlfluff)")
            continue

        # AM* rules: map join-related AM codes to cross_join_without_condition
        if rule_code.startswith("AM"):
            # If the description mentions cross-join/join qualification or ambiguous
            # result columns (common symptoms of implicit joins), map to our error.
            if any(k in desc_l for k in ("cross", "implicit", "join", "unknown number of result columns", "join clauses")):
                errors.append(
                    VerificationError(
                        error_type="cross_join_without_condition",
                        message=f"sqlfluff:{rule_code} {desc}",
                    )
                )
                continue


    # Supplement: detect non-sargable functions in WHERE/HAVING using sqlglot
    try:
        import sqlglot

        parsed = sqlglot.parse_one(sql)
        for select in parsed.find_all(exp.Select):
            for filter_node in (select.find(exp.Where), select.find(exp.Having)):
                if filter_node is None:
                    continue
                # For each column in the filter, check if it is wrapped by a function
                non_sargable_found = False
                for col in filter_node.find_all(exp.Column):
                    # walk ancestors up to the filter_node
                    anc = col.parent
                    # Build a safe tuple of function-like node types present in this sqlglot version
                    func_type_names = (
                        "Anonymous",
                        "Func",
                        "Lower",
                        "Upper",
                        "Cast",
                    )
                    func_types = tuple(getattr(exp, n) for n in func_type_names if getattr(exp, n, None) is not None)
                    func_name = None
                    while anc is not None and anc is not filter_node:
                        func_class_name = anc.__class__.__name__.lower()
                        if isinstance(anc, func_types) or func_class_name in (
                            "lower",
                            "upper",
                            "trim",
                            "ltrim",
                            "rtrim",
                            "replace",
                            "substring",
                            "substr",
                            "coalesce",
                            "concat",
                        ):
                            non_sargable_found = True
                            func_name = anc.__class__.__name__
                            break
                        anc = anc.parent
                    if non_sargable_found:
                        warnings.append(
                            f"Potential non-sargable predicate detected: function {func_name} applied to column."
                        )
                        break
        # Detect ORDER BY in subqueries without LIMIT
        for select in parsed.find_all(exp.Select):
            order = select.find(exp.Order)
            limit = select.find(exp.Limit)
            if order is None:
                continue
            # If this select is not the top-level SELECT (has ancestor Select that's different),
            # and has no LIMIT, warn about ORDER BY without LIMIT in subquery
            parent_select = select.find_ancestor(exp.Select)
            if parent_select is not None and parent_select is not select and limit is None:
                warnings.append(
                    "ORDER BY used inside a subquery without LIMIT; this may be ineffective or expensive."
                )
    except Exception:
        # Parsing failure: don't block linting
        pass

    return errors, warnings


def run_sqllineage_checks(sql: str) -> List[str]:
    """Detect unused CTEs using sqllineage and return list of warning strings.

    Returns an empty list if sqllineage isn't available.
    """
    warnings: List[str] = []
    if LineageRunner is None:
        warnings.append("sqllineage not installed; skipping CTE lineage checks")
        return warnings

    try:
        runner = LineageRunner(sql)
    except Exception as exc:
        warnings.append(f"sqllineage parse error: {exc}")
        return warnings

    # Attempt to find declared CTE names via sqlglot parsing and detect usage via table refs.
    try:
        import sqlglot

        parsed = sqlglot.parse_one(sql)
    except Exception:
        parsed = None

    cte_names = set()
    if parsed is not None:
        for cte in parsed.find_all(exp.CTE):
            if cte.alias_or_name:
                cte_names.add(cte.alias_or_name.lower())

    used_tables = set()
    if parsed is not None:
        for table in parsed.find_all(exp.Table):
            if table.name:
                used_tables.add(table.name.lower())

    # If sqllineage provides more structured usage, attempt to include it
    try:
        for q in runner.queries:
            for src in getattr(q, "sources", []) or []:
                used_tables.add(str(src).lower())
            for tgt in getattr(q, "targets", []) or []:
                used_tables.add(str(tgt).lower())
    except Exception:
        pass

    for name in sorted(cte_names):
        if name not in used_tables:
            warnings.append(f"CTE '{name}' is declared but never used in the query.")

    return warnings

