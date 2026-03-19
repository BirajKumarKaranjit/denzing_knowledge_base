from __future__ import annotations

from sql_worker.sql_verifier import verify_sql
from sql_worker.schema_linker import build_column_registry


def registry_fixture():
    ddl = {
        "t": "CREATE TABLE t (a numeric, b numeric, id text);",
        "u": "CREATE TABLE u (x numeric, id text);",
    }
    return build_column_registry(ddl)


def test_window_function_in_where_error():
    reg = registry_fixture()
    sql = (
        "SELECT t.a FROM t WHERE ROW_NUMBER() OVER (ORDER BY t.a) = 1"
    )
    res = verify_sql(sql, reg)
    assert not res.is_valid
    assert any(e.error_type == "window_function_in_where" for e in res.errors)


def test_window_function_in_subquery_not_flagged():
    reg = registry_fixture()
    sql = (
        "SELECT * FROM ("
        "  SELECT t.a FROM t WHERE ROW_NUMBER() OVER (ORDER BY t.a) = 1"
        ") s"
    )
    res = verify_sql(sql, reg)
    assert res.is_valid


def test_limit_inside_cte_error():
    reg = registry_fixture()
    sql = (
        "WITH c AS (SELECT a FROM t ORDER BY a LIMIT 1) "
        "SELECT * FROM c"
    )
    res = verify_sql(sql, reg)
    assert not res.is_valid
    assert any(e.error_type == "limit_inside_cte" for e in res.errors)


def test_limit_inside_cte_not_flagged_when_absent():
    reg = registry_fixture()
    sql = (
        "WITH c AS (SELECT a FROM t) SELECT * FROM c"
    )
    res = verify_sql(sql, reg)
    assert res.is_valid


def test_self_join_without_alias_error():
    reg = registry_fixture()
    sql = (
        "SELECT * FROM t JOIN t ON t.id = t.id"
    )
    res = verify_sql(sql, reg)
    assert not res.is_valid
    assert any(e.error_type == "self_join_without_alias" for e in res.errors)


def test_self_join_with_alias_ok():
    reg = registry_fixture()
    sql = (
        "SELECT * FROM t t1 JOIN t t2 ON t1.id = t2.id"
    )
    res = verify_sql(sql, reg)
    assert res.is_valid


def test_missing_nullif_in_division_error():
    reg = registry_fixture()
    sql = "SELECT a / b AS ratio FROM t"
    res = verify_sql(sql, reg)
    assert not res.is_valid
    assert any(e.error_type == "missing_nullif_in_division" for e in res.errors)


def test_missing_nullif_in_division_guarded_ok():
    reg = registry_fixture()
    sql = "SELECT a / NULLIF(b, 0) AS ratio FROM t"
    res = verify_sql(sql, reg)
    assert res.is_valid


def test_order_by_without_limit_warns_in_subquery():
    reg = registry_fixture()
    sql = (
        "SELECT * FROM (SELECT a FROM t ORDER BY a) s"
    )
    res = verify_sql(sql, reg)
    assert res.is_valid
    assert any("ORDER BY without LIMIT" in w for w in res.warnings)


def test_order_by_without_limit_not_warn_top_level():
    reg = registry_fixture()
    sql = "SELECT a FROM t ORDER BY a"
    res = verify_sql(sql, reg)
    assert res.is_valid
    assert not any("ORDER BY without LIMIT" in w for w in res.warnings)


def test_bare_aggregate_in_final_select_warns():
    reg = registry_fixture()
    sql = "SELECT SUM(a) FROM t"
    res = verify_sql(sql, reg)
    assert res.is_valid
    assert any("aggregate" in w.lower() for w in res.warnings)


def test_bare_aggregate_in_final_select_ok_with_dimension():
    reg = registry_fixture()
    sql = "SELECT id, SUM(a) FROM t GROUP BY id"
    res = verify_sql(sql, reg)
    assert res.is_valid
    assert not any("aggregate" in w.lower() for w in res.warnings)

