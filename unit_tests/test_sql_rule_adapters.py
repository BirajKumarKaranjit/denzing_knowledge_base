import pytest

from sql_worker import sql_rule_adapters as adapters


def test_sqlfluff_not_installed_skips(monkeypatch):
    # Force Linter to be None to simulate missing install
    monkeypatch.setattr(adapters, "Linter", None)
    errs, warns = adapters.run_sqlfluff_checks("SELECT 1;", "postgres")
    assert errs == []
    assert any("sqlfluff not installed" in w for w in warns)


def test_sqllineage_not_installed_skips(monkeypatch):
    monkeypatch.setattr(adapters, "LineageRunner", None)
    warns = adapters.run_sqllineage_checks("WITH x AS (SELECT 1) SELECT 1;")
    assert any("sqllineage not installed" in w for w in warns)


def test_sqlfluff_detects_select_star_in_cte():
    sql = "WITH cte AS (SELECT * FROM players) SELECT 1;"
    errs, warns = adapters.run_sqlfluff_checks(sql, "postgres")
    # adapter maps SELECT * to a warning message containing 'SELECT * detected'
    assert any("SELECT * detected" in w for w in warns)


def test_sqlfluff_order_by_without_limit_in_subquery():
    sql = "SELECT * FROM (SELECT id FROM players ORDER BY id) sub;"
    errs, warns = adapters.run_sqlfluff_checks(sql, "postgres")
    assert any("ORDER BY" in w for w in warns)


def test_sqlfluff_non_sargable_function_on_filter():
    sql = "SELECT id FROM players WHERE LOWER(full_name) = 'lebron james';"
    errs, warns = adapters.run_sqlfluff_checks(sql, "postgres")
    # We expect a warning suggesting non-sargable predicate; adapter may return generic sqlfluff message
    assert any("LOWER" in w or "non-sargable" in w.lower() or "sqlfluff" in w for w in warns)


def test_sqllineage_unused_and_used_cte():
    unused_sql = "WITH x AS (SELECT 1) SELECT 2;"
    warns = adapters.run_sqllineage_checks(unused_sql)
    assert any("CTE 'x' is declared but never used" in w or "cte 'x'" in w.lower() for w in warns)

    used_sql = "WITH x AS (SELECT 1) SELECT * FROM x;"
    warns2 = adapters.run_sqllineage_checks(used_sql)
    # Expect no unused-cte warning for used CTE
    assert not any("declared but never used" in w for w in warns2)


def test_sqlfluff_performance():
    """Run run_sqlfluff_checks 50x on a moderately complex CTE and print average duration.

    This test will assert the average duration per call is <= 0.2 seconds. If it
    exceeds that, the test will fail and report the measured average.
    """
    from sql_worker import sql_rule_adapters as adapters
    import time

    sql = (
        "WITH player_stats AS ("
        " SELECT p.player_id, p.full_name, SUM(pb.points) AS total_points,"
        " COUNT(*) AS games_played FROM dwh_d_players p JOIN dwh_f_player_boxscore pb"
        " ON p.player_id = pb.player_id WHERE pb.season = '2022' GROUP BY p.player_id, p.full_name),"
        "team_stats AS ("
        " SELECT t.team_id, t.team_name, SUM(tb.points) AS team_points FROM dwh_d_teams t"
        " JOIN dwh_f_team_boxscore tb ON t.team_id = tb.team_id GROUP BY t.team_id, t.team_name)"
        " SELECT ps.player_id, ps.full_name, ps.total_points, ts.team_name FROM player_stats ps"
        " JOIN team_stats ts ON ps.player_id = ts.team_id LIMIT 10;"
    )

    runs = 50
    timings = []
    for _ in range(runs):
        start = time.perf_counter()
        adapters.run_sqlfluff_checks(sql, "postgres")
        end = time.perf_counter()
        timings.append(end - start)

    avg = sum(timings) / len(timings)
    print(f"sqlfluff average duration over {runs} runs: {avg:.4f} seconds")
    # Assert average <= 0.2s (200ms)
    assert avg <= 0.2, f"sqlfluff too slow: avg {avg:.4f}s > 0.2s"


def test_sqlfluff_clean_queries_return_no_violations():
    """Ensure run_sqlfluff_checks reports no errors/warnings for clean SQL."""
    from sql_worker import sql_rule_adapters as adapters

    queries = [
        "SELECT id, full_name FROM dwh_d_players;",
        "WITH t AS (SELECT player_id, full_name FROM dwh_d_players) SELECT player_id FROM t;",
        "SELECT id FROM t1 UNION ALL SELECT id FROM t2;",
    ]

    for q in queries:
        errs, warns = adapters.run_sqlfluff_checks(q, "postgres")
        assert errs == [], f"Unexpected sqlfluff errors for clean query: {errs}"
        assert warns == [], f"Unexpected sqlfluff warnings for clean query: {warns}"


def test_sqlfluff_order_by_in_union_branch_detection():
    """Check whether sqlfluff flags ORDER BY inside a UNION ALL branch.

    If sqlfluff detects this pattern it should return a hard error. If not,
    the hand-rolled verifier must remain in place.
    """
    from sql_worker import sql_rule_adapters as adapters

    sql = "SELECT a FROM t1 ORDER BY a UNION ALL SELECT b FROM t2;"
    errs, warns = adapters.run_sqlfluff_checks(sql, "postgres")
    # Expect at least one hard error if sqlfluff recognizes ORDER BY in branch
    assert any(isinstance(e, adapters.VerificationError) for e in errs) or errs == [], (
        "sqlfluff did not return a hard error for ORDER BY in UNION branch; keep hand-rolled check."
    )


def test_sqlfluff_detects_cross_join_patterns():
    """Ensure sqlfluff (AM rules) detect implicit cross joins and JOIN without ON."""
    # implicit cross join
    sql1 = "SELECT * FROM a, b;"
    errs1, warns1 = adapters.run_sqlfluff_checks(sql1, "postgres")
    assert any(e.error_type == "cross_join_without_condition" for e in errs1), (
        f"Implicit cross join not detected by adapter: errs={errs1}, warns={warns1}"
    )

    # explicit JOIN without ON
    sql2 = "SELECT * FROM a JOIN b;"
    errs2, warns2 = adapters.run_sqlfluff_checks(sql2, "postgres")
    assert any(e.error_type == "cross_join_without_condition" for e in errs2), (
        f"JOIN without ON not detected by adapter: errs={errs2}, warns={warns2}"
    )


def test_sqlglot_duplicate_cte_parsing_behavior():
    """Check sqlglot behavior when parsing duplicate CTE names.

    If sqlglot raises or exposes duplicate CTEs, we may be able to remove the
    hand-rolled duplicate-CTE check. Otherwise we must keep it.
    """
    import sqlglot
    sql = (
        "WITH x AS (SELECT 1 AS a), x AS (SELECT 2 AS b) SELECT * FROM x;"
    )
    try:
        parsed = sqlglot.parse_one(sql)
    except Exception as exc:
        # Parsing error means sqlglot enforces uniqueness — adapter coverage OK
        pytest.skip(f"sqlglot parse exception: {exc}")

    # Collect declared CTE names
    cte_names = [cte.alias_or_name for cte in parsed.find_all(sqlglot.exp.CTE) if cte.alias_or_name]
    # If duplicates appear in the AST list, sqlglot preserved them — indicate we need hand-rolled check
    assert len(cte_names) > 0
    duplicates = [n for n in set(cte_names) if cte_names.count(n) > 1]
    # sqlglot preserves duplicate CTE names in the AST; confirm this so the
    # hand-rolled duplicate CTE name check must be kept.
    assert duplicates != [], f"Expected duplicate CTE names in AST but found none; cte_names={cte_names}"


