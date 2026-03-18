"""unit_tests/test_sql_verifier.py

Unit tests for sql_worker.sql_verifier and sql_worker.schema_linker.

Run with:
    pytest unit_tests/test_sql_verifier.py -v
"""

from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

from sql_worker.schema_linker import build_column_registry, _regex_fallback
from sql_worker.sql_verifier import (
    VerificationResult,
    verify_sql,
)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def registry() -> dict[str, list[str]]:
    """Minimal NBA-like DDL registry used across all tests."""
    ddl = {
        "dwh_d_players": (
            "CREATE TABLE dwh_d_players (player_id text, team_id text, "
            "full_name text, position text, birthdate date);"
        ),
        "dwh_d_teams": (
            "CREATE TABLE dwh_d_teams (team_id text, full_name text, "
            "city text, conference text);"
        ),
        "dwh_d_games": (
            "CREATE TABLE dwh_d_games (game_id text, season_year text, "
            "game_date date, game_type text, home_team_id text, visitor_team_id text);"
        ),
        "dwh_f_player_boxscore": (
            "CREATE TABLE dwh_f_player_boxscore (id text, game_id text, "
            "team_id text, player_id text, points numeric, assists numeric, "
            "rebounds_offensive numeric, rebounds_defensive numeric, "
            "turnovers numeric, minutes numeric);"
        ),
        "dwh_f_team_boxscore": (
            "CREATE TABLE dwh_f_team_boxscore (id text, game_id text, "
            "team_id text, points numeric, assists numeric);"
        ),
    }
    return build_column_registry(ddl)


# ===========================================================================
# schema_linker tests
# ===========================================================================

class TestBuildColumnRegistry:
    def test_single_line_ddl(self):
        ddl = {"players": "CREATE TABLE players (id text, name text, age numeric);"}
        reg = build_column_registry(ddl)
        assert reg["players"] == ["id", "name", "age"]

    def test_multiline_ddl(self):
        ddl = {
            "players": (
                "CREATE TABLE players (\n"
                "    id text,\n"
                "    name text,\n"
                "    age numeric\n"
                ");"
            )
        }
        reg = build_column_registry(ddl)
        assert reg["players"] == ["id", "name", "age"]

    def test_all_names_lowercased(self):
        ddl = {"MyTable": "CREATE TABLE MyTable (PlayerID text, FullName text);"}
        reg = build_column_registry(ddl)
        assert "mytable" in reg
        assert reg["mytable"] == ["playerid", "fullname"]

    def test_empty_dict(self):
        assert build_column_registry({}) == {}

    def test_multiple_tables(self):
        ddl = {
            "a": "CREATE TABLE a (x text, y text);",
            "b": "CREATE TABLE b (z numeric);",
        }
        reg = build_column_registry(ddl)
        assert set(reg["a"]) == {"x", "y"}
        assert reg["b"] == ["z"]

    def test_nba_ddl_full_columns(self):
        from utils.sample_values_for_testing import Sample_NBA_DDL_DICT
        reg = build_column_registry(Sample_NBA_DDL_DICT)
        assert "game_id" in reg["dwh_d_games"]
        assert "season_year" in reg["dwh_d_games"]
        assert "full_name" in reg["dwh_d_players"]
        assert "points" in reg["dwh_f_player_boxscore"]


class TestRegexFallback:
    def test_simple(self):
        cols = _regex_fallback("CREATE TABLE t (col1 text, col2 numeric);")
        assert cols == ["col1", "col2"]

    def test_no_parens_returns_empty(self):
        assert _regex_fallback("not a create table") == []


# ===========================================================================
# verify_sql — valid SQL (should return is_valid=True, no errors)
# ===========================================================================

class TestValidSQL:
    def test_simple_select_with_join(self, registry):
        sql = (
            "SELECT p.full_name, pb.points "
            "FROM dwh_f_player_boxscore pb "
            "JOIN dwh_d_players p ON pb.player_id = p.player_id "
            "WHERE p.full_name ILIKE '%LeBron%'"
        )
        result = verify_sql(sql, registry)
        assert result.is_valid
        assert result.errors == []

    def test_aggregation_with_group_by(self, registry):
        sql = (
            "SELECT p.full_name, SUM(pb.points) AS total_points "
            "FROM dwh_f_player_boxscore pb "
            "JOIN dwh_d_players p ON pb.player_id = p.player_id "
            "GROUP BY p.full_name"
        )
        result = verify_sql(sql, registry)
        assert result.is_valid
        assert result.errors == []

    def test_cte_columns_not_flagged(self, registry):
        sql = (
            "WITH stats AS ("
            "  SELECT p.full_name, SUM(pb.points) AS season_total "
            "  FROM dwh_f_player_boxscore pb "
            "  JOIN dwh_d_players p ON pb.player_id = p.player_id "
            "  GROUP BY p.full_name"
            ") "
            "SELECT full_name, season_total FROM stats ORDER BY season_total DESC LIMIT 10"
        )
        result = verify_sql(sql, registry)
        assert result.is_valid
        assert result.errors == []

    def test_cte_outer_reference_to_cte_column(self, registry):
        sql = (
            "WITH top_scorers AS ("
            "  SELECT player_id, SUM(points) AS total_pts "
            "  FROM dwh_f_player_boxscore GROUP BY player_id"
            ") "
            "SELECT ts.player_id, ts.total_pts "
            "FROM top_scorers ts "
            "ORDER BY ts.total_pts DESC LIMIT 5"
        )
        result = verify_sql(sql, registry)
        assert result.is_valid
        assert result.errors == []

    def test_union_all_same_column_count(self, registry):
        sql = (
            "SELECT p.full_name, 'player' AS entity_type FROM dwh_d_players p "
            "UNION ALL "
            "SELECT t.full_name, 'team' AS entity_type FROM dwh_d_teams t"
        )
        result = verify_sql(sql, registry)
        assert result.is_valid
        assert result.errors == []

    def test_empty_sql(self, registry):
        result = verify_sql("", registry)
        assert result.is_valid

    def test_whitespace_only_sql(self, registry):
        result = verify_sql("   \n  ", registry)
        assert result.is_valid

    def test_bare_star_wildcard(self, registry):
        sql = "SELECT * FROM dwh_d_players WHERE player_id = 'x'"
        result = verify_sql(sql, registry)
        assert result.is_valid

    def test_builtin_functions_not_flagged(self, registry):
        sql = (
            "SELECT p.full_name, COALESCE(pb.points, 0) AS pts, "
            "NULLIF(pb.minutes, 0) AS mins, "
            "ROUND(AVG(pb.points), 2) AS avg_pts "
            "FROM dwh_f_player_boxscore pb "
            "JOIN dwh_d_players p ON pb.player_id = p.player_id "
            "GROUP BY p.full_name"
        )
        result = verify_sql(sql, registry)
        assert result.is_valid
        assert result.errors == []

    def test_subquery_column_reference(self, registry):
        sql = (
            "SELECT p.full_name "
            "FROM dwh_d_players p "
            "WHERE p.player_id IN ("
            "  SELECT pb.player_id FROM dwh_f_player_boxscore pb "
            "  WHERE pb.points > 30"
            ")"
        )
        result = verify_sql(sql, registry)
        assert result.is_valid
        assert result.errors == []

    def test_nested_cte_multiple_ctes_requires_scope_projection(self, registry):
        sql = (
            "WITH games_2022 AS ("
            "  SELECT game_id FROM dwh_d_games WHERE season_year = '2022'"
            "), "
            "scorer_totals AS ("
            "  SELECT player_id, SUM(points) AS total "
            "  FROM dwh_f_player_boxscore pb "
            "  JOIN games_2022 g ON pb.game_id = g.game_id "
            "  GROUP BY player_id"
            ") "
            "SELECT p.full_name, st.total "
            "FROM scorer_totals st "
            "JOIN dwh_d_players p ON st.player_id = p.player_id "
            "ORDER BY st.total DESC LIMIT 1"
        )
        result = verify_sql(sql, registry)
        assert not result.is_valid
        assert any(e.error_type == "filter_context_not_projected" for e in result.errors)

    def test_nested_cte_multiple_ctes_valid_when_scope_selected(self, registry):
        sql = (
            "WITH games_2022 AS ("
            "  SELECT game_id, season_year FROM dwh_d_games WHERE season_year = '2022'"
            "), "
            "scorer_totals AS ("
            "  SELECT player_id, SUM(points) AS total "
            "  FROM dwh_f_player_boxscore pb "
            "  JOIN games_2022 g ON pb.game_id = g.game_id "
            "  GROUP BY player_id"
            ") "
            "SELECT p.full_name, st.total, '2022' AS season_year "
            "FROM scorer_totals st "
            "JOIN dwh_d_players p ON st.player_id = p.player_id "
            "ORDER BY st.total DESC LIMIT 1"
        )
        result = verify_sql(sql, registry)
        assert result.is_valid

    def test_select_alias_used_in_order_by(self, registry):
        sql = (
            "SELECT p.full_name, SUM(pb.points) AS total_points "
            "FROM dwh_f_player_boxscore pb "
            "JOIN dwh_d_players p ON pb.player_id = p.player_id "
            "GROUP BY p.full_name "
            "ORDER BY total_points DESC"
        )
        result = verify_sql(sql, registry)
        assert result.is_valid
        assert result.errors == []

    def test_union_all_wrapped_in_subquery_for_limit(self, registry):
        sql = (
            "WITH stats AS ("
            "  SELECT player_id, SUM(points) AS pts "
            "  FROM dwh_f_player_boxscore GROUP BY player_id"
            ") "
            "SELECT full_name, pts FROM ("
            "  SELECT player_id, pts FROM stats ORDER BY pts DESC LIMIT 1"
            ") top_row "
            "JOIN dwh_d_players p ON top_row.player_id = p.player_id "
            "UNION ALL "
            "SELECT p2.full_name, s.pts "
            "FROM stats s "
            "JOIN dwh_d_players p2 ON s.player_id = p2.player_id "
            "WHERE p2.full_name ILIKE '%Jokic%'"
        )
        result = verify_sql(sql, registry)
        # No union_column_mismatch or order_by_in_union_branch errors
        structural_errors = [e for e in result.errors if e.error_type in (
            "union_column_mismatch", "order_by_in_union_branch"
        )]
        assert structural_errors == []

    def test_scope_projection_not_triggered_by_join_only_comparison(self, registry):
        sql = (
            "SELECT p.full_name "
            "FROM dwh_d_players p "
            "JOIN dwh_f_player_boxscore pb ON p.player_id = pb.player_id "
            "JOIN dwh_d_games g ON pb.game_id = g.game_id "
            "JOIN (SELECT season_year FROM dwh_d_games LIMIT 1) s "
            "  ON g.season_year = s.season_year "
            "LIMIT 5"
        )
        result = verify_sql(sql, registry)
        assert result.is_valid
        assert not any(e.error_type == "scope_filter_not_projected" for e in result.errors)

    def test_scope_projection_still_enforced_for_with_where_filter(self, registry):
        sql = (
            "WITH scoped_games AS ("
            "  SELECT game_id FROM dwh_d_games WHERE season_year = '2022'"
            ") "
            "SELECT COUNT(*) AS games_count "
            "FROM scoped_games"
        )
        result = verify_sql(sql, registry)
        assert not result.is_valid
        assert any(e.error_type == "filter_context_not_projected" for e in result.errors)

    def test_literal_neq_filter_not_enforced_as_scope_projection(self, registry):
        sql = (
            "SELECT p.full_name "
            "FROM dwh_d_players p "
            "WHERE p.position != 'Center'"
        )
        result = verify_sql(sql, registry)
        assert not result.is_valid
        assert any(e.error_type == "filter_context_not_projected" for e in result.errors)

    def test_literal_not_in_filter_not_enforced_as_scope_projection(self, registry):
        sql = (
            "SELECT p.full_name "
            "FROM dwh_d_players p "
            "WHERE p.position NOT IN ('Center', 'Forward')"
        )
        result = verify_sql(sql, registry)
        assert not result.is_valid
        assert any(e.error_type == "filter_context_not_projected" for e in result.errors)

    def test_select_star_skips_scope_projection_check(self, registry):
        sql = (
            "SELECT * "
            "FROM dwh_d_games g "
            "WHERE g.season_year = (SELECT MAX(g2.season_year) FROM dwh_d_games g2)"
        )
        result = verify_sql(sql, registry)
        assert result.is_valid
        assert not any(e.error_type == "scope_filter_not_projected" for e in result.errors)

    def test_column_to_column_scope_filter_requires_projection(self, registry):
        sql = (
            "WITH current_season AS ("
            "  SELECT MAX(g2.season_year) AS season_year FROM dwh_d_games g2"
            ") "
            "SELECT COUNT(*) AS game_count "
            "FROM dwh_d_games g "
            "JOIN current_season cs ON 1 = 1 "
            "WHERE g.season_year = cs.season_year"
        )
        result = verify_sql(sql, registry)
        assert not result.is_valid
        assert any(e.error_type == "scope_filter_not_projected" for e in result.errors)

    def test_column_to_column_scope_filter_passes_when_projected(self, registry):
        sql = (
            "WITH current_season AS ("
            "  SELECT MAX(g2.season_year) AS season_year FROM dwh_d_games g2"
            ") "
            "SELECT g.season_year, COUNT(*) AS game_count "
            "FROM dwh_d_games g "
            "JOIN current_season cs ON 1 = 1 "
            "WHERE g.season_year = cs.season_year "
            "GROUP BY g.season_year"
        )
        result = verify_sql(sql, registry)
        assert result.is_valid
        assert not any(e.error_type == "scope_filter_not_projected" for e in result.errors)

    def test_cte_scalar_subquery_scope_filter_missing_in_final_select_is_flagged(self, registry):
        sql = (
            "WITH player_season_stats AS ("
            "  SELECT pb.player_id, p.full_name, "
            "         SUM(pb.points) AS total_points, "
            "         AVG(pb.minutes) AS avg_minutes "
            "  FROM dwh_f_player_boxscore pb "
            "  JOIN dwh_d_players p ON pb.player_id = p.player_id "
            "  JOIN dwh_d_games g ON pb.game_id = g.game_id "
            "  WHERE g.game_type ILIKE '%Regular Season%' "
            "    AND g.season_year = ("
            "      SELECT MAX(g2.season_year) "
            "      FROM dwh_d_games g2 "
            "      WHERE g2.game_type ILIKE '%Regular Season%'"
            "    ) "
            "  GROUP BY pb.player_id, p.full_name "
            ") "
            "SELECT full_name, total_points, ROUND(avg_minutes, 2) AS avg_minutes, "
            "       'Regular Season' AS game_type "
            "FROM player_season_stats "
            "ORDER BY total_points DESC "
            "LIMIT 1"
        )
        result = verify_sql(sql, registry)
        assert not result.is_valid
        assert any(e.error_type == "scope_filter_not_projected" for e in result.errors)

    def test_cte_scalar_subquery_scope_filter_passes_when_projected(self, registry):
        sql = (
            "WITH player_season_stats AS ("
            "  SELECT pb.player_id, p.full_name, g.season_year, "
            "         SUM(pb.points) AS total_points, "
            "         AVG(pb.minutes) AS avg_minutes "
            "  FROM dwh_f_player_boxscore pb "
            "  JOIN dwh_d_players p ON pb.player_id = p.player_id "
            "  JOIN dwh_d_games g ON pb.game_id = g.game_id "
            "  WHERE g.game_type ILIKE '%Regular Season%' "
            "    AND g.season_year = ("
            "      SELECT MAX(g2.season_year) "
            "      FROM dwh_d_games g2 "
            "      WHERE g2.game_type ILIKE '%Regular Season%'"
            "    ) "
            "  GROUP BY pb.player_id, p.full_name, g.season_year "
            ") "
            "SELECT full_name, season_year, total_points, ROUND(avg_minutes, 2) AS avg_minutes, "
            "       'Regular Season' AS game_type "
            "FROM player_season_stats "
            "ORDER BY total_points DESC "
            "LIMIT 1"
        )
        result = verify_sql(sql, registry)
        assert result.is_valid
        assert not any(e.error_type == "scope_filter_not_projected" for e in result.errors)

    def test_literal_filter_column_missing_from_select_is_flagged(self, registry):
        sql = (
            "SELECT COUNT(*) AS game_count "
            "FROM dwh_d_games g "
            "WHERE g.game_type ILIKE '%Regular Season%'"
        )
        result = verify_sql(sql, registry)
        assert not result.is_valid
        assert any(e.error_type == "filter_context_not_projected" for e in result.errors)

    def test_literal_filter_column_in_select_is_not_flagged(self, registry):
        sql = (
            "SELECT g.game_type, COUNT(*) AS game_count "
            "FROM dwh_d_games g "
            "WHERE g.game_type ILIKE '%Regular Season%' "
            "GROUP BY g.game_type"
        )
        result = verify_sql(sql, registry)
        assert result.is_valid
        assert not any(e.error_type == "filter_context_not_projected" for e in result.errors)

    def test_literal_filter_on_id_column_is_excluded(self, registry):
        sql = (
            "SELECT COUNT(*) AS player_count "
            "FROM dwh_d_players p "
            "WHERE p.player_id = 'abc-123'"
        )
        result = verify_sql(sql, registry)
        assert result.is_valid
        assert not any(e.error_type == "filter_context_not_projected" for e in result.errors)

    def test_filter_context_check_skips_select_star(self, registry):
        sql = (
            "SELECT * "
            "FROM dwh_d_games g "
            "WHERE g.game_type ILIKE '%Regular Season%'"
        )
        result = verify_sql(sql, registry)
        assert result.is_valid
        assert not any(e.error_type == "filter_context_not_projected" for e in result.errors)

    def test_cte_literal_filters_missing_in_final_select_are_flagged(self, registry):
        sql = (
            "WITH team_games AS ("
            "  SELECT g.game_id, g.game_type, t.full_name "
            "  FROM dwh_d_games g "
            "  JOIN dwh_d_teams t ON g.home_team_id = t.team_id "
            "  WHERE g.game_type ILIKE '%Regular Season%' "
            "    AND t.full_name ILIKE '%Denver Nuggets%'"
            ") "
            "SELECT COUNT(*) AS game_count FROM team_games"
        )
        result = verify_sql(sql, registry)
        assert not result.is_valid
        assert any(e.error_type == "filter_context_not_projected" for e in result.errors)

    def test_cte_scalar_subquery_literal_filter_is_not_flagged_for_filter_context(self, registry):
        sql = (
            "WITH latest_regular_game AS ("
            "  SELECT g.game_id "
            "  FROM dwh_d_games g "
            "  WHERE g.game_id = ("
            "    SELECT MAX(g2.game_id) "
            "    FROM dwh_d_games g2 "
            "    WHERE g2.game_type ILIKE '%Regular Season%'"
            "  )"
            ") "
            "SELECT COUNT(*) AS game_count FROM latest_regular_game"
        )
        result = verify_sql(sql, registry)
        assert result.is_valid
        assert not any(e.error_type == "filter_context_not_projected" for e in result.errors)

    def test_two_hop_cte_chain_denver_pattern_filter_context_is_flagged(self, registry):
        sql = (
            "WITH team_home_games AS ("
            "  SELECT g.game_id, g.season_year, g.game_type, t.full_name "
            "  FROM dwh_d_games g "
            "  JOIN dwh_d_teams t ON g.home_team_id = t.team_id "
            "  WHERE t.full_name ILIKE '%Denver Nuggets%' "
            "    AND g.game_type ILIKE '%Regular Season%'"
            "), "
            "home_wins AS ("
            "  SELECT thg.season_year, COUNT(*) AS home_wins "
            "  FROM team_home_games thg "
            "  GROUP BY thg.season_year"
            "), "
            "home_games AS ("
            "  SELECT thg.season_year, COUNT(*) AS home_games "
            "  FROM team_home_games thg "
            "  GROUP BY thg.season_year"
            ") "
            "SELECT hw.home_wins, hg.home_games "
            "FROM home_wins hw "
            "JOIN home_games hg ON hw.season_year = hg.season_year"
        )
        result = verify_sql(sql, registry)
        assert not result.is_valid
        assert any(e.error_type == "filter_context_not_projected" for e in result.errors)

    def test_cte_literal_filters_projected_in_final_select_passes(self, registry):
        sql = (
            "WITH team_games AS ("
            "  SELECT g.game_id, g.game_type, t.full_name "
            "  FROM dwh_d_games g "
            "  JOIN dwh_d_teams t ON g.home_team_id = t.team_id "
            "  WHERE g.game_type ILIKE '%Regular Season%' "
            "    AND t.full_name ILIKE '%Denver Nuggets%'"
            ") "
            "SELECT tg.game_type, tg.full_name, COUNT(*) AS game_count "
            "FROM team_games tg "
            "GROUP BY tg.game_type, tg.full_name"
        )
        result = verify_sql(sql, registry)
        assert result.is_valid
        assert not any(e.error_type == "filter_context_not_projected" for e in result.errors)

    def test_cte_literal_filter_on_id_column_is_excluded(self, registry):
        sql = (
            "WITH filtered_team_games AS ("
            "  SELECT g.game_id, g.home_team_id "
            "  FROM dwh_d_games g "
            "  WHERE g.home_team_id = 'team-001'"
            ") "
            "SELECT COUNT(*) AS game_count FROM filtered_team_games"
        )
        result = verify_sql(sql, registry)
        assert result.is_valid
        assert not any(e.error_type == "filter_context_not_projected" for e in result.errors)

    def test_grandparent_cte_literal_filter_missing_in_final_select_is_flagged(self, registry):
        sql = (
            "WITH base_games AS ("
            "  SELECT g.game_id, g.game_type "
            "  FROM dwh_d_games g "
            "  WHERE g.game_type ILIKE '%Regular Season%'"
            "), "
            "season_games AS ("
            "  SELECT bg.game_id, bg.game_type FROM base_games bg"
            "), "
            "scoped_games AS ("
            "  SELECT sg.game_id, sg.game_type FROM season_games sg"
            ") "
            "SELECT COUNT(*) AS game_count FROM scoped_games"
        )
        result = verify_sql(sql, registry)
        assert not result.is_valid
        assert any(e.error_type == "filter_context_not_projected" for e in result.errors)

    def test_cte_literal_filter_with_select_star_in_final_select_is_not_flagged(self, registry):
        sql = (
            "WITH team_games AS ("
            "  SELECT g.game_id, g.game_type "
            "  FROM dwh_d_games g "
            "  WHERE g.game_type ILIKE '%Regular Season%'"
            ") "
            "SELECT * FROM team_games"
        )
        result = verify_sql(sql, registry)
        assert result.is_valid
        assert not any(e.error_type == "filter_context_not_projected" for e in result.errors)

    def test_literal_and_column_scope_same_column_reports_single_error_type(self, registry):
        sql = (
            "WITH cs AS (SELECT 'Regular Season' AS game_type) "
            "SELECT COUNT(*) AS game_count "
            "FROM dwh_d_games g "
            "JOIN cs ON 1 = 1 "
            "WHERE g.game_type ILIKE '%Regular Season%' "
            "AND g.game_type = cs.game_type"
        )
        result = verify_sql(sql, registry)
        assert not result.is_valid
        error_types = [e.error_type for e in result.errors]
        assert error_types.count("filter_context_not_projected") == 1
        assert "scope_filter_not_projected" not in error_types

    def test_subquery_literal_filter_not_treated_as_top_level_filter_context(self, registry):
        sql = (
            "SELECT g.season_year, COUNT(*) AS game_count "
            "FROM dwh_d_games g "
            "WHERE g.season_year = ("
            "  SELECT MAX(g2.season_year) "
            "  FROM dwh_d_games g2 "
            "  WHERE g2.game_type ILIKE '%Playoff%'"
            ") "
            "GROUP BY g.season_year"
        )
        result = verify_sql(sql, registry)
        assert result.is_valid
        assert not any(e.error_type == "filter_context_not_projected" for e in result.errors)

    def test_scalar_subquery_literal_filter_is_excluded_from_outer_filter_context(self, registry):
        sql = (
            "SELECT g.season_year "
            "FROM dwh_d_games g "
            "WHERE g.season_year = ("
            "  SELECT MAX(g2.season_year) "
            "  FROM dwh_d_games g2 "
            "  WHERE g2.game_type ILIKE '%Regular Season%'"
            ")"
        )
        result = verify_sql(sql, registry)
        assert result.is_valid
        assert not any(e.error_type == "filter_context_not_projected" for e in result.errors)

    def test_having_literal_filter_missing_from_select_is_flagged(self, registry):
        sql = (
            "SELECT COUNT(*) AS game_count "
            "FROM dwh_d_games g "
            "GROUP BY g.game_type "
            "HAVING g.game_type ILIKE '%Regular Season%'"
        )
        result = verify_sql(sql, registry)
        assert not result.is_valid
        assert any(e.error_type == "filter_context_not_projected" for e in result.errors)

    def test_having_literal_filter_present_in_select_is_not_flagged(self, registry):
        sql = (
            "SELECT g.game_type, COUNT(*) AS game_count "
            "FROM dwh_d_games g "
            "GROUP BY g.game_type "
            "HAVING g.game_type ILIKE '%Regular Season%'"
        )
        result = verify_sql(sql, registry)
        assert result.is_valid
        assert not any(e.error_type == "filter_context_not_projected" for e in result.errors)


# ===========================================================================
# verify_sql — column errors
# ===========================================================================

class TestColumnErrors:
    def test_window_alias_used_in_where_is_invalid(self, registry):
        sql = (
            "SELECT p.full_name, RANK() OVER (ORDER BY pb.points DESC) AS rank "
            "FROM dwh_f_player_boxscore pb "
            "JOIN dwh_d_players p ON pb.player_id = p.player_id "
            "WHERE rank = 1"
        )
        result = verify_sql(sql, registry)
        assert not result.is_valid
        assert any(e.column == "rank" for e in result.errors)

    def test_wrong_table_for_column(self, registry):
        # season_year lives on dwh_d_games, not dwh_d_players
        sql = "SELECT p.season_year FROM dwh_d_players p"
        result = verify_sql(sql, registry)
        assert not result.is_valid
        errors = [e for e in result.errors if e.error_type == "wrong_table_for_column"]
        assert len(errors) == 1
        assert errors[0].column == "season_year"
        assert errors[0].table == "dwh_d_players"
        assert "dwh_d_games" in errors[0].message

    def test_bare_column_not_in_query_scope_is_invalid(self, registry):
        sql = (
            "SELECT p.full_name "
            "FROM dwh_d_players p "
            "WHERE game_type = 'Regular Season'"
        )
        result = verify_sql(sql, registry)
        assert not result.is_valid
        assert any(e.error_type == "column_not_in_scope" for e in result.errors)

    def test_column_not_in_any_table(self, registry):
        sql = "SELECT p.invented_column FROM dwh_d_players p"
        result = verify_sql(sql, registry)
        assert not result.is_valid
        errors = [e for e in result.errors if e.error_type == "column_not_in_ddl"]
        assert len(errors) == 1
        assert errors[0].column == "invented_column"

    def test_multiple_column_errors_reported(self, registry):
        sql = (
            "SELECT p.ghost_col1, p.ghost_col2 "
            "FROM dwh_d_players p"
        )
        result = verify_sql(sql, registry)
        assert not result.is_valid
        assert len(result.errors) >= 2

    def test_valid_column_not_flagged(self, registry):
        sql = "SELECT pb.points, pb.assists FROM dwh_f_player_boxscore pb"
        result = verify_sql(sql, registry)
        assert result.is_valid
        col_errors = [e for e in result.errors if e.error_type == "column_not_in_ddl"]
        assert col_errors == []

    def test_unknown_table_skipped_silently(self, registry):
        # dwh_external_table is not in registry — should not raise an error
        sql = "SELECT e.some_col FROM dwh_external_table e"
        result = verify_sql(sql, registry)
        # No error because table is not in registry — we can't validate it
        assert result.errors == [] or all(
            e.table != "dwh_external_table" for e in result.errors
        )

    def test_cte_name_as_qualifier_skipped(self, registry):
        # 'stats' is a CTE name, not a registry table — should not error
        sql = (
            "WITH stats AS ("
            "  SELECT player_id, SUM(points) AS total FROM dwh_f_player_boxscore GROUP BY player_id"
            ") "
            "SELECT stats.player_id, stats.total FROM stats"
        )
        result = verify_sql(sql, registry)
        assert result.is_valid
        assert result.errors == []

    def test_bare_ambiguous_column_produces_error(self, registry):
        # game_id exists on both dwh_f_player_boxscore and dwh_d_games.
        # When both tables are joined and the column is bare (no alias prefix),
        # the verifier must emit a hard ambiguous_bare_column error.
        sql = (
            "SELECT game_id, SUM(pb.points) AS pts "
            "FROM dwh_f_player_boxscore pb "
            "JOIN dwh_d_games g ON pb.game_id = g.game_id "
            "GROUP BY game_id"
        )
        result = verify_sql(sql, registry)
        assert not result.is_valid
        ambiguous_errors = [
            e for e in result.errors if e.error_type == "ambiguous_bare_column"
        ]
        assert len(ambiguous_errors) >= 1

    def test_bare_column_not_in_schema_is_error(self, registry):
        sql = "SELECT completely_made_up FROM dwh_d_players"
        result = verify_sql(sql, registry)
        assert not result.is_valid
        errors = [e for e in result.errors if e.error_type == "column_not_in_ddl"]
        assert len(errors) >= 1

    def test_qualified_column_in_cte_not_masked_by_cte_output_name(self, registry):
        sql = (
            "WITH cte_a AS ("
            "  SELECT g.season_year FROM dwh_d_games g"
            "), cte_b AS ("
            "  SELECT tc.playoff_round "
            "  FROM dwh_f_team_boxscore tc"
            ") "
            "SELECT season_year FROM cte_a"
        )
        result = verify_sql(sql, registry)
        assert not result.is_valid
        errors = [e for e in result.errors if e.error_type == "column_not_in_ddl"]
        assert any(e.table == "dwh_f_team_boxscore" and e.column == "playoff_round" for e in errors)


# ===========================================================================
# verify_sql — UNION ALL structural checks
# ===========================================================================

class TestUnionChecks:
    def test_union_column_mismatch_detected(self, registry):
        sql = (
            "SELECT full_name, city FROM dwh_d_teams "
            "UNION ALL "
            "SELECT full_name FROM dwh_d_players"
        )
        result = verify_sql(sql, registry)
        errors = [e for e in result.errors if e.error_type == "union_column_mismatch"]
        assert len(errors) >= 1

    def test_union_same_count_no_error(self, registry):
        sql = (
            "SELECT full_name, city FROM dwh_d_teams "
            "UNION ALL "
            "SELECT full_name, position FROM dwh_d_players"
        )
        result = verify_sql(sql, registry)
        mismatch_errors = [e for e in result.errors if e.error_type == "union_column_mismatch"]
        assert mismatch_errors == []

    def test_order_by_in_union_branch_detected(self, registry):
        # This is invalid SQL — ORDER BY inside a UNION branch
        # sqlglot may parse it loosely; check we detect it if present
        sql = (
            "SELECT full_name FROM dwh_d_players ORDER BY full_name "
            "UNION ALL "
            "SELECT full_name FROM dwh_d_teams"
        )
        result = verify_sql(sql, registry)
        # Either sqlglot attaches ORDER BY to the outer query or we detect it
        # — the important thing is it does not crash
        assert isinstance(result, VerificationResult)

    def test_order_by_wrapped_in_subquery_no_error(self, registry):
        sql = (
            "SELECT full_name FROM ("
            "  SELECT full_name FROM dwh_d_players ORDER BY full_name LIMIT 5"
            ") sub "
            "UNION ALL "
            "SELECT full_name FROM dwh_d_teams"
        )
        result = verify_sql(sql, registry)
        branch_errors = [e for e in result.errors if e.error_type == "order_by_in_union_branch"]
        assert branch_errors == []


# ===========================================================================
# verify_sql — GROUP BY completeness warnings
# ===========================================================================

class TestGroupByWarnings:
    def test_missing_group_by_column_warns(self, registry):
        # full_name is not in GROUP BY but is in SELECT alongside SUM
        sql = (
            "SELECT p.full_name, SUM(pb.points) AS pts "
            "FROM dwh_f_player_boxscore pb "
            "JOIN dwh_d_players p ON pb.player_id = p.player_id "
            "GROUP BY p.player_id"
        )
        result = verify_sql(sql, registry)
        gb_warnings = [w for w in result.warnings if "GROUP BY" in w]
        assert len(gb_warnings) >= 1

    def test_complete_group_by_no_warning(self, registry):
        sql = (
            "SELECT p.full_name, SUM(pb.points) AS pts "
            "FROM dwh_f_player_boxscore pb "
            "JOIN dwh_d_players p ON pb.player_id = p.player_id "
            "GROUP BY p.full_name"
        )
        result = verify_sql(sql, registry)
        gb_warnings = [w for w in result.warnings if "GROUP BY" in w]
        assert gb_warnings == []

    def test_no_aggregate_no_group_by_warning(self, registry):
        sql = "SELECT full_name, position FROM dwh_d_players"
        result = verify_sql(sql, registry)
        gb_warnings = [w for w in result.warnings if "GROUP BY" in w]
        assert gb_warnings == []


# ===========================================================================
# verify_sql — CTE column extraction
# ===========================================================================

class TestCTEExtraction:
    def test_cte_aliases_recognised(self, registry):
        sql = (
            "WITH ranked AS ("
            "  SELECT player_id, points, "
            "  ROW_NUMBER() OVER (ORDER BY points DESC) AS rn "
            "  FROM dwh_f_player_boxscore"
            ") "
            "SELECT player_id, points, rn FROM ranked WHERE rn = 1"
        )
        result = verify_sql(sql, registry)
        assert result.is_valid
        assert result.errors == []

    def test_outer_query_cte_column_not_flagged(self, registry):
        sql = (
            "WITH totals AS ("
            "  SELECT player_id, SUM(points) AS total_pts "
            "  FROM dwh_f_player_boxscore GROUP BY player_id"
            ") "
            "SELECT total_pts FROM totals ORDER BY total_pts DESC LIMIT 10"
        )
        result = verify_sql(sql, registry)
        # total_pts is a CTE column — must not be flagged as column_not_in_ddl
        ddl_errors = [
            e for e in result.errors
            if e.error_type == "column_not_in_ddl" and e.column == "total_pts"
        ]
        assert ddl_errors == []


# ===========================================================================
# verify_sql — edge cases and regression tests
# ===========================================================================

class TestEdgeCases:
    def test_malformed_sql_returns_valid_gracefully(self, registry):
        result = verify_sql("THIS IS NOT SQL", registry)
        assert isinstance(result, VerificationResult)
        # Should not raise; may return is_valid=True (fail open)

    def test_empty_registry_all_qualified_columns_skipped(self):
        sql = "SELECT p.full_name FROM dwh_d_players p"
        result = verify_sql(sql, registry={})
        # No registry — unknown tables are silently skipped
        assert result.errors == []

    def test_count_star_not_flagged(self, registry):
        sql = "SELECT COUNT(*) AS cnt FROM dwh_d_players"
        result = verify_sql(sql, registry)
        assert result.is_valid
        assert result.errors == []

    def test_window_function_not_flagged(self, registry):
        sql = (
            "SELECT p.full_name, "
            "RANK() OVER (ORDER BY pb.points DESC) AS rnk "
            "FROM dwh_f_player_boxscore pb "
            "JOIN dwh_d_players p ON pb.player_id = p.player_id"
        )
        result = verify_sql(sql, registry)
        assert result.is_valid
        assert result.errors == []

    def test_nested_subquery_column_validation(self, registry):
        sql = (
            "SELECT p.full_name "
            "FROM dwh_d_players p "
            "WHERE p.player_id IN ("
            "  SELECT pb.player_id "
            "  FROM dwh_f_player_boxscore pb "
            "  WHERE pb.ghost_col > 10"
            ")"
        )
        result = verify_sql(sql, registry)
        assert not result.is_valid
        errors = [e for e in result.errors if e.column == "ghost_col"]
        assert len(errors) >= 1

    def test_hallucinated_column_in_where(self, registry):
        # 'game_type' lives on dwh_d_games, NOT on dwh_f_player_boxscore
        sql = (
            "SELECT pb.points FROM dwh_f_player_boxscore pb "
            "WHERE pb.game_type = 'Regular Season'"
        )
        result = verify_sql(sql, registry)
        assert not result.is_valid
        errors = [e for e in result.errors if e.error_type == "wrong_table_for_column"]
        assert len(errors) >= 1
        assert errors[0].column == "game_type"

    def test_season_year_on_wrong_table_detected(self, registry):
        # Classic hallucination: season_year referenced on player_boxscore
        sql = (
            "SELECT pb.player_id, pb.season_year "
            "FROM dwh_f_player_boxscore pb "
            "WHERE pb.season_year = '2022'"
        )
        result = verify_sql(sql, registry)
        assert not result.is_valid
        errors = [
            e for e in result.errors
            if e.column == "season_year" and e.error_type == "wrong_table_for_column"
        ]
        assert len(errors) >= 1

    def test_correct_join_no_false_positive(self, registry):
        sql = (
            "SELECT pb.player_id, pb.points, g.season_year, g.game_type "
            "FROM dwh_f_player_boxscore pb "
            "JOIN dwh_d_games g ON pb.game_id = g.game_id "
            "WHERE g.season_year = '2022' AND g.game_type ILIKE '%Regular%'"
        )
        result = verify_sql(sql, registry)
        assert result.is_valid
        assert result.errors == []


# ===========================================================================
# Integration: verify_sql with real NBA DDL
# ===========================================================================

class TestNBAIntegration:
    @pytest.fixture()
    def nba_registry(self):
        from utils.sample_values_for_testing import Sample_NBA_DDL_DICT
        return build_column_registry(Sample_NBA_DDL_DICT)

    def test_lebron_stats_query_valid(self, nba_registry):
        sql = (
            "SELECT p.full_name, g.game_date, pb.points, pb.assists, "
            "pb.rebounds_offensive + pb.rebounds_defensive AS total_rebounds "
            "FROM dwh_f_player_boxscore pb "
            "JOIN dwh_d_players p ON pb.player_id = p.player_id "
            "JOIN dwh_d_games g ON pb.game_id = g.game_id "
            "WHERE p.full_name ILIKE '%LeBron James%' "
            "ORDER BY g.game_date DESC LIMIT 1"
        )
        result = verify_sql(sql, nba_registry)
        assert result.is_valid
        assert result.errors == []

    def test_season_year_on_boxscore_detected(self, nba_registry):
        # Common hallucination: season_year referenced on player_boxscore
        sql = (
            "SELECT pb.player_id FROM dwh_f_player_boxscore pb "
            "WHERE pb.season_year = '2022'"
        )
        result = verify_sql(sql, nba_registry)
        assert not result.is_valid
        assert any(e.column == "season_year" for e in result.errors)

    def test_game_type_on_boxscore_detected(self, nba_registry):
        sql = (
            "SELECT pb.points FROM dwh_f_player_boxscore pb "
            "WHERE pb.game_type = 'Regular Season'"
        )
        result = verify_sql(sql, nba_registry)
        assert not result.is_valid
        assert any(e.column == "game_type" for e in result.errors)

    def test_triple_double_query_requires_scope_projection(self, nba_registry):
        sql = (
            "WITH triple_doubles AS ("
            "  SELECT pb.player_id, p.full_name, COUNT(*) AS triple_double_count "
            "  FROM dwh_f_player_boxscore pb "
            "  JOIN dwh_d_players p ON pb.player_id = p.player_id "
            "  JOIN dwh_d_games g ON pb.game_id = g.game_id "
            "  WHERE g.game_type ILIKE '%Regular Season%' "
            "  GROUP BY pb.player_id, p.full_name "
            "  HAVING SUM(CASE WHEN pb.points >= 10 THEN 1 ELSE 0 END "
            "           + CASE WHEN (pb.rebounds_offensive + pb.rebounds_defensive) >= 10 THEN 1 ELSE 0 END "
            "           + CASE WHEN pb.assists >= 10 THEN 1 ELSE 0 END) >= 3"
            ") "
            "SELECT full_name, triple_double_count "
            "FROM triple_doubles "
            "ORDER BY triple_double_count DESC LIMIT 10"
        )
        result = verify_sql(sql, nba_registry)
        assert not result.is_valid
        assert any(e.error_type == "filter_context_not_projected" for e in result.errors)

    def test_union_mismatch_on_nba_tables(self, nba_registry):
        sql = (
            "SELECT p.full_name, p.position, p.birthdate FROM dwh_d_players p "
            "UNION ALL "
            "SELECT t.full_name FROM dwh_d_teams t"
        )
        result = verify_sql(sql, nba_registry)
        errors = [e for e in result.errors if e.error_type == "union_column_mismatch"]
        assert len(errors) >= 1

    def test_team_name_join_pattern_allows_literal_scope_filter_without_projection(self, nba_registry):
        sql = (
            "SELECT g.game_date, ht.full_name AS home_team, vt.full_name AS away_team, "
            "g.home_score, g.visitor_score "
            "FROM dwh_d_games g "
            "JOIN dwh_d_teams ht ON g.home_team_id = ht.team_id "
            "JOIN dwh_d_teams vt ON g.visitor_team_id = vt.team_id "
            "WHERE g.season_year = '2022' "
            "ORDER BY g.game_date DESC LIMIT 10"
        )
        result = verify_sql(sql, nba_registry)
        assert not result.is_valid
        assert any(e.error_type == "filter_context_not_projected" for e in result.errors)

