"""unit_tests/test_peer_patch.py

Unit tests for the PEER SQL token-level patching logic in kb_system/peer.py.

Tests cover:
  - Multi-word prefix probing (_build_word_prefixes)
  - Fuzzy matching with dual scorer (token_sort + token_set ratio)
  - Basic equality and LIKE/ILIKE patching
  - Escaped single-quotes (O''Neal round-trip)
  - Function-wrapped LHS (LOWER, TRIM)
  - Qualified column (alias.column)
  - Nested subquery WHERE clauses
  - Comment safety
  - Idempotence (already-correct values are not re-patched)
  - Multiple comparisons in one WHERE
  - RHS inside a CAST() token-list
  - Dollar-quoted and E-prefix strings (unsupported — must be left unchanged)
  - Table-qualifier disambiguation (same column name in two tables)
  - OR-clause same-column multiple values (reported regression)
  - Wildcard variants (leading-only, trailing-only, both)

Run with:
    pytest unit_tests/test_peer_patch.py -v
"""

from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import kb_system.peer as peer_mod
from kb_system.peer import (
    _EntityMatch,
    _patch_python,
    _sql_escape,
    _sql_unescape,
    _build_new_literal,
    _get_column_name,
    _get_qualifier,
    _collect_comparisons,
    _build_word_prefixes,
    _fuzzy_match,
    _probe_ilike_fallback,
    _probe_candidates,
)
import sqlparse


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sub(column: str, table: str, value: str, corrected: str) -> _EntityMatch:
    """Build a substitution-ready _EntityMatch."""
    e = _EntityMatch(column=column, table=table, value=value, operator="ILIKE")
    e.corrected = corrected
    e.action = "auto_sub"
    return e


class _FakeCursor:
    def __init__(self, conn: "_FakeConn"):
        self._conn = conn

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params=None):
        self._conn.calls.append((str(query), tuple(params or ())))

    def fetchall(self):
        if self._conn.fetchall_queue:
            return self._conn.fetchall_queue.pop(0)
        return []

    def fetchone(self):
        rows = self.fetchall()
        return rows[0] if rows else None


class _FakeConn:
    def __init__(self, fetchall_queue=None):
        self.fetchall_queue = list(fetchall_queue or [])
        self.calls = []
        self.rollback_count = 0

    def cursor(self):
        return _FakeCursor(self)

    def rollback(self):
        self.rollback_count += 1


# ---------------------------------------------------------------------------
# Unit tests: _build_word_prefixes
# ---------------------------------------------------------------------------

class TestBuildWordPrefixes:
    def test_single_word(self):
        assert _build_word_prefixes("Joel") == ["jo"]

    def test_two_words(self):
        assert _build_word_prefixes("Joel Embiid") == ["jo", "em"]

    def test_three_words(self):
        assert _build_word_prefixes("LeBron James Jr") == ["le", "ja", "jr"]

    def test_deduplication(self):
        # "Joe" and "John" both start with "jo" — only one prefix kept
        assert _build_word_prefixes("Joe John") == ["jo"]

    def test_accented_name(self):
        # Prefix extraction works on unicode names
        prefixes = _build_word_prefixes("Luka Dončić")
        assert prefixes[0] == "lu"
        assert len(prefixes) == 2

    def test_empty_string(self):
        assert _build_word_prefixes("") == []

    def test_single_char_word(self):
        # Word shorter than PEER_PROBE_PREFIX_LEN — still yields what it can
        prefixes = _build_word_prefixes("A Jordan")
        assert "a" in prefixes
        assert "jo" in prefixes

    def test_output_is_lowercase(self):
        assert _build_word_prefixes("JOEL EMBIID") == ["jo", "em"]

    def test_hyphenated_name_is_split(self):
        assert _build_word_prefixes("Karl-Anthony Towns") == ["ka", "an", "to"]

    def test_apostrophe_name_is_split(self):
        assert _build_word_prefixes("Shaquille O'Neal") == ["sh", "o", "ne"]

    def test_smart_apostrophe_is_split(self):
        assert _build_word_prefixes("D'Angelo O\u2019Neal") == ["d", "an", "o", "ne"]

    def test_repeated_delimiters_are_ignored(self):
        assert _build_word_prefixes("  Karl--Anthony   O''Neal  ") == ["ka", "an", "o", "ne"]


class TestProbeIlikeFallback:
    def test_layer_1_hits_without_fallback(self):
        conn = _FakeConn(fetchall_queue=[[('Karl-Anthony Towns',)]])
        result = _probe_ilike_fallback(conn, "dw.players", "full_name", "Karl Anthony Towns")
        assert result == ["Karl-Anthony Towns"]
        assert len(conn.calls) == 1
        _, params = conn.calls[0]
        assert params[:-1] == ("ka%", "% an%", "% to%")

    def test_layer_2_runs_after_layer_1_miss(self):
        conn = _FakeConn(fetchall_queue=[[], [('Minnesota Timberwolves',)]])
        result = _probe_ilike_fallback(conn, "dw.teams", "full_name", "Minnesota Timber")
        assert result == ["Minnesota Timberwolves"]
        assert len(conn.calls) == 2
        assert conn.calls[1][1][:-1] == ("mi%",)

    def test_layer_3_uses_space_hyphen_and_apostrophe_boundaries(self):
        conn = _FakeConn(fetchall_queue=[[], [], [('Karl-Anthony Towns',)]])
        _probe_ilike_fallback(conn, "dw.players", "full_name", "Anthony Towns")
        assert len(conn.calls) == 3
        layer3_query, layer3_params = conn.calls[2]
        assert "OR full_name ILIKE %s OR full_name ILIKE %s OR full_name ILIKE %s" in layer3_query
        assert layer3_params[:-1] == (
            "an%", "% an%", "%-an%", "%'an%",
            "to%", "% to%", "%-to%", "%'to%",
        )

    def test_layer_3_catches_apostrophe_word_start_patterns(self):
        conn = _FakeConn(fetchall_queue=[[], [], [("Shaquille O'Neal",)]])
        result = _probe_ilike_fallback(conn, "dw.players", "full_name", "O Neal")
        assert result == ["Shaquille O'Neal"]
        layer3_params = conn.calls[2][1][:-1]
        assert "%'ne%" in layer3_params


class TestProbeCandidates:
    def test_empty_value_short_circuits_without_db_calls(self):
        conn = _FakeConn()
        assert _probe_candidates(conn, "dwh_d_players", "full_name", "%%%", "dw") == []
        assert conn.calls == []

    def test_calls_ilike_fallback_with_wildcards_removed(self, monkeypatch):
        conn = _FakeConn()

        monkeypatch.setattr(peer_mod, "_trgm_available", lambda _conn: False)

        captured = {}

        def _fake_probe(_conn, qualified_table, column, value):
            captured["qualified_table"] = qualified_table
            captured["column"] = column
            captured["value"] = value
            return ["LeBron James"]

        monkeypatch.setattr(peer_mod, "_probe_ilike_fallback", _fake_probe)

        result = _probe_candidates(conn, "dwh_d_players", "full_name", "'%LeBron James%'", "dw_dwh")
        assert result == ["LeBron James"]
        assert captured == {
            "qualified_table": "dw_dwh.dwh_d_players",
            "column": "full_name",
            "value": "LeBron James",
        }


# ---------------------------------------------------------------------------
# Unit tests: _fuzzy_match (dual scorer)
# ---------------------------------------------------------------------------

class TestFuzzyMatch:
    def test_exact_match_scores_100(self):
        best, score = _fuzzy_match("LeBron James", ["LeBron James", "Kobe Bryant"])
        assert best == "LeBron James"
        assert score == 100

    def test_suffix_variant_scores_high(self):
        # token_set_ratio handles "Joel Embiid" vs "Joel Embiid III"
        best, score = _fuzzy_match("Joel Embiid", ["Joel Embiid III", "Joe Fabel"])
        assert best == "Joel Embiid III"
        assert score > 80

    def test_word_order_variant_scores_high(self):
        # token_sort_ratio handles reversed word order
        best, score = _fuzzy_match("James LeBron", ["LeBron James", "Kobe Bryant"])
        assert best == "LeBron James"
        assert score > 80

    def test_typo_scores_lower_than_correct(self):
        best, score = _fuzzy_match(
            "Luka Doncic", ["Luka Dončić", "Luke Walton", "Joe Fabel"]
        )
        assert best == "Luka Dončić"

    def test_completely_unrelated_scores_low(self):
        _, score = _fuzzy_match("Mount Everest", ["LeBron James", "Stephen Curry"])
        assert score < 50

    def test_empty_candidates(self):
        best, score = _fuzzy_match("Joel Embiid", [])
        assert best == ""
        assert score == 0


# ---------------------------------------------------------------------------
# Unit tests: _sql_unescape / _sql_escape
# ---------------------------------------------------------------------------

class TestSqlEscapeUnescape:
    def test_unescape_simple(self):
        assert _sql_unescape("'regular'") == "regular"

    def test_unescape_doubled_quote(self):
        assert _sql_unescape("'O''Neal'") == "O'Neal"

    def test_unescape_with_wildcards(self):
        assert _sql_unescape("'%Jimmy Butler%'") == "%Jimmy Butler%"

    def test_unescape_double_quote_style(self):
        assert _sql_unescape('"value"') == "value"

    def test_escape_no_quote(self):
        assert _sql_escape("LeBron James") == "LeBron James"

    def test_escape_with_apostrophe(self):
        assert _sql_escape("O'Brien") == "O''Brien"

    def test_roundtrip(self):
        original = "O'Neal"
        assert _sql_unescape(f"'{_sql_escape(original)}'") == original


# ---------------------------------------------------------------------------
# Unit tests: _build_new_literal
# ---------------------------------------------------------------------------

class TestBuildNewLiteral:
    def test_no_wildcards(self):
        assert _build_new_literal("'regular'", "Regular Season") == "'Regular Season'"

    def test_both_wildcards(self):
        assert _build_new_literal("'%Jimmy Butler%'", "Jimmy Butler III") == "'%Jimmy Butler III%'"

    def test_leading_wildcard_only(self):
        assert _build_new_literal("'%curry'", "Stephen Curry") == "'%Stephen Curry'"

    def test_trailing_wildcard_only(self):
        assert _build_new_literal("'curry%'", "Stephen Curry") == "'Stephen Curry%'"

    def test_preserves_double_quote_style(self):
        assert _build_new_literal('"value"', "New Value") == '"New Value"'

    def test_escaped_apostrophe_in_corrected(self):
        # corrected_escaped has already been through _sql_escape
        assert _build_new_literal("'O''Neal'", "O''Brien") == "'O''Brien'"

    def test_empty_raw_returns_empty(self):
        assert _build_new_literal("", "anything") == ""


# ---------------------------------------------------------------------------
# Unit tests: _get_column_name
# ---------------------------------------------------------------------------

class TestGetColumnName:
    def _left(self, sql: str):
        """Parse a minimal WHERE clause and return the Comparison left token."""
        from kb_system.peer import _collect_comparisons
        parsed = sqlparse.parse(f"SELECT 1 FROM t WHERE {sql}")[0]
        comps: list = []
        _collect_comparisons(parsed, comps)
        assert comps, f"No Comparison found for: {sql}"
        return comps[0].left

    def test_plain_identifier(self):
        assert _get_column_name(self._left("game_type = 'x'")) == "game_type"

    def test_qualified_identifier(self):
        assert _get_column_name(self._left("p.full_name ILIKE '%x%'")) == "full_name"

    def test_lower_unqualified(self):
        assert _get_column_name(self._left("LOWER(game_type) = 'x'")) == "game_type"

    def test_lower_qualified(self):
        assert _get_column_name(self._left("LOWER(p.full_name) = 'x'")) == "full_name"

    def test_trim_qualified(self):
        assert _get_column_name(self._left("TRIM(p.full_name) ILIKE '%x%'")) == "full_name"


# ---------------------------------------------------------------------------
# Unit tests: _get_qualifier
# ---------------------------------------------------------------------------

class TestGetQualifier:
    def _left(self, sql: str):
        from kb_system.peer import _collect_comparisons
        parsed = sqlparse.parse(f"SELECT 1 FROM t WHERE {sql}")[0]
        comps: list = []
        _collect_comparisons(parsed, comps)
        assert comps
        return comps[0].left

    def test_no_qualifier(self):
        assert _get_qualifier(self._left("game_type = 'x'")) == ""

    def test_alias_qualifier(self):
        assert _get_qualifier(self._left("p.full_name ILIKE '%x%'")) == "p"

    def test_function_qualified(self):
        assert _get_qualifier(self._left("LOWER(p.full_name) = 'x'")) == "p"

    def test_function_unqualified(self):
        assert _get_qualifier(self._left("LOWER(game_type) = 'x'")) == ""


# ---------------------------------------------------------------------------
# Integration tests: _patch_python
# ---------------------------------------------------------------------------

class TestPatchPython:

    # --- Basic cases ---

    def test_ilike_with_both_wildcards(self):
        sql = "SELECT * FROM t WHERE p.full_name ILIKE '%Jimmy Butler%'"
        result = _patch_python(sql, [_sub("full_name", "dwh_d_players", "Jimmy Butler", "Jimmy Butler III")])
        assert "Jimmy Butler III" in result
        assert "'%Jimmy Butler%'" not in result
        assert result == "SELECT * FROM t WHERE p.full_name ILIKE '%Jimmy Butler III%'"

    def test_equality_match(self):
        sql = "SELECT * FROM t WHERE g.game_type = 'regular'"
        result = _patch_python(sql, [_sub("game_type", "dwh_d_games", "regular", "Regular Season")])
        assert result == "SELECT * FROM t WHERE g.game_type = 'Regular Season'"

    def test_like_no_wildcards(self):
        sql = "SELECT * FROM t WHERE name LIKE 'lebron james'"
        result = _patch_python(sql, [_sub("name", "dwh_d_players", "lebron james", "LeBron James")])
        assert result == "SELECT * FROM t WHERE name LIKE 'LeBron James'"

    def test_leading_wildcard_only(self):
        sql = "SELECT * FROM t WHERE full_name ILIKE '%curry'"
        result = _patch_python(sql, [_sub("full_name", "dwh_d_players", "curry", "Stephen Curry")])
        assert result == "SELECT * FROM t WHERE full_name ILIKE '%Stephen Curry'"

    def test_trailing_wildcard_only(self):
        sql = "SELECT * FROM t WHERE full_name ILIKE 'curry%'"
        result = _patch_python(sql, [_sub("full_name", "dwh_d_players", "curry", "Stephen Curry")])
        assert result == "SELECT * FROM t WHERE full_name ILIKE 'Stephen Curry%'"

    # --- Escaped single-quote round-trip ---

    def test_escaped_quote_in_literal(self):
        sql = "SELECT * FROM t WHERE name = 'O''Neal'"
        result = _patch_python(sql, [_sub("name", "players", "O'Neal", "O'Brien")])
        assert result == "SELECT * FROM t WHERE name = 'O''Brien'"

    def test_escaped_quote_idempotence(self):
        sql = "SELECT * FROM t WHERE name = 'O''Brien'"
        result = _patch_python(sql, [_sub("name", "players", "O'Neal", "O'Brien")])
        # O'Brien is already in the SQL — must not patch
        assert result == sql

    # --- Idempotence ---

    def test_already_correct_value_unchanged(self):
        sql = "SELECT * FROM t WHERE g.game_type = 'Regular Season'"
        result = _patch_python(sql, [_sub("game_type", "dwh_d_games", "regular", "Regular Season")])
        assert result == sql

    def test_double_run_idempotence(self):
        sql = "SELECT * FROM t WHERE p.full_name ILIKE '%Jimmy Butler%' AND g.game_type = 'regular'"
        subs = [
            _sub("full_name", "dwh_d_players", "Jimmy Butler", "Jimmy Butler III"),
            _sub("game_type", "dwh_d_games", "regular", "Regular Season"),
        ]
        first = _patch_python(sql, subs)
        second = _patch_python(first, subs)
        assert first == second

    # --- Comment safety ---

    def test_comment_line_not_modified(self):
        sql = "-- Jimmy Butler should not be changed here\nSELECT * FROM t WHERE 1=1"
        result = _patch_python(sql, [_sub("full_name", "dwh_d_players", "Jimmy Butler", "Jimmy Butler III")])
        assert result == sql

    def test_inline_comment_not_modified(self):
        sql = "SELECT * FROM t /* Jimmy Butler */ WHERE game_type = 'regular'"
        result = _patch_python(sql, [_sub("game_type", "dwh_d_games", "regular", "Regular Season")])
        assert "Jimmy Butler" in result  # comment untouched
        assert "Regular Season" in result  # filter patched

    # --- Function-wrapped LHS ---

    def test_lower_unqualified_col(self):
        sql = "SELECT * FROM t WHERE LOWER(game_type) = 'regular'"
        result = _patch_python(sql, [_sub("game_type", "dwh_d_games", "regular", "Regular Season")])
        assert "Regular Season" in result
        assert "'regular'" not in result

    def test_lower_qualified_col(self):
        sql = "SELECT * FROM t WHERE LOWER(p.full_name) = 'lebron james'"
        result = _patch_python(sql, [_sub("full_name", "dwh_d_players", "lebron james", "LeBron James")])
        assert "LeBron James" in result

    def test_trim_qualified_col(self):
        sql = "SELECT * FROM t WHERE TRIM(p.full_name) ILIKE '%curry%'"
        result = _patch_python(sql, [_sub("full_name", "dwh_d_players", "curry", "Stephen Curry")])
        assert "Stephen Curry" in result

    # --- Nested subquery ---

    def test_nested_subquery_inner_where_patched(self):
        sql = (
            "SELECT * FROM t\n"
            "WHERE player_id IN (\n"
            "    SELECT player_id FROM dwh_d_players\n"
            "    WHERE full_name ILIKE '%Jimmy Butler%'\n"
            ")\n"
            "AND game_type = 'regular'"
        )
        subs = [
            _sub("full_name", "dwh_d_players", "Jimmy Butler", "Jimmy Butler III"),
            _sub("game_type", "dwh_d_games", "regular", "Regular Season"),
        ]
        result = _patch_python(sql, subs)
        assert "Jimmy Butler III" in result
        assert "Regular Season" in result
        assert "'%Jimmy Butler%'" not in result
        assert "'regular'" not in result

    # --- Multiple comparisons, only target column replaced ---

    def test_multiple_comparisons_season_year_untouched(self):
        sql = (
            "SELECT * FROM t\n"
            "WHERE p.full_name ILIKE '%Jimmy Butler%'\n"
            "  AND g.game_type = 'regular'\n"
            "  AND g.season_year = '2022'"
        )
        subs = [
            _sub("full_name", "dwh_d_players", "Jimmy Butler", "Jimmy Butler III"),
            _sub("game_type", "dwh_d_games", "regular", "Regular Season"),
        ]
        result = _patch_python(sql, subs)
        assert "Jimmy Butler III" in result
        assert "Regular Season" in result
        assert "'2022'" in result  # untouched

    # --- Table-qualifier disambiguation ---

    def test_qualifier_mismatch_does_not_patch(self):
        """Two tables with same column name: only the matching alias is patched."""
        sql = (
            "SELECT * FROM t\n"
            "WHERE a.full_name ILIKE '%curry%'\n"
            "  AND b.full_name ILIKE '%james%'"
        )
        sub_a = _sub("full_name", "a", "curry", "Stephen Curry")
        result = _patch_python(sql, [sub_a])
        assert "Stephen Curry" in result
        assert "%james%" in result  # b.full_name unchanged

    def test_unqualified_column_patched_when_no_alias(self):
        sql = "SELECT * FROM t WHERE full_name ILIKE '%curry%'"
        result = _patch_python(sql, [_sub("full_name", "dwh_d_players", "curry", "Stephen Curry")])
        assert "Stephen Curry" in result

    # --- OR clause: same column, two different player names ---

    def test_or_clause_both_players_patched_independently(self):
        """Each OR branch must be patched to its own corrected value, not both to the first."""
        sql = (
            "SELECT * FROM t WHERE "
            "(p.full_name ILIKE '%Luka Doncic%' OR p.full_name ILIKE '%Joel Embiid%')"
        )
        subs = [
            _sub("full_name", "dwh_d_players", "Luka Doncic", "Luka Dončić"),
            _sub("full_name", "dwh_d_players", "Joel Embiid", "Joel Embiid"),
        ]
        result = _patch_python(sql, subs)
        assert "Luka Dončić" in result
        assert "Joel Embiid" in result
        # The critical regression: second slot must NOT be overwritten by first corrected value
        assert result.count("Luka Dončić") == 1

    def test_or_clause_no_match_player_preserved_as_original(self):
        """When one player is no_match (not in to_substitute), their original value stays in SQL."""
        sql = (
            "SELECT * FROM t WHERE "
            "(p.full_name ILIKE '%Luka Doncic%' OR p.full_name ILIKE '%Joel Embiid%')"
        )
        # Only Luka is in to_substitute; Joel had no_match so is NOT substituted
        subs = [
            _sub("full_name", "dwh_d_players", "Luka Doncic", "Luka Dončić"),
        ]
        result = _patch_python(sql, subs)
        assert "Luka Dončić" in result
        # Joel's original text must be preserved exactly as the LLM wrote it
        assert "Joel Embiid" in result
        assert result.count("Luka Dončić") == 1

    def test_or_clause_regression_luka_joel(self):
        """Direct reproduction of the reported bug: OR clause with two full_name ILIKE filters."""
        sql = (
            "WITH player_rebounds AS (\n"
            "    SELECT pb.player_id, p.full_name,\n"
            "           SUM(pb.rebounds_offensive + pb.rebounds_defensive) AS total_rebounds\n"
            "    FROM dwh_f_player_boxscore pb\n"
            "    JOIN dwh_d_players p ON pb.player_id = p.player_id\n"
            "    JOIN dwh_d_games g ON pb.game_id = g.game_id\n"
            "    WHERE g.season_year = (SELECT MAX(season_year) FROM dwh_d_games)\n"
            "      AND g.game_type ILIKE '%regular%'\n"
            "      AND (p.full_name ILIKE '%Luka Doncic%' OR p.full_name ILIKE '%Joel Embiid%')\n"
            "    GROUP BY pb.player_id, p.full_name\n"
            ")\n"
            "SELECT full_name, total_rebounds FROM player_rebounds"
        )
        # game_type corrected; Luka corrected; Joel is no_match so NOT in to_substitute
        subs = [
            _sub("game_type", "dwh_d_games", "regular", "Regular Season"),
            _sub("full_name", "dwh_d_players", "Luka Doncic", "Luka Dončić"),
        ]
        result = _patch_python(sql, subs)
        # game_type patched
        assert "Regular Season" in result
        assert "'%regular%'" not in result
        # Luka slot patched
        assert "Luka Dončić" in result
        # Joel slot must remain as original — not replaced by Luka's corrected value
        assert "Joel Embiid" in result
        # Luka's name must appear exactly once
        assert result.count("Luka Dončić") == 1



    def test_rhs_cast_expression(self):
        sql = "SELECT * FROM t WHERE name = CAST('x' AS TEXT)"
        result = _patch_python(sql, [_sub("name", "dwh_d_players", "x", "Stephen Curry")])
        assert "Stephen Curry" in result

    # --- Unsupported literal forms: leave unchanged, no crash ---

    def test_dollar_quoted_no_crash(self):
        sql = "SELECT * FROM t WHERE game_type = $$regular$$"
        result = _patch_python(sql, [_sub("game_type", "dwh_d_games", "regular", "Regular Season")])
        assert result == sql

    def test_e_prefix_no_crash(self):
        sql = "SELECT * FROM t WHERE name = E'O''Neal'"
        result = _patch_python(sql, [_sub("name", "dwh_d_players", "O'Neal", "O'Brien")])
        assert result is not None

    # --- No substitutions ---

    def test_empty_substitutions_returns_original(self):
        sql = "SELECT * FROM t WHERE name = 'x'"
        assert _patch_python(sql, []) == sql

    # --- Casing ---

    def test_case_only_correction(self):
        sql = "SELECT * FROM t WHERE name LIKE 'lebron james'"
        result = _patch_python(sql, [_sub("name", "dwh_d_players", "lebron james", "LeBron James")])
        assert result == "SELECT * FROM t WHERE name LIKE 'LeBron James'"

    # --- Complex real-world style query ---

    def test_full_nba_query(self):
        sql = (
            "SELECT pb.player_id, p.full_name,\n"
            "       AVG(pb.points) AS avg_points\n"
            "FROM dwh_f_player_boxscore pb\n"
            "JOIN dwh_d_players p ON pb.player_id = p.player_id\n"
            "JOIN dwh_d_games g ON pb.game_id = g.game_id\n"
            "WHERE p.full_name ILIKE '%LeBron James%'\n"
            "  AND g.game_type ILIKE '%regular%'\n"
            "  AND g.season_year = '2022'\n"
            "GROUP BY pb.player_id, p.full_name"
        )
        subs = [
            _sub("full_name", "dwh_d_players", "LeBron James", "LeBron James"),
            _sub("game_type", "dwh_d_games", "regular", "Regular Season"),
        ]
        result = _patch_python(sql, subs)
        assert "'%LeBron James%'" in result
        assert "Regular Season" in result
        assert "'%regular%'" not in result
        assert "'2022'" in result

