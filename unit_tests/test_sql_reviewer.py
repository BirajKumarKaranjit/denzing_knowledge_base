"""unit_tests/test_sql_reviewer.py

Unit tests for sql_validator.sql_reviewer.

All tests are network-free: the OpenAI client is replaced by a stub that
returns a controlled raw response string.

Run with:
    pytest unit_tests/test_sql_reviewer.py -v
"""

from __future__ import annotations

import sys
import os
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

from sql_validator.sql_reviewer import (
    ReviewResult,
    review_sql,
    _parse_response,
    _build_user_prompt,
)
from sql_validator.schema_linker import build_column_registry
from sql_validator.sql_verifier import verify_sql


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SIMPLE_SQL = (
    "SELECT p.full_name, SUM(pb.points) AS total_points "
    "FROM dwh_f_player_boxscore pb "
    "JOIN dwh_d_players p ON pb.player_id = p.player_id "
    "GROUP BY p.full_name ORDER BY total_points DESC LIMIT 10"
)

_DDL = "CREATE TABLE dwh_d_players (player_id text, full_name text, position text);"
_GUIDELINES = "Always use table aliases. Apply ILIKE for text filters."


def _make_client(content: str) -> MagicMock:
    """Build a minimal OpenAI client stub returning *content* as the LLM response."""
    choice = MagicMock()
    choice.message.content = content
    response = MagicMock()
    response.choices = [choice]
    client = MagicMock()
    client.chat.completions.create.return_value = response
    return client


# ---------------------------------------------------------------------------
# _parse_response
# ---------------------------------------------------------------------------

class TestParseResponse:
    def test_approved_simple(self):
        result = _parse_response("APPROVED")
        assert result.approved is True
        assert result.revised_sql is None
        assert result.changes == []

    def test_approved_with_trailing_whitespace(self):
        result = _parse_response("  APPROVED  ")
        assert result.approved is True

    def test_revised_with_sql_and_changes(self):
        raw = (
            "REVISED\n"
            "```sql\n"
            "SELECT full_name FROM dwh_d_players;\n"
            "```\n"
            "CHANGES:\n"
            "- Added missing column to SELECT\n"
            "- Removed unnecessary JOIN\n"
        )
        result = _parse_response(raw)
        assert result.approved is False
        assert result.revised_sql == "SELECT full_name FROM dwh_d_players;"
        assert len(result.changes) == 2
        assert "Added missing column" in result.changes[0]
        assert "Removed unnecessary JOIN" in result.changes[1]

    def test_revised_no_sql_block_treated_as_approved(self):
        raw = "REVISED\nCHANGES:\n- something changed\n"
        result = _parse_response(raw)
        assert result.approved is True

    def test_unparseable_response_treated_as_approved(self):
        result = _parse_response("I cannot determine this.")
        assert result.approved is True

    def test_empty_string_treated_as_approved(self):
        result = _parse_response("")
        assert result.approved is True

    def test_revised_sql_block_with_language_tag(self):
        raw = (
            "REVISED\n"
            "```sql\nSELECT id FROM t;\n```\n"
            "CHANGES:\n- Fixed column reference\n"
        )
        result = _parse_response(raw)
        assert result.approved is False
        assert result.revised_sql == "SELECT id FROM t;"

    def test_revised_no_changes_section(self):
        raw = "REVISED\n```sql\nSELECT 1;\n```\n"
        result = _parse_response(raw)
        assert result.approved is False
        assert result.revised_sql == "SELECT 1;"
        assert result.changes == []

    def test_changes_list_stripped_correctly(self):
        raw = (
            "REVISED\n"
            "```sql\nSELECT x FROM t;\n```\n"
            "CHANGES:\n"
            "-  Leading spaces trimmed  \n"
            "-  Another change\n"
        )
        result = _parse_response(raw)
        assert result.changes[0] == "Leading spaces trimmed"
        assert result.changes[1] == "Another change"


# ---------------------------------------------------------------------------
# _build_user_prompt
# ---------------------------------------------------------------------------

class TestBuildUserPrompt:
    def test_contains_all_sections(self):
        prompt = _build_user_prompt("Q?", "SELECT 1;", "DDL here", "Guidelines here")
        assert "Q?" in prompt
        assert "SELECT 1;" in prompt
        assert "DDL here" in prompt
        assert "Guidelines here" in prompt

    def test_empty_ddl_section_omitted(self):
        prompt = _build_user_prompt("Q?", "SELECT 1;", "", "Guidelines here")
        assert "RELEVANT TABLE DDL" not in prompt
        assert "Guidelines here" in prompt

    def test_empty_guidelines_section_omitted(self):
        prompt = _build_user_prompt("Q?", "SELECT 1;", "DDL here", "   ")
        assert "SQL GUIDELINES" not in prompt
        assert "DDL here" in prompt

    def test_sql_wrapped_in_code_block(self):
        prompt = _build_user_prompt("Q?", "SELECT 1;", "", "")
        assert "```sql" in prompt
        assert "SELECT 1;" in prompt


# ---------------------------------------------------------------------------
# review_sql — approved path
# ---------------------------------------------------------------------------

class TestReviewSQLApproved:
    def test_approved_response(self):
        client = _make_client("APPROVED")
        result = review_sql("query", _SIMPLE_SQL, _DDL, _GUIDELINES, client, "gpt-4o")
        assert result.approved is True
        assert result.revised_sql is None
        assert result.changes == []

    def test_openai_error_treated_as_approved(self):
        import openai as _openai
        client = MagicMock()
        client.chat.completions.create.side_effect = _openai.OpenAIError("timeout")
        result = review_sql("query", _SIMPLE_SQL, _DDL, _GUIDELINES, client, "gpt-4o")
        assert result.approved is True

    def test_unparseable_llm_response_treated_as_approved(self):
        client = _make_client("I'm not sure how to review this SQL.")
        result = review_sql("query", _SIMPLE_SQL, _DDL, _GUIDELINES, client, "gpt-4o")
        assert result.approved is True


# ---------------------------------------------------------------------------
# review_sql — revised path
# ---------------------------------------------------------------------------

class TestReviewSQLRevised:
    def test_revised_response_parsed(self):
        raw = (
            "REVISED\n"
            "```sql\nSELECT full_name, total_points FROM stats;\n```\n"
            "CHANGES:\n"
            "- Added season_year to SELECT for context\n"
        )
        client = _make_client(raw)
        result = review_sql("query", _SIMPLE_SQL, _DDL, _GUIDELINES, client, "gpt-4o")
        assert result.approved is False
        assert "SELECT full_name, total_points FROM stats;" in (result.revised_sql or "")
        assert len(result.changes) == 1

    def test_llm_called_with_correct_model(self):
        client = _make_client("APPROVED")
        review_sql("q", "SELECT 1;", "", "", client, "gpt-4o-mini")
        call_kwargs = client.chat.completions.create.call_args
        assert call_kwargs.kwargs.get("model") == "gpt-4o-mini"

    def test_temperature_zero(self):
        client = _make_client("APPROVED")
        review_sql("q", "SELECT 1;", "", "", client, "gpt-4o")
        call_kwargs = client.chat.completions.create.call_args
        assert call_kwargs.kwargs.get("temperature") == 0.0

    def test_system_prompt_included(self):
        client = _make_client("APPROVED")
        review_sql("q", "SELECT 1;", "", "", client, "gpt-4o")
        messages = client.chat.completions.create.call_args.kwargs["messages"]
        system_msgs = [m for m in messages if m["role"] == "system"]
        assert len(system_msgs) == 1
        assert "SQL quality reviewer" in system_msgs[0]["content"]

    def test_user_query_in_user_message(self):
        client = _make_client("APPROVED")
        review_sql("What is the top scorer?", "SELECT 1;", "", "", client, "gpt-4o")
        messages = client.chat.completions.create.call_args.kwargs["messages"]
        user_msgs = [m for m in messages if m["role"] == "user"]
        assert any("What is the top scorer?" in m["content"] for m in user_msgs)


# ---------------------------------------------------------------------------
# Integration: reviewer + schema verifier guard
# ---------------------------------------------------------------------------

class TestReviewerWithSchemaGuard:
    """Simulates the main.py logic: if reviewer proposes a revised SQL,
    run schema verification on it before accepting."""

    @pytest.fixture()
    def nba_registry(self):
        from utils.sample_values_for_testing import Sample_NBA_DDL_DICT
        return build_column_registry(Sample_NBA_DDL_DICT)

    def test_valid_revised_sql_accepted(self, nba_registry):
        revised = (
            "SELECT p.full_name, SUM(pb.points) AS total_points, COUNT(*) AS games_played "
            "FROM dwh_f_player_boxscore pb "
            "JOIN dwh_d_players p ON pb.player_id = p.player_id "
            "GROUP BY p.full_name ORDER BY total_points DESC LIMIT 10"
        )
        raw = f"REVISED\n```sql\n{revised}\n```\nCHANGES:\n- Added games_played\n"
        client = _make_client(raw)
        result = review_sql("top scorers?", _SIMPLE_SQL, _DDL, _GUIDELINES, client, "gpt-4o")

        assert result.approved is False
        assert result.revised_sql is not None

        schema_check = verify_sql(result.revised_sql, nba_registry)
        assert schema_check.is_valid
        # Schema passed → use revised SQL
        final_sql = result.revised_sql

        assert "games_played" in final_sql

    def test_hallucinated_column_in_revised_rejected(self, nba_registry):
        revised = (
            "SELECT p.full_name, pb.season_year "  # season_year does not live on pb
            "FROM dwh_f_player_boxscore pb "
            "JOIN dwh_d_players p ON pb.player_id = p.player_id"
        )
        raw = f"REVISED\n```sql\n{revised}\n```\nCHANGES:\n- Added season_year\n"
        client = _make_client(raw)
        result = review_sql("query", _SIMPLE_SQL, _DDL, _GUIDELINES, client, "gpt-4o")

        assert result.approved is False
        assert result.revised_sql is not None

        schema_check = verify_sql(result.revised_sql, nba_registry)
        # Schema fails → fall back to original SQL
        assert not schema_check.is_valid
        final_sql = _SIMPLE_SQL  # original kept

        assert "season_year" not in final_sql

    def test_approved_sql_passes_through_unchanged(self, nba_registry):
        client = _make_client("APPROVED")
        result = review_sql("top scorers?", _SIMPLE_SQL, _DDL, _GUIDELINES, client, "gpt-4o")

        assert result.approved is True
        # No schema re-check needed — original SQL used directly
        schema_check = verify_sql(_SIMPLE_SQL, nba_registry)
        assert schema_check.is_valid

    def test_reviewer_disabled_skips_entirely(self):
        """Simulate SQL_REVIEWER_ENABLED=False — review_sql is never called."""
        client = _make_client("APPROVED")
        sql_reviewer_enabled = False

        final_sql = _SIMPLE_SQL
        if sql_reviewer_enabled:
            result = review_sql("q", final_sql, "", "", client, "gpt-4o")
            if not result.approved and result.revised_sql:
                final_sql = result.revised_sql

        client.chat.completions.create.assert_not_called()
        assert final_sql == _SIMPLE_SQL

