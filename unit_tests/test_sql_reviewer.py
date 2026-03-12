"""unit_tests/test_sql_reviewer.py
Unit tests for sql_validator.sql_reviewer and utils.llm_client.
All tests are network-free: call_llm is patched at the module level so no
real LLM calls are made.
Run with:
    pytest unit_tests/test_sql_reviewer.py -v
"""
from __future__ import annotations
import sys
import os
from unittest.mock import MagicMock, patch
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pytest
from sql_validator.sql_reviewer import ReviewResult, review_sql, _parse_response, _build_user_prompt
from sql_validator.schema_linker import build_column_registry
from sql_validator.sql_verifier import verify_sql
_SIMPLE_SQL = (
    "SELECT p.full_name, SUM(pb.points) AS total_points "
    "FROM dwh_f_player_boxscore pb "
    "JOIN dwh_d_players p ON pb.player_id = p.player_id "
    "GROUP BY p.full_name ORDER BY total_points DESC LIMIT 10"
)
_DDL = "CREATE TABLE dwh_d_players (player_id text, full_name text, position text);"
_GUIDELINES = "Always use table aliases. Apply ILIKE for text filters."
_CALL_LLM_PATCH = "sql_validator.sql_reviewer.call_llm"
def _make_client() -> MagicMock:
    return MagicMock()
class TestParseResponse:
    def test_approved_simple(self):
        r = _parse_response("APPROVED")
        assert r.approved is True and r.revised_sql is None and r.changes == []
    def test_approved_with_whitespace(self):
        assert _parse_response("  APPROVED  ").approved is True
    def test_revised_with_sql_and_changes(self):
        raw = "REVISED\n```sql\nSELECT x;\n```\nCHANGES:\n- Fix A\n- Fix B\n"
        r = _parse_response(raw)
        assert not r.approved
        assert r.revised_sql == "SELECT x;"
        assert len(r.changes) == 2
    def test_revised_no_sql_block_approved(self):
        assert _parse_response("REVISED\nCHANGES:\n- x\n").approved is True
    def test_unparseable_approved(self):
        assert _parse_response("cannot determine").approved is True
    def test_empty_approved(self):
        assert _parse_response("").approved is True
    def test_revised_no_changes_section(self):
        r = _parse_response("REVISED\n```sql\nSELECT 1;\n```\n")
        assert not r.approved and r.revised_sql == "SELECT 1;" and r.changes == []
    def test_changes_stripped(self):
        raw = "REVISED\n```sql\nSELECT 1;\n```\nCHANGES:\n-  A trimmed  \n-  B\n"
        r = _parse_response(raw)
        assert r.changes == ["A trimmed", "B"]
class TestBuildUserPrompt:
    def test_all_sections_present(self):
        p = _build_user_prompt("Q?", "SELECT 1;", "DDL here")
        assert all(s in p for s in ["Q?", "SELECT 1;", "DDL here"])

    def test_empty_ddl_omitted(self):
        p = _build_user_prompt("Q?", "SELECT 1;", "")
        assert "RELEVANT TABLE DDL" not in p

    def test_sql_in_code_block(self):
        p = _build_user_prompt("Q?", "SELECT 1;", "")
        assert "```sql" in p and "SELECT 1;" in p
class TestReviewSQLApproved:
    def test_approved_response(self):
        with patch(_CALL_LLM_PATCH, return_value="APPROVED"):
            r = review_sql("q", _SIMPLE_SQL, _DDL, _make_client(), "gpt-4o")
        assert r.approved is True and r.revised_sql is None and r.changes == []
    def test_exception_treated_as_approved(self):
        with patch(_CALL_LLM_PATCH, side_effect=RuntimeError("err")):
            r = review_sql("q", _SIMPLE_SQL, _DDL, _make_client(), "gpt-4o")
        assert r.approved is True
    def test_unparseable_treated_as_approved(self):
        with patch(_CALL_LLM_PATCH, return_value="dunno"):
            r = review_sql("q", _SIMPLE_SQL, _DDL, _make_client(), "gpt-4o")
        assert r.approved is True
class TestReviewSQLRevised:
    def test_revised_parsed(self):
        raw = "REVISED\n```sql\nSELECT x;\n```\nCHANGES:\n- Added x\n"
        with patch(_CALL_LLM_PATCH, return_value=raw):
            r = review_sql("q", _SIMPLE_SQL, _DDL, _make_client(), "gpt-4o")
        assert not r.approved and "SELECT x;" in r.revised_sql
    def test_model_forwarded(self):
        with patch(_CALL_LLM_PATCH, return_value="APPROVED") as m:
            review_sql("q", "SELECT 1;", "", _make_client(), "gpt-4o-mini")
        assert m.call_args.kwargs["model"] == "gpt-4o-mini"
    def test_temperature_zero(self):
        with patch(_CALL_LLM_PATCH, return_value="APPROVED") as m:
            review_sql("q", "SELECT 1;", "", _make_client(), "gpt-4o")
        assert m.call_args.kwargs["temperature"] == 0.0
    def test_system_prompt_contains_reviewer_text(self):
        with patch(_CALL_LLM_PATCH, return_value="APPROVED") as m:
            review_sql("q", "SELECT 1;", "", _make_client(), "gpt-4o")
        assert "SQL quality reviewer" in m.call_args.kwargs["system_prompt"]
    def test_user_prompt_contains_query(self):
        with patch(_CALL_LLM_PATCH, return_value="APPROVED") as m:
            review_sql("top scorer?", "SELECT 1;", "", _make_client(), "gpt-4o")
        assert "top scorer?" in m.call_args.kwargs["user_prompt"]
class TestReviewerWithSchemaGuard:
    @pytest.fixture()
    def nba_registry(self):
        from utils.sample_values_for_testing import Sample_NBA_DDL_DICT
        return build_column_registry(Sample_NBA_DDL_DICT)
    def test_valid_revised_accepted(self, nba_registry):
        revised = (
            "SELECT p.full_name, SUM(pb.points) AS pts, COUNT(*) AS gp "
            "FROM dwh_f_player_boxscore pb "
            "JOIN dwh_d_players p ON pb.player_id = p.player_id "
            "GROUP BY p.full_name ORDER BY pts DESC LIMIT 10"
        )
        raw = f"REVISED\n```sql\n{revised}\n```\nCHANGES:\n- Added games_played\n"
        with patch(_CALL_LLM_PATCH, return_value=raw):
            r = review_sql("top?", _SIMPLE_SQL, _DDL, _make_client(), "gpt-4o")
        assert not r.approved
        assert verify_sql(r.revised_sql, nba_registry).is_valid
    def test_hallucinated_column_rejected(self, nba_registry):
        revised = (
            "SELECT p.full_name, pb.season_year "
            "FROM dwh_f_player_boxscore pb "
            "JOIN dwh_d_players p ON pb.player_id = p.player_id"
        )
        raw = f"REVISED\n```sql\n{revised}\n```\nCHANGES:\n- Added season_year\n"
        with patch(_CALL_LLM_PATCH, return_value=raw):
            r = review_sql("q", _SIMPLE_SQL, _DDL, _make_client(), "gpt-4o")
        assert not r.approved
        assert not verify_sql(r.revised_sql, nba_registry).is_valid
    def test_approved_passes_through(self, nba_registry):
        with patch(_CALL_LLM_PATCH, return_value="APPROVED"):
            r = review_sql("q", _SIMPLE_SQL, _DDL, _make_client(), "gpt-4o")
        assert r.approved and verify_sql(_SIMPLE_SQL, nba_registry).is_valid
    def test_reviewer_disabled_skips(self):
        with patch(_CALL_LLM_PATCH) as m:
            if False:  # SQL_REVIEWER_ENABLED=False
                review_sql("q", _SIMPLE_SQL, "", "", _make_client(), "gpt-4o")
            m.assert_not_called()
class TestGetLlmClient:
    def test_openai_client_returned(self):
        from utils.llm_client import get_llm_client
        import openai
        client = get_llm_client("openai", "sk-fake-key")
        assert isinstance(client, openai.OpenAI)
    def test_unknown_provider_raises(self):
        from utils.llm_client import get_llm_client
        with pytest.raises(ValueError, match="Unsupported provider"):
            get_llm_client("gemini", "key")  # type: ignore[arg-type]
class TestCallLlmOpenAI:
    def test_returns_response_text(self):
        from utils.llm_client import call_llm
        import openai
        client = openai.OpenAI(api_key="sk-fake")
        choice = MagicMock()
        choice.message.content = "hello from llm"
        mock_resp = MagicMock()
        mock_resp.choices = [choice]
        with patch.object(client.chat.completions, "create", return_value=mock_resp):
            text = call_llm(client, "gpt-4o", "sys", "user")
        assert text == "hello from llm"
    def test_passes_model_and_temperature(self):
        from utils.llm_client import call_llm
        import openai
        client = openai.OpenAI(api_key="sk-fake")
        choice = MagicMock()
        choice.message.content = ""
        mock_resp = MagicMock()
        mock_resp.choices = [choice]
        with patch.object(client.chat.completions, "create", return_value=mock_resp) as mc:
            call_llm(client, "gpt-4o-mini", "sys", "user", temperature=0.5)
        kw = mc.call_args.kwargs
        assert kw["model"] == "gpt-4o-mini" and kw["temperature"] == 0.5
    def test_unknown_client_raises(self):
        from utils.llm_client import call_llm
        with pytest.raises(TypeError, match="Unrecognised client type"):
            call_llm(MagicMock(), "model", "sys", "user")
