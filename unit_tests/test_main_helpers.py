"""Unit tests for helper functions in main.py."""

from __future__ import annotations

from types import SimpleNamespace

import main
import pytest


def test_extract_table_names_handles_empty_and_comment_only_sql() -> None:
    assert main._extract_table_names_from_sql("") == set()
    assert main._extract_table_names_from_sql("   ") == set()
    assert main._extract_table_names_from_sql("-- no sql") == set()
    assert main._extract_table_names_from_sql(";") == set()


def test_extract_table_names_handles_invalid_text_without_crash() -> None:
    assert main._extract_table_names_from_sql("```sql\n```") == set()


def test_extract_table_names_returns_unique_table_names() -> None:
    sql = (
        "SELECT p.full_name "
        "FROM dwh_d_players p "
        "JOIN dwh_f_player_boxscore pb ON p.player_id = pb.player_id "
        "JOIN dwh_d_players p2 ON p2.player_id = pb.player_id"
    )
    assert main._extract_table_names_from_sql(sql) == {
        "dwh_d_players",
        "dwh_f_player_boxscore",
    }


def test_is_executable_sql_edge_cases() -> None:
    assert main._is_executable_sql("-- comment only") is False
    assert main._is_executable_sql("/* block */\n-- line") is False
    assert main._is_executable_sql("SELECT 1;") is True


def test_build_reviewer_ddl_context_includes_retrieved_and_sql_referenced(monkeypatch) -> None:
    fake_ddls = {
        "table_a": "CREATE TABLE table_a (id INT);",
        "table_b": "CREATE TABLE table_b (id INT);",
    }
    monkeypatch.setattr(main, "Sample_NBA_DDL_DICT", fake_ddls)

    retrieval_result = {
        "matched_tables": [
            {"table_name": "table_a", "content": "CREATE TABLE table_a (id INT);"},
        ]
    }
    sql = "SELECT a.id FROM table_a a JOIN table_b b ON a.id = b.id"

    ddl_context = main._build_reviewer_ddl_context(retrieval_result, sql)
    assert "CREATE TABLE table_a" in ddl_context
    assert "CREATE TABLE table_b" in ddl_context


def test_build_reviewer_ddl_context_handles_empty_sql(monkeypatch) -> None:
    fake_ddls = {"table_x": "CREATE TABLE table_x (id INT);"}
    monkeypatch.setattr(main, "Sample_NBA_DDL_DICT", fake_ddls)

    retrieval_result = {
        "matched_tables": [
            {"table_name": "table_x", "content": "CREATE TABLE table_x (id INT);"},
        ]
    }

    ddl_context = main._build_reviewer_ddl_context(retrieval_result, "")
    assert ddl_context.strip() == "CREATE TABLE table_x (id INT);"


class _FakeConn:
    def __init__(self, label: str) -> None:
        self.label = label
        self.closed = False

    def close(self) -> None:
        self.closed = True


@pytest.mark.usefixtures("monkeypatch")
def test_cmd_query_pipeline_order_forwards_verifier_errors_and_executes(monkeypatch) -> None:
    trace: list[str] = []
    review_payload: dict[str, object] = {}

    query_conn = _FakeConn("query")
    remote_conn = _FakeConn("remote")

    def _fake_get_connection(dsn: str):
        trace.append(f"get_connection:{dsn}")
        if dsn == main.POSTGRES_DSN:
            return query_conn
        return remote_conn

    def _fake_retrieve_context_for_query(conn, user_query: str):
        trace.append("retrieve")
        return {"matched_tables": [{"table_name": "dwh_d_games", "content": "CREATE TABLE dwh_d_games (season_year text, game_type text);"}]}

    def _fake_build_sql_prompt(*, user_query: str, retrieval_result: dict, agent_backstory: str):
        trace.append("prompt")
        return ("PROMPT", "CITATION")

    def _fake_is_query_relevant(user_query: str, schema_context: str):
        trace.append("relevance")
        return True, "SQL_RELEVANT", "", []

    def _fake_generate_sql(prompt: str, temperature: float = 0.0):
        trace.append("generate")
        return "```sql\nSELECT g.season_year, SUM(pb.points) AS total_points FROM dwh_f_player_boxscore pb JOIN dwh_d_games g ON pb.game_id=g.game_id WHERE g.season_year = cs.season_year GROUP BY g.season_year;\n```"

    def _fake_extract_sql_from_response(resp: str):
        trace.append("extract")
        return "SELECT SUM(pb.points) AS total_points FROM dwh_f_player_boxscore pb JOIN dwh_d_games g ON pb.game_id=g.game_id JOIN current_season cs ON 1=1 WHERE g.season_year = cs.season_year;"

    verif_error = SimpleNamespace(error_type="scope_filter_not_projected", message="Columns used to restrict query scope are not in SELECT output: ['season_year'].")

    def _fake_verify_sql(sql: str, registry: dict[str, list[str]], dialect: str = ""):
        if "SELECT SUM(pb.points)" in sql:
            trace.append("verify_initial")
            return SimpleNamespace(is_valid=False, warnings=[], errors=[verif_error])
        trace.append("verify_final")
        return SimpleNamespace(is_valid=True, warnings=[], errors=[])

    def _fake_review_sql(**kwargs):
        trace.append("review")
        review_payload.update(kwargs)
        return SimpleNamespace(
            approved=False,
            revised_sql=(
                "SELECT g.season_year, SUM(pb.points) AS total_points "
                "FROM dwh_f_player_boxscore pb "
                "JOIN dwh_d_games g ON pb.game_id=g.game_id "
                "JOIN current_season cs ON 1=1 "
                "WHERE g.season_year = cs.season_year "
                "GROUP BY g.season_year"
            ),
            changes=["Added season_year to SELECT and GROUP BY."],
        )

    def _fake_run_peer(sql: str, conn):
        trace.append("peer")
        return SimpleNamespace(sql=sql, patched=False, messages=[], unvalidatable=[], error="")

    def _fake_execute_sql(sql: str):
        trace.append("execute")
        return True, None

    monkeypatch.setattr(main, "_build_reviewer_ddl_context", lambda retrieval_result, sql: "CREATE TABLE dwh_d_games (season_year text, game_type text);")
    monkeypatch.setattr(main, "_execute_sql", _fake_execute_sql)
    monkeypatch.setattr(main, "SQL_REVIEWER_ENABLED", True)

    import kb_system.kb_store as kb_store
    import kb_system.kb_retriever as kb_retriever
    import kb_system.peer as peer
    import utils.prompt_builder as prompt_builder
    import sql_worker.sql_generator as sql_generator
    import sql_worker.sql_verifier as sql_verifier
    import sql_worker.sql_reviewer as sql_reviewer
    import utils.llm_client as llm_client

    monkeypatch.setattr(kb_store, "get_connection", _fake_get_connection)
    monkeypatch.setattr(kb_retriever, "retrieve_context_for_query", _fake_retrieve_context_for_query)
    monkeypatch.setattr(peer, "run_peer", _fake_run_peer)
    monkeypatch.setattr(prompt_builder, "build_sql_prompt", _fake_build_sql_prompt)
    monkeypatch.setattr(sql_generator, "is_query_relevant", _fake_is_query_relevant)
    monkeypatch.setattr(sql_generator, "generate_sql", _fake_generate_sql)
    monkeypatch.setattr(sql_generator, "extract_sql_from_response", _fake_extract_sql_from_response)
    monkeypatch.setattr(sql_verifier, "verify_sql", _fake_verify_sql)
    monkeypatch.setattr(sql_reviewer, "review_sql", _fake_review_sql)
    monkeypatch.setattr(llm_client, "get_llm_client", lambda provider, api_key: object())

    main.cmd_query("sample question")

    assert "verify_initial" in trace
    assert "review" in trace
    assert "verify_final" in trace
    assert "peer" in trace
    assert "execute" in trace
    assert trace.index("verify_initial") < trace.index("review") < trace.index("verify_final") < trace.index("peer") < trace.index("execute")

    forwarded_errors = review_payload.get("verifier_errors")
    assert isinstance(forwarded_errors, list)
    assert forwarded_errors
    assert "scope_filter_not_projected" in str(forwarded_errors[0])


@pytest.mark.usefixtures("monkeypatch")
def test_cmd_query_hard_gate_blocks_peer_and_execution(monkeypatch) -> None:
    trace: list[str] = []

    query_conn = _FakeConn("query")
    remote_conn = _FakeConn("remote")

    def _fake_get_connection(dsn: str):
        return query_conn if dsn == main.POSTGRES_DSN else remote_conn

    monkeypatch.setattr(main, "SQL_REVIEWER_ENABLED", True)
    monkeypatch.setattr(main, "_build_reviewer_ddl_context", lambda retrieval_result, sql: "CREATE TABLE dwh_d_games (season_year text);")
    monkeypatch.setattr(main, "_execute_sql", lambda sql: (_ for _ in ()).throw(AssertionError("execute should not run")))

    import kb_system.kb_store as kb_store
    import kb_system.kb_retriever as kb_retriever
    import kb_system.peer as peer
    import utils.prompt_builder as prompt_builder
    import sql_worker.sql_generator as sql_generator
    import sql_worker.sql_verifier as sql_verifier
    import sql_worker.sql_reviewer as sql_reviewer
    import utils.llm_client as llm_client

    monkeypatch.setattr(kb_store, "get_connection", _fake_get_connection)
    monkeypatch.setattr(kb_retriever, "retrieve_context_for_query", lambda conn, user_query: {"matched_tables": []})
    monkeypatch.setattr(prompt_builder, "build_sql_prompt", lambda **kwargs: ("PROMPT", "CITATION"))
    monkeypatch.setattr(sql_generator, "is_query_relevant", lambda user_query, schema_context: (True, "SQL_RELEVANT", "", []))
    monkeypatch.setattr(sql_generator, "generate_sql", lambda prompt, temperature=0.0: "```sql\nSELECT 1;\n```")
    monkeypatch.setattr(sql_generator, "extract_sql_from_response", lambda llm_response: "SELECT 1")
    monkeypatch.setattr(sql_reviewer, "review_sql", lambda **kwargs: SimpleNamespace(approved=True, revised_sql=None, changes=[]))
    monkeypatch.setattr(llm_client, "get_llm_client", lambda provider, api_key: object())

    gate_error = SimpleNamespace(error_type="scope_filter_not_projected", message="missing scope")
    call_count = {"n": 0}

    def _fake_verify_sql(sql: str, registry: dict[str, list[str]], dialect: str = ""):
        call_count["n"] += 1
        if call_count["n"] == 1:
            trace.append("verify_initial")
            return SimpleNamespace(is_valid=True, warnings=[], errors=[])
        trace.append("verify_final")
        return SimpleNamespace(is_valid=False, warnings=[], errors=[gate_error])

    def _peer_should_not_run(sql: str, conn):
        raise AssertionError("peer should not run when hard verification gate fails")

    monkeypatch.setattr(sql_verifier, "verify_sql", _fake_verify_sql)
    monkeypatch.setattr(peer, "run_peer", _peer_should_not_run)

    main.cmd_query("sample question")

    assert trace == ["verify_initial", "verify_final"]
    assert query_conn.closed is True
    assert remote_conn.closed is True


