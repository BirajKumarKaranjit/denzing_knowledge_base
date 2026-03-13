"""Unit tests for helper functions in main.py."""

from __future__ import annotations

import main


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

