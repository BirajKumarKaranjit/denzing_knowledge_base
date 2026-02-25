"""main.py

Command-line entry point for the Knowledge Base system.

Commands:
    generate   — Generate all KB markdown files using the LLM
    build      — Parse .md files, compute embeddings, load into Postgres
    status     — Show what is currently stored in the database
    query      — Run a single natural language query through the full pipeline

Usage:
    python main.py generate
    python main.py build
    python main.py status
    python main.py query "Who had the most assists per game last season?"

Pipeline order:
    generate → build → query
"""

from __future__ import annotations

from utils.config import overwrite
from utils.sample_values_for_testing import Sample_NBA_DDL_DICT
import sys

_SCHEMA_SUMMARY = (
    "NBA basketball analytics database containing tables for players, teams, games, "
    "player box scores, team box scores, player awards, player tracking, "
    "player season stats, and team championships."
)


def cmd_generate() -> None:
    """Generate KB markdown files for all tables using the LLM."""
    from kb_system import generate_all_kb_files
    generate_all_kb_files(ddl_dict=Sample_NBA_DDL_DICT, overwrite=overwrite)


def cmd_build() -> None:
    """Parse all .md files, compute embeddings, and load into Postgres."""
    from kb_system.kb_builder import build_kb
    build_kb(verbose=True)


def cmd_status() -> None:
    """Print a summary of all files currently stored in the database."""
    from kb_system.kb_builder import status_kb
    status_kb()


def cmd_query(user_query: str) -> None:
    """Run a full pipeline query: relevance gate → retrieve → assemble → generate SQL.

    Parameters
    ----------
    user_query:
        Natural language question to convert to SQL.
    """
    from kb_system.kb_store import get_connection
    from kb_system.kb_retriever import retrieve_context_for_query
    from utils.prompt_builder import build_sql_prompt
    from sql_generator import generate_sql, extract_sql_from_response, is_query_relevant

    print(f"\n{'=' * 60}")
    print(f"  Query: {user_query}")
    print(f"{'=' * 60}")

    # relevant, reason, suggested_questions = is_query_relevant(user_query, _SCHEMA_SUMMARY)
    # if not relevant:
    #     print(f"\n[main] Query rejected by relevance gate.")
    #     print(f"  Reason: {reason}")
    #     formatted_questions = "\n".join(
    #         f"{i + 1}. {q}" for i, q in enumerate(suggested_questions)
    #     )
    #     print(
    #         "This question cannot be answered from the available database.\n"
    #         "Please ask a question related to your NBA analytics data, such as:\n"
    #         f"{formatted_questions}"
    #     )
    #     return

    conn = get_connection()
    retrieval_result = retrieve_context_for_query(conn, user_query)

    prompt, citation_md = build_sql_prompt(
        user_query=user_query,
        retrieval_result=retrieval_result,
        agent_backstory="You are an NBA analytics assistant with deep knowledge of basketball statistics.",
    )
    print(f"\n The following prompt will be sent to the LLM for SQL generation:")
    print(f"\n{'=' * 60}")
    print(prompt)
    print(f"{'=' * 60}")
    print(f"\n[main] Calling LLM for SQL generation...")
    llm_response = generate_sql(prompt)
    sql = extract_sql_from_response(llm_response)

    print(f"\n{'=' * 60}")
    print("  Generated SQL:")
    print(f"{'=' * 60}")
    print(sql)
    print(f"{'=' * 60}\n")
    print(citation_md)

    conn.close()


def main() -> None:
    """Parse CLI arguments and dispatch to the appropriate command."""
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    command = sys.argv[1].lower()

    if command == "generate":
        cmd_generate()
    elif command == "build":
        cmd_build()
    elif command == "status":
        cmd_status()
    elif command == "query":
        if len(sys.argv) < 3:
            print('Usage: python main.py query "your question here"')
            sys.exit(1)
        user_query = " ".join(sys.argv[2:])
        cmd_query(user_query)

    else:
        print(f"Unknown command: '{command}'")
        print("Valid commands: generate | build | status | query")
        sys.exit(1)


if __name__ == "__main__":
    main()
