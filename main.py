"""
main.py
-------
Command-line entry point for the NBA Knowledge Base system.

Commands:
    generate   — Generate all KB markdown files using the LLM
                 (run once, or when you add new tables)
    build      — Parse all .md files, compute embeddings, load into Postgres
                 (run after generate, or after editing .md files)
    status     — Show what is currently stored in the database
    query      — Run a single natural language query through the full pipeline
                 and print the generated SQL

Usage:
    python main.py generate
    python main.py build
    python main.py status
    python main.py query "Who had the most assists per game last season?"

Pipeline order:
    generate → build → query
    (generate creates .md files; build loads them into DB; query uses the DB)
"""

from __future__ import annotations

import sys


def cmd_generate() -> None:
    """
    Generate KB markdown files for all NBA tables using the LLM.

    Calls kb_generator.py which uses the OpenAI API to write:
      - knowledge_base_files/ddl/KB.md  (section index)
      - knowledge_base_files/ddl/players.md
      - knowledge_base_files/ddl/games.md
      - ... (one file per table)
      - knowledge_base_files/sql_guidelines/KB.md
      - knowledge_base_files/response_guidelines/KB.md
      - knowledge_base_files/business_rules/KB.md

    After generation, review and edit the files before running build.
    """
    # Import from the package __init__ which explicitly re-exports this symbol
    from kb_system import generate_all_kb_files
    generate_all_kb_files()


def cmd_build() -> None:
    """
    Parse all .md files, compute embeddings, and load into Postgres.

    Requires:
      - knowledge_base_files/ directory with .md files (run generate first)
      - Local Postgres running with nba_kb database (see README)
      - OPENAI_API_KEY set in utils/config.py

    This is the step that populates the vector store for retrieval.
    """
    from kb_system.kb_builder import build_kb
    build_kb(verbose=True)


def cmd_status() -> None:
    """
    Print a summary of all files currently stored in the database.

    Shows which sections are loaded, which files have embeddings,
    and which are entry points. Use this to verify a successful build.
    """
    from kb_system.kb_builder import status_kb
    status_kb()


def cmd_query(user_query: str) -> None:
    """
    Run a full pipeline query: retrieve KB context → assemble prompt → generate SQL.

    This demonstrates the complete flow end-to-end:
      1. Embed the user query
      2. Classify which KB sections to search via keyword matching
      3. Vector search for relevant table files within those sections
      4. Fetch always-inject sections (sql_guidelines, response_guidelines)
      5. Assemble the SQL generation prompt with all retrieved context
      6. Call the LLM and print the resulting SQL

    Parameters
    ----------
    user_query : str
        Natural language question to convert to SQL.
    """
    # All imports are lazy (inside the function) so that missing dependencies
    # in one command do not prevent other commands from running.
    from kb_system.kb_store import get_connection
    from kb_system.kb_retriever import retrieve_context_for_query
    from utils.prompt_builder import build_sql_prompt
    from sql_generator import generate_sql, extract_sql_from_response

    print(f"\n{'=' * 60}")
    print(f"  Query: {user_query}")
    print(f"{'=' * 60}")

    # Open DB connection
    conn = get_connection()

    # Stage 1 + 2: Retrieve relevant context from KB
    retrieval_result = retrieve_context_for_query(conn, user_query)

    # Assemble the prompt from retrieved context
    prompt = build_sql_prompt(
        user_query=user_query,
        retrieval_result=retrieval_result,
        agent_backstory="You are an NBA analytics assistant with deep knowledge of basketball statistics.",
    )

    print(f"\n[main] Assembled prompt ({len(prompt)} chars). Calling LLM...")

    # Generate SQL
    llm_response = generate_sql(prompt)
    sql = extract_sql_from_response(llm_response)

    print(f"\n{'=' * 60}")
    print("  Generated SQL:")
    print(f"{'=' * 60}")
    print(sql)
    print(f"{'=' * 60}\n")

    conn.close()


def main() -> None:
    """
    Parse CLI arguments and dispatch to the appropriate command function.

    Exits with code 1 and prints usage if an unknown command is given.
    """
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
            print("Usage: python main.py query \"your question here\"")
            sys.exit(1)
        user_query = " ".join(sys.argv[2:])
        cmd_query(user_query)

    else:
        print(f"Unknown command: '{command}'")
        print("Valid commands: generate | build | status | query")
        sys.exit(1)


if __name__ == "__main__":
    main()
