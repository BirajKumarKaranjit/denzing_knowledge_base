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

from utils.config import overwrite, NBA_POSTGRES_DSN, POSTGRES_DSN
from utils.sample_values_for_testing import Sample_NBA_DDL_DICT
import sys


def _build_schema_context(ddl_dict: dict[str, str]) -> str:
    """Derive a compact table+column context string from the DDL dict.

    Extracts table names and column names from each CREATE TABLE statement
    so the relevance gate can infer domain vocabulary without a hardcoded
    schema summary. Works for any domain — not NBA-specific.

    Parameters
    ----------
    ddl_dict:
        Mapping of table_name -> raw CREATE TABLE SQL string.

    Returns
    -------
    str
        Compact multi-line string listing each table and its columns.
    """
    import re
    lines: list[str] = []
    for table_name, ddl_sql in ddl_dict.items():
        # Extract column names: first word on each indented line inside the CREATE TABLE body
        col_matches = re.findall(r'^\s{2,}(\w+)\s+\w+', ddl_sql, re.MULTILINE)
        # Fall back: grab all word tokens that look like column names
        if not col_matches:
            col_matches = re.findall(r'\b([a-z][a-z0-9_]{2,})\b', ddl_sql)
        cols = ", ".join(col_matches[:15])  # cap at 15 columns to keep context compact
        lines.append(f"- {table_name}: {cols}")
    return "\n".join(lines)


# Build schema context once at module load from the actual DDL
_SCHEMA_CONTEXT = _build_schema_context(Sample_NBA_DDL_DICT)


def _load_meta_kb_context() -> str:
    """Load KB files relevant to meta/project questions.

    Reads the root KB.md and business_rules sub-files from the local filesystem.
    This is intentionally a local file read — meta queries bypass the DB pipeline
    and do not require embeddings or Postgres.

    Returns
    -------
    str
        Concatenated markdown content from root KB.md, business_rules/KB.md,
        and project_information.md, separated by dividers.
    """
    import pathlib

    kb_root = pathlib.Path(__file__).parent / "knowledge_base_files"
    candidate_paths = [
        kb_root / "KB.md",
        kb_root / "business_rules" / "KB.md",
        kb_root / "business_rules" / "project_information.md",
    ]
    chunks: list[str] = []
    for path in candidate_paths:
        if path.exists():
            chunks.append(path.read_text(encoding="utf-8"))
    return "\n\n---\n\n".join(chunks)


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
    """Run a full pipeline query: relevance gate → retrieve → assemble → generate SQL → PEER.

    Parameters
    ----------
    user_query:
        Natural language question to convert to SQL.
    """
    from kb_system.kb_store import get_connection
    from kb_system.kb_retriever import retrieve_context_for_query
    from kb_system.peer import run_peer
    from utils.prompt_builder import build_sql_prompt
    from sql_generator import generate_sql, extract_sql_from_response, is_query_relevant, answer_meta_query

    print(f"\n{'=' * 60}")
    print(f"  Query: {user_query}")
    print(f"{'=' * 60}")

    relevant, category, response_msg, suggested_questions = is_query_relevant(
        user_query, _SCHEMA_CONTEXT
    )

    if category == "META_QUERY":
        print(f"\n[main] Meta query detected — answering from knowledge base documentation.")
        kb_context = _load_meta_kb_context()
        answer = answer_meta_query(user_query, kb_context)
        print(f"\n{answer}\n")
        return

    if not relevant:
        print(f"\n[main] Query rejected — category: {category}")
        if response_msg:
            print(f"  {response_msg}")
        if suggested_questions:
            print("\n  Try one of these instead:")
            for i, q in enumerate(suggested_questions, 1):
                print(f"    {i}. {q}")
        return

    conn = get_connection(POSTGRES_DSN)
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
    raw_sql = extract_sql_from_response(llm_response)

    # --- PEER: Pre-Execution Entity Resolution ---
    remote_conn = get_connection(NBA_POSTGRES_DSN)
    peer_result = run_peer(raw_sql, remote_conn)

    # Surface PEER messages to the user before presenting the SQL
    if peer_result.messages:
        print(f"\n{'=' * 60}")
        print("  Entity Resolution Notes:")
        print(f"{'=' * 60}")
        for msg in peer_result.messages:
            print(f"  {msg}")

    if peer_result.unvalidatable:
        print(f"\n[peer] Could not validate: {', '.join(peer_result.unvalidatable)}")

    if peer_result.error:
        print(f"\n[peer] Warning: PEER encountered an error: {peer_result.error}")

    final_sql = peer_result.sql

    print(f"\n{'=' * 60}")
    if peer_result.patched:
        print("  Generated SQL (PEER-patched):")
    else:
        print("  Generated SQL:")
    print(f"{'=' * 60}")
    print(final_sql)
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
    # cmd_query(input("\nEnter a natural language question to convert to SQL: "))
