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

from utils.config import (
    overwrite,
    NBA_POSTGRES_DSN,
    POSTGRES_DSN,
    SQL_REVIEWER_ENABLED,
    SQL_REVIEWER_MODEL,
    SQL_REVIEWER_PROVIDER,
    SQL_REVIEWER_API_KEY,
    SQL_DIALECT,
)
from utils.sample_values_for_testing import Sample_NBA_DDL_DICT
from sql_worker.schema_linker import build_column_registry
import sys

_COLUMN_REGISTRY: dict[str, list[str]] = build_column_registry(Sample_NBA_DDL_DICT)


def _build_schema_context(ddl_dict: dict[str, str]) -> str:
    """
    Build schema context string from the NBA DDL dictionary.

    Output format:
    table_name: col1, col2, col3
    """

    import re

    lines = []

    for table_name, ddl_sql in ddl_dict.items():

        # extract content inside parentheses
        match = re.search(r"\((.*)\)", ddl_sql)
        if not match:
            continue

        column_block = match.group(1)

        columns = []

        # split columns by comma
        for col_def in column_block.split(","):
            col_def = col_def.strip()

            if not col_def:
                continue

            col_name = col_def.split()[0]
            columns.append(col_name)

        lines.append(f"{table_name}: {', '.join(columns)}")

    return "\n".join(lines)

_SCHEMA_CONTEXT = _build_schema_context(Sample_NBA_DDL_DICT)

def _load_meta_kb_context() -> str:
    """Load KB files relevant to meta/project questions.
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


def _execute_sql(sql: str) -> tuple[bool, str | None]:
    """Execute *sql* against NBA_POSTGRES_DSN and pretty-print results.

    Parameters
    ----------
    sql:
        The SQL string to execute.

    Returns
    -------
    tuple[bool, str | None]
        ``(True, None)`` on success, ``(False, error_message)`` on SQL error.
        Connection errors are printed and returned as failures.
    """
    import psycopg2
    from utils.config import NBA_POSTGRES_DSN, nba_db_config

    try:
        conn = psycopg2.connect(NBA_POSTGRES_DSN,
                                options=f"-c search_path={nba_db_config['schema']}")
    except psycopg2.Error as exc:
        print(f"\n[executor] Could not connect to database: {exc}")
        return False, str(exc)

    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            col_names = [desc[0] for desc in cur.description] if cur.description else []

        if not col_names:
            print("\n[executor] Query executed successfully (no columns returned).")
            return True, None

        if not rows:
            print("\n[executor] Query returned no results.")
            _print_table(col_names, [])
            return True, None

        _print_table(col_names, rows)
        return True, None

    except psycopg2.Error as exc:
        error_msg = str(exc).strip()
        print(f"\n[executor] SQL Error: {error_msg}")
        conn.rollback()
        return False, error_msg
    finally:
        conn.close()


def _print_table(headers: list[str], rows: list[tuple]) -> None:
    """Render *headers* and *rows* as a fixed-width table to stdout.

    Parameters
    ----------
    headers:
        Column names.
    rows:
        Result rows — each element is a tuple matching *headers* in length.
    """
    str_rows = [[str(v) if v is not None else "NULL" for v in row] for row in rows]
    col_widths = [
        max(len(h), max((len(r[i]) for r in str_rows), default=0))
        for i, h in enumerate(headers)
    ]

    sep = "+-" + "-+-".join("-" * w for w in col_widths) + "-+"
    header_line = "| " + " | ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers)) + " |"

    print(f"\n{'=' * 60}")
    print(f"  QUERY EXECUTION RESULT  ({len(rows)} row{'s' if len(rows) != 1 else ''})")
    print(f"{'=' * 60}\n")
    print(sep)
    print(header_line)
    print(sep)
    for row in str_rows:
        print("| " + " | ".join(row[i].ljust(col_widths[i]) for i in range(len(headers))) + " |")
    print(sep)
    print()


def _extract_table_names_from_sql(sql: str) -> set[str]:
    """Extract referenced physical table names from SQL text."""
    from sqlglot import exp, parse

    tables: set[str] = set()
    if not sql or not sql.strip():
        return tables

    try:
        statements = parse(sql)
    except Exception:
        return tables

    for stmt in statements:
        if stmt is None or not hasattr(stmt, "find_all"):
            continue
        for table in stmt.find_all(exp.Table):
            name = (table.name or "").strip()
            if name:
                tables.add(name)
    return tables


def _build_reviewer_ddl_context(retrieval_result: dict, sql: str) -> str:
    """Build reviewer DDL context from retrieval plus SQL-referenced tables."""
    chunks: list[str] = []
    seen: set[str] = set()

    for table in retrieval_result.get("matched_tables", []):
        table_name = (table.get("table_name") or "").strip()
        content = (table.get("content") or "").strip()
        if table_name and content and table_name not in seen:
            seen.add(table_name)
            chunks.append(content)

    for table_name in sorted(_extract_table_names_from_sql(sql)):
        if table_name in seen:
            continue
        ddl = (Sample_NBA_DDL_DICT.get(table_name) or "").strip()
        if ddl:
            seen.add(table_name)
            chunks.append(ddl)

    return "\n\n".join(chunks)


def _is_executable_sql(sql: str) -> bool:
    """Return True when SQL has at least one executable statement."""
    import re

    if not sql or not sql.strip():
        return False

    without_block_comments = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    without_line_comments = re.sub(r"--.*?$", "", without_block_comments, flags=re.MULTILINE)
    candidate = without_line_comments.strip().strip(";").strip()
    if not candidate:
        return False

    try:
        from sqlglot import parse

        statements = parse(candidate)
    except Exception:
        return False

    return bool(statements)


def _format_sql(sql: str) -> str:
    """Format SQL for terminal display only."""
    import sqlparse

    cleaned = (sql or "").strip()
    if not cleaned:
        return cleaned

    try:
        return sqlparse.format(
            cleaned,
            reindent=True,
            keyword_case="upper",
            strip_comments=False,
            use_space_around_operators=True,
        ).strip()
    except Exception:
        return cleaned


def cmd_query(user_query: str) -> None:
    """Run a full pipeline query: relevance gate → retrieve → assemble → generate SQL → PEER.

    Parameters
    ----------
    user_query:
        Natural language question to convert to SQL.
    """
    import time
    start_time = time.time()
    sql_generation_time = 0.0
    sql_verification_time = 0.0
    sql_reviewer_time = 0.0
    peer_module_time = 0.0
    sql_execution_time = 0.0
    query_classification_time = 0.0
    query_mutation_kb_retrieval_time = 0.0
    from kb_system.kb_store import get_connection
    from kb_system.kb_retriever import retrieve_context_for_query
    from kb_system.peer import run_peer
    from utils.prompt_builder import build_sql_prompt
    from sql_worker.sql_generator import generate_sql, extract_sql_from_response, build_retry_prompt, is_query_relevant, answer_meta_query
    from sql_worker.sql_verifier import verify_sql
    from sql_worker.sql_reviewer import review_sql
    from utils.llm_client import get_llm_client

    print(f"\n{'=' * 60}")
    print(f"  Query: {user_query}")
    print(f"{'=' * 60}")

    t0 = time.perf_counter()
    relevant, category, response_msg, suggested_questions = is_query_relevant(
        user_query, _SCHEMA_CONTEXT
    )
    query_classification_time += time.perf_counter() - t0

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
    t0 = time.perf_counter()
    retrieval_result = retrieve_context_for_query(conn, user_query)
    query_mutation_kb_retrieval_time += time.perf_counter() - t0

    augmented_query = (
        f"{user_query}\n"
        "Include all filter/scope columns, readable names, and aggregate sample sizes in SELECT. Hence, the output must be fully self-descriptive.")
    user_query=augmented_query
    prompt, citation_md = build_sql_prompt(
        user_query=user_query,
        retrieval_result=retrieval_result,
        agent_backstory="You are an NBA analytics assistant with deep knowledge of basketball statistics.",
    )
    # print(f"\n The following prompt will be sent to the LLM for SQL generation:")
    # print(f"\n{'=' * 60}")
    # print(prompt)
    # print(f"{'=' * 60}")
    print(f"\n[main] Calling LLM for SQL generation...")
    t0 = time.perf_counter()
    llm_response = generate_sql(prompt)
    raw_sql = extract_sql_from_response(llm_response)
    sql_generation_time += time.perf_counter() - t0
    raw_sql_display = _format_sql(raw_sql)
    print(f"Generated RAW SQL:")
    print(f"\n{'=' * 60}")
    print(raw_sql_display)

    remote_conn = get_connection(NBA_POSTGRES_DSN)
    final_sql = raw_sql
    t0 = time.perf_counter()
    initial_verification = verify_sql(final_sql, _COLUMN_REGISTRY, dialect=SQL_DIALECT)
    sql_verification_time += time.perf_counter() - t0
    verifier_error_messages: list[str] = [
        f"[{err.error_type}] {err.message}" for err in initial_verification.errors
    ]

    print(f"\n{'=' * 60}")
    if initial_verification.warnings:
        for w in initial_verification.warnings:
            print(f"\n[verifier] Warning: {w}")

    if initial_verification.is_valid:
        print("\n[verifier] Passed — no schema errors.")
    else:
        print("\n[verifier] Schema errors detected — forwarding to reviewer:")
        for msg in verifier_error_messages:
            print(f"  {msg}")

    if SQL_REVIEWER_ENABLED:
        _reviewer_client = get_llm_client(SQL_REVIEWER_PROVIDER, SQL_REVIEWER_API_KEY)
        ddl_context = _build_reviewer_ddl_context(retrieval_result, final_sql)

        t0 = time.perf_counter()
        review = review_sql(
            user_query=user_query,
            generated_sql=final_sql,
            ddl_context=ddl_context,
            client=_reviewer_client,
            model=SQL_REVIEWER_MODEL,
            dialect=SQL_DIALECT,
            verifier_errors=verifier_error_messages,
        )
        sql_reviewer_time += time.perf_counter() - t0
        print(f"\n{'=' * 60}")
        if review.approved:
            print("\n[sql_reviewer] Approved — no changes.")
        else:
            print(f"\n[sql_reviewer] Revised. Changes:")
            for change in review.changes:
                print(f"  - {change}")

            revised = review.revised_sql or ""
            if revised and not _is_executable_sql(revised):
                print("\n[sql_reviewer] Revised SQL is empty/comment-only — using original.")
                revised = ""

            if revised:
                final_sql = revised

    t0 = time.perf_counter()
    peer_result = run_peer(final_sql, remote_conn)
    peer_module_time += time.perf_counter() - t0

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
    print(_format_sql(final_sql))
    print(f"{'=' * 60}\n")

    if not _is_executable_sql(final_sql):
        print("[main] Generated SQL is empty/comment-only and not executable.")
        exec_error = "Generated SQL is empty/comment-only"
        success = False
    else:
        print("\n[executor] Executing SQL against remote database...")
        t0 = time.perf_counter()
        success, exec_error = _execute_sql(final_sql)
        sql_execution_time += time.perf_counter() - t0

    if not success and exec_error:
        print(f"\n[sql_generator] Attempt 1 failed: {exec_error}. Retrying...")

        retry_prompt = build_retry_prompt(prompt, final_sql, exec_error)
        print(f"\n[sql_generator] Regenerating SQL with error context...")
        t0 = time.perf_counter()
        retry_response = generate_sql(retry_prompt)
        retry_raw_sql = extract_sql_from_response(retry_response)
        sql_generation_time += time.perf_counter() - t0

        print(f"\n{'=' * 60}")
        print("  Retry SQL (before PEER patching):")
        print(f"{'=' * 60}")
        print(_format_sql(retry_raw_sql))
        print(f"{'=' * 60}")

        t0 = time.perf_counter()
        retry_peer = run_peer(retry_raw_sql, remote_conn)
        peer_module_time += time.perf_counter() - t0

        if retry_peer.messages:
            print(f"\n{'=' * 60}")
            print("  Entity Resolution Notes (retry):")
            print(f"{'=' * 60}")
            for msg in retry_peer.messages:
                print(f"  {msg}")

        final_retry_sql = retry_peer.sql

        print(f"\n{'=' * 60}")
        if retry_peer.patched:
            print("  Retry SQL (PEER-patched):")
        else:
            print("  Retry SQL:")
        print(f"{'=' * 60}")
        print(_format_sql(final_retry_sql))
        print(f"{'=' * 60}\n")

        if not _is_executable_sql(final_retry_sql):
            print("[main] Retry SQL is empty/comment-only and not executable.")
            retry_success, retry_error = False, "Retry SQL is empty/comment-only"
        else:
            print("[executor] Executing retry SQL against remote database...")
            t0 = time.perf_counter()
            retry_success, retry_error = _execute_sql(final_retry_sql)
            sql_execution_time += time.perf_counter() - t0

        if not retry_success:
            print(f"\n[sql_generator] Attempt 2 failed: {retry_error}")
            print(
                "\n[main] Could not generate a valid SQL query after 2 attempts.\n"
                "  The question may reference columns or logic not present in the schema.\n"
                f"  Last error: {retry_error}"
            )

    print(citation_md)
    query_classify_mutate_retrieve_time = (
        query_classification_time + query_mutation_kb_retrieval_time
    )
    print(f"\n{'=' * 60}")
    print("  MODULE TIMING SUMMARY")
    print(f"{'=' * 60}")
    print(
        "[timing] Query classification + Mutation + KB Retrieval : "
        f"{query_classify_mutate_retrieve_time:.2f} seconds"
    )
    print(f"[timing] SQL generation   : {sql_generation_time:.2f} seconds")
    print(f"[timing] SQL verification : {sql_verification_time:.2f} seconds")
    print(f"[timing] SQL reviewer     : {sql_reviewer_time:.2f} seconds")
    print(f"[timing] PEER module      : {peer_module_time:.2f} seconds")
    print(f"[timing] SQL execution    : {sql_execution_time:.2f} seconds")
    end_time = time.time()
    Total_Query_Execution_Time = end_time - start_time
    print(f"\n[main] Total time taken to Execute the query is: {Total_Query_Execution_Time:.2f} seconds")

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
