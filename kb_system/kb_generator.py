"""kb_system/kb_generator.py

Generates knowledge base markdown files from raw DDL SQL using GPT-4.

Run once (or when schema changes) to bootstrap the knowledge base.

Workflow:
    1. Run DDL extraction script → get raw SQL strings per table.
    2. Call generate_all_kb_files() → writes all .md files to disk.
    3. Review and edit the generated files.
    4. Run `python main.py build` to embed and load into Postgres.
"""

from __future__ import annotations

import re
import sys
import os
from pathlib import Path
from typing import Optional

import openai
from openai.types.chat import ChatCompletionSystemMessageParam, ChatCompletionUserMessageParam

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config import (
    OPENAI_API_KEY,
    OPENAI_GENERATION_MODEL,
    KB_SECTIONS,
    KB_ROOT,
    SQL_GUIDELINES_SUB_SECTIONS,
)
from utils.sample_values_for_testing import (
    Sample_NBA_DDL_DICT,
    sql_guidelines_content,
    biz_rules_content,
    response_guidelines_content,
)
from utils.prompts.kb_generation_prompts import (
    TABLE_FILE_SYSTEM_PROMPT,
    SECTION_KB_SYSTEM_PROMPT,
    ROOT_KB_SYSTEM_PROMPT,
    SQL_GUIDELINES_SUB_SYSTEM_PROMPT,
    table_file_user_prompt,
    section_kb_user_prompt,
    root_kb_user_prompt,
    sql_guideline_sub_file_user_prompt,
)

_client = openai.OpenAI(api_key=OPENAI_API_KEY)

# Generic, schema-agnostic descriptions for each SQL guideline category.
# Domain-specific examples are produced by the LLM using the actual DDL.
_SQL_GUIDELINE_HINTS: dict[str, str] = {
    "joins": (
        "JOIN patterns for linking tables across the schema. Include standard join pairs, "
        "double-join gotchas (e.g., self-referencing tables or joining the same table twice), "
        "and the correct columns to join on based on the DDL foreign key relationships."
    ),
    "aggregations": (
        "GROUP BY patterns, SUM/AVG/COUNT usage, per-record calculations, totals by dimension, "
        "HAVING clauses, window functions (RANK, DENSE_RANK, ROW_NUMBER, LAG/LEAD), "
        "and the NULLIF trick for safe division to avoid divide-by-zero errors."
    ),
    "filters": (
        "WHERE clause patterns for text lookup, categorical filtering, numeric range filters, "
        "status/flag filtering, and NULL handling. Include ILIKE for case-insensitive text "
        "matching and IN/ANY for multi-value filters."
    ),
    "comparisons": (
        "CASE WHEN expressions, subquery comparisons against aggregated values, "
        "side-by-side entity comparisons, HAVING threshold filters, "
        "and conditional aggregations using FILTER (WHERE ...)."
    ),
    "date_handling": (
        "DATE_TRUNC for period grouping, date range filters, EXTRACT for year/month components, "
        "finding the latest available period dynamically with MAX(), "
        "and avoiding hard-coded dates in production queries."
    ),
    "performance": (
        "Filter-before-join optimisation, which columns are typically indexed (PKs, FKs, common "
        "filter columns), LIMIT for large result sets, EXISTS vs IN for subqueries, "
        "and avoiding expensive full-table scans by pushing predicates early."
    ),
}


def _strip_llm_fences(llm_output: str) -> str:
    """Strip outer markdown code fences if the LLM wraps its response in them."""
    stripped = llm_output.strip()
    outer_fence = re.match(r"^```[a-zA-Z]*\n(.*)\n```$", stripped, re.DOTALL)
    if outer_fence:
        return outer_fence.group(1).strip()
    return stripped


def _build_ddl_summary(ddl_dict: dict[str, str]) -> str:
    """Concatenate all DDL statements into a single string for prompt injection."""
    return "\n\n".join(f"-- Table: {name}\n{sql}" for name, sql in ddl_dict.items())


def generate_table_md_file(
    table_name: str,
    ddl_sql: str,
    domain: str = "analytics",
) -> str:
    """Call the LLM to generate a structured KB markdown file for a single table.

    Parameters
    ----------
    table_name:
        Database table name.
    ddl_sql:
        Raw CREATE TABLE SQL string for this table.
    domain:
        Domain context string (e.g. "NBA basketball analytics").

    Returns
    -------
    str
        Generated markdown content (not yet written to disk).
    """
    messages: list[ChatCompletionSystemMessageParam | ChatCompletionUserMessageParam] = [
        ChatCompletionSystemMessageParam(role="system", content=TABLE_FILE_SYSTEM_PROMPT),
        ChatCompletionUserMessageParam(
            role="user",
            content=table_file_user_prompt(table_name, ddl_sql, domain),
        ),
    ]
    response = _client.chat.completions.create(
        model=OPENAI_GENERATION_MODEL,
        messages=messages,
        temperature=0.2,
        max_tokens=2000,
    )
    return _strip_llm_fences(response.choices[0].message.content.strip())


def generate_section_kb_md(
    section_name: str,
    table_names: list[str],
    section_description: str,
) -> str:
    """Generate the KB.md index file for a KB section.

    Parameters
    ----------
    section_name:
        Name of the KB section (e.g. "ddl", "business_rules").
    table_names:
        Names of all files/tables in this section.
    section_description:
        What this section covers.

    Returns
    -------
    str
        Generated KB.md content.
    """
    messages: list[ChatCompletionSystemMessageParam | ChatCompletionUserMessageParam] = [
        ChatCompletionSystemMessageParam(role="system", content=SECTION_KB_SYSTEM_PROMPT),
        ChatCompletionUserMessageParam(
            role="user",
            content=section_kb_user_prompt(section_name, table_names, section_description),
        ),
    ]
    response = _client.chat.completions.create(
        model=OPENAI_GENERATION_MODEL,
        messages=messages,
        temperature=0.1,
        max_tokens=800,
    )
    return _strip_llm_fences(response.choices[0].message.content.strip())


def generate_root_kb_md(sections: dict[str, str], domain: str = "analytics") -> str:
    """Generate the root KB.md file that describes the entire knowledge base.

    Parameters
    ----------
    sections:
        Mapping of section_name to brief description.
    domain:
        Domain label for the knowledge base.

    Returns
    -------
    str
        Generated root KB.md content.
    """
    messages: list[ChatCompletionSystemMessageParam | ChatCompletionUserMessageParam] = [
        ChatCompletionSystemMessageParam(role="system", content=ROOT_KB_SYSTEM_PROMPT),
        ChatCompletionUserMessageParam(
            role="user",
            content=root_kb_user_prompt(sections, domain),
        ),
    ]
    response = _client.chat.completions.create(
        model=OPENAI_GENERATION_MODEL,
        messages=messages,
        temperature=0.1,
        max_tokens=600,
    )
    return _strip_llm_fences(response.choices[0].message.content.strip())


def generate_sql_guideline_sub_file(
    sub_section: str,
    ddl_dict: dict[str, str],
) -> str:
    """Call the LLM to generate a SQL guideline sub-file for one category.

    The full DDL is injected into the prompt so all SQL examples use only
    real column names and table names — eliminating hallucinated columns.

    Parameters
    ----------
    sub_section:
        Guideline category name (e.g. "joins", "aggregations", "filters").
    ddl_dict:
        Mapping of table_name to CREATE TABLE SQL. Used to ground examples.

    Returns
    -------
    str
        Generated markdown content.
    """
    hint = _SQL_GUIDELINE_HINTS.get(
        sub_section,
        f"SQL patterns and best practices for the '{sub_section}' category.",
    )
    ddl_summary = _build_ddl_summary(ddl_dict)

    messages: list[ChatCompletionSystemMessageParam | ChatCompletionUserMessageParam] = [
        ChatCompletionSystemMessageParam(role="system", content=SQL_GUIDELINES_SUB_SYSTEM_PROMPT),
        ChatCompletionUserMessageParam(
            role="user",
            content=sql_guideline_sub_file_user_prompt(sub_section, hint, ddl_summary),
        ),
    ]
    response = _client.chat.completions.create(
        model=OPENAI_GENERATION_MODEL,
        messages=messages,
        temperature=0.2,
        max_tokens=2500,
    )
    return _strip_llm_fences(response.choices[0].message.content.strip())


def generate_all_table_files(
    ddl_dict: dict[str, str],
    output_dir: Optional[Path] = None,
    overwrite: bool = False,
    domain: str = "analytics",
) -> list[Path]:
    """Generate KB markdown files for all tables and write to disk.

    Parameters
    ----------
    ddl_dict:
        Mapping of table_name to raw CREATE TABLE SQL string.
    output_dir:
        Directory to write files into. Defaults to KB_SECTIONS["ddl"].
    overwrite:
        If False, skip tables that already have a .md file.
    domain:
        Domain context string passed to the LLM.

    Returns
    -------
    list[Path]
        Paths of all .md files that were written.
    """
    if output_dir is None:
        output_dir = KB_SECTIONS["ddl"]

    output_dir.mkdir(parents=True, exist_ok=True)
    written_files: list[Path] = []
    table_names = list(ddl_dict.keys())

    for table_name, ddl_sql in ddl_dict.items():
        output_path = output_dir / f"{table_name}.md"

        if output_path.exists() and not overwrite:
            print(f"[kb_generator] Skipping {table_name}.md — use overwrite=True to regenerate.")
            continue

        print(f"[kb_generator] Generating {table_name}.md ...")
        content = generate_table_md_file(table_name, ddl_sql, domain=domain)
        output_path.write_text(content, encoding="utf-8")
        written_files.append(output_path)
        print(f"[kb_generator] Written: {output_path}")

    ddl_kb_path = output_dir / "KB.md"
    if not ddl_kb_path.exists() or overwrite:
        print("[kb_generator] Generating ddl/KB.md ...")
        ddl_kb_content = generate_section_kb_md(
            section_name="ddl",
            table_names=table_names,
            section_description=(
                f"Database table schemas, column definitions, and semantic descriptions "
                f"for the {domain} database."
            ),
        )
        ddl_kb_path.write_text(ddl_kb_content, encoding="utf-8")
        written_files.append(ddl_kb_path)
        print(f"[kb_generator] Written: {ddl_kb_path}")

    root_kb_path = KB_ROOT / "KB.md"
    if not root_kb_path.exists() or overwrite:
        KB_ROOT.mkdir(parents=True, exist_ok=True)
        print("[kb_generator] Generating root KB.md ...")
        root_content = generate_root_kb_md(
            sections={
                "ddl": "Table schemas and column definitions for all database tables.",
                "business_rules": "Domain metric definitions, KPI formulas, and business logic.",
                "sql_guidelines": "SQL query patterns, join conventions, and best practices.",
                "response_guidelines": "Output formatting and response quality guidelines.",
            },
            domain=domain,
        )
        root_kb_path.write_text(root_content, encoding="utf-8")
        written_files.append(root_kb_path)
        print(f"[kb_generator] Written: {root_kb_path}")

    return written_files


def generate_sql_guidelines_sub_files(
    ddl_dict: Optional[dict[str, str]] = None,
    overwrite: bool = False,
) -> list[Path]:
    """Generate all SQL guideline sub-files (joins.md, aggregations.md, etc.).

    Parameters
    ----------
    ddl_dict:
        Mapping of table_name to CREATE TABLE SQL used to ground SQL examples.
        If None, uses Sample_NBA_DDL_DICT.
    overwrite:
        If False, skip sub-files that already exist on disk.

    Returns
    -------
    list[Path]
        Paths of all sub-files that were written.
    """
    if ddl_dict is None:
        ddl_dict = Sample_NBA_DDL_DICT

    section_dir = KB_SECTIONS["sql_guidelines"]
    section_dir.mkdir(parents=True, exist_ok=True)
    written_files: list[Path] = []

    for sub_section in SQL_GUIDELINES_SUB_SECTIONS:
        output_path = section_dir / f"{sub_section}.md"

        if output_path.exists() and not overwrite:
            print(
                f"[kb_generator] Skipping sql_guidelines/{sub_section}.md (already exists). "
                "Use overwrite=True to regenerate."
            )
            continue

        print(f"[kb_generator] Generating sql_guidelines/{sub_section}.md via LLM...")
        content = generate_sql_guideline_sub_file(sub_section, ddl_dict)
        output_path.write_text(content, encoding="utf-8")
        written_files.append(output_path)
        print(f"[kb_generator] Written: {output_path}")

    return written_files


def generate_static_section_files(overwrite: bool = False) -> None:
    """Write static KB.md files for non-DDL sections using sample content.

    Parameters
    ----------
    overwrite:
        If False, skip files that already exist.
    """
    _static_files: list[tuple[str, str]] = [
        ("sql_guidelines", sql_guidelines_content),
        ("business_rules", biz_rules_content),
        ("response_guidelines", response_guidelines_content),
    ]

    for section_name, content in _static_files:
        section_dir = KB_SECTIONS[section_name]
        section_dir.mkdir(parents=True, exist_ok=True)
        kb_path = section_dir / "KB.md"

        if kb_path.exists() and not overwrite:
            print(f"[kb_generator] Skipping {section_name}/KB.md (already exists).")
            continue

        cleaned = "\n".join(line.lstrip() for line in content.splitlines())
        kb_path.write_text(cleaned.strip() + "\n", encoding="utf-8")
        print(f"[kb_generator] Written: {kb_path}")


def generate_all_kb_files(
    ddl_dict: Optional[dict[str, str]] = None,
    overwrite: bool = False,
    domain: str = "analytics",
) -> None:
    """Top-level entry point: generate ALL KB markdown files.

    Steps:
        1. DDL table files (LLM-generated from CREATE TABLE SQL).
        2. DDL section KB.md index (LLM-generated).
        3. Root KB.md (LLM-generated).
        4. Static section KB.md files (sql_guidelines, business_rules, response_guidelines).
        5. SQL guideline sub-files grounded with real DDL.

    Parameters
    ----------
    ddl_dict:
        Mapping of table_name to raw CREATE TABLE SQL. Defaults to Sample_NBA_DDL_DICT.
    overwrite:
        If False, existing .md files are not regenerated.
    domain:
        Domain label used in LLM prompts (e.g. "NBA basketball analytics").
    """
    print("\n" + "=" * 60)
    print("  Knowledge Base — File Generation")
    print("=" * 60)

    if ddl_dict is None:
        print("[kb_generator] No ddl_dict provided — using Sample_NBA_DDL_DICT.")
        ddl_dict = Sample_NBA_DDL_DICT

    print("\n[kb_generator] Generating DDL table files + index files via LLM...")
    generate_all_table_files(ddl_dict=ddl_dict, overwrite=overwrite, domain=domain)

    print("\n[kb_generator] Writing static section KB.md files...")
    generate_static_section_files(overwrite=overwrite)

    print("\n[kb_generator] Writing SQL guideline sub-files (grounded with DDL)...")
    generate_sql_guidelines_sub_files(ddl_dict=ddl_dict, overwrite=overwrite)

    print("\n" + "=" * 60)
    print("  Generation complete.")
    print("  Review the files in knowledge_base_files/, then run:")
    print("      python main.py build")
    print("=" * 60 + "\n")

