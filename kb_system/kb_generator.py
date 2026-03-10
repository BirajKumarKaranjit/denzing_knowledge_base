"""kb_system/kb_generator.py

Generates knowledge base markdown files from raw DDL SQL using GPT-4.

Workflow:
    1. Supply a DDL dict (table_name → CREATE TABLE SQL).
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
    SECTION_SUB_SECTIONS,
    SECTION_HINTS,
    SECTION_DDL_REQUIRED,
)
from utils.sample_values_for_testing import Sample_NBA_DDL_DICT
from utils.prompts.kb_generation_prompts import (
    TABLE_FILE_SYSTEM_PROMPT,
    SECTION_KB_SYSTEM_PROMPT,
    ROOT_KB_SYSTEM_PROMPT,
    SECTION_SUB_FILE_SYSTEM_PROMPT,
    RESPONSE_FORMAT_SYSTEM_PROMPT,
    table_file_user_prompt,
    section_kb_user_prompt,
    root_kb_user_prompt,
    section_sub_file_user_prompt,
    response_format_user_prompt,
)

_client = openai.OpenAI(api_key=OPENAI_API_KEY)


def _strip_llm_fences(llm_output: str) -> str:
    """Strip outer markdown code fences if the LLM wraps its response in them."""
    stripped = llm_output.strip()
    outer_fence = re.match(r"^```[a-zA-Z]*\n(.*)\n```$", stripped, re.DOTALL)
    if outer_fence:
        return outer_fence.group(1).strip()
    return stripped


def _build_ddl_summary(ddl_dict: dict[str, str]) -> str:
    """Concatenate all DDL statements into one string for prompt injection."""
    return "\n\n".join(f"-- Table: {name}\n{sql}" for name, sql in ddl_dict.items())


def generate_table_md_file(
    table_name: str,
    ddl_sql: str,
    domain: str = "analytics",
) -> str:
    """Generate a structured KB markdown file for a single database table."""
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
    """Generate the KB.md index file for a KB section."""
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
    """Generate the root KB.md file that describes the entire knowledge base."""
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


def generate_section_sub_file(
    section: str,
    sub_section: str,
    hint: str,
    ddl_dict: Optional[dict[str, str]] = None,
) -> str:
    """Generate a single sub-file for any KB section via LLM.

    Uses a dedicated prompt for ``response_guidelines/response_format`` so the
    output is grounded in real DDL column names. All other sub-sections use the
    generic ``SECTION_SUB_FILE_SYSTEM_PROMPT``.
    """
    needs_ddl = SECTION_DDL_REQUIRED.get(section, False)
    ddl_summary = _build_ddl_summary(ddl_dict) if (needs_ddl and ddl_dict) else None

    if section == "response_guidelines" and sub_section == "response_format" and ddl_summary:
        system_content = RESPONSE_FORMAT_SYSTEM_PROMPT
        user_content = response_format_user_prompt(ddl_summary)
    else:
        system_content = SECTION_SUB_FILE_SYSTEM_PROMPT
        user_content = section_sub_file_user_prompt(section, sub_section, hint, ddl_summary)

    messages: list[ChatCompletionSystemMessageParam | ChatCompletionUserMessageParam] = [
        ChatCompletionSystemMessageParam(role="system", content=system_content),
        ChatCompletionUserMessageParam(role="user", content=user_content),
    ]
    response = _client.chat.completions.create(
        model=OPENAI_GENERATION_MODEL,
        messages=messages,
        temperature=0.2,
        max_tokens=2500,
    )
    return _strip_llm_fences(response.choices[0].message.content.strip())


def generate_all_sub_files_for_section(
    section: str,
    ddl_dict: Optional[dict[str, str]] = None,
    overwrite: bool = False,
) -> list[Path]:
    """Generate all sub-files for a given KB section.

    Iterates over ``SECTION_SUB_SECTIONS[section]`` and calls
    ``generate_section_sub_file`` for each entry. Works for any section
    registered in config — no dedicated function needed per section.

    Parameters
    ----------
    section:
        KB section key (e.g. ``"sql_guidelines"``, ``"business_rules"``).
    ddl_dict:
        DDL dict used for schema-grounded sections. Ignored when the
        section has ``SECTION_DDL_REQUIRED[section] == False``.
    overwrite:
        Re-generate and overwrite files that already exist on disk.
    """
    sub_sections = SECTION_SUB_SECTIONS.get(section, [])
    hints = SECTION_HINTS.get(section, {})
    section_dir = KB_SECTIONS[section]
    section_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []

    for sub in sub_sections:
        path = section_dir / f"{sub}.md"

        if path.exists() and not overwrite:
            print(f"[kb_generator] Skipping {section}/{sub}.md (already exists).")
            continue

        hint = hints.get(sub, f"Patterns and rules for the '{sub}' topic.")
        print(f"[kb_generator] Generating {section}/{sub}.md via LLM...")
        content = generate_section_sub_file(section, sub, hint, ddl_dict)
        path.write_text(content, encoding="utf-8")
        written.append(path)
        print(f"[kb_generator] Written: {path}")

    return written


def generate_all_table_files(
    ddl_dict: dict[str, str],
    output_dir: Optional[Path] = None,
    overwrite: bool = False,
    domain: str = "analytics",
) -> list[Path]:
    """Generate KB markdown files for all tables and write to disk."""
    if output_dir is None:
        output_dir = KB_SECTIONS["ddl"]

    output_dir.mkdir(parents=True, exist_ok=True)
    written_files: list[Path] = []
    table_names = list(ddl_dict.keys())

    for table_name, ddl_sql in ddl_dict.items():
        output_path = output_dir / f"{table_name}.md"

        if output_path.exists() and not overwrite:
            print(f"[kb_generator] Skipping {table_name}.md (already exists).")
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


def generate_all_kb_files(
    ddl_dict: Optional[dict[str, str]] = None,
    overwrite: bool = False,
    domain: str = "analytics",
) -> None:
    """Top-level entry point: generate ALL KB markdown files.

    Steps:
        1. DDL table files + ddl/KB.md + root KB.md (LLM-generated from DDL).
        2. Sub-files for all sections registered in ``SECTION_SUB_SECTIONS``
           (sql_guidelines, business_rules, and any future additions).
    """
    print("\n" + "=" * 60)
    print("  Knowledge Base — File Generation")
    print("=" * 60)

    if ddl_dict is None:
        print("[kb_generator] No ddl_dict provided — using Sample_NBA_DDL_DICT.")
        ddl_dict = Sample_NBA_DDL_DICT

    print("\n[kb_generator] Generating DDL table files + index files via LLM...")
    generate_all_table_files(ddl_dict=ddl_dict, overwrite=overwrite, domain=domain)

    for section in SECTION_SUB_SECTIONS:
        print(f"\n[kb_generator] Generating sub-files for section: {section}...")
        generate_all_sub_files_for_section(
            section=section,
            ddl_dict=ddl_dict,
            overwrite=overwrite,
        )

    print("\n" + "=" * 60)
    print("  Generation complete. Review knowledge_base_files/, then run:")
    print("      python main.py build")
    print("=" * 60 + "\n")

