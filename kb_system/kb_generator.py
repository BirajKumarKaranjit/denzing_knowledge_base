"""
kb_system/kb_generator.py
--------------------------
Generates knowledge base markdown files from raw DDL SQL using GPT-4.

This is the script you run ONCE (or when schema changes) to bootstrap your
knowledge base. It takes raw CREATE TABLE statements from your DDL extraction
script and produces structured .md files in the format:

    ---
    name: table_name
    description: "Routing description for embedding-based retrieval."
    tags: [tag1, tag2]
    priority: high | medium | low
    ---

    # DDL

    ```sql
    CREATE TABLE ...
    ```

    ## Column Semantics
    - col_name: business meaning, value ranges, nullability notes
    - ...

    ## Common Query Patterns
    - How this table is typically joined/filtered

The output files are written to knowledge_base_files/ddl/ and can (and should)
be manually edited after generation to add domain-specific knowledge that
GPT-4 cannot infer from DDL alone.

Workflow:
    1. Run your DDL extraction script → get raw SQL strings per table
    2. Call generate_all_kb_files() → writes ALL .md files to disk
       (DDL table files + section KB.md index files + root KB.md)
    3. Review and edit the .md files manually for accuracy
    4. Run `python main.py build` to embed + load into Postgres
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
from utils.config import OPENAI_API_KEY, OPENAI_GENERATION_MODEL, KB_SECTIONS, KB_ROOT
from utils.sample_values_for_testing import (
    Sample_NBA_DDL_DICT,
    sql_guidelines_content,
    biz_rules_content,
    response_guidelines_content,
)

_client = openai.OpenAI(api_key=OPENAI_API_KEY)


# ── LLM Response Helpers ──────────────────────────────────────────────────────

def _strip_llm_fences(llm_output: str) -> str:
    """
    Strip outer markdown code fences if the LLM wraps its response in them.

    GPT-4 sometimes wraps the entire .md file output in a ```markdown ... ```
    block even when instructed not to. This helper detects and removes those
    outer fences so the raw frontmatter (---) is at the very top of the file.

    Parameters
    ----------
    llm_output : str
        Raw string returned by the OpenAI chat completion.

    Returns
    -------
    str
        Cleaned content guaranteed to start with --- if the LLM correctly
        generated YAML frontmatter.
    """
    stripped = llm_output.strip()

    outer_fence = re.match(r"^```[a-zA-Z]*\n(.*)\n```$", stripped, re.DOTALL)
    if outer_fence:
        return outer_fence.group(1).strip()

    return stripped


_TABLE_FILE_SYSTEM_PROMPT = """
        You are a technical documentation expert specializing in NBA basketball analytics databases.
        Your job is to generate structured knowledge base files in a specific markdown format.
        
        These files are used by an AI SQL generation system. The files have two parts:
        1. YAML frontmatter (the "metadata field"): used for semantic routing via embedding similarity
        2. Markdown body (the "data field"): the actual DDL + column semantics injected into SQL prompts
        
        RULES:
        - The frontmatter 'description' field is CRITICAL — it must be a rich, natural-language description
          of when this table should be used. Write it as: "Use when the query involves X, Y, Z."
          This description is what gets embedded and compared against user queries.
        - The description should include NBA-specific terminology users would naturally use in questions.
        - In the body, add semantic meaning beyond what the DDL states — explain what each column means
          in basketball context, common value ranges, business rules, and join patterns.
        - Tags should be 2-5 lowercase keywords relevant to the table.
        - Priority: high = core fact tables, medium = dimension tables, low = lookup/reference tables.
        - Return ONLY the markdown content. No explanations, no code fences around the whole output.
    """

_SECTION_KB_SYSTEM_PROMPT = """
        You are a technical documentation expert. Generate a KB.md index file for a knowledge base section.
        This file describes what the section covers and is used by humans navigating the KB.
        It is NOT embedded for vector retrieval — it's injected as context alongside retrieved table files.
        
        Return ONLY the markdown content. No explanations outside the format.
    """

_ROOT_KB_SYSTEM_PROMPT = """
        You are a technical documentation expert. Generate the root KB.md index file for an NBA analytics
        knowledge base. This file gives an overview of all sections.
        
        Return ONLY the markdown content. No explanations outside the format.
    """


def generate_table_md_file(
    table_name: str,
    ddl_sql: str,
    domain: str = "NBA basketball analytics",
) -> str:
    """
    Call GPT-4 to generate a structured KB markdown file for a single database table.

    The generated file contains YAML frontmatter (metadata field used for embedding-based
    routing) and a markdown body (data field with DDL + column semantics injected into
    the SQL prompt at query time).

    Parameters
    ----------
    table_name : str
        Database table name (e.g., "players", "box_scores").
        Used in the frontmatter ``name`` field and in the LLM prompt.
    ddl_sql : str
        Raw CREATE TABLE SQL string for this table.
        Comes directly from your DDL extraction script.
    domain : str
        Domain context string passed to the LLM to improve description quality.
        Default is "NBA basketball analytics".
    Returns
    -------
    str
        Generated markdown content as a string (not yet written to disk).
        Caller should write this to knowledge_base_files/ddl/{table_name}.md.

    Raises
    ------
    openai.OpenAIError
        If the GPT-4 API call fails after retries.
    """
    user_prompt = f"""
        Generate a knowledge base markdown file for the following database table.
        
        Domain: {domain}
        Table name: {table_name}
        
        DDL:
        {ddl_sql}
        
        Required format:
        ---
        name: {table_name}
        description: "WRITE A RICH DESCRIPTION HERE. Start with: Use when the query involves..."
          Include all {domain} relevant terminology users might use when asking about this data.
          This is the most important field — make it comprehensive (3-5 sentences).
        tags: [tag1, tag2, tag3]
        priority: high | medium | low
        ---
        
        # DDL
        
        ```sql
        {ddl_sql}
        ```
        
        ## Column Semantics
        For each column, explain:
        - Business meaning in {domain} context
        - Value ranges or example values if inferrable
        - Whether it's typically used in WHERE, GROUP BY, or SELECT
        - Any gotchas (nullable, approximate values, etc.)
        
        ## Common Query Patterns
        2-4 bullet points showing how this table is typically used in queries.
        Include example WHERE conditions or JOIN patterns.
        
        ## Join Relationships
        How this table relates to other tables (foreign keys, typical join conditions).
        """

    messages: list[ChatCompletionSystemMessageParam | ChatCompletionUserMessageParam] = [
        ChatCompletionSystemMessageParam(role="system", content=_TABLE_FILE_SYSTEM_PROMPT),
        ChatCompletionUserMessageParam(role="user", content=user_prompt),
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
    """
    Generate the KB.md index file for a KB section (e.g., ddl/KB.md).

    This file is not embedded for vector retrieval. Instead it is injected
    alongside matched table files to give the LLM section-level context.
    Humans also use it to navigate the KB.

    Parameters
    ----------
    section_name : str
        Name of the KB section (e.g., "ddl", "business_rules").
    table_names : list[str]
        Names of all files/tables in this section (for listing in the index).
    section_description : str
        What this section covers — used by the LLM to understand section scope.

    Returns
    -------
    str
        Generated KB.md content ready to write to knowledge_base_files/{section}/KB.md.
    """
    user_prompt = f"""
        Generate a KB.md index file for the '{section_name}' section of an NBA analytics knowledge base.
        
        Section description: {section_description}
        
        Files in this section:
        {chr(10).join(f'- {name}' for name in table_names)}
        
        Required format:
        ---
        name: {section_name}_index
        description: "Brief description of what this section covers and when to use it."
        ---
        
        # {section_name.replace('_', ' ').title()} Section
        
        [2-3 sentence overview of what this section contains]
        
        ## Contents
        
        [List each file with a one-line description of what it covers]
        """

    messages: list[ChatCompletionSystemMessageParam | ChatCompletionUserMessageParam] = [
        ChatCompletionSystemMessageParam(role="system", content=_SECTION_KB_SYSTEM_PROMPT),
        ChatCompletionUserMessageParam(role="user", content=user_prompt),
    ]
    response = _client.chat.completions.create(
        model=OPENAI_GENERATION_MODEL,
        messages=messages,
        temperature=0.1,
        max_tokens=800,
    )

    return _strip_llm_fences(response.choices[0].message.content.strip())


def generate_root_kb_md(sections: dict[str, str], domain: str = "NBA basketball analytics") -> str:
    """
    Generate the root KB.md file that describes the entire knowledge base.

    The root KB.md gives the LLM (and humans) an overview of what sections
    exist and what each covers. It is injected into the SQL prompt as
    high-level context.

    Parameters
    ----------
    sections : dict[str, str]
        Mapping of section_name → brief description.
        e.g., {"ddl": "Table schemas...", "business_rules": "..."}

    Returns
    -------
    str
        Generated root KB.md content ready to write to knowledge_base_files/KB.md.
    """
    section_list = "\n".join(
        f"- {name}: {desc}" for name, desc in sections.items()
    )

    user_prompt = f"""
        Generate the root KB.md for the {domain} knowledge base.
        
        Sections:
        {section_list}
        
        Required format:
        ---
        name: root_knowledge_base
        description: "Root index of the {domain} knowledge base."
        ---
        
        # {domain} Knowledge Base
        
        [2-3 sentence overview of the entire KB]
        
        ## Sections
        [List each section with description]
        """

    messages: list[ChatCompletionSystemMessageParam | ChatCompletionUserMessageParam] = [
        ChatCompletionSystemMessageParam(role="system", content=_ROOT_KB_SYSTEM_PROMPT),
        ChatCompletionUserMessageParam(role="user", content=user_prompt),
    ]
    response = _client.chat.completions.create(
        model=OPENAI_GENERATION_MODEL,
        messages=messages,
        temperature=0.1,
        max_tokens=600,
    )

    return _strip_llm_fences(response.choices[0].message.content.strip())


def generate_all_table_files(
    ddl_dict: dict[str, str],
    output_dir: Optional[Path] = None,
    overwrite: bool = False,
) -> list[Path]:
    """
    Generate KB markdown files for all tables in ddl_dict and write to disk.

    This is the internal entry point for bootstrapping the DDL section of the KB.
    Prefer calling generate_all_kb_files() which also generates the static sections.

    The files are written to knowledge_base_files/ddl/{table_name}.md.
    After generation, REVIEW and EDIT the files manually — GPT-4 generates
    a solid first draft but you should add domain-specific knowledge it cannot
    infer from DDL alone (e.g., "salary data is only available from 2000 onward").

    Parameters
    ----------
    ddl_dict : dict[str, str]
        Mapping of table_name → raw CREATE TABLE SQL string.
        This comes directly from your DDL extraction script.
        Example: {"players": "CREATE TABLE players (id UUID, ...)", ...}
    output_dir : Path | None
        Directory to write .md files into. Defaults to KB_SECTIONS["ddl"]
        from config.py (i.e., knowledge_base_files/ddl/).
    overwrite : bool
        If False (default), skip tables that already have a .md file.
        Set to True to regenerate all files (e.g., after schema changes).

    Returns
    -------
    list[Path]
        Paths of all .md files that were written (skipped files excluded).
    """
    if output_dir is None:
        output_dir = KB_SECTIONS["ddl"]

    output_dir.mkdir(parents=True, exist_ok=True)

    written_files: list[Path] = []
    table_names = list(ddl_dict.keys())

    for table_name, ddl_sql in ddl_dict.items():
        output_path = output_dir / f"{table_name}.md"

        if output_path.exists() and not overwrite:
            print(f"[kb_generator] Skipping {table_name}.md Use overwrite=True to regenerate.")
            continue

        print(f"[kb_generator] Generating {table_name}.md ...")
        content = generate_table_md_file(table_name, ddl_sql)

        output_path.write_text(content, encoding="utf-8")
        written_files.append(output_path)
        print(f"[kb_generator] Written: {output_path}")

    ddl_kb_path = output_dir / "KB.md"
    if not ddl_kb_path.exists() or overwrite:
        print("[kb_generator] Generating ddl/KB.md ...")
        ddl_kb_content = generate_section_kb_md(
            section_name="ddl",
            table_names=table_names,
            section_description="Database table schemas, column definitions, and semantic descriptions for the NBA analytics database.",
        )
        ddl_kb_path.write_text(ddl_kb_content, encoding="utf-8")
        written_files.append(ddl_kb_path)
        print(f"[kb_generator] ✓ Written: {ddl_kb_path}")

    root_kb_path = KB_ROOT / "KB.md"
    if not root_kb_path.exists() or overwrite:
        KB_ROOT.mkdir(parents=True, exist_ok=True)
        print("[kb_generator] Generating root KB.md ...")
        root_content = generate_root_kb_md(
            sections={
                "ddl": "Table schemas and column definitions for all NBA database tables.",
                "business_rules": "NBA domain metrics, KPI definitions, and calculation formulas.",
                "sql_guidelines": "SQL query patterns, join conventions, and dialect-specific gotchas.",
                "response_guidelines": "Output formatting and response quality guidelines.",
            }
        )
        root_kb_path.write_text(root_content, encoding="utf-8")
        written_files.append(root_kb_path)
        print(f"[kb_generator] Written: {root_kb_path}")

    return written_files


def generate_static_section_files(overwrite: bool = False) -> None:
    """
    Write the static KB.md files for non-DDL sections using sample content.

    Unlike DDL files (generated from raw SQL via GPT-4), the business_rules,
    sql_guidelines, and response_guidelines sections are bootstrapped from
    the pre-written sample content in utils/sample_values_for_testing.py.

    Edit the generated files heavily after generation — the samples are
    solid starting points but you should tailor them to your exact domain.

    Parameters
    ----------
    overwrite : bool
        If False (default), skip files that already exist.
        Set to True to overwrite with fresh sample content.
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

        # Strip leading indentation that may exist in the sample string
        cleaned = "\n".join(line.lstrip() for line in content.splitlines())
        kb_path.write_text(cleaned.strip() + "\n", encoding="utf-8")
        print(f"[kb_generator] ✓ Written: {kb_path}")


def generate_all_kb_files(
    ddl_dict: Optional[dict[str, str]] = None,
    overwrite: bool = False,
) -> None:
    """
    Top-level entry point: generate ALL KB markdown files in one call.

    1. DDL table files (one .md per table, LLM-generated from CREATE TABLE SQL)
    2. DDL section KB.md index  (LLM-generated listing of all tables)
    3. Root KB.md               (LLM-generated overview of all sections)
    4. Static section files     (sql_guidelines, business_rules, response_guidelines)

    After running this, review and edit the .md files, then run:
        python main.py build
    to embed + load everything into Postgres.

    Parameters
    ----------
    ddl_dict : dict[str, str] | None
        Mapping of table_name → raw CREATE TABLE SQL string.
        If None, uses Sample_NBA_DDL_DICT from sample_values_for_testing.py.
    overwrite : bool
        If False (default), existing .md files are not regenerated.
        Set to True to force-regenerate all files (e.g. after schema changes).
    """
    print("\n" + "=" * 60)
    print("  NBA Knowledge Base — File Generation")
    print("=" * 60)

    if ddl_dict is None:
        print("[kb_generator] No ddl_dict provided — using Sample_NBA_DDL_DICT.")
        ddl_dict = Sample_NBA_DDL_DICT

    print("\n[kb_generator] Generating DDL table files + index files via LLM...")
    generate_all_table_files(ddl_dict=ddl_dict, overwrite=overwrite)

    print("\n[kb_generator] Writing static section files (sql/business/response guidelines)...")
    generate_static_section_files(overwrite=overwrite)

    print("\n" + "=" * 60)
    print("  Generation complete.")
    print("  Review the files in knowledge_base_files/, then run:")
    print("      python main.py build")
    print("=" * 60 + "\n")

