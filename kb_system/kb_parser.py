"""
kb_system/kb_parser.py
-----------------------
Parses markdown KB files into structured Python objects.

Each KB file has two parts:
    1. YAML frontmatter (between --- delimiters) — contains name, description,
       tags, priority. This is the metadata used for routing/embeddings.
    2. Markdown body — contains the actual DDL, business rules, SQL patterns.
       This is the "data field" injected into the SQL prompt.

The ParsedKBFile dataclass holds both parts plus derived fields needed by
kb_store.py (file_path, section, is_entry_point, embedding_text).

Embedding text is computed as:  "{name} — {description}"
This gives the embedding model both the topic name AND a semantic description
of when to use this file — which is exactly what we want for routing.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class ParsedKBFile:
    """
    Structured representation of a parsed KB markdown file.

    Attributes
    ----------
    file_path : str
        Relative path from KB_ROOT, used as the unique key in the database.
        e.g., "ddl/players.md" or "ddl/KB.md"
    section : str
        Top-level folder name — "ddl", "business_rules", etc.
        Derived from the first component of file_path.
    metadata : dict
        All parsed YAML frontmatter fields as a dict.
        e.g., {"name": "players", "description": "...", "tags": [...]}
    content : str
        Raw markdown body below the frontmatter (the DDL, rules, etc.)
        This is what gets injected into the SQL generation prompt.
    is_entry_point : bool
        True if this is a KB.md section index file.
        Entry points are injected as section context, never similarity-searched.
    embedding_text : str
        The text to embed for similarity search.
        Computed as "{name} — {description}" from frontmatter.
        Empty string for entry points (they don't get embeddings).
    """

    file_path: str
    section: str
    metadata: dict[str, Any]
    content: str
    is_entry_point: bool
    embedding_text: str = field(default="")


def parse_markdown_file(md_path: Path, kb_root: Path) -> ParsedKBFile:
    """
    Parse a single KB markdown file into a ParsedKBFile object.

    Splits the file at the YAML frontmatter delimiters (---), parses
    the YAML block, extracts the markdown body, and derives all fields
    needed for database storage and embedding computation.

    Parameters
    ----------
    md_path : Path
        Absolute path to the .md file to parse.
    kb_root : Path
        Absolute path to the KB root directory (knowledge_base_files/).
        Used to compute the relative file_path stored in the DB.

    Returns
    -------
    ParsedKBFile
        Fully populated dataclass ready for kb_store.upsert_kb_file().

    Raises
    ------
    ValueError
        If the file has no YAML frontmatter or the frontmatter is missing
        required fields (name, description).
    yaml.YAMLError
        If the frontmatter YAML is malformed.
    """
    raw_text = md_path.read_text(encoding="utf-8")

    # Split frontmatter from body using --- delimiters
    # Pattern: optional whitespace, ---, content, ---, rest
    frontmatter_pattern = re.compile(r"^---\s*\n(.*?)\n---\s*\n(.*)", re.DOTALL)
    match = frontmatter_pattern.match(raw_text)

    if not match:
        raise ValueError(
            f"File '{md_path}' has no valid YAML frontmatter. "
            "All KB files must start with --- ... --- frontmatter block."
        )

    frontmatter_str = match.group(1)
    body_content = match.group(2).strip()

    # Parse YAML frontmatter
    try:
        metadata: dict[str, Any] = yaml.safe_load(frontmatter_str) or {}
    except yaml.YAMLError as exc:
        raise yaml.YAMLError(f"Invalid YAML frontmatter in '{md_path}': {exc}") from exc

    # Derive relative file_path (e.g., "ddl/players.md")
    relative_path = str(md_path.relative_to(kb_root))
    relative_path = relative_path.replace("\\", "/")

    # Derive section from the first path component
    section = relative_path.split("/")[0]

    # Determine if this is a KB.md entry point
    is_entry_point = md_path.name == "KB.md"

    # Build embedding text from name + description
    # Entry points don't need embedding_text (they're not similarity-searched)
    name = metadata.get("name", "")
    description = metadata.get("description", "")

    if is_entry_point:
        embedding_text = ""  # Entry points are never embedded
    else:
        # This is the text the OpenAI embedding model will encode.
        # Format: "players — Use this when user asks about player profiles..."
        embedding_text = f"{name} — {description}".strip(" —")

    return ParsedKBFile(
        file_path=relative_path,
        section=section,
        metadata=metadata,
        content=body_content,
        is_entry_point=is_entry_point,
        embedding_text=embedding_text,
    )


def scan_kb_directory(kb_root: Path) -> list[ParsedKBFile]:
    """
    Recursively scan the KB root directory and parse all .md files.

    Walks kb_root looking for .md files in any subdirectory. Parses each
    one using parse_markdown_file(). Files that fail parsing are logged
    and skipped (so a single bad file doesn't abort the whole build).

    Parameters
    ----------
    kb_root : Path
        Absolute path to the KB root (knowledge_base_files/).

    Returns
    -------
    list[ParsedKBFile]
        All successfully parsed KB files. Entry points (KB.md) are
        included alongside table files.

    Notes
    -----
    Ordering: entry points come before table files within each section,
    which is the order they'll be upserted into the database.
    """
    if not kb_root.exists():
        raise FileNotFoundError(
            f"KB root directory not found: {kb_root}. "
            "Run 'python main.py generate' first to create KB files."
        )

    all_files: list[Path] = sorted(kb_root.rglob("*.md"))
    parsed_files: list[ParsedKBFile] = []

    print(f"[kb_parser] Found {len(all_files)} .md files under {kb_root}")

    for md_path in all_files:
        try:
            parsed = parse_markdown_file(md_path, kb_root)
            parsed_files.append(parsed)
            print(f"[kb_parser] ✓ Parsed: {parsed.file_path} "
                  f"(entry_point={parsed.is_entry_point})")
        except (ValueError, yaml.YAMLError) as exc:
            print(f"[kb_parser] ✗ Skipped '{md_path.name}': {exc}")

    return parsed_files
