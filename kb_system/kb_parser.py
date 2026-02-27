"""kb_system/kb_parser.py

Parses KB markdown files into structured Python objects.
Each file has YAML frontmatter (metadata/routing) and a markdown body (data/context).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class ParsedKBFile:
    """Structured representation of a parsed KB markdown file.

    Attributes
    ----------
    file_path : str
        Relative path from KB_ROOT (e.g. ``"ddl/players.md"``).
    section : str
        Top-level folder name (e.g. ``"ddl"``, ``"business_rules"``).
    metadata : dict
        Parsed YAML frontmatter fields.
    content : str
        Raw markdown body below the frontmatter.
    is_entry_point : bool
        True for KB.md section index files; these are not similarity-searched.
    embedding_text : str
        Text to embed for similarity search. Empty for entry points.
    """

    file_path: str
    section: str
    metadata: dict[str, Any]
    content: str
    is_entry_point: bool
    embedding_text: str = field(default="")


def parse_markdown_file(md_path: Path, kb_root: Path) -> ParsedKBFile:
    """Parse a single KB markdown file into a ParsedKBFile.

    Parameters
    ----------
    md_path : Path
        Absolute path to the .md file.
    kb_root : Path
        KB root directory used to compute the relative file_path.

    Returns
    -------
    ParsedKBFile

    Raises
    ------
    ValueError
        If the file has no YAML frontmatter.
    yaml.YAMLError
        If the frontmatter YAML is malformed.
    """
    raw_text = md_path.read_text(encoding="utf-8")

    frontmatter_pattern = re.compile(r"^---\s*\n(.*?)\n---\s*\n(.*)", re.DOTALL)
    match = frontmatter_pattern.match(raw_text)

    if not match:
        raise ValueError(
            f"File '{md_path}' has no valid YAML frontmatter. "
            "All KB files must start with --- ... --- frontmatter block."
        )

    frontmatter_str = match.group(1)
    body_content = match.group(2).strip()

    try:
        metadata: dict[str, Any] = yaml.safe_load(frontmatter_str) or {}
    except yaml.YAMLError as exc:
        raise yaml.YAMLError(f"Invalid YAML frontmatter in '{md_path}': {exc}") from exc

    relative_path = str(md_path.relative_to(kb_root)).replace("\\", "/")
    section = relative_path.split("/")[0]
    is_entry_point = md_path.name == "KB.md"

    name = metadata.get("name", "")
    description = metadata.get("description", "")
    tags = metadata.get("tags", [])

    embedding_text = "" if is_entry_point else f"{name} — {description}  — {tags}".strip(" —")

    return ParsedKBFile(
        file_path=relative_path,
        section=section,
        metadata=metadata,
        content=body_content,
        is_entry_point=is_entry_point,
        embedding_text=embedding_text,
    )


def scan_kb_directory(kb_root: Path) -> list[ParsedKBFile]:
    """Recursively scan the KB root and parse all .md files.

    Files that fail parsing are logged and skipped.

    Parameters
    ----------
    kb_root : Path
        Absolute path to the KB root directory.

    Returns
    -------
    list[ParsedKBFile]
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
            print(f"[kb_parser] Parsed: {parsed.file_path} (entry_point={parsed.is_entry_point})")
        except (ValueError, yaml.YAMLError) as exc:
            print(f"[kb_parser] Skipped '{md_path.name}': {exc}")

    return parsed_files
