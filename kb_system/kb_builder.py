"""kb_system/kb_builder.py

Orchestrates the full KB build pipeline:
    scan .md files → parse → compute embeddings → upsert to Postgres.

Change detection avoids redundant embedding API calls:
    - Unchanged files are skipped entirely.
    - Files where only body content changed reuse the existing embedding.
    - Files where the frontmatter description changed get a fresh embedding.
"""

from __future__ import annotations

import hashlib
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config import KB_ROOT, EMBEDDING_DIMENSION
from kb_system.kb_parser import scan_kb_directory
from kb_system.kb_embeddings import get_embeddings_batch
from kb_system.kb_store import get_connection, init_schema, upsert_kb_file, list_all_files


def _md5(text: str) -> str:
    """Return an MD5 hex digest for a string."""
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def _fetch_stored_hashes(conn) -> dict[str, dict[str, str]]:
    """Return {file_path: {content_hash, embed_hash}} for all rows in the DB."""
    import json as _json

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                file_path,
                content,
                metadata->>'name'        AS name,
                metadata->>'description' AS description,
                metadata->'tags'         AS tags_json
            FROM kb_files;
            """
        )
        rows = cur.fetchall()

    result: dict[str, dict[str, str]] = {}
    for file_path, content, name, description, tags_json in rows:
        if isinstance(tags_json, list):
            tags_list = tags_json
        elif isinstance(tags_json, str):
            try:
                tags_list = _json.loads(tags_json)
            except (ValueError, TypeError):
                tags_list = []
        else:
            tags_list = []

        tags_repr = str(tags_list)
        embed_text = f"{name or ''} — {description or ''}  — {tags_repr}".strip(" —")
        result[file_path] = {
            "content_hash": _md5(content or ""),
            "embed_hash": _md5(embed_text),
        }
    return result


def build_kb(verbose: bool = True) -> dict[str, int]:
    """Execute the full KB build pipeline end-to-end.

    Connects to Postgres, ensures the schema exists, scans all .md files,
    computes embeddings for changed table files, and upserts to the DB.

    Parameters
    ----------
    verbose : bool
        Print per-file progress messages.

    Returns
    -------
    dict
        Keys: total_files, entry_points, table_files, skipped, unchanged.
    """
    print("\n" + "=" * 60)
    print("  Knowledge Base Builder")
    print("=" * 60)

    print("\n[kb_builder] Connecting to Postgres...")
    conn = get_connection()
    print("[kb_builder] Connected")

    print("[kb_builder] Initializing schema...")
    init_schema(conn)

    print(f"\n[kb_builder] Scanning KB directory: {KB_ROOT}")
    all_parsed = scan_kb_directory(KB_ROOT)

    if not all_parsed:
        print("[kb_builder] No .md files found. Run 'python main.py generate' first.")
        conn.close()
        return {"total_files": 0, "entry_points": 0, "table_files": 0, "skipped": 0, "unchanged": 0}

    stored_hashes = _fetch_stored_hashes(conn)

    entry_points = [f for f in all_parsed if f.is_entry_point]
    table_files = [f for f in all_parsed if not f.is_entry_point]

    print(f"\n[kb_builder] Found {len(entry_points)} entry point(s) (KB.md files)")
    print(f"[kb_builder] Found {len(table_files)} table file(s)")

    print("\n[kb_builder] Storing entry points (no embeddings)...")
    for ep in entry_points:
        stored = stored_hashes.get(ep.file_path, {})
        new_hash = _md5(ep.content)
        if stored.get("content_hash") == new_hash:
            if verbose:
                print(f"[kb_builder] Unchanged (skipped): {ep.file_path}")
            continue
        row_id = upsert_kb_file(conn, ep, embedding=None)
        if verbose:
            print(f"[kb_builder] Updated entry point: {ep.file_path} (id={row_id})")

    files_to_embed: list = []
    files_content_only: list = []
    files_unchanged: list = []
    files_new: list = []

    for f in table_files:
        if not f.embedding_text.strip():
            continue

        stored = stored_hashes.get(f.file_path)

        if stored is None:
            files_new.append(f)
            continue

        new_content_hash = _md5(f.content)
        new_embed_hash = _md5(f.embedding_text)

        content_changed = stored["content_hash"] != new_content_hash
        embed_changed = stored["embed_hash"] != new_embed_hash

        if not content_changed and not embed_changed:
            files_unchanged.append(f)
        elif embed_changed:
            files_to_embed.append(f)
        else:
            files_content_only.append(f)

    all_needs_embedding = files_new + files_to_embed

    print(f"\n[kb_builder] Change summary:")
    print(f"  Need new embedding  : {len(all_needs_embedding)}")
    print(f"  Content-only update : {len(files_content_only)}")
    print(f"  Unchanged (skipped) : {len(files_unchanged)}")

    new_embeddings: list[list[float]] = []
    if all_needs_embedding:
        embedding_texts = [f.embedding_text for f in all_needs_embedding]
        print(f"\n[kb_builder] Calling OpenAI embeddings API for {len(embedding_texts)} file(s)...")
        new_embeddings = get_embeddings_batch(embedding_texts)

        if new_embeddings and len(new_embeddings[0]) != EMBEDDING_DIMENSION:
            raise ValueError(
                f"Embedding dimension mismatch: got {len(new_embeddings[0])}, "
                f"expected {EMBEDDING_DIMENSION}. Update EMBEDDING_DIMENSION in config.py."
            )

    for parsed_file, embedding in zip(all_needs_embedding, new_embeddings):
        row_id = upsert_kb_file(conn, parsed_file, embedding=embedding)
        tag = "(new)" if parsed_file in files_new else "(desc changed)"
        if verbose:
            print(f"[kb_builder] Upserted {tag}: {parsed_file.file_path} (id={row_id})")

    if files_content_only:
        print(f"\n[kb_builder] Upserting {len(files_content_only)} content-only change(s) "
              "(reusing existing embeddings)...")
        for parsed_file in files_content_only:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT embedding FROM kb_files WHERE file_path = %s;",
                    (parsed_file.file_path,),
                )
                row = cur.fetchone()

            existing_embedding: list[float] | None = None
            if row and row[0] is not None:
                raw = row[0]
                if isinstance(raw, str):
                    import json as _json
                    existing_embedding = _json.loads(raw)
                else:
                    existing_embedding = list(raw)

            row_id = upsert_kb_file(conn, parsed_file, embedding=existing_embedding)
            if verbose:
                print(
                    f"[kb_builder] Updated content: {parsed_file.file_path} "
                    f"(embedding reused, id={row_id})"
                )

    conn.close()

    skipped_count = len([f for f in table_files if not f.embedding_text.strip()])
    summary = {
        "total_files": len(all_parsed),
        "entry_points": len(entry_points),
        "table_files": len(all_needs_embedding) + len(files_content_only),
        "skipped": skipped_count,
        "unchanged": len(files_unchanged),
    }

    print("\n" + "=" * 60)
    print("  Build Complete")
    print("=" * 60)
    print(f"  Total files processed      : {summary['total_files']}")
    print(f"  Entry points stored        : {summary['entry_points']}")
    print(f"  Table files re-embedded    : {len(all_needs_embedding)}")
    print(f"  Table files content-synced : {len(files_content_only)}")
    print(f"  Unchanged (skipped)        : {summary['unchanged']}")
    print(f"  Skipped (no description)   : {skipped_count}")
    print("=" * 60)

    return summary


def status_kb() -> None:
    """Print a human-readable status report of what is currently stored in the DB."""
    print("\n[kb_builder] Fetching KB status from database...")
    conn = get_connection()
    files = list_all_files(conn)
    conn.close()

    if not files:
        print("[kb_builder] No files found in database. Run 'python main.py build' first.")
        return

    by_section: dict[str, list[dict]] = {}
    for f in files:
        by_section.setdefault(f["section"], []).append(f)

    print(f"\n{'=' * 60}")
    print(f"  KB Status — {len(files)} total files")
    print(f"{'=' * 60}")

    for section, section_files in sorted(by_section.items()):
        print(f"\n  [{section.upper()}]")
        for f in section_files:
            embed_marker = "✓ embedded" if f["has_embedding"] else "  no embed "
            entry_marker = "[ENTRY]" if f["is_entry_point"] else "       "
            name = f.get("name") or f["file_path"]
            print(f"    {embed_marker}  {entry_marker}  {f['file_path']}  ({name})")

    print(f"\n{'=' * 60}\n")
