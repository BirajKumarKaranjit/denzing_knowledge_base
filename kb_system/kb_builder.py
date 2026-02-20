"""
kb_system/kb_builder.py
------------------------
Orchestrates the full KB build pipeline:

    1. Scan knowledge_base_files/ for all .md files
    2. Parse each file (split frontmatter / body) via kb_parser
    3. Compute OpenAI embeddings for non-entry-point table files only
       — KB.md entry points are NOT embedded (they're fetched by name)
       — Individual table files (players.md, games.md) ARE embedded
         using their "name — description" text as the embedding input
    4. Upsert every file into the kb_files Postgres table

Run this script whenever you:
    - Add a new table .md file
    - Edit a description (which changes the embedding)
    - Add a new KB section

The upsert is idempotent — safe to re-run without duplicating data.
"""

from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config import KB_ROOT, EMBEDDING_DIMENSION
from kb_system.kb_parser import scan_kb_directory
from kb_system.kb_embeddings import get_embeddings_batch
from kb_system.kb_store import get_connection, init_schema, upsert_kb_file, list_all_files


def build_kb(verbose: bool = True) -> dict[str, int]:
    """
    Execute the full KB build pipeline end-to-end.

    Connects to Postgres, ensures the schema exists, scans all .md files,
    computes embeddings for table files, and upserts everything into the DB.

    This is the main entry point called by `main.py build`.

    Parameters
    ----------
    verbose : bool
        If True, print progress messages for each file processed.

    Returns
    -------
    dict with keys:
        - "total_files": total .md files found
        - "entry_points": number of KB.md section index files stored
        - "table_files": number of individual table files embedded + stored
        - "skipped": number of files that failed parsing
    """
    print("\n" + "=" * 60)
    print("  NBA Knowledge Base Builder")
    print("=" * 60)

    # ── Step 1: Connect to Postgres and ensure schema exists ──
    print("\n[kb_builder] Connecting to Postgres...")
    conn = get_connection()
    print("[kb_builder] ✓ Connected")

    print("[kb_builder] Initializing schema...")
    init_schema(conn)

    # ── Step 2: Scan and parse all .md files from disk ──
    print(f"\n[kb_builder] Scanning KB directory: {KB_ROOT}")
    all_parsed = scan_kb_directory(KB_ROOT)

    if not all_parsed:
        print("[kb_builder] No .md files found. Run 'python main.py generate' first.")
        conn.close()
        return {"total_files": 0, "entry_points": 0, "table_files": 0, "skipped": 0}

    # ── Step 3: Separate entry points from table files ──
    entry_points = [f for f in all_parsed if f.is_entry_point]
    table_files = [f for f in all_parsed if not f.is_entry_point]

    print(f"\n[kb_builder] Found {len(entry_points)} entry point(s) (KB.md files)")
    print(f"[kb_builder] Found {len(table_files)} table file(s) to embed")

    # ── Step 4: Store entry points WITHOUT embeddings ──
    # Entry points (KB.md) are fetched by section name at query time,
    # not by vector similarity — so they don't need embeddings.
    print("\n[kb_builder] Storing entry points (no embeddings)...")
    for ep in entry_points:
        row_id = upsert_kb_file(conn, ep, embedding=None)
        if verbose:
            print(f"[kb_builder] ✓ Stored entry point: {ep.file_path} (id={row_id})")

    # ── Step 5: Compute embeddings for table files ──
    # We embed the "name — description" text (embedding_text field).
    # This is what gets compared against the user query at retrieval time.
    print("\n[kb_builder] Computing embeddings for table files...")

    # Filter out any table files that somehow have empty embedding_text
    files_to_embed = [f for f in table_files if f.embedding_text.strip()]
    skipped_count = len(table_files) - len(files_to_embed)

    if skipped_count > 0:
        print(f"[kb_builder] ⚠ Skipping {skipped_count} table file(s) with empty descriptions")

    if files_to_embed:
        # Batch all embedding texts into one API call for efficiency
        embedding_texts = [f.embedding_text for f in files_to_embed]

        print(f"[kb_builder] Calling OpenAI embeddings API for {len(embedding_texts)} texts...")
        print("[kb_builder] Example embedding text:")
        print(f"  → '{embedding_texts[0][:120]}...'")

        # Single batched API call — much more efficient than one call per file
        embeddings = get_embeddings_batch(embedding_texts)

        # Validate that dimension matches config
        if embeddings and len(embeddings[0]) != EMBEDDING_DIMENSION:
            raise ValueError(
                f"Embedding dimension mismatch: got {len(embeddings[0])}, "
                f"expected {EMBEDDING_DIMENSION}. Update EMBEDDING_DIMENSION in config.py."
            )

        print(f"[kb_builder] ✓ Received {len(embeddings)} embeddings "
              f"(dimension={len(embeddings[0])})")

    else:
        embeddings = []

    # ── Step 6: Upsert table files WITH their embeddings ──
    print("\n[kb_builder] Storing table files with embeddings...")
    for parsed_file, embedding in zip(files_to_embed, embeddings):
        row_id = upsert_kb_file(conn, parsed_file, embedding=embedding)
        if verbose:
            print(f"[kb_builder] ✓ Stored: {parsed_file.file_path} "
                  f"(embed_text='{parsed_file.embedding_text[:60]}...', id={row_id})")

    # ── Step 7: Print final summary ──
    conn.close()

    summary = {
        "total_files": len(all_parsed),
        "entry_points": len(entry_points),
        "table_files": len(files_to_embed),
        "skipped": skipped_count,
    }

    print("\n" + "=" * 60)
    print("  Build Complete")
    print("=" * 60)
    print(f"  Total files processed : {summary['total_files']}")
    print(f"  Entry points stored   : {summary['entry_points']}")
    print(f"  Table files embedded  : {summary['table_files']}")
    print(f"  Skipped (no desc)     : {summary['skipped']}")
    print("=" * 60)

    return summary


def status_kb() -> None:
    """
    Print a human-readable status report of what is currently stored in the DB.

    Shows all files grouped by section, whether each has an embedding,
    and the total count. Useful for verifying a successful build before
    running queries.

    Called by `main.py status`.
    """
    print("\n[kb_builder] Fetching KB status from database...")
    conn = get_connection()
    files = list_all_files(conn)
    conn.close()

    if not files:
        print("[kb_builder] No files found in database. Run 'python main.py build' first.")
        return

    # Group by section for readable output
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
