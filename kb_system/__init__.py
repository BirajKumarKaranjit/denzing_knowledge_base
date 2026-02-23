"""
kb_system/__init__.py
---------------------
Package marker for the kb_system module.

Re-exports the primary public entry points so they are discoverable both
by the linter and by any caller that imports from the package directly.
"""

from kb_system.kb_builder import build_kb, status_kb
from kb_system.kb_generator import generate_all_kb_files, generate_sql_guidelines_sub_files
from kb_system.kb_retriever import classify_sections_with_llm, retrieve_context_for_query

__all__ = [
    "build_kb",
    "classify_sections_with_llm",
    "generate_all_kb_files",
    "generate_sql_guidelines_sub_files",
    "retrieve_context_for_query",
    "status_kb",
]


