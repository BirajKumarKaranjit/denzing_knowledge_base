"""Prompt regression tests for sql_worker.sql_generator."""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sql_worker.sql_generator import _SQL_GENERATION_SYSTEM_PROMPT


def test_scope_fields_rule_present() -> None:
    """Generation prompt requires resolved scope fields in output."""
    assert "resolved scope field(s) in SELECT" in _SQL_GENERATION_SYSTEM_PROMPT


def test_latest_boundary_rule_present() -> None:
    """Generation prompt requires computed boundary values in output."""
    assert "include the resolved boundary value in SELECT" in _SQL_GENERATION_SYSTEM_PROMPT
    assert "computed latest/max period boundary" in _SQL_GENERATION_SYSTEM_PROMPT

