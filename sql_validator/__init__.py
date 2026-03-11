"""sql_validator — SQL structural verification before execution."""

from .schema_linker import build_column_registry
from .sql_verifier import VerificationError, VerificationResult, verify_sql

__all__ = [
    "build_column_registry",
    "VerificationError",
    "VerificationResult",
    "verify_sql",
]

