"""
Builds structured citations explaining how KB retrieval selected the tables
that were injected into the SQL generation prompt.

PURPOSE — two audiences:
    1. The LLM (via XML in the prompt)
       Gives the model an explicit "approved table manifest" before it reads
       the schemas. This prevents it from hallucinating table names it was
       not given. Placed at the top of the SQL prompt.

    2. The end user (via markdown in the API/UI response)
       Shows which KB files were retrieved, why (description snippet that
       matched), and how confident the system is. Satisfies your senior's
       requirement for retrieval transparency.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

@dataclass
class TableCitation:
    """
    Structured citation for a single retrieved KB table file.

    One of these is created per matched table in the retrieval result.
    The collection of citations is used to produce both the XML block
    injected into the LLM prompt and the markdown shown to the end user.

    Attributes
    ----------
    rank : int
    table_name : str
    file_path : str
    description_snippet : str
    relevance_score : float
    retrieval_method : str
    confidence_label : str
    """

    rank: int
    table_name: str
    file_path: str
    description_snippet: str
    relevance_score: float
    retrieval_method: str
    confidence_label: str = field(init=False)

    def __post_init__(self) -> None:
        """Derive confidence label from relevance score after dataclass init."""
        if self.relevance_score >= 0.75:
            self.confidence_label = "High"
        elif self.relevance_score >= 0.55:
            self.confidence_label = "Medium"
        elif self.relevance_score >= 0.35:
            self.confidence_label = "Low"
        else:
            self.confidence_label = "Very Low"


def build_citations(matched_tables: list[dict[str, Any]]) -> list[TableCitation]:
    """
    Convert raw KB retrieval results into structured TableCitation objects.
    Parameters
    ----------
    matched_tables : list[dict]
    Returns
    -------
    list[TableCitation]
    """
    citations: list[TableCitation] = []

    for rank, table in enumerate(matched_tables, start=1):
        metadata = table.get("metadata") or {}

        table_name = (metadata.get("name") or "").strip() or table.get("file_path", "unknown")

        description = (metadata.get("description") or "").strip()
        # Truncate for display — 200 chars is enough to explain the match reason
        if len(description) > 200:
            description_snippet = description[:200].rstrip() + "..."
        else:
            description_snippet = description

        citation = TableCitation(
            rank=rank,
            table_name=table_name,
            file_path=table.get("file_path", ""),
            description_snippet=description_snippet,
            relevance_score=round(float(table.get("relevance_score", 0.0)), 4),
            retrieval_method=table.get("retrieval_method", "vector_similarity"),
        )
        citations.append(citation)

    return citations

def format_citations_as_xml(citations: list[TableCitation]) -> str:
    """
    Render citations as an XML block for injection into the SQL prompt.
    Parameters
    ----------
    citations : list[TableCitation]

    Returns
    -------
    str
    """
    if not citations:
        return (
            "<kb_retrieval_citations>\n"
            "  <warning>No tables were retrieved from the Knowledge Base for this query.\n"
            "  Do NOT invent table names or column names. If valid SQL cannot be generated\n"
            "  from the provided information, return an empty SQL block.</warning>\n"
            "</kb_retrieval_citations>"
        )

    methods_used = sorted({c.retrieval_method for c in citations})
    method_str = " + ".join(methods_used)

    lines: list[str] = [
        "<kb_retrieval_citations>",
        "  <instruction>Generate SQL using ONLY the tables listed below. "
        "Do not reference any table or column not present in this list.</instruction>",
        f"  <summary>{len(citations)} table(s) retrieved via {method_str}</summary>",
    ]

    for c in citations:
        lines.append(
            f'  <citation rank="{c.rank}" table="{c.table_name}" '
            f'score="{c.relevance_score}" confidence="{c.confidence_label}">'
        )
        lines.append(f"    <source_file>{c.file_path}</source_file>")
        lines.append(f"    <match_reason>{c.description_snippet}</match_reason>")
        lines.append("  </citation>")

    lines.append("</kb_retrieval_citations>")
    return "\n".join(lines)


def format_citations_for_user(citations: list[TableCitation]) -> str:
    """
    Render citations as human-readable markdown for the end user.

    Parameters
    ----------
    citations : list[TableCitation]
    Returns
    -------
    str

    """
    if not citations:
        return (
            "### Knowledge Base Citations\n\n"
            "**No KB tables matched this query.**\n\n"
            "SQL was generated without Knowledge Base context. "
            "This increases the risk of hallucinated column or table names. "
            "Consider improving the `description` and `example_queries` fields "
            "in your `.md` files, or lowering `SIMILARITY_THRESHOLD` in `config.py`."
        )
    lines: list[str] = [
        "###Knowledge Base Citations",
        "",
        "The following tables were retrieved from the Knowledge Base to generate this SQL:",
        "",
    ]

    for c in citations:
        lines.append(
            f"**[{c.rank}] `{c.table_name}`** — "
            f"{c.confidence_label} confidence (score: {c.relevance_score})"
        )
        lines.append(f"   - **Source file:** `{c.file_path}`")
        lines.append(f"   - **Why selected:** {c.description_snippet}")
        lines.append(f"   - **Retrieval method:** {c.retrieval_method}")
        lines.append("")

    return "\n".join(lines)