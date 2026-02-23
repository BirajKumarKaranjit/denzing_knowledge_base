
from dataclasses import dataclass


@dataclass
class TableCitation:
    """
    Represents a single citation for a retrieved KB table.

    Shown to the end user alongside the SQL result so they understand
    WHICH tables were used, WHY they were selected (relevance score),
    and WHERE the information came from in the KB.
    """
    table_name: str
    file_path: str
    description: str
    relevance_score: float
    retrieval_method: str
    matched_on: str


def build_citations(matched_tables: list[dict]) -> list[TableCitation]:
    """
    Build structured citation objects from retrieved KB table records.

    Parameters
    ----------
    matched_tables : list[dict]
        Output from kb_retriever — list of matched table records with
        metadata, content, relevance_score, and retrieval_method fields.

    Returns
    -------
    list[TableCitation]
        One citation per retrieved table, ordered by relevance score.
    """
    citations = []
    for table in matched_tables:
        metadata = table.get("metadata", {})
        citations.append(TableCitation(
            table_name=metadata.get("name", table["file_path"]),
            file_path=table["file_path"],
            description=metadata.get("description", "")[:200],
            relevance_score=round(table.get("relevance_score", 0.0), 4),
            retrieval_method=table.get("retrieval_method", "vector_similarity"),
            matched_on=f"Embedding of: \"{metadata.get('name', '')}: {metadata.get('description', '')[:80]}...\""
        ))
    return citations


def format_citations_for_user(citations: list[TableCitation]) -> str:
    """
    Format citations as a human-readable string for display to the end user.

    This is shown BELOW the SQL result so the user understands how
    the KB retrieved the relevant schema context.
    """
    if not citations:
        return "No KB tables were retrieved — SQL generated from full schema fallback."

    lines = ["**Knowledge Base Citations**", ""]
    lines.append("The following tables were retrieved from the Knowledge Base to generate this SQL:\n")

    for i, c in enumerate(citations, 1):
        lines.append(f"**[{i}] `{c.table_name}`**")
        lines.append(f"   - **Source:** `{c.file_path}`")
        lines.append(f"   - **Relevance Score:** {c.relevance_score:.4f} ({_score_label(c.relevance_score)})")
        lines.append(f"   - **Why retrieved:** {c.description[:150]}...")
        lines.append(f"   - **Method:** {c.retrieval_method}")
        lines.append("")

    return "\n".join(lines)


def format_citations_as_xml(citations: list[TableCitation]) -> str:
    """
    Format citations as XML for injection into the SQL prompt.

    This tells the LLM explicitly which tables were retrieved and why,
    reducing hallucination of non-retrieved tables.
    Mirrors the <knowledge_base> injection format from the PRD (Section 11.2).
    """
    lines = ["<retrieval_citations>"]
    for c in citations:
        lines.append(f'  <citation table="{c.table_name}" score="{c.relevance_score}" method="{c.retrieval_method}">')
        lines.append(f'    <reason>{c.description[:200]}</reason>')
        lines.append(f'  </citation>')
    lines.append("</retrieval_citations>")
    return "\n".join(lines)


def _score_label(score: float) -> str:
    if score >= 0.75:
        return "High confidence"
    elif score >= 0.55:
        return "Medium confidence"
    else:
        return "Low confidence — verify SQL carefully"