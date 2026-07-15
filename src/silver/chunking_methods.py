"""
Chunking implementations. Each function takes the sections list produced
by an extraction method (see extraction_methods.py's ExtractionSection)
and returns a list of Chunk objects.

chunk_size and overlap are interpreted as approximate token counts, using
a documented ~4-characters-per-token approximation -- consistent with the
same approximation used and flagged elsewhere in this project where an
exact tokenizer wasn't available. Relative comparisons and boundary
behavior are correct under this approximation; exact token counts are not.

chunk_index is local (0, 1, 2, ...) -- the pipeline layer combines this
with content_hash to produce the final deterministic chunk_id, per
docs/DESIGN.md. Keeping that combination out of this module is deliberate:
these functions have no Spark or content_hash dependency, so they stay
testable in isolation.

semantic_split is NOT implemented here -- it requires a real embedding
model call to detect topic boundaries, a genuine external dependency not
yet decided (same open status as OCR). It's only reachable via the one
config override gated behind azure_document_intelligence_ocr, which is
itself already deferred -- so this doesn't leave a reachable gap.
"""
from dataclasses import dataclass
from typing import Optional

CHARS_PER_TOKEN_APPROX = 4


@dataclass
class Chunk:
    chunk_index: int
    chunk_text: str
    section_heading: Optional[str]
    token_count: int  # approximate -- see module docstring


def _approx_tokens(text: str) -> int:
    return max(1, round(len(text) / CHARS_PER_TOKEN_APPROX))


def _split_text_fixed(text: str, chunk_size: int, overlap: int) -> list[str]:
    """
    Sliding-window split by approximate token count, used both as
    fixed_size_with_overlap's own logic and as the fallback inside
    structure-aware methods when a section is too large to keep whole.
    """
    char_window = chunk_size * CHARS_PER_TOKEN_APPROX
    char_overlap = overlap * CHARS_PER_TOKEN_APPROX
    if char_overlap >= char_window:
        raise ValueError("overlap must be smaller than chunk_size")

    if len(text) <= char_window:
        return [text]

    chunks = []
    start = 0
    while start < len(text):
        end = start + char_window
        chunks.append(text[start:end])
        start = end - char_overlap
    return chunks


def fixed_size_with_overlap(
    sections: list, chunk_size: int, overlap: int
) -> list[Chunk]:
    full_text = "\n\n".join(s.text for s in sections if s.text)
    pieces = _split_text_fixed(full_text, chunk_size, overlap)
    return [
        Chunk(chunk_index=i, chunk_text=p, section_heading=None, token_count=_approx_tokens(p))
        for i, p in enumerate(pieces)
    ]


def recursive_structure_aware(
    sections: list, chunk_size: int, overlap: int
) -> list[Chunk]:
    """
    Keeps each section (page, in PDF's case) whole if it fits. If a
    section is too large, splits on paragraph boundaries first, falling
    back to fixed-size splitting only for a paragraph that's still too
    large on its own.
    """
    chunks = []
    index = 0
    char_window = chunk_size * CHARS_PER_TOKEN_APPROX

    for section in sections:
        if not section.text:
            continue
        if len(section.text) <= char_window:
            chunks.append(Chunk(index, section.text, section.heading, _approx_tokens(section.text)))
            index += 1
            continue

        paragraphs = [p for p in section.text.split("\n\n") if p.strip()]
        for para in paragraphs:
            if len(para) <= char_window:
                chunks.append(Chunk(index, para, section.heading, _approx_tokens(para)))
                index += 1
            else:
                for piece in _split_text_fixed(para, chunk_size, overlap):
                    chunks.append(Chunk(index, piece, section.heading, _approx_tokens(piece)))
                    index += 1

    return chunks


def heading_hierarchy_split(
    sections: list, chunk_size: int, overlap: int
) -> list[Chunk]:
    """
    Mechanically similar to recursive_structure_aware, but the heading
    carried on each section (already grouped by python_docx_standard) is
    explicitly propagated onto every resulting chunk -- including ones
    produced by the fixed-size fallback -- so a chunk from a long section
    never loses which heading it belongs to.
    """
    return recursive_structure_aware(sections, chunk_size, overlap)


def table_aware_row_based(sections: list) -> list[Chunk]:
    """
    No merging, no splitting: openpyxl_standard already produces one
    section per row, and each row already carries full field-labeled
    context (e.g. "policy_id: POL-001, title: ..."). Preserving that
    one-row-one-chunk boundary is the whole point of this method --
    character-based chunking on tabular data produces near-meaningless
    fragments, which is exactly what this method exists to avoid.
    """
    return [
        Chunk(
            chunk_index=i,
            chunk_text=s.text,
            section_heading=s.heading,
            token_count=_approx_tokens(s.text),
        )
        for i, s in enumerate(sections)
        if s.text
    ]