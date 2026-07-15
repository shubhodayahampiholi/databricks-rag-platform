"""
Chunking method resolution: given a piece of extracted content's file_type
and the extraction_method_used that produced it, determines which chunking
method and parameters should be applied.

Resolution order (see docs/DESIGN.md, Silver Layer, Chunking):
  1. extraction_method_used override, if one matches. Deliberately NOT
     path-prefix, unlike extraction's resolver -- chunking operates on
     silver_extracted_documents, keyed by content_hash after
     deduplication. A single content_hash can have multiple instances
     with different source_paths, so there is no single governing path
     to key an override on. extraction_method_used is already stored
     once per content_hash, unambiguous by construction.
  2. file_type default, if no override matches.
  3. No rule matched -- a genuinely unsupported combination.
"""
from dataclasses import dataclass
from typing import Optional
import yaml


@dataclass
class ChunkingDecision:
    method: Optional[str]
    chunk_size: Optional[int]
    overlap: Optional[int]
    matched_rule: str  # "override" | "default" | "none"


def resolve_chunking_method(
    file_type: str, extraction_method_used: str, config: dict
) -> ChunkingDecision:
    chunking_config = config.get("chunking", {})
    overrides = chunking_config.get("overrides", [])

    for rule in overrides:
        if rule.get("extraction_method_used") == extraction_method_used:
            return ChunkingDecision(
                method=rule["method"],
                chunk_size=rule.get("chunk_size"),
                overlap=rule.get("overlap"),
                matched_rule="override",
            )

    defaults = chunking_config.get("defaults", {})
    if file_type in defaults:
        d = defaults[file_type]
        return ChunkingDecision(
            method=d["method"],
            chunk_size=d.get("chunk_size"),
            overlap=d.get("overlap"),
            matched_rule="default",
        )

    return ChunkingDecision(method=None, chunk_size=None, overlap=None, matched_rule="none")


def load_chunking_config(config_path: str) -> dict:
    with open(config_path) as f:
        return yaml.safe_load(f)