"""
Extraction method resolution: given a file's source_path and detected
file_type, determines which extraction method should be applied.

Resolution order (see docs/DESIGN.md, Silver Layer, Extraction):
  1. Path-prefix override, if one matches both file_type and path.
  2. File-type default, if no override matches.
  3. No rule matched -- a genuinely unsupported combination.
"""
from dataclasses import dataclass
from typing import Optional
import yaml


@dataclass
class ExtractionDecision:
    method: Optional[str]
    on_failure: Optional[str]
    matched_rule: str  # "override" | "default" | "none"


def _path_matches_prefix(source_path: str, prefix: str) -> bool:
    """
    Checks whether source_path falls under the given prefix, anchored to
    a real path boundary -- not a raw substring match. Without anchoring,
    a prefix like "contracts_scanned/" would incorrectly match a path like
    "my_contracts_scanned_backup/file.pdf", which shares the substring but
    is a genuinely different directory.
    """
    normalized_prefix = prefix if prefix.endswith("/") else prefix + "/"
    anchored = "/" + normalized_prefix
    return anchored in source_path


def resolve_extraction_method(
    source_path: str, file_type: str, config: dict
) -> ExtractionDecision:
    """
    Resolves which extraction method applies to a given file, following
    the two-tier default/override design: a team with no overrides gets
    correct behavior from defaults alone; a team with specific needs can
    scope an override to a path and file_type without affecting anything
    else in their corpus.
    """
    extraction_config = config.get("extraction", {})
    overrides = extraction_config.get("overrides", [])

    for rule in overrides:
        rule_file_type = rule.get("file_type")
        rule_prefix = rule.get("path_prefix", "")
        if rule_file_type == file_type and _path_matches_prefix(source_path, rule_prefix):
            return ExtractionDecision(
                method=rule["method"],
                on_failure=rule.get("on_failure", "atomic"),
                matched_rule="override",
            )

    defaults = extraction_config.get("defaults", {})
    if file_type in defaults:
        return ExtractionDecision(
            method=defaults[file_type],
            on_failure="atomic",  # atomic is the platform default; partial is opt-in only
            matched_rule="default",
        )

    return ExtractionDecision(method=None, on_failure=None, matched_rule="none")


def load_extraction_config(config_path: str) -> dict:
    with open(config_path) as f:
        return yaml.safe_load(f)