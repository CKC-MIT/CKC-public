from __future__ import annotations

from dataclasses import dataclass
import logging
import re

from .capabilities import CAPABILITY_BY_NAME, CAPABILITY_DEFINITIONS, GENERAL_TASK, normalize_capability_name

_log = logging.getLogger("ckc.routing")

_LOW_CONFIDENCE_GAP = 0.75
_ALIAS_MATCH_WEIGHT = 1.25


@dataclass(frozen=True)
class CapabilityInferenceResult:
    capability: str
    low_confidence: bool
    tokens: tuple[str, ...]
    matched_keywords: dict[str, list[dict[str, float | str]]]
    scores: dict[str, float]


def _tokenize_prompt(prompt: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", str(prompt or "").lower())


def _contains_keyword(prompt_lower: str, keyword: str, token_set: set[str]) -> bool:
    normalized_keyword = str(keyword or "").strip().lower()
    if not normalized_keyword:
        return False
    if " " in normalized_keyword:
        return re.search(r"\b" + re.escape(normalized_keyword) + r"\b", prompt_lower) is not None
    return normalized_keyword in token_set


def infer_primary_capability_details(prompt: str) -> CapabilityInferenceResult:
    """Infer CKC's primary capability and expose deterministic routing metadata.

    This is the authoritative entry point for top-level capability inference.
    Downstream systems such as task decomposition should consume this result as
    guidance instead of reclassifying the prompt with a separate taxonomy.

    Developer note:
    - "Translate this paragraph from English to Chinese." -> translation
    - "Debug this Python function and fix the failing test." -> coding
    - "Summarize this report into three bullet points." -> summarization
    - "Compare two vendors using CSV metrics and charts." -> data_analysis
    - "Research competitors and summarize findings." -> low-confidence between
      web_research and summarization, but still returns the top canonical score.
    """
    prompt_text = str(prompt or "")
    prompt_lower = prompt_text.lower()
    tokens = _tokenize_prompt(prompt_text)
    token_set = set(tokens)
    scores: dict[str, float] = {}
    matched_keywords: dict[str, list[dict[str, float | str]]] = {}

    for definition in CAPABILITY_DEFINITIONS:
        capability_name = definition.capability_name
        if capability_name == GENERAL_TASK:
            continue

        score = 0.0
        matches: list[dict[str, float | str]] = []

        for keyword in definition.routing_keywords:
            if _contains_keyword(prompt_lower, keyword, token_set):
                weight = float(definition.routing_keyword_weights.get(keyword, 1.0))
                score += weight
                matches.append({"keyword": keyword, "weight": weight, "source": "routing_keyword"})

        for alias in definition.aliases:
            if _contains_keyword(prompt_lower, alias.replace("_", " "), token_set):
                score += _ALIAS_MATCH_WEIGHT
                matches.append({"keyword": alias, "weight": _ALIAS_MATCH_WEIGHT, "source": "alias"})

        scores[capability_name] = score
        matched_keywords[capability_name] = matches

    ranked = sorted(scores.items(), key=lambda item: (-item[1], item[0]))
    if not ranked or ranked[0][1] <= 0.0:
        selected_capability = GENERAL_TASK
        low_confidence = True
    else:
        selected_capability = normalize_capability_name(ranked[0][0], fallback=GENERAL_TASK)
        if selected_capability not in CAPABILITY_BY_NAME:
            selected_capability = GENERAL_TASK
        second_score = ranked[1][1] if len(ranked) > 1 else 0.0
        low_confidence = (ranked[0][1] - second_score) <= _LOW_CONFIDENCE_GAP

    _log.debug(
        "Primary capability inference: prompt=%r tokens=%s matched_keywords=%s scores=%s selected_capability=%s low_confidence=%s",
        prompt_text,
        tokens,
        matched_keywords,
        scores,
        selected_capability,
        low_confidence,
    )
    return CapabilityInferenceResult(
        capability=selected_capability,
        low_confidence=low_confidence,
        tokens=tuple(tokens),
        matched_keywords=matched_keywords,
        scores=scores,
    )


def infer_primary_capability(prompt: str) -> str:
    """Infer CKC's primary capability exactly once for a user prompt."""
    return infer_primary_capability_details(prompt).capability
