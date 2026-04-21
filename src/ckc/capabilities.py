from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Mapping

# This module is the single source of truth for CKC's routing vocabulary.
# Capability inference, decomposition, registry normalization, and execution
# should all import capability names from here instead of hardcoding strings.

WEB_RESEARCH = "web_research"
DATA_ANALYSIS = "data_analysis"
SUMMARIZATION = "summarization"
CODING = "coding"
TEXT_GENERATION = "text_generation"
TRANSLATION = "translation"
IMAGE_GENERATION = "image_generation"
OPERATIONS_OPTIMIZATION = "operations_optimization"
GENERAL_TASK = "general_task"


@dataclass(frozen=True)
class CapabilityDefinition:
    capability_name: str
    aliases: tuple[str, ...] = ()
    description: str = ""
    routing_keywords: tuple[str, ...] = ()
    routing_keyword_weights: Mapping[str, float] = field(default_factory=dict)


CAPABILITY_DEFINITIONS: tuple[CapabilityDefinition, ...] = (
    CapabilityDefinition(
        capability_name=OPERATIONS_OPTIMIZATION,
        description="Investigates processes, workflows, and operational efficiency improvements.",
        routing_keywords=("operations", "operational", "process", "workflow", "efficiency", "optimization"),
        routing_keyword_weights={"workflow": 1.25, "efficiency": 1.5, "optimization": 1.75},
    ),
    CapabilityDefinition(
        capability_name=WEB_RESEARCH,
        description="Searches, gathers, and synthesizes information from external sources.",
        routing_keywords=("research", "search", "find", "look up", "lookup", "market", "browse", "investigate"),
        routing_keyword_weights={"research": 1.5, "search": 1.25, "browse": 1.25, "investigate": 1.5},
    ),
    CapabilityDefinition(
        capability_name=DATA_ANALYSIS,
        aliases=("analysis", "market_analysis"),
        description="Evaluates structured information, compares options, and produces analytical outputs.",
        routing_keywords=("analysis", "analyze", "analyse", "evaluate", "score", "data", "chart", "graph", "statistic", "statistics", "csv", "excel"),
        routing_keyword_weights={"csv": 2.0, "excel": 2.0, "statistics": 1.75, "chart": 1.5, "graph": 1.5},
    ),
    CapabilityDefinition(
        capability_name=SUMMARIZATION,
        description="Condenses documents, reports, and long-form content into concise summaries.",
        routing_keywords=("summarize", "summarise", "summary", "brief", "report", "digest", "tldr", "recap", "condense"),
        routing_keyword_weights={"summarize": 1.75, "summary": 1.75, "tldr": 2.0, "digest": 1.5, "recap": 1.5},
    ),
    CapabilityDefinition(
        capability_name=CODING,
        description="Writes, reviews, debugs, and implements software changes.",
        routing_keywords=("code", "program", "script", "function", "debug", "implement", "develop"),
        routing_keyword_weights={"debug": 2.0, "script": 1.5, "function": 1.25, "implement": 1.5},
    ),
    CapabilityDefinition(
        capability_name=TEXT_GENERATION,
        description="Drafts and composes text such as articles, stories, and messages.",
        routing_keywords=("write", "draft", "compose", "essay", "article", "blog", "story", "text"),
        routing_keyword_weights={"essay": 1.5, "article": 1.5, "blog": 1.5, "story": 1.5, "draft": 1.25},
    ),
    CapabilityDefinition(
        capability_name=TRANSLATION,
        description="Translates content between languages with contextual fidelity.",
        routing_keywords=("translate", "translation", "language", "convert language"),
        routing_keyword_weights={"translate": 2.0, "translation": 2.0, "convert language": 2.0},
    ),
    CapabilityDefinition(
        capability_name=IMAGE_GENERATION,
        description="Creates or designs visual content such as images and illustrations.",
        routing_keywords=("image", "draw", "design", "picture", "photo", "illustration", "visual"),
        routing_keyword_weights={"draw": 1.75, "illustration": 1.75, "picture": 1.25, "photo": 1.25},
    ),
    CapabilityDefinition(
        capability_name=GENERAL_TASK,
        aliases=("general",),
        description="Fallback capability for general-purpose tasks that do not map to a specific specialization.",
        routing_keywords=(),
    ),
)

CAPABILITY_BY_NAME: dict[str, CapabilityDefinition] = {
    definition.capability_name: definition
    for definition in CAPABILITY_DEFINITIONS
}

CAPABILITY_ALIASES: dict[str, str] = {}
for definition in CAPABILITY_DEFINITIONS:
    CAPABILITY_ALIASES[definition.capability_name] = definition.capability_name
    for alias in definition.aliases:
        CAPABILITY_ALIASES[alias] = definition.capability_name


def normalize_capability_name(value: str, *, fallback: str | None = None) -> str:
    """Return CKC's canonical capability name for raw or legacy inputs."""
    normalized = re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower()).strip("_")
    if not normalized:
        return fallback or ""
    return CAPABILITY_ALIASES.get(normalized, normalized)
