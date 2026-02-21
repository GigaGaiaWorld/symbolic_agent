"""ER model primitives: Mention/Canonical mixins and policy config."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal


class MentionMixin:
    """Marker mixin for mention-layer entities (record-address identities)."""

    __er_layer__ = "mention"


class CanonicalMixin:
    """Marker mixin for canonical-layer entities (stable business identity)."""

    __er_layer__ = "canonical"


CanonPolicyMode = Literal[
    "error",
    "prefer_source",
    "max_confidence",
    "latest",
    "min_assertion_id",
    "min_canonical_id",
]


@dataclass(frozen=True)
class CanonPolicyConfig:
    """Policy config for resolving canon_of mapping conflicts."""

    mode: CanonPolicyMode = "error"
    source_priority: tuple[str, ...] = field(default_factory=tuple)
    source_key: str = "source"
    confidence_key: str = "confidence"
    time_key: str = "ingested_at"
    stable_tie_break: tuple[str, ...] = ("assertion_id",)

    def with_defaults(self) -> CanonPolicyConfig:
        if self.stable_tie_break:
            return self
        return CanonPolicyConfig(
            mode=self.mode,
            source_priority=self.source_priority,
            source_key=self.source_key,
            confidence_key=self.confidence_key,
            time_key=self.time_key,
            stable_tie_break=("assertion_id",),
        )
