"""Rule concept registry with persistence."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

from new_symbolic_agent.errors import RuleRegistryError
from new_symbolic_agent.rules.concepts import RuleConcept


class RuleRegistry:
    """Manage rule concepts by category and persist them to JSON."""

    def __init__(self) -> None:
        self._categories: dict[str, list[RuleConcept]] = {}

    @property
    def categories(self) -> dict[str, list[RuleConcept]]:
        return self._categories

    def add(self, concept: RuleConcept) -> None:
        bucket = self._categories.setdefault(concept.category, [])
        if any(c.name == concept.name for c in bucket):
            raise RuleRegistryError(
                f"Rule concept {concept.name} already exists in category {concept.category}."
            )
        bucket.append(concept)

    def all_concepts(self) -> list[RuleConcept]:
        concepts: list[RuleConcept] = []
        for bucket in self._categories.values():
            concepts.extend(bucket)
        return concepts

    def find(self, name: str) -> RuleConcept:
        for concept in self.all_concepts():
            if concept.name == name:
                return concept
        raise RuleRegistryError(f"Rule concept not found: {name}")

    def to_dict(self) -> dict[str, object]:
        return {
            "categories": {
                cat: [concept.to_dict() for concept in concepts]
                for cat, concepts in self._categories.items()
            }
        }

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            json.dump(self.to_dict(), handle, ensure_ascii=False, indent=2)

    @staticmethod
    def from_dict(data: dict[str, object]) -> "RuleRegistry":
        registry = RuleRegistry()
        categories = data.get("categories", {})
        if not isinstance(categories, dict):
            raise RuleRegistryError("Registry categories must be a dict.")
        for cat, items in categories.items():
            if not isinstance(items, list):
                raise RuleRegistryError(f"Category {cat} must be a list of rules.")
            for item in items:
                if not isinstance(item, dict):
                    raise RuleRegistryError(f"Rule concept entry in {cat} must be dict.")
                registry.add(RuleConcept.from_dict(item))
        return registry

    @staticmethod
    def load(path: Path) -> "RuleRegistry":
        if not path.exists():
            raise RuleRegistryError(f"Registry file not found: {path}")
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        return RuleRegistry.from_dict(data)
