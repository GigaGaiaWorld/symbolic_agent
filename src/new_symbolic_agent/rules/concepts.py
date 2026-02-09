"""Rule concept definitions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Any

from new_symbolic_agent.errors import SchemaError
from new_symbolic_agent.ir.types import IRPredicateRef, RuleKind


@dataclass(frozen=True)
class RuleConcept:
    """Definition of a rule-layer concept."""

    name: str
    arity: int
    description: str
    head: IRPredicateRef
    allowed_body_predicates: list[IRPredicateRef]
    category: str = "default"
    arg_types: Optional[list[str]] = None
    kind: RuleKind = "rule_node"
    is_nullary: bool = False

    def __post_init__(self) -> None:
        if not self.name:
            raise SchemaError("RuleConcept name must be non-empty.")
        if self.arity < 0:
            raise SchemaError("RuleConcept arity must be non-negative.")
        if self.head.layer != "rule":
            raise SchemaError("RuleConcept head predicate must be in rule layer.")
        if self.head.arity != self.arity:
            raise SchemaError("RuleConcept arity must match head predicate arity.")
        if self.is_nullary and self.arity != 0:
            raise SchemaError("Nullary RuleConcept must have arity 0.")
        if self.arg_types is not None and len(self.arg_types) != self.arity:
            raise SchemaError("arg_types length must match rule arity.")

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "arity": self.arity,
            "description": self.description,
            "head": self.head.to_dict(),
            "allowed_body_predicates": [p.to_dict() for p in self.allowed_body_predicates],
            "category": self.category,
            "arg_types": self.arg_types,
            "kind": self.kind,
            "is_nullary": self.is_nullary,
        }

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "RuleConcept":
        return RuleConcept(
            name=data["name"],
            arity=int(data["arity"]),
            description=data.get("description", ""),
            head=IRPredicateRef.from_dict(data["head"]),
            allowed_body_predicates=[IRPredicateRef.from_dict(p) for p in data.get("allowed_body_predicates", [])],
            category=data.get("category", "default"),
            arg_types=data.get("arg_types"),
            kind=data.get("kind", "rule_node"),
            is_nullary=bool(data.get("is_nullary", False)),
        )
