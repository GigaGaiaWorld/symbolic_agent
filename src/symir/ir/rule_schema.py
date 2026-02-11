"""Rule schema definitions (predicate + conditions) and literals."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional, Union

from symir.errors import SchemaError
from symir.ir.fact_schema import PredicateSchema
from symir.ir.expr_ir import ExprIR, Var, Const, ExprTerm, Ref, expr_from_dict


@dataclass(frozen=True)
class Expr:
    expr: ExprIR

    def to_dict(self) -> dict[str, Any]:
        return {"kind": "expr", "expr": self.expr.to_dict()}

Literal = Union[Ref, Expr]


def literal_from_dict(data: dict[str, Any]) -> Literal:
    kind = data.get("kind")
    if kind == "ref":
        ref = expr_from_dict(data)
        if not isinstance(ref, Ref):
            raise SchemaError("Ref literal parsing failed.")
        return ref
    if kind == "expr":
        return Expr(expr=expr_from_dict(data["expr"]))
    raise SchemaError(f"Unknown Literal kind: {kind}")


@dataclass(frozen=True)
class Cond:
    literals: list[Literal] = field(default_factory=list)
    prob: Optional[float] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "literals": [lit.to_dict() for lit in self.literals],
            "prob": self.prob,
        }

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "Cond":
        lits = [literal_from_dict(item) for item in data.get("literals", [])]
        return Cond(literals=lits, prob=data.get("prob"))


@dataclass(frozen=True)
class Rule:
    predicate: PredicateSchema
    conditions: list[Cond] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        data = self.predicate.to_dict()
        data["conditions"] = [cond.to_dict() for cond in self.conditions]
        return data

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "Rule":
        return Rule(
            predicate=PredicateSchema.from_dict(data),
            conditions=[Cond.from_dict(c) for c in data.get("conditions", [])],
        )


@dataclass(frozen=True)
class Query:
    """Query over a predicate (fact or rule-level)."""

    predicate_id: Optional[str] = None
    predicate: Optional[PredicateSchema] = None
    terms: list[ExprTerm] = field(default_factory=list)

    def __post_init__(self) -> None:
        if (self.predicate_id is None) == (self.predicate is None):
            raise SchemaError("Query requires exactly one of predicate_id or predicate.")
        if self.predicate is not None:
            if len(self.terms) != self.predicate.arity:
                raise SchemaError("Query terms length must match predicate arity.")
        for term in self.terms:
            if not isinstance(term, (Var, Const)):
                raise SchemaError("Query terms must be Var or Const.")

    def to_dict(self) -> dict[str, Any]:
        return {
            "predicate_id": self.predicate_id,
            "predicate": self.predicate.to_dict() if self.predicate else None,
            "terms": [t.to_dict() for t in self.terms],
        }

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "Query":
        predicate = data.get("predicate")
        return Query(
            predicate_id=data.get("predicate_id"),
            predicate=PredicateSchema.from_dict(predicate) if predicate else None,
            terms=[expr_from_dict(t) for t in data.get("terms", [])],
        )
