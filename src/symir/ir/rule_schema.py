"""Rule schema definitions (head + bodies) and literals."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional, Union

from symir.errors import SchemaError
from symir.ir.fact_schema import PredicateSchema
from symir.ir.expr_ir import ExprIR, Var, Const, ExprTerm, expr_from_dict


@dataclass(frozen=True)
class RefLiteral:
    """Predicate reference literal."""

    predicate_id: str
    terms: list[ExprTerm]
    negated: bool = False

    def __post_init__(self) -> None:
        for term in self.terms:
            if not isinstance(term, (Var, Const)):
                raise SchemaError("RefLiteral terms must be Var or Const.")

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": "ref",
            "predicate_id": self.predicate_id,
            "terms": [t.to_dict() for t in self.terms],
            "negated": self.negated,
        }


@dataclass(frozen=True)
class ExprLiteral:
    expr: ExprIR

    def to_dict(self) -> dict[str, Any]:
        return {"kind": "expr", "expr": self.expr.to_dict()}


Literal = Union[RefLiteral, ExprLiteral]


def literal_from_dict(data: dict[str, Any]) -> Literal:
    kind = data.get("kind")
    if kind == "ref":
        terms = [expr_from_dict(t) for t in data.get("terms", [])]
        for term in terms:
            if not isinstance(term, (Var, Const)):
                raise SchemaError("RefLiteral terms must be Var or Const.")
        return RefLiteral(
            predicate_id=data["predicate_id"],
            terms=terms,
            negated=bool(data.get("negated", False)),
        )
    if kind == "expr":
        return ExprLiteral(expr=expr_from_dict(data["expr"]))
    raise SchemaError(f"Unknown Literal kind: {kind}")


@dataclass(frozen=True)
class HeadSchema:
    predicate: PredicateSchema
    terms: list[Var]

    def __post_init__(self) -> None:
        if len(self.terms) != self.predicate.arity:
            raise SchemaError("HeadSchema terms length must match predicate arity.")
        for term in self.terms:
            if not isinstance(term, Var):
                raise SchemaError("HeadSchema terms must be variables (var-only).")

    def to_dict(self) -> dict[str, Any]:
        return {
            "predicate": self.predicate.to_dict(),
            "terms": [t.to_dict() for t in self.terms],
        }

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "HeadSchema":
        return HeadSchema(
            predicate=PredicateSchema.from_dict(data["predicate"]),
            terms=[expr_from_dict(t) for t in data.get("terms", [])],
        )


@dataclass(frozen=True)
class Body:
    literals: list[Literal] = field(default_factory=list)
    prob: Optional[float] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "literals": [lit.to_dict() for lit in self.literals],
            "prob": self.prob,
        }

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "Body":
        lits = [literal_from_dict(item) for item in data.get("literals", [])]
        return Body(literals=lits, prob=data.get("prob"))


@dataclass(frozen=True)
class Rule:
    head: HeadSchema
    bodies: list[Body]

    def to_dict(self) -> dict[str, Any]:
        return {
            "head": self.head.to_dict(),
            "bodies": [body.to_dict() for body in self.bodies],
        }

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "Rule":
        return Rule(
            head=HeadSchema.from_dict(data["head"]),
            bodies=[Body.from_dict(b) for b in data.get("bodies", [])],
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
