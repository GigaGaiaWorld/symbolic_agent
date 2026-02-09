"""Expression IR for rule bodies."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from symir.errors import SchemaError


class ExprIR:
    """Base class for expression IR."""

    def to_dict(self) -> dict[str, Any]:  # pragma: no cover - interface
        raise NotImplementedError


@dataclass(frozen=True)
class Var(ExprIR):
    name: str
    datatype: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.name:
            raise SchemaError("Var name must be non-empty.")

    def to_dict(self) -> dict[str, Any]:
        return {"kind": "var", "name": self.name, "datatype": self.datatype}


@dataclass(frozen=True)
class Const(ExprIR):
    value: object
    datatype: str

    def __post_init__(self) -> None:
        if not self.datatype:
            raise SchemaError("Const datatype must be provided.")

    def to_dict(self) -> dict[str, Any]:
        return {"kind": "const", "value": self.value, "datatype": self.datatype}


@dataclass(frozen=True)
class Call(ExprIR):
    op: str
    args: list[ExprIR]

    def __post_init__(self) -> None:
        if not self.op:
            raise SchemaError("Call op must be non-empty.")

    def to_dict(self) -> dict[str, Any]:
        return {"kind": "call", "op": self.op, "args": [a.to_dict() for a in self.args]}


@dataclass(frozen=True)
class Unify(ExprIR):
    lhs: ExprIR
    rhs: ExprIR

    def to_dict(self) -> dict[str, Any]:
        return {"kind": "unify", "lhs": self.lhs.to_dict(), "rhs": self.rhs.to_dict()}


@dataclass(frozen=True)
class If(ExprIR):
    cond: ExprIR
    then: ExprIR
    else_: ExprIR

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": "if",
            "cond": self.cond.to_dict(),
            "then": self.then.to_dict(),
            "else": self.else_.to_dict(),
        }


@dataclass(frozen=True)
class NotExpr(ExprIR):
    expr: ExprIR

    def to_dict(self) -> dict[str, Any]:
        return {"kind": "not", "expr": self.expr.to_dict()}


ExprTerm = Var | Const


def expr_from_dict(data: dict[str, Any]) -> ExprIR:
    kind = data.get("kind")
    if kind == "var":
        return Var(name=data["name"], datatype=data.get("datatype"))
    if kind == "const":
        return Const(value=data.get("value"), datatype=data.get("datatype"))
    if kind == "call":
        return Call(op=data["op"], args=[expr_from_dict(a) for a in data.get("args", [])])
    if kind == "unify":
        return Unify(lhs=expr_from_dict(data["lhs"]), rhs=expr_from_dict(data["rhs"]))
    if kind == "if":
        return If(
            cond=expr_from_dict(data["cond"]),
            then=expr_from_dict(data["then"]),
            else_=expr_from_dict(data["else"]),
        )
    if kind == "not":
        return NotExpr(expr=expr_from_dict(data["expr"]))
    raise SchemaError(f"Unknown ExprIR kind: {kind}")
