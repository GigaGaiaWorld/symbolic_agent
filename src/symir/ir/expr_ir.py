"""Expression IR for rule conditions."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

from symir.errors import SchemaError
from symir.ir.fact_schema import PredicateSchema


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
    datatype: Optional[str] = None

    def __post_init__(self) -> None:
        dtype = self.datatype
        if dtype is None:
            dtype = _infer_datatype(self.value)
        if dtype is not None:
            if not isinstance(dtype, str) or not dtype.strip():
                raise SchemaError("Const datatype must be a non-empty string if provided.")
            dtype = dtype.strip()
            _validate_const_value(self.value, dtype)
            object.__setattr__(self, "datatype", dtype)

    def to_dict(self) -> dict[str, Any]:
        data = {"kind": "const", "value": self.value}
        if self.datatype is not None:
            data["datatype"] = self.datatype
        return data


@dataclass(frozen=True, init=False)
class Ref(ExprIR):
    schema_id: str
    terms: list["ExprTerm"]
    negated: bool = False

    def __init__(
        self,
        schema: str | PredicateSchema | None = None,
        terms: list["ExprTerm"] | None = None,
        negated: bool = False,
        *,
        schema_id: str | None = None,
        predicate_id: str | None = None,
    ) -> None:
        pred_obj: PredicateSchema | None = None
        pred_id: str | None
        if schema is not None and (schema_id is not None or predicate_id is not None):
            raise SchemaError("Ref schema provided more than once.")
        if isinstance(schema, PredicateSchema):
            pred_obj = schema
            pred_id = schema.schema_id
        elif schema is not None:
            pred_id = str(schema)
        else:
            pred_id = schema_id or predicate_id
        if not pred_id:
            raise SchemaError("Ref requires schema_id.")
        if terms is None:
            raise SchemaError("Ref terms must be provided.")
        for term in terms:
            if not isinstance(term, (Var, Const)):
                raise SchemaError("Ref terms must be Var or Const.")
        if pred_obj is not None:
            if len(terms) != pred_obj.arity:
                raise SchemaError(
                    f"Ref terms length must match predicate arity: "
                    f"expected {pred_obj.arity}, got {len(terms)}."
                )
            for term, arg in zip(terms, pred_obj.signature):
                if isinstance(term, Const) and arg.datatype:
                    _validate_const_value(term.value, arg.datatype)
                if (
                    isinstance(term, Var)
                    and term.datatype
                    and arg.datatype
                    and term.datatype.strip().lower() != arg.datatype.strip().lower()
                ):
                    raise SchemaError(
                        f"Var datatype mismatch for {term.name}: "
                        f"expected {arg.datatype}, got {term.datatype}."
                    )
        object.__setattr__(self, "schema_id", pred_id)
        object.__setattr__(self, "terms", list(terms))
        object.__setattr__(self, "negated", bool(negated))

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": "ref",
            "schema_id": self.schema_id,
            "terms": [t.to_dict() for t in self.terms],
            "negated": self.negated,
        }


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
    if kind == "ref":
        pred_id = data.get("schema_id") or data.get("schema") or data.get("predicate_id")
        if pred_id is None and "predicate" in data:
            predicate = data.get("predicate")
            pred_obj = PredicateSchema.from_dict(predicate) if predicate else None
            if pred_obj is not None:
                pred_id = pred_obj.schema_id
        terms = [expr_from_dict(t) for t in data.get("terms", [])]
        for term in terms:
            if not isinstance(term, (Var, Const)):
                raise SchemaError("Ref terms must be Var or Const.")
        return Ref(
            schema=pred_id or "",
            terms=terms,
            negated=bool(data.get("negated", False)),
        )
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


def _infer_datatype(value: object) -> Optional[str]:
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int) and not isinstance(value, bool):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "string"
    return None


def _validate_const_value(value: object, datatype: str) -> None:
    dtype = datatype.strip().lower()
    if dtype == "string":
        if not isinstance(value, str):
            raise SchemaError(f"Const value must be string for datatype 'string': {value}")
        return
    if dtype == "int":
        if not isinstance(value, int) or isinstance(value, bool):
            raise SchemaError(f"Const value must be int for datatype 'int': {value}")
        return
    if dtype == "float":
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise SchemaError(f"Const value must be float for datatype 'float': {value}")
        return
    if dtype == "bool":
        if not isinstance(value, bool):
            raise SchemaError(f"Const value must be bool for datatype 'bool': {value}")
        return
