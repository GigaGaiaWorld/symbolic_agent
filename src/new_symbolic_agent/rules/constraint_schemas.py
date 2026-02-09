"""Constraint schemas for rule IR decoding."""

from __future__ import annotations

from typing import Any, Literal, Annotated, Union

from pydantic import BaseModel, Field, model_validator

from new_symbolic_agent.errors import SchemaError
from new_symbolic_agent.ir.fact_schema import FactView, PredicateSchema, ArgSpec
from new_symbolic_agent.rules.library import Library, LibrarySpec


def build_pydantic_rule_model(
    view: FactView,
    library: Library | None = None,
    *,
    mode: Literal["verbose", "compact"] = "verbose",
) -> type[BaseModel]:
    """Build a Pydantic model that validates rule bodies only."""

    allowed_predicate_ids = {pred.schema_id: pred.arity for pred in view.predicates()}
    predicate_info: dict[
        str, tuple[str, int, list[tuple[str | None, str | None, str | None]] | None, str | None]
    ] = {}
    for pred in view.predicates():
        predicate_info[pred.schema_id] = (
            pred.name,
            pred.arity,
            [(a.datatype, a.role, a.namespace) for a in pred.signature],
            pred.description,
        )
    if library is not None:
        for pred_id, spec in library.predicate_ids().items():
            allowed_predicate_ids[pred_id] = spec.arity
            signature = None
            if spec.signature is not None:
                signature = [(item, None, None) for item in spec.signature]
            predicate_info[pred_id] = (spec.name, spec.arity, signature, spec.description)
    allowed_expr_ops = set(_builtin_ops())
    if library is not None:
        allowed_expr_ops.update(library.expr_ops())

    class VarModel(BaseModel):
        kind: Literal["var"] = "var"
        name: str
        datatype: str | None = None

    class ConstModel(BaseModel):
        kind: Literal["const"] = "const"
        value: str | int | float | bool
        datatype: str

    ExprTermModel = Annotated[Union[VarModel, ConstModel], Field(discriminator="kind")]

    class ArgValueModel(BaseModel):
        name: str
        value: ExprTermModel

    class CallModel(BaseModel):
        kind: Literal["call"] = "call"
        op: str
        args: list["ExprModel"]

        @model_validator(mode="after")
        def _check_op(self):
            if self.op not in allowed_expr_ops:
                raise ValueError(f"Expr op not allowed: {self.op}")
            return self

    class UnifyModel(BaseModel):
        kind: Literal["unify"] = "unify"
        lhs: "ExprModel"
        rhs: "ExprModel"

    class IfModel(BaseModel):
        kind: Literal["if"] = "if"
        cond: "ExprModel"
        then: "ExprModel"
        else_: "ExprModel" = Field(alias="else")

    class NotModel(BaseModel):
        kind: Literal["not"] = "not"
        expr: "ExprModel"

    ExprModel = Annotated[
        Union[VarModel, ConstModel, CallModel, UnifyModel, IfModel, NotModel],
        Field(discriminator="kind"),
    ]

    CallModel.model_rebuild()
    UnifyModel.model_rebuild()
    IfModel.model_rebuild()
    NotModel.model_rebuild()

    class ArgSpecModel(BaseModel):
        datatype: str
        role: str | None = None
        namespace: str | None = None

    class PredicateInfoModel(BaseModel):
        name: str
        arity: int
        signature: list[ArgSpecModel] | None = None
        description: str | None = None

    class RefLiteralModel(BaseModel):
        kind: Literal["ref"] = "ref"
        predicate_id: str
        predicate: PredicateInfoModel | None = None
        terms: list[ExprTermModel] | None = None
        args: list[ArgValueModel] | None = None
        negated: bool = False

        @model_validator(mode="after")
        def _check_allowed(self):
            if self.predicate_id not in allowed_predicate_ids:
                raise ValueError(f"RefLiteral predicate not in FactView: {self.predicate_id}")
            expected = allowed_predicate_ids[self.predicate_id]
            if mode == "verbose":
                if self.terms is None:
                    raise ValueError("RefLiteral terms required in verbose mode.")
                if len(self.terms) != expected:
                    raise ValueError(
                        f"RefLiteral arity mismatch: expected {expected}, got {len(self.terms)}"
                    )
            else:
                if self.args is None:
                    raise ValueError("RefLiteral args required in compact mode.")
                if len(self.args) != expected:
                    raise ValueError(
                        f"RefLiteral arity mismatch: expected {expected}, got {len(self.args)}"
                    )
            info = predicate_info.get(self.predicate_id)
            if info is not None:
                name, arity, signature, description = info
                if self.predicate is not None:
                    if self.predicate.name != name or self.predicate.arity != arity:
                        raise ValueError("RefLiteral predicate info mismatch.")
                    if signature is not None and self.predicate.signature is not None:
                        sig_tuple = [(a.datatype, a.role, a.namespace) for a in self.predicate.signature]
                        if sig_tuple != signature:
                            raise ValueError("RefLiteral predicate signature mismatch.")
                    if self.predicate.description is not None and description is not None:
                        if self.predicate.description != description:
                            raise ValueError("RefLiteral predicate description mismatch.")
                if mode == "compact" and signature is not None and self.args is not None:
                    # enforce const datatype matches signature when provided
                    for arg_value, expected_sig in zip(self.args, signature):
                        expected_datatype = expected_sig[0]
                        if isinstance(arg_value.value, ConstModel):
                            if arg_value.value.datatype != expected_datatype:
                                raise ValueError(
                                    f"Const datatype mismatch: expected {expected_datatype}, got {arg_value.value.datatype}"
                                )
            return self

    class ExprLiteralModel(BaseModel):
        kind: Literal["expr"] = "expr"
        expr: ExprModel

    LiteralModel = Annotated[
        Union[RefLiteralModel, ExprLiteralModel],
        Field(discriminator="kind"),
    ]

    class BodyModel(BaseModel):
        literals: list[LiteralModel]
        prob: float | None = Field(default=None, ge=0.0, le=1.0)

    class RuleInstanceModel(BaseModel):
        bodies: list[BodyModel]

    return RuleInstanceModel


def _ref_literal_schema(
    predicate_id: str,
    name: str,
    arity: int,
    signature: list[ArgSpec] | None,
    description: str | None,
) -> dict[str, Any]:
    if signature is None:
        signature = [ArgSpec(datatype="any") for _ in range(arity)]
    description_value = description if description is not None else ""
    term_schema = {
        "anyOf": [
            {
                "type": "object",
                "properties": {
                    "kind": {"type": "string", "const": "var"},
                    "name": {"type": "string"},
                    "datatype": {"type": "string"},
                },
                "required": ["kind", "name", "datatype"],
                "additionalProperties": False,
            },
            {
                "type": "object",
                "properties": {
                    "kind": {"type": "string", "const": "const"},
                    "value": {"type": ["string", "number", "boolean"]},
                    "datatype": {"type": "string"},
                },
                "required": ["kind", "value", "datatype"],
                "additionalProperties": False,
            },
        ]
    }
    return {
        "type": "object",
        "properties": {
            "kind": {"type": "string", "const": "ref"},
            "predicate_id": {"type": "string", "const": predicate_id},
            "predicate": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "const": name},
                    "arity": {"type": "integer", "const": arity},
                    "signature": {
                        "type": "array",
                        "const": [
                            {"datatype": a.datatype, "role": a.role, "namespace": a.namespace}
                            for a in signature
                        ],
                    },
                    "description": {
                        "type": "string",
                        "const": description_value,
                    },
                },
                "required": ["name", "arity", "signature", "description"],
                "additionalProperties": False,
            },
            "terms": {
                "type": "array",
                "items": term_schema,
                "minItems": arity,
                "maxItems": arity,
            },
            "negated": {"type": "boolean"},
        },
        "required": ["kind", "predicate_id", "predicate", "terms", "negated"],
        "additionalProperties": False,
    }


def build_responses_schema(
    view: FactView,
    library: Library | None = None,
    *,
    mode: Literal["verbose", "compact"] = "verbose",
) -> dict[str, Any]:
    """Build a JSON schema for OpenAI Responses API strict decoding (bodies only)."""
    allowed_preds = view.predicates()
    if not allowed_preds and (library is None or not library.predicate_ids()):
        raise SchemaError("FactView has no predicates.")

    # Expr IR schema
    expr_ref = {"$ref": "#/$defs/expr"}
    defs: dict[str, Any] = {
        "var": {
            "type": "object",
            "properties": {
                "kind": {"type": "string", "const": "var"},
                "name": {"type": "string"},
                "datatype": {"type": "string"},
            },
            "required": ["kind", "name", "datatype"],
            "additionalProperties": False,
        },
        "const": {
            "type": "object",
            "properties": {
                "kind": {"type": "string", "const": "const"},
                "value": {"type": ["string", "number", "boolean"]},
                "datatype": {"type": "string"},
            },
            "required": ["kind", "value", "datatype"],
            "additionalProperties": False,
        },
        "call": {
            "type": "object",
            "properties": {
                "kind": {"type": "string", "const": "call"},
                "op": {"type": "string"},
                "args": {"type": "array", "items": expr_ref},
            },
            "required": ["kind", "op", "args"],
            "additionalProperties": False,
        },
        "unify": {
            "type": "object",
            "properties": {
                "kind": {"type": "string", "const": "unify"},
                "lhs": expr_ref,
                "rhs": expr_ref,
            },
            "required": ["kind", "lhs", "rhs"],
            "additionalProperties": False,
        },
        "if": {
            "type": "object",
            "properties": {
                "kind": {"type": "string", "const": "if"},
                "cond": expr_ref,
                "then": expr_ref,
                "else": expr_ref,
            },
            "required": ["kind", "cond", "then", "else"],
            "additionalProperties": False,
        },
        "not": {
            "type": "object",
            "properties": {
                "kind": {"type": "string", "const": "not"},
                "expr": expr_ref,
            },
            "required": ["kind", "expr"],
            "additionalProperties": False,
        },
    }
    defs["expr"] = {
        "anyOf": [
            {"$ref": "#/$defs/var"},
            {"$ref": "#/$defs/const"},
            {"$ref": "#/$defs/call"},
            {"$ref": "#/$defs/unify"},
            {"$ref": "#/$defs/if"},
            {"$ref": "#/$defs/not"},
        ]
    }

    if mode == "verbose":
        ref_literal_items: list[dict[str, Any]] = []
        for pred in allowed_preds:
            ref_literal_items.append(
                _ref_literal_schema(
                    pred.schema_id,
                    pred.name,
                    pred.arity,
                    pred.signature,
                    pred.description,
                )
            )
        if library is not None:
            for pred_id, spec in library.predicate_ids().items():
                signature = None
                if spec.signature is not None:
                    signature = [ArgSpec(datatype=item) for item in spec.signature]
                ref_literal_items.append(
                    _ref_literal_schema(pred_id, spec.name, spec.arity, signature, spec.description)
                )
        ref_literal_schema = {"anyOf": ref_literal_items}
    else:
        # compact: predicate_id enum + args array
        allowed_ids = [p.schema_id for p in allowed_preds]
        if library is not None:
            allowed_ids.extend(list(library.predicate_ids().keys()))
        ref_literal_schema = {
            "type": "object",
            "properties": {
                "kind": {"type": "string", "const": "ref"},
                "predicate_id": {"type": "string", "enum": allowed_ids},
                "args": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "value": {
                                "anyOf": [
                                    {"$ref": "#/$defs/var"},
                                    {"$ref": "#/$defs/const"},
                                ]
                            },
                        },
                        "required": ["name", "value"],
                        "additionalProperties": False,
                    },
                },
                "negated": {"type": "boolean"},
            },
            "required": ["kind", "predicate_id", "args", "negated"],
            "additionalProperties": False,
        }
    expr_literal_schema = {
        "type": "object",
        "properties": {
            "kind": {"type": "string", "const": "expr"},
            "expr": expr_ref,
        },
        "required": ["kind", "expr"],
        "additionalProperties": False,
    }
    literal_schema = {"anyOf": [ref_literal_schema, expr_literal_schema]}
    body_schema = {
        "type": "object",
        "properties": {
            "literals": {"type": "array", "items": literal_schema},
            "prob": {"type": "number", "minimum": 0.0, "maximum": 1.0},
        },
        "required": ["literals", "prob"],
        "additionalProperties": False,
    }

    result = {
        "type": "object",
        "properties": {
            "bodies": {"type": "array", "items": body_schema},
        },
        "required": ["bodies"],
        "additionalProperties": False,
        "$defs": defs,
    }
    return _normalize_strict_schema(result)


def build_predicate_catalog(view: FactView, library: Library | None = None) -> dict[str, Any]:
    """Build a compact predicate catalog for LLM prompting (not for decoding)."""

    catalog: dict[str, Any] = {}
    for pred in view.predicates():
        catalog[pred.schema_id] = {
            "name": pred.name,
            "arity": pred.arity,
            "arg_types": [arg.datatype for arg in pred.signature],
            "description": pred.description,
        }
    if library is not None:
        for pred_id, spec in library.predicate_ids().items():
            catalog[pred_id] = {
                "name": spec.name,
                "arity": spec.arity,
                "arg_types": spec.signature or [],
                "description": spec.description,
            }
    return catalog


def _builtin_ops() -> list[str]:
    return [
        "eq",
        "ne",
        "lt",
        "le",
        "gt",
        "ge",
        "add",
        "sub",
        "mul",
        "div",
        "mod",
    ]


def _normalize_strict_schema(schema: Any) -> Any:
    """Normalize schema to strict JSON schema subset for structured outputs."""

    if isinstance(schema, dict):
        if schema.get("type") == "object" and isinstance(schema.get("properties"), dict):
            properties = schema["properties"]
            schema["required"] = sorted(properties.keys())
            if "additionalProperties" not in schema:
                schema["additionalProperties"] = False
        for value in schema.values():
            _normalize_strict_schema(value)
    elif isinstance(schema, list):
        for item in schema:
            _normalize_strict_schema(item)
    return schema
