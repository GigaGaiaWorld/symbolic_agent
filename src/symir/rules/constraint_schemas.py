"""Constraint schemas for rule IR decoding."""

from __future__ import annotations

from typing import Any, Literal, Annotated, Union

from pydantic import BaseModel, Field, model_validator

from symir.errors import SchemaError
from symir.ir.fact_schema import FactView, PredicateSchema, ArgSpec
from symir.rules.library import Library, LibrarySpec


def build_pydantic_rule_model(
    view: FactView,
    library: Library | None = None,
    *,
    mode: Literal["verbose", "compact"] = "verbose",
) -> type[BaseModel]:
    """Build a Pydantic model that validates rule conditions only."""

    allowed_predicate_ids = {pred.schema_id: pred.arity for pred in view.predicates()}
    predicate_info: dict[
        str,
        tuple[
            str,
            int,
            list[tuple[str | None, str | None, str | None, str | None]] | None,
            str | None,
        ],
    ] = {}
    for pred in view.predicates():
        predicate_info[pred.schema_id] = (
            pred.name,
            pred.arity,
            [(a.datatype, a.role, a.namespace, a.arg_name) for a in pred.signature],
            pred.description,
        )
    if library is not None:
        for pred_id, spec in library.predicate_ids().items():
            allowed_predicate_ids[pred_id] = spec.arity
            signature = None
            if spec.signature is not None:
                signature = [(item, None, None, None) for item in spec.signature]
            predicate_info[pred_id] = (spec.name, spec.arity, signature, spec.description)
    allowed_expr_ops = set(_builtin_ops())
    if library is not None:
        allowed_expr_ops.update(library.expr_ops())

    def _validate_const_value(value: object, datatype: str | None) -> None:
        if datatype is None:
            return
        dtype = datatype.strip().lower()
        if dtype == "string":
            if not isinstance(value, str):
                raise ValueError(f"Const value must be string for datatype 'string': {value}")
            return
        if dtype == "int":
            if not isinstance(value, int) or isinstance(value, bool):
                raise ValueError(f"Const value must be int for datatype 'int': {value}")
            return
        if dtype == "float":
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                raise ValueError(f"Const value must be float for datatype 'float': {value}")
            return
        if dtype == "bool":
            if not isinstance(value, bool):
                raise ValueError(f"Const value must be bool for datatype 'bool': {value}")
            return

    class VarModel(BaseModel):
        kind: Literal["var"] = "var"
        name: str

    class ConstModel(BaseModel):
        kind: Literal["const"] = "const"
        value: str | int | float | bool

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

    class ArgSpecModel(BaseModel):
        datatype: str
        role: str | None = None
        namespace: str | None = None
        arg_name: str | None = None

    class PredicateInfoModel(BaseModel):
        name: str
        arity: int
        signature: list[ArgSpecModel] | None = None
        description: str | None = None

    class RefExprModel(BaseModel):
        kind: Literal["ref"] = "ref"
        schema: str
        terms: list[ExprTermModel] | None = None
        args: list[ArgValueModel] | None = None
        negated: bool = False

        @model_validator(mode="after")
        def _check_allowed(self):
            if self.schema not in allowed_predicate_ids:
                raise ValueError(f"Ref predicate not in FactView: {self.schema}")
            expected = allowed_predicate_ids[self.schema]
            if mode == "verbose":
                if self.terms is None:
                    raise ValueError("Ref terms required in verbose mode.")
                if len(self.terms) != expected:
                    raise ValueError(
                        f"Ref arity mismatch: expected {expected}, got {len(self.terms)}"
                    )
            else:
                if self.args is None:
                    raise ValueError("Ref args required in compact mode.")
                if len(self.args) != expected:
                    raise ValueError(
                        f"Ref arity mismatch: expected {expected}, got {len(self.args)}"
                    )
            info = predicate_info.get(self.schema)
            if info is not None:
                _, _, signature, _ = info
                if signature is not None:
                    if mode == "verbose" and self.terms is not None:
                        for term, expected_sig in zip(self.terms, signature):
                            expected_datatype = expected_sig[0]
                            if isinstance(term, ConstModel):
                                _validate_const_value(term.value, expected_datatype)
                    if mode == "compact" and self.args is not None:
                        for arg_value, expected_sig in zip(self.args, signature):
                            expected_datatype = expected_sig[0]
                            if isinstance(arg_value.value, ConstModel):
                                _validate_const_value(arg_value.value.value, expected_datatype)
            return self

    ExprModel = Annotated[
        Union[VarModel, ConstModel, CallModel, UnifyModel, IfModel, NotModel, RefExprModel],
        Field(discriminator="kind"),
    ]

    CallModel.model_rebuild()
    UnifyModel.model_rebuild()
    IfModel.model_rebuild()
    NotModel.model_rebuild()
    RefExprModel.model_rebuild()

    class RefModel(BaseModel):
        kind: Literal["ref"] = "ref"
        schema: str
        terms: list[ExprTermModel] | None = None
        args: list[ArgValueModel] | None = None
        negated: bool = False

        @model_validator(mode="after")
        def _check_allowed(self):
            if self.schema not in allowed_predicate_ids:
                raise ValueError(f"Ref predicate not in FactView: {self.schema}")
            expected = allowed_predicate_ids[self.schema]
            if mode == "verbose":
                if self.terms is None:
                    raise ValueError("Ref terms required in verbose mode.")
                if len(self.terms) != expected:
                    raise ValueError(
                        f"Ref arity mismatch: expected {expected}, got {len(self.terms)}"
                    )
            else:
                if self.args is None:
                    raise ValueError("Ref args required in compact mode.")
                if len(self.args) != expected:
                    raise ValueError(
                        f"Ref arity mismatch: expected {expected}, got {len(self.args)}"
                    )
            info = predicate_info.get(self.schema)
            if info is not None:
                _, _, signature, _ = info
                if signature is not None:
                    if mode == "verbose" and self.terms is not None:
                        for term, expected_sig in zip(self.terms, signature):
                            expected_datatype = expected_sig[0]
                            if isinstance(term, ConstModel):
                                _validate_const_value(term.value, expected_datatype)
                    if mode == "compact" and self.args is not None:
                        for arg_value, expected_sig in zip(self.args, signature):
                            expected_datatype = expected_sig[0]
                            if isinstance(arg_value.value, ConstModel):
                                _validate_const_value(arg_value.value.value, expected_datatype)
            return self

    class ExprWrapperModel(BaseModel):
        kind: Literal["expr"] = "expr"
        expr: ExprModel

    LiteralModel = Annotated[
        Union[RefModel, ExprWrapperModel],
        Field(discriminator="kind"),
    ]

    class CondModel(BaseModel):
        literals: list[LiteralModel]
        prob: float | None = Field(default=None, ge=0.0, le=1.0)

    class RuleInstanceModel(BaseModel):
        conditions: list[CondModel]

    return RuleInstanceModel


def _ref_schema(
    schema_id: str,
    name: str,
    arity: int,
    signature: list[ArgSpec] | None,
    description: str | None,
) -> dict[str, Any]:
    if signature is None:
        signature = [ArgSpec(spec="any") for _ in range(arity)]
    term_schema = {
        "anyOf": [
            {
                "type": "object",
                "properties": {
                    "kind": {"type": "string", "const": "var"},
                    "name": {"type": "string"},
                },
                "required": ["kind", "name"],
                "additionalProperties": False,
            },
            {
                "type": "object",
                "properties": {
                    "kind": {"type": "string", "const": "const"},
                    "value": {"type": ["string", "number", "boolean"]},
                },
                "required": ["kind", "value"],
                "additionalProperties": False,
            },
        ]
    }
    return {
        "type": "object",
        "properties": {
            "kind": {"type": "string", "const": "ref"},
            "schema": {"type": "string", "const": schema_id},
            "terms": {
                "type": "array",
                "items": term_schema,
                "minItems": arity,
                "maxItems": arity,
            },
            "negated": {"type": "boolean"},
        },
        "required": ["kind", "schema", "terms", "negated"],
        "additionalProperties": False,
    }


def build_responses_schema(
    view: FactView,
    library: Library | None = None,
    *,
    mode: Literal["verbose", "compact"] = "verbose",
) -> dict[str, Any]:
    """Build a JSON schema for OpenAI Responses API strict decoding (conditions only)."""
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
            },
            "required": ["kind", "name"],
            "additionalProperties": False,
        },
        "const": {
            "type": "object",
            "properties": {
                "kind": {"type": "string", "const": "const"},
                "value": {"type": ["string", "number", "boolean"]},
            },
            "required": ["kind", "value"],
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
        "ref": {
            "type": "object",
            "properties": {
                "kind": {"type": "string", "const": "ref"},
                "schema": {"type": "string"},
                "terms": {
                    "type": "array",
                    "items": {
                        "anyOf": [
                            {"$ref": "#/$defs/var"},
                            {"$ref": "#/$defs/const"},
                        ]
                    },
                },
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
            "required": ["kind", "schema", "negated"],
            "additionalProperties": False,
        },
    }
    if mode == "verbose":
        defs["ref"]["required"] = ["kind", "schema", "terms", "negated"]
    else:
        defs["ref"]["required"] = ["kind", "schema", "args", "negated"]

    defs["expr"] = {
        "anyOf": [
            {"$ref": "#/$defs/var"},
            {"$ref": "#/$defs/const"},
            {"$ref": "#/$defs/call"},
            {"$ref": "#/$defs/unify"},
            {"$ref": "#/$defs/if"},
            {"$ref": "#/$defs/not"},
            {"$ref": "#/$defs/ref"},
        ]
    }

    if mode == "verbose":
        ref_items: list[dict[str, Any]] = []
        for pred in allowed_preds:
            ref_items.append(
                _ref_schema(
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
                    signature = [ArgSpec(spec=item) for item in spec.signature]
                ref_items.append(
                    _ref_schema(pred_id, spec.name, spec.arity, signature, spec.description)
                )
        ref_schema = {"anyOf": ref_items}
    else:
        # compact: schema enum + args array
        allowed_ids = [p.schema_id for p in allowed_preds]
        if library is not None:
            allowed_ids.extend(list(library.predicate_ids().keys()))
        ref_schema = {
            "type": "object",
            "properties": {
                "kind": {"type": "string", "const": "ref"},
                "schema": {"type": "string", "enum": allowed_ids},
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
            "required": ["kind", "schema", "args", "negated"],
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
    literal_schema = {"anyOf": [ref_schema, expr_literal_schema]}
    cond_schema = {
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
            "conditions": {"type": "array", "items": cond_schema},
        },
        "required": ["conditions"],
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
