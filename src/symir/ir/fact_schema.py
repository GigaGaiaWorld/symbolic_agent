"""Predicate schema and view definitions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Iterable
import hashlib
import json

from symir.errors import SchemaError


@dataclass(frozen=True)
class ArgSpec:
    """Argument specification for predicate signatures."""

    datatype: str
    role: Optional[str] = None
    namespace: Optional[str] = None

    def to_dict(self) -> dict[str, object]:
        return {
            "datatype": self.datatype,
            "role": self.role,
            "namespace": self.namespace,
        }

    @staticmethod
    def from_dict(data: dict[str, object]) -> "ArgSpec":
        if "datatype" not in data:
            raise SchemaError("ArgSpec requires datatype.")
        return ArgSpec(
            datatype=str(data["datatype"]),
            role=data.get("role") if data.get("role") is not None else None,
            namespace=data.get("namespace") if data.get("namespace") is not None else None,
        )


@dataclass(frozen=True)
class PredicateSchema:
    """Schema for a predicate (fact or rule-level)."""

    name: str
    arity: int
    signature: list[ArgSpec]
    description: str | None = None

    def __post_init__(self) -> None:
        if not self.name:
            raise SchemaError("Predicate name must be non-empty.")
        if self.arity < 0:
            raise SchemaError("Predicate arity must be non-negative.")
        if self.arity != len(self.signature):
            raise SchemaError("Predicate arity must match signature length.")
        if self.description is not None and not isinstance(self.description, str):
            raise SchemaError("Predicate description must be a string if provided.")

    @property
    def schema_id(self) -> str:
        payload = json.dumps(
            {
                "name": self.name,
                "arity": self.arity,
                "signature": [s.to_dict() for s in self.signature],
            },
            sort_keys=True,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "arity": self.arity,
            "signature": [s.to_dict() for s in self.signature],
            "schema_id": self.schema_id,
            "description": self.description,
        }

    @staticmethod
    def from_dict(data: dict[str, object]) -> "PredicateSchema":
        if "name" not in data or "arity" not in data or "signature" not in data:
            raise SchemaError("PredicateSchema requires name, arity, signature.")
        signature = data.get("signature")
        if not isinstance(signature, list):
            raise SchemaError("PredicateSchema signature must be a list.")
        return PredicateSchema(
            name=str(data["name"]),
            arity=int(data["arity"]),
            signature=[ArgSpec.from_dict(item) for item in signature],
            description=data.get("description"),
        )


class FactSchema:
    """Collection of predicate schemas for facts."""

    def __init__(self, predicates: Iterable[PredicateSchema]):
        self._predicates = list(predicates)
        self._by_id: dict[str, PredicateSchema] = {}
        self._validate()

    def _validate(self) -> None:
        seen = set()
        for pred in self._predicates:
            key = (pred.name, pred.arity, tuple((s.datatype, s.role, s.namespace) for s in pred.signature))
            if key in seen:
                raise SchemaError(
                    f"Duplicate predicate schema: {pred.name}/{pred.arity} with same signature."
                )
            seen.add(key)
            self._by_id[pred.schema_id] = pred

    def predicates(self) -> list[PredicateSchema]:
        return list(self._predicates)

    def get(self, schema_id: str) -> PredicateSchema:
        if schema_id not in self._by_id:
            raise SchemaError(f"Unknown predicate schema_id: {schema_id}")
        return self._by_id[schema_id]

    def to_dict(self) -> dict[str, object]:
        return {"predicates": [p.to_dict() for p in self._predicates]}

    @staticmethod
    def from_dict(data: dict[str, object]) -> "FactSchema":
        items = data.get("predicates")
        if not isinstance(items, list):
            raise SchemaError("FactSchema requires a list of predicates.")
        return FactSchema([PredicateSchema.from_dict(item) for item in items])

    def view(self, schema_ids: Iterable[str]) -> "FactView":
        return FactView(self, schema_ids)

    def view_from_filter(self, filt) -> "FactView":
        from symir.ir.filters import apply_filter

        filtered = apply_filter(self._predicates, filt)
        return FactView(self, [p.schema_id for p in filtered])


class FactView:
    """View over a FactSchema containing a subset of predicate schemas."""

    def __init__(self, schema: FactSchema, schema_ids: Iterable[str]):
        self.schema = schema
        self.schema_ids = set(schema_ids)
        for schema_id in self.schema_ids:
            schema.get(schema_id)

    def allows(self, schema_id: str) -> bool:
        return schema_id in self.schema_ids

    def predicates(self) -> list[PredicateSchema]:
        return [self.schema.get(schema_id) for schema_id in self.schema_ids]
