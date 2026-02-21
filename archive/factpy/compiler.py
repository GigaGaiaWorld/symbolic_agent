"""FactPy compiler/runtime layer for append-only claim/meta semantics."""

from __future__ import annotations

from collections import defaultdict
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import date, datetime
import base64
import hashlib
import json
import math
import uuid
from typing import Any, Callable, Iterable, Literal, Sequence

from .ir import (
    EntitySchemaIR,
    FieldMappingIR,
    FieldSpecIR,
    IdentitySpecIR,
    PredicateRuleSpecIR,
    PredicateIR,
    SchemaIR,
    TieBreakSpec,
    TemporalMappingIR,
)
from .model import (
    Entity,
    Field,
    ValidTimeValue,
    VersionedValue,
)


class FactPyCompileError(ValueError):
    """Raised when compile-time semantic checks fail."""


def _annotation_label(annotation: object) -> str:
    if isinstance(annotation, str):
        return annotation
    if getattr(annotation, "__name__", None):
        return str(annotation.__name__)
    return str(annotation)


def _stable_hash(payload: object) -> str:
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _stable_entity_ref(entity: Entity) -> str:
    payload = {
        "entity_type": entity.__class__.__name__,
        "identity": sorted(entity.identity_values.items(), key=lambda item: item[0]),
    }
    return _stable_hash(payload)


def view_predicate_name(base_predicate: str) -> str:
    """Deterministic mapping from storage predicate id to user-facing view predicate id."""
    normalized = base_predicate.replace(":", "_")
    return f"{normalized}_view"


@dataclass(frozen=True)
class EntityRef:
    entity_type: str
    ref: str


@dataclass(frozen=True)
class EntityRefConst:
    """Typed wrapper used in canonical tuple encoding."""

    ref: str


@dataclass(frozen=True)
class EncodedTerm:
    tag: str
    value: object


_TYPED_TUPLE_PREFIX = "tup1:"


class CanonicalTupleCodec:
    """Deterministic and reversible typed tuple codec."""

    @classmethod
    def encode(cls, terms: Sequence[object]) -> str:
        payload = {
            "v": 1,
            "terms": [cls._encode_term(item) for item in terms],
        }
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        encoded = base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")
        return f"{_TYPED_TUPLE_PREFIX}{encoded}"

    @classmethod
    def decode(cls, token: str) -> tuple[EncodedTerm, ...]:
        if not isinstance(token, str) or not token.startswith(_TYPED_TUPLE_PREFIX):
            raise FactPyCompileError("Claim object must be encoded as typed tuple constant.")
        b64 = token[len(_TYPED_TUPLE_PREFIX) :]
        padded = b64 + "=" * ((4 - len(b64) % 4) % 4)
        try:
            raw = base64.urlsafe_b64decode(padded.encode("ascii"))
            payload = json.loads(raw.decode("utf-8"))
        except Exception as exc:  # pragma: no cover - defensive parse boundary
            raise FactPyCompileError("Invalid canonical tuple encoding.") from exc

        if not isinstance(payload, dict) or payload.get("v") != 1:
            raise FactPyCompileError("Unsupported canonical tuple payload version.")
        terms = payload.get("terms")
        if not isinstance(terms, list):
            raise FactPyCompileError("Canonical tuple payload must contain a terms list.")
        return tuple(cls._decode_term(item) for item in terms)

    @classmethod
    def _encode_term(cls, value: object) -> dict[str, object]:
        if isinstance(value, Entity):
            return {"t": "entity_ref", "v": _stable_entity_ref(value)}
        if isinstance(value, EntityRef):
            return {"t": "entity_ref", "v": value.ref}
        if isinstance(value, EntityRefConst):
            return {"t": "entity_ref", "v": value.ref}
        if value is None:
            return {"t": "null", "v": None}
        if isinstance(value, bool):
            return {"t": "bool", "v": value}
        if isinstance(value, int):
            return {"t": "int", "v": str(value)}
        if isinstance(value, float):
            if not math.isfinite(value):
                raise FactPyCompileError("Float value in canonical tuple must be finite.")
            return {"t": "float", "v": format(value, ".17g")}
        if isinstance(value, datetime):
            return {"t": "datetime", "v": value.isoformat()}
        if isinstance(value, date):
            return {"t": "date", "v": value.isoformat()}
        if isinstance(value, str):
            return {"t": "string", "v": value}
        raise FactPyCompileError(
            f"Unsupported canonical tuple term type: {type(value).__name__}."
        )

    @classmethod
    def _decode_term(cls, payload: object) -> EncodedTerm:
        if not isinstance(payload, dict):
            raise FactPyCompileError("Canonical tuple term must be an object.")
        tag = payload.get("t")
        value = payload.get("v")
        if tag == "entity_ref":
            if not isinstance(value, str):
                raise FactPyCompileError("entity_ref term must carry string value.")
            return EncodedTerm(tag="entity_ref", value=value)
        if tag == "null":
            return EncodedTerm(tag="null", value=None)
        if tag == "bool":
            if not isinstance(value, bool):
                raise FactPyCompileError("bool term must carry bool value.")
            return EncodedTerm(tag="bool", value=value)
        if tag == "int":
            if not isinstance(value, str):
                raise FactPyCompileError("int term must carry decimal string.")
            return EncodedTerm(tag="int", value=int(value))
        if tag == "float":
            if not isinstance(value, str):
                raise FactPyCompileError("float term must carry decimal string.")
            return EncodedTerm(tag="float", value=float(value))
        if tag == "datetime":
            if not isinstance(value, str):
                raise FactPyCompileError("datetime term must carry iso string.")
            return EncodedTerm(tag="datetime", value=datetime.fromisoformat(value))
        if tag == "date":
            if not isinstance(value, str):
                raise FactPyCompileError("date term must carry iso string.")
            return EncodedTerm(tag="date", value=date.fromisoformat(value))
        if tag == "string":
            if not isinstance(value, str):
                raise FactPyCompileError("string term must carry string value.")
            return EncodedTerm(tag="string", value=value)
        raise FactPyCompileError(f"Unsupported canonical tuple term tag: {tag}")


MetaType = Literal["str", "num", "bool", "time"]


@dataclass(frozen=True)
class MetaFieldSpec:
    value_type: MetaType
    required: bool = False
    default: object | None = None


@dataclass(frozen=True)
class PredicateMetaPolicy:
    allowed_keys: frozenset[str] | None = None
    required_keys: frozenset[str] = frozenset()
    denied_keys: frozenset[str] = frozenset()


class AssertionMetaSchema:
    """Global assertion meta key schema with optional per-predicate overrides."""

    def __init__(
        self,
        *,
        fields: dict[str, MetaFieldSpec],
        per_predicate: dict[str, PredicateMetaPolicy] | None = None,
        strict: bool = True,
    ) -> None:
        if not fields:
            raise FactPyCompileError("AssertionMetaSchema requires at least one declared key.")
        self.fields = dict(fields)
        self.per_predicate = dict(per_predicate or {})
        self.strict = strict

    @classmethod
    def default(cls, *, strict: bool = True) -> AssertionMetaSchema:
        return cls(
            fields={
                "trace_id": MetaFieldSpec("str"),
                "source": MetaFieldSpec("str"),
                "ingested_at": MetaFieldSpec("time"),
                "confidence": MetaFieldSpec("num"),
                "doc_id": MetaFieldSpec("str"),
                "field_source": MetaFieldSpec("str"),
                "factpy_op": MetaFieldSpec("str"),
                "active": MetaFieldSpec("bool"),
            },
            strict=strict,
        )

    def defaults(self, *, predicate: str | None = None) -> dict[str, object]:
        _ = predicate
        out: dict[str, object] = {}
        for key, spec in self.fields.items():
            if spec.default is not None:
                out[key] = spec.default
        return out

    def validate(self, *, predicate: str, meta: dict[str, object]) -> dict[str, object]:
        policy = self.per_predicate.get(predicate)
        result: dict[str, object] = {}

        for key, value in meta.items():
            if policy is not None:
                if key in policy.denied_keys:
                    if self.strict:
                        raise FactPyCompileError(
                            f"Meta key '{key}' is denied for predicate '{predicate}'."
                        )
                    continue
                if policy.allowed_keys is not None and key not in policy.allowed_keys:
                    if self.strict:
                        raise FactPyCompileError(
                            f"Meta key '{key}' is not allowed for predicate '{predicate}'."
                        )
                    continue

            spec = self.fields.get(key)
            if spec is None:
                if self.strict:
                    raise FactPyCompileError(f"Unknown assertion meta key: '{key}'.")
                continue
            result[key] = self._coerce_value(key=key, value=value, spec=spec)

        missing_required = [
            key
            for key, spec in self.fields.items()
            if spec.required and key not in result and spec.default is None
        ]
        if policy is not None:
            missing_required.extend(
                key
                for key in policy.required_keys
                if key not in result and self.fields.get(key, MetaFieldSpec("str")).default is None
            )
        if missing_required:
            raise FactPyCompileError(
                f"Missing required assertion meta keys: {sorted(set(missing_required))}"
            )

        for key, spec in self.fields.items():
            if key not in result and spec.default is not None:
                result[key] = self._coerce_value(key=key, value=spec.default, spec=spec)

        return result

    def _coerce_value(self, *, key: str, value: object, spec: MetaFieldSpec) -> object:
        kind = spec.value_type
        if kind == "str":
            if isinstance(value, str):
                return value
            if self.strict:
                raise FactPyCompileError(f"Meta key '{key}' expects string value.")
            return str(value)

        if kind == "num":
            if isinstance(value, bool):
                if self.strict:
                    raise FactPyCompileError(f"Meta key '{key}' expects numeric value.")
                return int(value)
            if isinstance(value, (int, float)):
                return value
            if self.strict:
                raise FactPyCompileError(f"Meta key '{key}' expects numeric value.")
            try:
                return float(value)
            except Exception as exc:  # pragma: no cover - defensive branch
                raise FactPyCompileError(f"Meta key '{key}' cannot be coerced to number.") from exc

        if kind == "bool":
            if isinstance(value, bool):
                return value
            if self.strict:
                raise FactPyCompileError(f"Meta key '{key}' expects boolean value.")
            if isinstance(value, str):
                lowered = value.strip().lower()
                if lowered in {"true", "1", "yes"}:
                    return True
                if lowered in {"false", "0", "no"}:
                    return False
            return bool(value)

        if kind == "time":
            if isinstance(value, datetime):
                return value.isoformat()
            if isinstance(value, date):
                return value.isoformat()
            if isinstance(value, str):
                return value
            if self.strict:
                raise FactPyCompileError(f"Meta key '{key}' expects time/date string value.")
            return str(value)

        raise FactPyCompileError(f"Unsupported meta value type config: {kind}")


@dataclass(frozen=True)
class PredicateSemantics:
    cardinality: Literal["functional", "multi", "temporal_role", "unknown"]
    fact_key_arity: int = 0


@dataclass(frozen=True)
class AssertionClaim:
    assertion_id: str
    predicate: str
    subject: str
    object_token: str


class SchemaCompiler:
    """Compile authoring schema classes into SchemaIR."""

    def compile(self, entities: Iterable[type[Entity]]) -> SchemaIR:
        entity_list = list(entities)
        if not entity_list:
            raise FactPyCompileError("SchemaCompiler requires at least one Entity class.")
        names = [item.__name__ for item in entity_list]
        if len(set(names)) != len(names):
            raise FactPyCompileError("Entity class names must be unique.")
        entity_name_set = set(names)

        entities_ir: list[EntitySchemaIR] = []
        predicates: list[PredicateIR] = []
        field_mappings: list[FieldMappingIR] = []
        rule_predicates: list[PredicateRuleSpecIR] = []

        for entity_cls in entity_list:
            identity_ir: list[IdentitySpecIR] = []
            for name, spec in entity_cls.__identity_specs__.items():
                identity_ir.append(
                    IdentitySpecIR(
                        name=name,
                        annotation=_annotation_label(spec.annotation),
                        has_default_factory=spec.default_factory is not None,
                    )
                )

            field_ir: list[FieldSpecIR] = []
            for field_name, field_spec in entity_cls.__field_specs__.items():
                self._validate_field_dims(entity_cls, field_name, field_spec)
                predicate_name = self._field_predicate_name(entity_cls, field_name, field_spec)
                field_ir.append(
                    FieldSpecIR(
                        owner_entity=entity_cls.__name__,
                        field_name=field_name,
                        predicate_name=predicate_name,
                        annotation=_annotation_label(field_spec.annotation),
                        cardinality=field_spec.cardinality,
                        fact_key_dims=tuple(field_spec.fact_key),
                        temporal_mode=field_spec.temporal_mode,
                        aliases=tuple(field_spec.aliases),
                        description=field_spec.description,
                        subject_position=0,
                    )
                )
                mapping, pred_items = self._compile_field_mapping(
                    entity_cls=entity_cls,
                    field_name=field_name,
                    field_spec=field_spec,
                    predicate_name=predicate_name,
                )
                field_mappings.append(mapping)
                predicates.extend(pred_items)
                rule_predicates.extend(
                    self._compile_rule_predicates(
                        entity_cls=entity_cls,
                        field_name=field_name,
                        field_spec=field_spec,
                        mapping=mapping,
                        entity_name_set=entity_name_set,
                    )
                )

            entities_ir.append(
                EntitySchemaIR(
                    name=entity_cls.__name__,
                    doc=entity_cls.__doc_meta__,
                    identity_fields=tuple(identity_ir),
                    field_specs=tuple(field_ir),
                    metadata=dict(entity_cls.__meta__),
                )
            )

        return SchemaIR(
            entities=tuple(entities_ir),
            predicates=tuple(predicates),
            field_mappings=tuple(field_mappings),
            rule_predicates=tuple(rule_predicates),
        )

    def _validate_field_dims(self, entity_cls: type[Entity], field_name: str, field_spec: Field) -> None:
        for dim in field_spec.fact_key:
            if dim in entity_cls.__identity_specs__:
                continue
            if dim in entity_cls.__field_specs__:
                continue
            raise FactPyCompileError(
                f"{entity_cls.__name__}.{field_name} fact_key dim '{dim}' not declared."
            )

    def _field_predicate_name(self, entity_cls: type[Entity], field_name: str, field_spec: Field) -> str:
        if field_spec.predicate_name is not None:
            return field_spec.predicate_name
        return f"{entity_cls.__name__.lower()}:{field_name}"

    def _compile_field_mapping(
        self,
        *,
        entity_cls: type[Entity],
        field_name: str,
        field_spec: Field,
        predicate_name: str,
    ) -> tuple[FieldMappingIR, list[PredicateIR]]:
        if field_spec.cardinality != "temporal":
            key_arity = 1 + len(field_spec.fact_key)
            pred = PredicateIR(
                name=predicate_name,
                arity=key_arity + 1,
                key_arity=key_arity,
                value_index=key_arity,
                source_entity=entity_cls.__name__,
                source_field=field_name,
                kind="field",
                temporal_mode=None,
                subject_position=0,
            )
            return (
                FieldMappingIR(
                    owner_entity=entity_cls.__name__,
                    field_name=field_name,
                    base_predicate=predicate_name,
                    cardinality=field_spec.cardinality,
                    fact_key_dims=tuple(field_spec.fact_key),
                    temporal_mode=None,
                    temporal=None,
                    subject_position=0,
                ),
                [pred],
            )

        temporal_mode = field_spec.temporal_mode
        assert temporal_mode is not None
        assertion_entity = f"{entity_cls.__name__}_{field_name}_assertion"
        owner_pred = f"{predicate_name}:owner"
        value_pred = f"{predicate_name}:value"
        dim_preds = tuple(
            (dim, f"{predicate_name}:dim:{dim}") for dim in field_spec.fact_key
        )
        roles: list[PredicateIR] = [
            PredicateIR(
                name=owner_pred,
                arity=2,
                key_arity=1,
                value_index=1,
                source_entity=entity_cls.__name__,
                source_field=field_name,
                kind="temporal_role",
                temporal_mode=temporal_mode,
                subject_position=0,
            ),
            PredicateIR(
                name=value_pred,
                arity=2,
                key_arity=1,
                value_index=1,
                source_entity=entity_cls.__name__,
                source_field=field_name,
                kind="temporal_role",
                temporal_mode=temporal_mode,
                subject_position=0,
            ),
        ]
        for _, pred_name in dim_preds:
            roles.append(
                PredicateIR(
                    name=pred_name,
                    arity=2,
                    key_arity=1,
                    value_index=1,
                    source_entity=entity_cls.__name__,
                    source_field=field_name,
                    kind="temporal_role",
                    temporal_mode=temporal_mode,
                    subject_position=0,
                )
            )

        start_pred = end_pred = version_pred = current_pred = None
        if temporal_mode == "valid_time":
            start_pred = f"{predicate_name}:start"
            end_pred = f"{predicate_name}:end"
            roles.append(
                PredicateIR(
                    name=start_pred,
                    arity=2,
                    key_arity=1,
                    value_index=1,
                    source_entity=entity_cls.__name__,
                    source_field=field_name,
                    kind="temporal_role",
                    temporal_mode=temporal_mode,
                    subject_position=0,
                )
            )
            roles.append(
                PredicateIR(
                    name=end_pred,
                    arity=2,
                    key_arity=1,
                    value_index=1,
                    source_entity=entity_cls.__name__,
                    source_field=field_name,
                    kind="temporal_role",
                    temporal_mode=temporal_mode,
                    subject_position=0,
                )
            )
        else:
            version_pred = f"{predicate_name}:version"
            current_pred = f"{predicate_name}:current"
            roles.append(
                PredicateIR(
                    name=version_pred,
                    arity=2,
                    key_arity=1,
                    value_index=1,
                    source_entity=entity_cls.__name__,
                    source_field=field_name,
                    kind="temporal_role",
                    temporal_mode=temporal_mode,
                    subject_position=0,
                )
            )
            roles.append(
                PredicateIR(
                    name=current_pred,
                    arity=2,
                    key_arity=1,
                    value_index=1,
                    source_entity=entity_cls.__name__,
                    source_field=field_name,
                    kind="temporal_role",
                    temporal_mode=temporal_mode,
                    subject_position=0,
                )
            )

        return (
            FieldMappingIR(
                owner_entity=entity_cls.__name__,
                field_name=field_name,
                base_predicate=predicate_name,
                cardinality="temporal",
                fact_key_dims=tuple(field_spec.fact_key),
                temporal_mode=temporal_mode,
                temporal=TemporalMappingIR(
                    owner_entity=entity_cls.__name__,
                    field_name=field_name,
                    temporal_mode=temporal_mode,
                    assertion_entity=assertion_entity,
                    owner_pred=owner_pred,
                    value_pred=value_pred,
                    dim_preds=dim_preds,
                    start_pred=start_pred,
                    end_pred=end_pred,
                    version_pred=version_pred,
                    current_pred=current_pred,
                ),
                subject_position=0,
            ),
            roles,
        )

    def _compile_rule_predicates(
        self,
        *,
        entity_cls: type[Entity],
        field_name: str,
        field_spec: Field,
        mapping: FieldMappingIR,
        entity_name_set: set[str],
    ) -> list[PredicateRuleSpecIR]:
        value_annotation = _annotation_label(field_spec.annotation)
        value_domain: Literal["entity", "value"] = (
            "entity" if value_annotation in entity_name_set else "value"
        )
        is_mapping = self._is_single_mapping_field(
            entity_cls=entity_cls,
            field_name=field_name,
            base_predicate=mapping.base_predicate,
            value_domain=value_domain,
        )
        tie_break = (
            TieBreakSpec(mode="error")
            if is_mapping
            else None
        )
        out: list[PredicateRuleSpecIR] = []

        if mapping.cardinality != "temporal":
            logical_arity = 2 + len(mapping.fact_key_dims)
            arg_domains: tuple[Literal["entity", "value"], ...] = (
                "entity",
                *("value" for _ in mapping.fact_key_dims),
                value_domain,
            )
            out.append(
                PredicateRuleSpecIR(
                    base_predicate=mapping.base_predicate,
                    view_predicate=view_predicate_name(mapping.base_predicate),
                    logical_arity=logical_arity,
                    arg_domains=arg_domains,
                    subject_position=mapping.subject_position,
                    # Conflict groups are schema-driven: (predicate, subject, declared fact_key dims...).
                    group_key_indexes=(0, *tuple(range(1, 1 + len(mapping.fact_key_dims)))),
                    cardinality=mapping.cardinality,
                    owner_entity=entity_cls.__name__,
                    owner_field=field_name,
                    is_mapping=is_mapping,
                    mapping_kind="single_valued" if is_mapping else None,
                    mapping_key_positions=(0,) if is_mapping else tuple(),
                    mapping_value_positions=(1,) if is_mapping else tuple(),
                    tie_break=tie_break,
                )
            )
            return out

        temporal = mapping.temporal
        if temporal is None:
            return out

        def _append(
            *,
            base_pred: str,
            arg_domains: tuple[Literal["entity", "value"], ...],
        ) -> None:
            out.append(
                PredicateRuleSpecIR(
                    base_predicate=base_pred,
                    view_predicate=view_predicate_name(base_pred),
                    logical_arity=len(arg_domains),
                    arg_domains=arg_domains,
                    subject_position=0,
                    group_key_indexes=(0,),
                    cardinality="multi",
                    owner_entity=entity_cls.__name__,
                    owner_field=field_name,
                    is_mapping=False,
                    mapping_kind=None,
                    mapping_key_positions=tuple(),
                    mapping_value_positions=tuple(),
                    tie_break=None,
                )
            )

        _append(base_pred=temporal.owner_pred, arg_domains=("entity", "entity"))
        _append(base_pred=temporal.value_pred, arg_domains=("entity", value_domain))
        for _, pred_name in temporal.dim_preds:
            _append(base_pred=pred_name, arg_domains=("entity", "value"))
        if temporal.start_pred is not None:
            _append(base_pred=temporal.start_pred, arg_domains=("entity", "value"))
        if temporal.end_pred is not None:
            _append(base_pred=temporal.end_pred, arg_domains=("entity", "value"))
        if temporal.version_pred is not None:
            _append(base_pred=temporal.version_pred, arg_domains=("entity", "value"))
        if temporal.current_pred is not None:
            _append(base_pred=temporal.current_pred, arg_domains=("entity", "value"))

        return out

    def _is_single_mapping_field(
        self,
        *,
        entity_cls: type[Entity],
        field_name: str,
        base_predicate: str,
        value_domain: Literal["entity", "value"],
    ) -> bool:
        if value_domain != "entity":
            return False
        normalized_field = field_name.strip().lower()
        normalized_pred = base_predicate.strip().lower().replace(":", "_")
        if normalized_field == "canon_of":
            return True
        if normalized_pred.endswith("_canon_of"):
            return True
        if normalized_pred == "canon_of":
            return True
        _ = entity_cls
        return False


class EDBStore:
    """Append-only EDB storage for claim/meta facts."""

    def __init__(self) -> None:
        self._facts: dict[str, set[tuple[object, ...]]] = defaultdict(set)
        self._claim_by_assertion: dict[str, tuple[str, str, str, str]] = {}
        self._meta_by_assertion: dict[str, dict[str, object]] = defaultdict(dict)
        self._assertion_order: dict[str, int] = {}
        self._seq = 0

    def add_claim(
        self,
        *,
        assertion_id: str,
        predicate: str,
        subject: str,
        object_token: str,
    ) -> None:
        row = (assertion_id, predicate, subject, object_token)
        existing = self._claim_by_assertion.get(assertion_id)
        if existing is not None:
            if existing == row:
                return
            raise FactPyCompileError(
                f"AssertionId '{assertion_id}' already bound to a different claim."
            )

        self._facts["claim"].add(row)
        self._claim_by_assertion[assertion_id] = row
        self._seq += 1
        self._assertion_order[assertion_id] = self._seq

    def add_meta(self, *, assertion_id: str, key: str, value: object) -> None:
        pred = self._meta_predicate(value)
        self._facts[pred].add((assertion_id, key, value))
        self._meta_by_assertion[assertion_id][key] = value

    def add_tag(self, *, assertion_id: str, tag: str) -> None:
        self._facts["tag"].add((assertion_id, tag))

    def rows(self, predicate: str) -> set[tuple[object, ...]]:
        return set(self._facts.get(predicate, set()))

    def all_facts(self) -> dict[str, set[tuple[object, ...]]]:
        return {name: set(rows) for name, rows in self._facts.items()}

    def claim(self, assertion_id: str) -> tuple[str, str, str, str] | None:
        return self._claim_by_assertion.get(assertion_id)

    def claims(self, predicate: str | None = None) -> list[AssertionClaim]:
        rows = list(self._claim_by_assertion.values())
        if predicate is not None:
            rows = [item for item in rows if item[1] == predicate]
        rows.sort(key=lambda item: self._assertion_order[item[0]])
        return [
            AssertionClaim(
                assertion_id=row[0],
                predicate=row[1],
                subject=row[2],
                object_token=row[3],
            )
            for row in rows
        ]

    def assertion_order(self, assertion_id: str) -> int:
        return self._assertion_order[assertion_id]

    def assertion_meta(self, assertion_id: str) -> dict[str, object]:
        return dict(self._meta_by_assertion.get(assertion_id, {}))

    def _meta_predicate(self, value: object) -> str:
        if isinstance(value, bool):
            return "meta_bool"
        if isinstance(value, (int, float)):
            return "meta_num"
        if isinstance(value, str):
            return "meta_str"
        return "meta_json"


class FactCompiler:
    """Compile entities into append-only claim/meta EDB deltas."""

    def __init__(
        self,
        schema_ir: SchemaIR,
        *,
        store: EDBStore | None = None,
        meta_schema: AssertionMetaSchema | None = None,
        assertion_id_mode: Literal["generated", "replayable"] = "generated",
        replayable_meta_keys: Iterable[str] | None = None,
    ) -> None:
        if assertion_id_mode not in {"generated", "replayable"}:
            raise FactPyCompileError("assertion_id_mode must be 'generated' or 'replayable'.")

        self.schema_ir = schema_ir
        self.store = store or EDBStore()
        self.meta_schema = meta_schema or AssertionMetaSchema.default(strict=True)
        self.assertion_id_mode = assertion_id_mode
        self.replayable_meta_keys = tuple(sorted(set(replayable_meta_keys or [])))

        self._mapping_index = {
            (item.owner_entity, item.field_name): item for item in schema_ir.field_mappings
        }
        self._field_predicates = {item.base_predicate for item in schema_ir.field_mappings}
        self._predicate_semantics: dict[str, PredicateSemantics] = {}
        for mapping in schema_ir.field_mappings:
            if mapping.cardinality == "multi":
                sem = PredicateSemantics("multi", fact_key_arity=len(mapping.fact_key_dims))
            elif mapping.cardinality == "functional":
                sem = PredicateSemantics("functional", fact_key_arity=len(mapping.fact_key_dims))
            else:
                sem = PredicateSemantics("unknown", fact_key_arity=0)
            self._predicate_semantics[mapping.base_predicate] = sem

            temporal = mapping.temporal
            if temporal is not None:
                self._predicate_semantics[temporal.owner_pred] = PredicateSemantics(
                    "temporal_role", fact_key_arity=0
                )
                self._predicate_semantics[temporal.value_pred] = PredicateSemantics(
                    "temporal_role", fact_key_arity=0
                )
                for _, pred_name in temporal.dim_preds:
                    self._predicate_semantics[pred_name] = PredicateSemantics(
                        "temporal_role", fact_key_arity=0
                    )
                if temporal.start_pred is not None:
                    self._predicate_semantics[temporal.start_pred] = PredicateSemantics(
                        "temporal_role", fact_key_arity=0
                    )
                if temporal.end_pred is not None:
                    self._predicate_semantics[temporal.end_pred] = PredicateSemantics(
                        "temporal_role", fact_key_arity=0
                    )
                if temporal.version_pred is not None:
                    self._predicate_semantics[temporal.version_pred] = PredicateSemantics(
                        "temporal_role", fact_key_arity=0
                    )
                if temporal.current_pred is not None:
                    self._predicate_semantics[temporal.current_pred] = PredicateSemantics(
                        "temporal_role", fact_key_arity=0
                    )

    def ingest(
        self,
        entity: Entity,
        *,
        batch_meta: dict[str, object] | None = None,
        save_meta: dict[str, object] | None = None,
    ) -> EntityRef:
        entity_name = entity.__class__.__name__
        entity_ref = EntityRef(entity_type=entity_name, ref=_stable_entity_ref(entity))
        for field_name, raw_value in entity.field_values.items():
            mapping = self._mapping_index.get((entity_name, field_name))
            if mapping is None:
                raise FactPyCompileError(f"Field mapping not found for {entity_name}.{field_name}")
            op = "add" if mapping.cardinality in {"multi", "temporal"} else "set"
            self.apply_field_operation(
                owner_entity=entity_name,
                entity_ref=entity_ref,
                field_name=field_name,
                operation=op,
                payload=raw_value,
                field_meta=None,
                batch_meta=batch_meta,
                save_meta=save_meta,
            )
        return entity_ref

    def apply_field_operation(
        self,
        *,
        owner_entity: str,
        entity_ref: EntityRef,
        field_name: str,
        operation: Literal["set", "add", "remove"],
        payload: object,
        field_meta: dict[str, object] | None,
        batch_meta: dict[str, object] | None,
        save_meta: dict[str, object] | None,
    ) -> list[str]:
        mapping = self._mapping_index.get((owner_entity, field_name))
        if mapping is None:
            raise FactPyCompileError(f"Field mapping not found for {owner_entity}.{field_name}")

        if mapping.cardinality == "temporal":
            return self._apply_temporal_operation(
                entity_ref=entity_ref,
                mapping=mapping,
                operation=operation,
                payload=payload,
                field_meta=field_meta,
                batch_meta=batch_meta,
                save_meta=save_meta,
            )

        if mapping.cardinality == "functional" and operation == "add":
            raise FactPyCompileError(
                f"{owner_entity}.{field_name} is functional; use set() not add()."
            )

        items = payload if isinstance(payload, list) else [payload]
        assertion_ids: list[str] = []
        for item in items:
            value, dims = self._normalize_value_and_dims(item)
            self._validate_dims(list(mapping.fact_key_dims), dims, mapping)
            rest_terms = [self._literal_term(dims[name]) for name in mapping.fact_key_dims]
            rest_terms.append(self._literal_term(value))

            op_meta = {"factpy_op": operation}
            if field_meta:
                op_meta.update(field_meta)

            assertion_ids.append(
                self._emit_claim(
                    predicate=mapping.base_predicate,
                    subject=entity_ref.ref,
                    rest_terms=rest_terms,
                    batch_meta=batch_meta,
                    save_meta=save_meta,
                    field_meta=op_meta,
                )
            )
        return assertion_ids

    def emit(
        self,
        *,
        pred: str,
        s: object,
        o: object,
        meta: dict[str, object] | None = None,
    ) -> str:
        subject = self._normalize_subject(s)
        rest_terms = list(o) if isinstance(o, (list, tuple)) else [o]
        return self._emit_claim(
            predicate=pred,
            subject=subject,
            rest_terms=[self._literal_term(item) for item in rest_terms],
            batch_meta=None,
            save_meta=None,
            field_meta=meta,
        )

    def view_rows(
        self,
        predicate: str,
        *,
        active: Callable[[str, EDBStore], bool] | None = None,
        chosen: Callable[[str, AssertionClaim, EDBStore], bool] | None = None,
    ) -> set[tuple[object, ...]]:
        claims = self.store.claims(predicate)
        if not claims:
            return set()

        if active is None:
            active_ids = self._default_active(claims)
        else:
            active_ids = {item.assertion_id for item in claims if active(item.assertion_id, self.store)}

        active_claims = [item for item in claims if item.assertion_id in active_ids]
        if chosen is None:
            chosen_ids = self._default_chosen(predicate, active_claims)
        else:
            chosen_ids = {
                item.assertion_id
                for item in active_claims
                if chosen(item.assertion_id, item, self.store)
            }

        out: set[tuple[object, ...]] = set()
        for item in active_claims:
            if item.assertion_id not in chosen_ids:
                continue
            decoded = CanonicalTupleCodec.decode(item.object_token)
            row = (item.subject, *[term.value for term in decoded])
            out.add(row)
        return out

    def active_assertions(self, predicate: str | None = None) -> set[str]:
        claims = self.store.claims(predicate)
        return self._default_active(claims)

    def chosen_assertions(
        self,
        predicate: str | None = None,
        *,
        active_ids: set[str] | None = None,
    ) -> set[str]:
        if predicate is not None:
            claims = self.store.claims(predicate)
            if active_ids is not None:
                claims = [item for item in claims if item.assertion_id in active_ids]
            return self._default_chosen(predicate, claims)

        claims = self.store.claims()
        by_predicate: dict[str, list[AssertionClaim]] = defaultdict(list)
        for item in claims:
            if active_ids is not None and item.assertion_id not in active_ids:
                continue
            by_predicate[item.predicate].append(item)
        chosen: set[str] = set()
        for pred_name, pred_claims in by_predicate.items():
            chosen.update(self._default_chosen(pred_name, pred_claims))
        return chosen

    def current_valid_time(
        self,
        *,
        owner_entity: str,
        field_name: str,
        entity_ref: str,
        now: object,
        dims: dict[str, object] | None = None,
    ) -> set[str]:
        mapping = self._require_mapping(owner_entity, field_name, temporal_mode="valid_time")
        temporal = mapping.temporal
        assert temporal is not None

        dim_payload = dict(dims or {})
        dim_names = [name for name, _ in temporal.dim_preds]
        self._validate_dims(dim_names, dim_payload, mapping)

        owner_rows = self.view_rows(temporal.owner_pred)
        start_rows = self.view_rows(temporal.start_pred or "")
        end_rows = self.view_rows(temporal.end_pred or "")

        start_by_assertion = {row[0]: row[1] for row in start_rows}
        end_by_assertion = {row[0]: row[1] for row in end_rows}

        dim_maps: list[tuple[str, dict[object, object]]] = []
        for dim_name, pred_name in temporal.dim_preds:
            dim_rows = self.view_rows(pred_name)
            dim_maps.append((dim_name, {row[0]: row[1] for row in dim_rows}))

        current: set[str] = set()
        for assertion, owner in owner_rows:
            if owner != entity_ref:
                continue
            if assertion not in start_by_assertion:
                continue
            start_value = start_by_assertion[assertion]
            end_value = end_by_assertion.get(assertion)
            if not (start_value <= now and (end_value is None or now < end_value)):
                continue
            matches_dims = True
            for idx, (_, dim_map) in enumerate(dim_maps):
                if dim_map.get(assertion) != dim_payload[dim_names[idx]]:
                    matches_dims = False
                    break
            if matches_dims:
                current.add(str(assertion))
        return current

    def current_versioned(
        self,
        *,
        owner_entity: str,
        field_name: str,
        entity_ref: str,
        dims: dict[str, object] | None = None,
    ) -> set[str]:
        mapping = self._require_mapping(owner_entity, field_name, temporal_mode="versioned")
        temporal = mapping.temporal
        assert temporal is not None

        dim_payload = dict(dims or {})
        dim_names = [name for name, _ in temporal.dim_preds]
        self._validate_dims(dim_names, dim_payload, mapping)

        owner_rows = self.view_rows(temporal.owner_pred)
        current_rows = self.view_rows(temporal.current_pred or "")
        current_true = {row[0] for row in current_rows if len(row) == 2 and bool(row[1])}

        dim_maps: list[tuple[str, dict[object, object]]] = []
        for dim_name, pred_name in temporal.dim_preds:
            dim_rows = self.view_rows(pred_name)
            dim_maps.append((dim_name, {row[0]: row[1] for row in dim_rows}))

        results: set[str] = set()
        for assertion, owner in owner_rows:
            if owner != entity_ref or assertion not in current_true:
                continue
            matches_dims = True
            for idx, (_, dim_map) in enumerate(dim_maps):
                if dim_map.get(assertion) != dim_payload[dim_names[idx]]:
                    matches_dims = False
                    break
            if matches_dims:
                results.add(str(assertion))
        return results

    def _apply_temporal_operation(
        self,
        *,
        entity_ref: EntityRef,
        mapping: FieldMappingIR,
        operation: Literal["set", "add", "remove"],
        payload: object,
        field_meta: dict[str, object] | None,
        batch_meta: dict[str, object] | None,
        save_meta: dict[str, object] | None,
    ) -> list[str]:
        temporal = mapping.temporal
        if temporal is None:
            raise FactPyCompileError("Temporal mapping missing.")
        items = payload if isinstance(payload, list) else [payload]

        assertion_ids: list[str] = []
        for item in items:
            if temporal.temporal_mode == "valid_time":
                payload_v = self._as_valid_time(item, mapping)
                assertion_ids.extend(
                    self._emit_valid_time_roles(
                        entity_ref=entity_ref,
                        mapping=mapping,
                        payload=payload_v,
                        operation=operation,
                        field_meta=field_meta,
                        batch_meta=batch_meta,
                        save_meta=save_meta,
                    )
                )
            else:
                payload_v = self._as_versioned(item, mapping)
                assertion_ids.extend(
                    self._emit_versioned_roles(
                        entity_ref=entity_ref,
                        mapping=mapping,
                        payload=payload_v,
                        operation=operation,
                        field_meta=field_meta,
                        batch_meta=batch_meta,
                        save_meta=save_meta,
                    )
                )
        return assertion_ids

    def _emit_valid_time_roles(
        self,
        *,
        entity_ref: EntityRef,
        mapping: FieldMappingIR,
        payload: ValidTimeValue,
        operation: Literal["set", "add", "remove"],
        field_meta: dict[str, object] | None,
        batch_meta: dict[str, object] | None,
        save_meta: dict[str, object] | None,
    ) -> list[str]:
        temporal = mapping.temporal
        assert temporal is not None

        dim_names = [name for name, _ in temporal.dim_preds]
        self._validate_dims(dim_names, payload.dims, mapping)
        assertion_entity_ref = self._new_assertion_entity_ref(
            mapping=mapping,
            owner_ref=entity_ref.ref,
            payload=payload,
        )

        op_meta = {"factpy_op": operation}
        if field_meta:
            op_meta.update(field_meta)

        ids = [
            self._emit_claim(
                predicate=temporal.owner_pred,
                subject=assertion_entity_ref,
                rest_terms=[EntityRefConst(entity_ref.ref)],
                batch_meta=batch_meta,
                save_meta=save_meta,
                field_meta=op_meta,
            ),
            self._emit_claim(
                predicate=temporal.value_pred,
                subject=assertion_entity_ref,
                rest_terms=[self._literal_term(payload.value)],
                batch_meta=batch_meta,
                save_meta=save_meta,
                field_meta=op_meta,
            ),
        ]
        for dim_name, pred_name in temporal.dim_preds:
            ids.append(
                self._emit_claim(
                    predicate=pred_name,
                    subject=assertion_entity_ref,
                    rest_terms=[self._literal_term(payload.dims[dim_name])],
                    batch_meta=batch_meta,
                    save_meta=save_meta,
                    field_meta=op_meta,
                )
            )

        if temporal.start_pred is None:
            raise FactPyCompileError("valid_time mapping missing start predicate.")
        ids.append(
            self._emit_claim(
                predicate=temporal.start_pred,
                subject=assertion_entity_ref,
                rest_terms=[self._literal_term(payload.start)],
                batch_meta=batch_meta,
                save_meta=save_meta,
                field_meta=op_meta,
            )
        )

        if payload.end is not None:
            if temporal.end_pred is None:
                raise FactPyCompileError("valid_time mapping missing end predicate.")
            ids.append(
                self._emit_claim(
                    predicate=temporal.end_pred,
                    subject=assertion_entity_ref,
                    rest_terms=[self._literal_term(payload.end)],
                    batch_meta=batch_meta,
                    save_meta=save_meta,
                    field_meta=op_meta,
                )
            )
        return ids

    def _emit_versioned_roles(
        self,
        *,
        entity_ref: EntityRef,
        mapping: FieldMappingIR,
        payload: VersionedValue,
        operation: Literal["set", "add", "remove"],
        field_meta: dict[str, object] | None,
        batch_meta: dict[str, object] | None,
        save_meta: dict[str, object] | None,
    ) -> list[str]:
        temporal = mapping.temporal
        assert temporal is not None

        dim_names = [name for name, _ in temporal.dim_preds]
        self._validate_dims(dim_names, payload.dims, mapping)
        key_dims = {name: payload.dims[name] for name in dim_names}

        op_meta = {"factpy_op": operation}
        if field_meta:
            op_meta.update(field_meta)

        old_current_refs: set[str] = set()
        if operation != "remove":
            old_current_refs = self.current_versioned(
                owner_entity=mapping.owner_entity,
                field_name=mapping.field_name,
                entity_ref=entity_ref.ref,
                dims=key_dims,
            )

        assertion_entity_ref = self._new_assertion_entity_ref(
            mapping=mapping,
            owner_ref=entity_ref.ref,
            payload=payload,
        )

        ids = [
            self._emit_claim(
                predicate=temporal.owner_pred,
                subject=assertion_entity_ref,
                rest_terms=[EntityRefConst(entity_ref.ref)],
                batch_meta=batch_meta,
                save_meta=save_meta,
                field_meta=op_meta,
            ),
            self._emit_claim(
                predicate=temporal.value_pred,
                subject=assertion_entity_ref,
                rest_terms=[self._literal_term(payload.value)],
                batch_meta=batch_meta,
                save_meta=save_meta,
                field_meta=op_meta,
            ),
        ]

        for dim_name, pred_name in temporal.dim_preds:
            ids.append(
                self._emit_claim(
                    predicate=pred_name,
                    subject=assertion_entity_ref,
                    rest_terms=[self._literal_term(payload.dims[dim_name])],
                    batch_meta=batch_meta,
                    save_meta=save_meta,
                    field_meta=op_meta,
                )
            )

        if temporal.version_pred is None or temporal.current_pred is None:
            raise FactPyCompileError("versioned mapping missing version/current predicates.")

        ids.append(
            self._emit_claim(
                predicate=temporal.version_pred,
                subject=assertion_entity_ref,
                rest_terms=[self._literal_term(payload.version)],
                batch_meta=batch_meta,
                save_meta=save_meta,
                field_meta=op_meta,
            )
        )

        if operation == "remove":
            ids.append(
                self._emit_claim(
                    predicate=temporal.current_pred,
                    subject=assertion_entity_ref,
                    rest_terms=[False],
                    batch_meta=batch_meta,
                    save_meta=save_meta,
                    field_meta=op_meta,
                )
            )
            return ids

        # Append-only current transition: write explicit false for prior current assertions.
        for old_ref in old_current_refs:
            ids.append(
                self._emit_claim(
                    predicate=temporal.current_pred,
                    subject=old_ref,
                    rest_terms=[False],
                    batch_meta=batch_meta,
                    save_meta=save_meta,
                    field_meta={"factpy_op": "set"},
                )
            )

        ids.append(
            self._emit_claim(
                predicate=temporal.current_pred,
                subject=assertion_entity_ref,
                rest_terms=[True],
                batch_meta=batch_meta,
                save_meta=save_meta,
                field_meta=op_meta,
            )
        )
        return ids

    def _emit_claim(
        self,
        *,
        predicate: str,
        subject: str,
        rest_terms: Sequence[object],
        batch_meta: dict[str, object] | None,
        save_meta: dict[str, object] | None,
        field_meta: dict[str, object] | None,
    ) -> str:
        object_token = CanonicalTupleCodec.encode(rest_terms)

        effective_meta = self._merge_meta(
            predicate=predicate,
            batch_meta=batch_meta,
            save_meta=save_meta,
            field_meta=field_meta,
        )
        validated_meta = self.meta_schema.validate(predicate=predicate, meta=effective_meta)

        assertion_id = self._new_assertion_id(
            predicate=predicate,
            subject=subject,
            object_token=object_token,
            meta=validated_meta,
        )
        self.store.add_claim(
            assertion_id=assertion_id,
            predicate=predicate,
            subject=subject,
            object_token=object_token,
        )
        for key, value in validated_meta.items():
            self.store.add_meta(assertion_id=assertion_id, key=key, value=value)
        return assertion_id

    def _merge_meta(
        self,
        *,
        predicate: str,
        batch_meta: dict[str, object] | None,
        save_meta: dict[str, object] | None,
        field_meta: dict[str, object] | None,
    ) -> dict[str, object]:
        merged = self.meta_schema.defaults(predicate=predicate)
        merged.update(batch_meta or {})
        merged.update(save_meta or {})
        merged.update(field_meta or {})
        return merged

    def _new_assertion_id(
        self,
        *,
        predicate: str,
        subject: str,
        object_token: str,
        meta: dict[str, object],
    ) -> str:
        if self.assertion_id_mode == "generated":
            return str(uuid.uuid4())
        replay_meta = {key: meta[key] for key in self.replayable_meta_keys if key in meta}
        payload = {
            "predicate": predicate,
            "subject": subject,
            "object": object_token,
            "meta": replay_meta,
        }
        return _stable_hash(payload)

    def _default_active(self, claims: list[AssertionClaim]) -> set[str]:
        by_exact_fact: dict[tuple[str, str, str], list[AssertionClaim]] = defaultdict(list)
        for item in claims:
            by_exact_fact[(item.predicate, item.subject, item.object_token)].append(item)

        retracted_facts: set[tuple[str, str, str]] = set()
        for fact_key, group in by_exact_fact.items():
            latest = max(group, key=lambda item: self.store.assertion_order(item.assertion_id))
            op = str(self.store.assertion_meta(latest.assertion_id).get("factpy_op", "add")).lower()
            if op == "remove":
                retracted_facts.add(fact_key)

        active_ids: set[str] = set()
        for item in claims:
            if (item.predicate, item.subject, item.object_token) in retracted_facts:
                continue
            meta = self.store.assertion_meta(item.assertion_id)
            if str(meta.get("factpy_op", "add")).lower() == "remove":
                continue
            if meta.get("active") is False:
                continue
            active_ids.add(item.assertion_id)
        return active_ids

    def _default_chosen(self, predicate: str, claims: list[AssertionClaim]) -> set[str]:
        semantics = self._predicate_semantics.get(predicate, PredicateSemantics("unknown", 0))
        if semantics.cardinality == "multi":
            return {item.assertion_id for item in claims}

        if semantics.cardinality == "unknown":
            return {item.assertion_id for item in claims}

        by_group: dict[tuple[object, ...], list[AssertionClaim]] = defaultdict(list)
        for item in claims:
            decoded = CanonicalTupleCodec.decode(item.object_token)
            dims = tuple(decoded[idx].value for idx in range(semantics.fact_key_arity))
            group = (item.predicate, item.subject, *dims)
            by_group[group].append(item)

        chosen: set[str] = set()
        for _, group_items in by_group.items():
            winner = max(
                group_items,
                key=lambda item: (self.store.assertion_order(item.assertion_id), item.assertion_id),
            )
            chosen.add(winner.assertion_id)
        return chosen

    def _require_mapping(
        self,
        owner_entity: str,
        field_name: str,
        *,
        temporal_mode: str,
    ) -> FieldMappingIR:
        mapping = self._mapping_index.get((owner_entity, field_name))
        if mapping is None:
            raise FactPyCompileError(f"Unknown mapping: {owner_entity}.{field_name}")
        if mapping.cardinality != "temporal" or mapping.temporal_mode != temporal_mode:
            raise FactPyCompileError(
                f"{owner_entity}.{field_name} is not temporal mode '{temporal_mode}'."
            )
        return mapping

    def _new_assertion_entity_ref(self, *, mapping: FieldMappingIR, owner_ref: str, payload: object) -> str:
        token = {
            "mapping": f"{mapping.owner_entity}.{mapping.field_name}",
            "owner": owner_ref,
            "payload": str(payload),
            "nonce": str(uuid.uuid4()),
        }
        return _stable_hash(token)

    def _normalize_subject(self, value: object) -> str:
        if isinstance(value, EntityRef):
            return value.ref
        if isinstance(value, Entity):
            return _stable_entity_ref(value)
        if isinstance(value, str):
            return value
        raise FactPyCompileError(f"Subject must be EntityRef/Entity/string, got {type(value).__name__}")

    def _normalize_value_and_dims(self, item: object) -> tuple[object, dict[str, object]]:
        if isinstance(item, dict) and "value" in item:
            return item["value"], dict(item.get("dims") or {})
        return item, {}

    def _as_valid_time(self, item: object, mapping: FieldMappingIR) -> ValidTimeValue:
        if isinstance(item, ValidTimeValue):
            return item
        if isinstance(item, dict) and "value" in item and "start" in item:
            return ValidTimeValue(
                value=item["value"],
                start=item["start"],
                end=item.get("end"),
                dims=dict(item.get("dims") or {}),
            )
        raise FactPyCompileError(
            f"{mapping.owner_entity}.{mapping.field_name} expects ValidTimeValue payload."
        )

    def _as_versioned(self, item: object, mapping: FieldMappingIR) -> VersionedValue:
        if isinstance(item, VersionedValue):
            return item
        if isinstance(item, dict) and "value" in item and "version" in item:
            return VersionedValue(
                value=item["value"],
                version=item["version"],
                dims=dict(item.get("dims") or {}),
            )
        raise FactPyCompileError(
            f"{mapping.owner_entity}.{mapping.field_name} expects VersionedValue payload."
        )

    def _validate_dims(
        self,
        expected_dims: list[str],
        dims: dict[str, object],
        mapping: FieldMappingIR,
    ) -> None:
        required = set(expected_dims)
        provided = set(dims.keys())
        missing = sorted(required - provided)
        extra = sorted(provided - required)
        if missing or extra:
            raise FactPyCompileError(
                f"{mapping.owner_entity}.{mapping.field_name} dims mismatch: missing={missing}, extra={extra}"
            )

    def _literal_term(self, value: object) -> object:
        if isinstance(value, EntityRef):
            return EntityRefConst(value.ref)
        if isinstance(value, Entity):
            return EntityRefConst(_stable_entity_ref(value))
        return value


_BATCH_STATE: ContextVar[tuple["Store", dict[str, object], Literal["abort", "skip_record", "collect"], list[str]] | None] = ContextVar(
    "factpy_batch_state",
    default=None,
)


class BatchContext:
    def __init__(
        self,
        *,
        store: "Store",
        meta: dict[str, object] | None,
        on_error: Literal["abort", "skip_record", "collect"],
    ) -> None:
        self.store = store
        self.meta = dict(meta or {})
        self.on_error = on_error
        self.errors: list[str] = []
        self._token = None

    def __enter__(self) -> "BatchContext":
        self._token = _BATCH_STATE.set((self.store, self.meta, self.on_error, self.errors))
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._token is not None:
            _BATCH_STATE.reset(self._token)
        self._token = None


class Store:
    """Non-intrusive runtime store with schema registry and append-only assertion log."""

    def __init__(
        self,
        *,
        meta_schema: AssertionMetaSchema | None = None,
        assertion_id_mode: Literal["generated", "replayable"] = "generated",
        replayable_meta_keys: Iterable[str] | None = None,
    ) -> None:
        self.edb = EDBStore()
        self.meta_schema = meta_schema or AssertionMetaSchema.default(strict=True)
        self.assertion_id_mode = assertion_id_mode
        self.replayable_meta_keys = tuple(replayable_meta_keys or ())

        self._entity_registry: dict[str, type[Entity]] = {}
        self.schema_ir: SchemaIR | None = None
        self._compiler: FactCompiler | None = None
        self.rules: list[object] = []

    def save(self, entity: Entity, *, meta: dict[str, object] | None = None) -> list[str]:
        batch_state = _BATCH_STATE.get()
        if batch_state is not None and batch_state[0] is self:
            batch_meta, on_error, errors = batch_state[1], batch_state[2], batch_state[3]
        else:
            batch_meta, on_error, errors = {}, "abort", []

        try:
            compiler = self._ensure_compiler(entity.__class__)
            entity_ref = EntityRef(entity.__class__.__name__, _stable_entity_ref(entity))

            pending = entity._pending_operations()
            if not pending:
                return []

            assertion_ids: list[str] = []
            for field_name, operation, payload, field_meta in pending:
                assertion_ids.extend(
                    compiler.apply_field_operation(
                        owner_entity=entity.__class__.__name__,
                        entity_ref=entity_ref,
                        field_name=field_name,
                        operation=operation,
                        payload=payload,
                        field_meta=field_meta,
                        batch_meta=batch_meta,
                        save_meta=meta,
                    )
                )

            entity._clear_pending_operations()
            return assertion_ids
        except Exception as exc:
            if on_error == "abort":
                raise
            if on_error in {"skip_record", "collect"}:
                errors.append(str(exc))
                return []
            raise

    def emit(
        self,
        *,
        pred: str,
        s: object,
        o: object,
        meta: dict[str, object] | None = None,
    ) -> str:
        compiler = self._compiler or self._new_compiler_for_emit()
        self._compiler = compiler
        return compiler.emit(pred=pred, s=s, o=o, meta=meta)

    def view(self, predicate: str) -> set[tuple[object, ...]]:
        compiler = self._compiler
        if compiler is None:
            return set()
        return compiler.view_rows(predicate)

    def register_rule(self, rule: object) -> None:
        self.rules.append(rule)

    def active_assertions(self, predicate: str | None = None) -> set[str]:
        if self._compiler is None:
            return set()
        return self._compiler.active_assertions(predicate)

    def chosen_assertions(
        self,
        predicate: str | None = None,
        *,
        active_ids: set[str] | None = None,
    ) -> set[str]:
        if self._compiler is None:
            return set()
        return self._compiler.chosen_assertions(predicate, active_ids=active_ids)

    def current_valid_time(
        self,
        *,
        owner_entity: str,
        field_name: str,
        entity_ref: str,
        now: object,
        dims: dict[str, object] | None = None,
    ) -> set[str]:
        if self._compiler is None:
            return set()
        return self._compiler.current_valid_time(
            owner_entity=owner_entity,
            field_name=field_name,
            entity_ref=entity_ref,
            now=now,
            dims=dims,
        )

    def current_versioned(
        self,
        *,
        owner_entity: str,
        field_name: str,
        entity_ref: str,
        dims: dict[str, object] | None = None,
    ) -> set[str]:
        if self._compiler is None:
            return set()
        return self._compiler.current_versioned(
            owner_entity=owner_entity,
            field_name=field_name,
            entity_ref=entity_ref,
            dims=dims,
        )

    def facts(self) -> dict[str, set[tuple[object, ...]]]:
        return self.edb.all_facts()

    def _ensure_compiler(self, entity_cls: type[Entity]) -> FactCompiler:
        entity_name = entity_cls.__name__
        if entity_name not in self._entity_registry:
            self._entity_registry[entity_name] = entity_cls
            schema = SchemaCompiler().compile(self._entity_registry.values())
            self.schema_ir = schema
            self._compiler = FactCompiler(
                schema,
                store=self.edb,
                meta_schema=self.meta_schema,
                assertion_id_mode=self.assertion_id_mode,
                replayable_meta_keys=self.replayable_meta_keys,
            )
        if self._compiler is None:
            assert self.schema_ir is not None
            self._compiler = FactCompiler(
                self.schema_ir,
                store=self.edb,
                meta_schema=self.meta_schema,
                assertion_id_mode=self.assertion_id_mode,
                replayable_meta_keys=self.replayable_meta_keys,
            )
        return self._compiler

    def _new_compiler_for_emit(self) -> FactCompiler:
        empty_schema = SchemaIR(entities=tuple(), predicates=tuple(), field_mappings=tuple())
        return FactCompiler(
            empty_schema,
            store=self.edb,
            meta_schema=self.meta_schema,
            assertion_id_mode=self.assertion_id_mode,
            replayable_meta_keys=self.replayable_meta_keys,
        )


def batch(
    *,
    store: Store,
    meta: dict[str, object] | None = None,
    on_error: Literal["abort", "skip_record", "collect"] = "abort",
) -> BatchContext:
    if on_error not in {"abort", "skip_record", "collect"}:
        raise FactPyCompileError("batch(on_error=...) must be abort|skip_record|collect")
    return BatchContext(store=store, meta=meta, on_error=on_error)


def get_active_store() -> Store | None:
    state = _BATCH_STATE.get()
    if state is None:
        return None
    return state[0]
