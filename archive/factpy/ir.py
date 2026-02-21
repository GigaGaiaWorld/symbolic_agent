"""FactPy target-agnostic IR structures."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


Cardinality = Literal["functional", "multi", "temporal"]
TemporalMode = Literal["valid_time", "versioned"]
ArgDomain = Literal["entity", "value", "typed_tuple_v1"]
MappingKind = Literal["single_valued", "multi_valued"]
ConstTypeTag = Literal[
    "string",
    "int",
    "float",
    "bool",
    "entity_ref",
    "typed_tuple_v1",
    "date",
    "datetime",
    "null",
]


@dataclass(frozen=True)
class IdentitySpecIR:
    name: str
    annotation: str
    has_default_factory: bool


@dataclass(frozen=True)
class FieldSpecIR:
    owner_entity: str
    field_name: str
    predicate_name: str
    annotation: str
    cardinality: Cardinality
    fact_key_dims: tuple[str, ...] = field(default_factory=tuple)
    temporal_mode: TemporalMode | None = None
    aliases: tuple[str, ...] = field(default_factory=tuple)
    description: str | None = None
    subject_position: int = 0


@dataclass(frozen=True)
class EntitySchemaIR:
    name: str
    doc: str
    identity_fields: tuple[IdentitySpecIR, ...]
    field_specs: tuple[FieldSpecIR, ...]
    metadata: dict[str, object]


@dataclass(frozen=True)
class PredicateIR:
    name: str
    arity: int
    key_arity: int
    value_index: int
    source_entity: str
    source_field: str
    kind: Literal["field", "temporal_role"]
    temporal_mode: TemporalMode | None = None
    subject_position: int = 0


@dataclass(frozen=True)
class TemporalMappingIR:
    owner_entity: str
    field_name: str
    temporal_mode: TemporalMode
    assertion_entity: str
    owner_pred: str
    value_pred: str
    dim_preds: tuple[tuple[str, str], ...]
    start_pred: str | None = None
    end_pred: str | None = None
    version_pred: str | None = None
    current_pred: str | None = None


@dataclass(frozen=True)
class FieldMappingIR:
    owner_entity: str
    field_name: str
    base_predicate: str
    cardinality: Cardinality
    fact_key_dims: tuple[str, ...]
    temporal_mode: TemporalMode | None
    temporal: TemporalMappingIR | None
    subject_position: int = 0


@dataclass(frozen=True)
class TieBreakSpec:
    """Schema-level tie-break hint for mapping predicates."""

    mode: str
    source_priority: tuple[str, ...] = field(default_factory=tuple)
    confidence_key: str = "confidence"
    time_key: str = "ingested_at"
    stable_tie_break: tuple[str, ...] = ("assertion_id",)


@dataclass(frozen=True)
class PredicateRuleSpecIR:
    """Schema-backed predicate signature used by RuleCompiler and policy generation."""

    base_predicate: str
    view_predicate: str
    logical_arity: int
    arg_domains: tuple[ArgDomain, ...]
    subject_position: int
    group_key_indexes: tuple[int, ...]
    cardinality: Cardinality
    owner_entity: str
    owner_field: str
    is_mapping: bool = False
    mapping_kind: MappingKind | None = None
    mapping_key_positions: tuple[int, ...] = field(default_factory=tuple)
    mapping_value_positions: tuple[int, ...] = field(default_factory=tuple)
    tie_break: TieBreakSpec | None = None


@dataclass(frozen=True)
class Var:
    name: str


@dataclass(frozen=True)
class Const:
    value: Any
    type_tag: ConstTypeTag


RuleTerm = Var | Const


@dataclass(frozen=True)
class Atom:
    predicate: str
    args: tuple[RuleTerm, ...]


@dataclass(frozen=True)
class Builtin:
    op: Literal["eq", "neq", "lt", "le", "gt", "ge"]
    left: RuleTerm
    right: RuleTerm


RuleBodyItem = Atom | Builtin


@dataclass(frozen=True)
class Clause:
    head: Atom
    body: tuple[RuleBodyItem, ...]


@dataclass(frozen=True)
class TupleEncodingIR:
    """Encoding policy for claim object tuples."""

    name: Literal["typed_tuple_v1"] = "typed_tuple_v1"
    deterministic: bool = True
    reversible: bool = True


@dataclass(frozen=True)
class SchemaIR:
    entities: tuple[EntitySchemaIR, ...]
    predicates: tuple[PredicateIR, ...]
    field_mappings: tuple[FieldMappingIR, ...]
    rule_predicates: tuple[PredicateRuleSpecIR, ...] = field(default_factory=tuple)
    claim_predicate: str = "claim"
    term_encoding: TupleEncodingIR = field(default_factory=TupleEncodingIR)
