# symir

Lightweight, IR-first logic representation and mapping. ProbLog is a plugin mapper.

## Installation

```bash
pip install .
```

For editable installs during development:

```bash
pip install -e .
```

## Components

- `ir/`: Core IR types and predicate schemas (`Fact`/`Rel`/`FactLayer`), plus `Instance`.
- `fact_store/`: CSV-backed fact loading (preferred: `CSVProvider`).
- `rules/`: Rule concepts, registry persistence, and constraint schemas (Pydantic + JSON schema).
- `mappers/`: Target language mapping (ProbLog implemented).
- `rule_ir.py`: Unified entrypoint for rule schema/IR (rules, filters, providers, renderers).
- `tests/`: Minimal tests for CSV loading, schema generation, and mapping.

## Fact/Rel Schema (preferred)

`Fact` and `Rel` are the canonical schema layer:

- `Fact`: entity-like predicate, arbitrary arity.
- `Rel`: directed edge-like predicate, fixed `sub`/`obj` endpoints plus relation `props`.
- Neo4j-oriented mapping convention: node label = `Fact.name`; `namespace` is a logical domain tag, not the node label.

### ArgSpec Syntax

```python
from symir.ir.fact_schema import ArgSpec

# Constructor contract
# ArgSpec(
#     spec: str,
#     namespace: str | None = None,
#     role: str | None = None,
#     name: str | None = None,
# )

# Sugar: "Name:type" -> name="Name", datatype="type"
city_name = ArgSpec("City:string", namespace="geo", role="key")

# Type-only form: "type" -> datatype="type", name auto-filled later
age = ArgSpec("int", namespace="person")

# Explicit override (equivalent to "Country:string")
country = ArgSpec(spec="string", name="Country", namespace="geo")
```

Rules:

- `spec` supports `"Name:type"` and `"type"`.
- When both `spec` and `name` provide a name, they must match.
- `ArgSpec("Name:string", name="Other")` raises `SchemaError`.
- `arg_name` (`name`) is auto-filled when missing.
- key-like roles (`key|id|name`) default to `Name`; other roles default to `Param`.
- duplicate auto names are suffixed (`Name2`, `Param2`, ...).
- Internal field is `name`; compatibility accessor `arg_name` is still available.
- `to_dict()` emits `arg_name`, not `name`.

Serialization and compatibility:

```python
# New-style payload
ArgSpec.from_dict({"datatype": "string", "arg_name": "Name", "role": "key"})

# Also accepted: `name` as alias for `arg_name`
ArgSpec.from_dict({"datatype": "string", "name": "Name"})

# Also accepted: `spec` as fallback when `datatype` is missing
ArgSpec.from_dict({"spec": "Country:string", "namespace": "geo"})
```

### Define Facts and Relations

```python
from symir.ir.fact_schema import ArgSpec, Fact, Rel, FactLayer

person = Fact(
    "person",
    [
        ArgSpec("Name:string", namespace="person", role="key"),
        ArgSpec("Address:string", namespace="address"),
        ArgSpec("Age:int", namespace="person"),
    ],
    description="Person entity",
    merge_policy="latest",
)

company = Fact(
    "company",
    [
        ArgSpec("Company:string", namespace="org", role="key"),
        ArgSpec("Worth:float", namespace="org"),
    ],
    description="Company entity",
)

employment = Rel(
    "employment",
    sub=person,
    obj=company,
    props=[
        ArgSpec("Since:int", namespace="time"),
        ArgSpec("Title:string", namespace="org"),
    ],
    description="person works at company",
    merge_policy="keep_all",
)

schema = FactLayer([person, company, employment])
```

Design details:

- `Fact.key_fields` are derived with fallback order: `key > id > name > pos0` (signature order preserved).
- `Rel` inherits endpoint keys from `sub`/`obj` by default and builds relation signature as `sub_<key> + obj_<key> + props`.
- `Rel.arity = len(sub_key_fields) + len(obj_key_fields) + len(props)`.
- `schema_id` is stable hash over canonical schema fields.
- `Fact` hash input: `kind + name + signature + key_fields`.
- `Rel` hash input: `kind + name + sub_schema_id + obj_schema_id + endpoints + props`.
- `merge_policy` is schema-level (`Fact`/`Rel`), never stored in `meta`.
- Allowed `merge_policy`: `max | latest | noisy_or | overwrite | keep_all`.
- invalid `merge_policy` values raise `SchemaError` at schema construction.
- current runtime does not execute merge algorithms yet; `merge_policy` is declarative except for `keep_all` record identity behavior (see Instances section).

### Canonical Schema Payload (`to_dict` / `from_dict`)

`Fact` and `Rel` are authored with object references, but persisted as stable payloads.
For `Rel`, object references are frozen to `sub_schema_id` / `obj_schema_id`.

`Fact` payload shape:

```json
{
  "name": "person",
  "arity": 3,
  "schema_id": "<sha256>",
  "description": "Person entity",
  "kind": "fact",
  "merge_policy": "latest",
  "signature": [
    {"datatype": "string", "role": "key", "namespace": "person", "arg_name": "Name"},
    {"datatype": "string", "role": null, "namespace": "address", "arg_name": "Address"},
    {"datatype": "int", "role": null, "namespace": "person", "arg_name": "Age"}
  ],
  "key_fields": ["Name"]
}
```

`Rel` payload shape:

```json
{
  "name": "employment",
  "arity": 4,
  "schema_id": "<sha256>",
  "description": "person works at company",
  "kind": "rel",
  "merge_policy": "keep_all",
  "sub_schema_id": "<person_schema_id>",
  "obj_schema_id": "<company_schema_id>",
  "endpoints": {
    "sub_key_fields": ["Name"],
    "obj_key_fields": ["Company"]
  },
  "props": [
    {"datatype": "int", "role": null, "namespace": "time", "arg_name": "Since"},
    {"datatype": "string", "role": null, "namespace": "org", "arg_name": "Title"}
  ],
  "derived_signature": {
    "derived": true,
    "sub_args": [
      {"arg_name": "Sub", "datatype": "Fact"},
      {"datatype": "string", "role": "sub_key", "namespace": "person", "arg_name": "sub_Name"},
      {"datatype": "string", "role": "sub_attr", "namespace": "address", "arg_name": "sub_Address"},
      {"datatype": "int", "role": "sub_attr", "namespace": "person", "arg_name": "sub_Age"}
    ],
    "obj_args": [
      {"arg_name": "Obj", "datatype": "Fact"},
      {"datatype": "string", "role": "obj_key", "namespace": "org", "arg_name": "obj_Company"},
      {"datatype": "float", "role": "obj_attr", "namespace": "org", "arg_name": "obj_Worth"}
    ],
    "prop_args": [
      {"datatype": "int", "role": "prop", "namespace": "time", "arg_name": "Since"},
      {"datatype": "string", "role": "prop", "namespace": "org", "arg_name": "Title"}
    ]
  }
}
```

Notes:

- `Rel.to_dict()` intentionally omits `signature` and exports `derived_signature` for flat/LLM-friendly shape.
- `FactLayer.from_dict(payload)` validates provided `schema_id` values against recomputed hashes and raises on mismatch.

### Create Rel Schema From Persisted Facts

If fact schemas are already persisted, load them first, then construct new `Rel` from loaded fact objects.

Why this is required:

- `Rel(...)` computes `endpoints`, `signature`, and `schema_id` from concrete `sub`/`obj` fact schemas.
- current API does not support constructing `Rel` from only `sub_schema_id` / `obj_schema_id`.
- this schema-first flow is the standard graph modeling pattern: define node schemas first, then edge schemas.

Case A: persisted full `FactLayer` payload (recommended):

```python
import json
from symir.ir.fact_schema import FactLayer, ArgSpec, Rel

payload = json.load(open("schema.json", "r", encoding="utf-8"))
registry = FactLayer.from_dict(payload)

person = registry.fact("person")
company = registry.fact("company")

works_at = Rel(
    "works_at",
    sub=person,
    obj=company,
    props=[ArgSpec("Since:int"), ArgSpec("Title:string")],
)

registry = FactLayer([*registry.predicates(), works_at])
```

Case B: only persisted fact predicate dicts:

```python
import json
from symir.ir.fact_schema import PredicateSchema, FactLayer, ArgSpec, Rel

items = json.load(open("facts_only.json", "r", encoding="utf-8"))
fact_preds = [
    PredicateSchema.from_dict(item)
    for item in items
    if str(item.get("kind") or "fact") == "fact"
]

registry = FactLayer(fact_preds)
works_at = Rel("works_at", sub=registry.fact("person"), obj=registry.fact("company"), props=[ArgSpec("Since:int")])
registry = FactLayer([*registry.predicates(), works_at])
```

Case C: load cached predicate schemas and build new rel:

```python
from symir.ir.fact_schema import load_predicate_schemas_from_cache, FactLayer, ArgSpec, Rel

cached = [pred for pred in load_predicate_schemas_from_cache() if pred.kind == "fact"]
registry = FactLayer(cached)

works_at = Rel("works_at", sub=registry.fact("person"), obj=registry.fact("company"), props=[ArgSpec("Since:int")])
registry = FactLayer([*registry.predicates(), works_at])
```

If you only persisted schema IDs, resolve them through a registry first:

```python
person = registry.get(person_schema_id)
company = registry.get(company_schema_id)
if person.kind != "fact" or company.kind != "fact":
    raise ValueError("Rel endpoints must resolve to fact schemas.")
```

### LLM Compatibility (Schema Generation)

`Fact`/`Rel` are suitable LLM targets, but only after normalization and validation.

Hard requirement:

- LLM output must be pure data payloads.
- LLM output must not contain Python object references like `sub=person` or `obj=city`.

Why:

- Runtime object references are authoring convenience only.
- Persistence and replay require canonical identifiers (`schema_id`, endpoint keys).

Two practical contracts:

1. Canonical contract (directly consumable by `FactLayer.from_dict`):
- facts + rels in one payload,
- rel entries must contain `sub_schema_id` and `obj_schema_id`,
- optional `schema_id` values are revalidated.

2. Draft contract (LLM-friendly, name-based; requires conversion):
- rel entries use `sub_name` / `obj_name`,
- application converts names to object references and constructs `Rel`.

Draft example (LLM output):

```json
{
  "predicates": [
    {
      "kind": "fact",
      "name": "person",
      "signature": [
        {"arg_name": "Name", "datatype": "string", "role": "key"},
        {"arg_name": "Address", "datatype": "string"}
      ]
    },
    {
      "kind": "fact",
      "name": "city",
      "signature": [
        {"arg_name": "Name", "datatype": "string", "role": "key"},
        {"arg_name": "Country", "datatype": "string"}
      ]
    },
    {
      "kind": "rel",
      "name": "lives_in",
      "sub_name": "person",
      "obj_name": "city",
      "props": [{"arg_name": "Since", "datatype": "int"}]
    }
  ]
}
```

Draft-to-registry conversion:

```python
from symir.ir.fact_schema import ArgSpec, Fact, Rel, FactLayer

def load_llm_draft(draft: dict) -> FactLayer:
    def parse_args(raw):
        return [ArgSpec.from_dict(item) for item in raw]

    facts = []
    by_name = {}
    for item in draft["predicates"]:
        if item.get("kind") != "fact":
            continue
        fact = Fact(
            name=item["name"],
            args=parse_args(item["signature"]),
            description=item.get("description"),
            key_fields=item.get("key_fields"),
            merge_policy=item.get("merge_policy"),
        )
        facts.append(fact)
        by_name[fact.name] = fact

    rels = []
    for item in draft["predicates"]:
        if item.get("kind") != "rel":
            continue
        sub = by_name[item["sub_name"]]
        obj = by_name[item["obj_name"]]
        rel = Rel(
            name=item["name"],
            sub=sub,
            obj=obj,
            props=parse_args(item.get("props", [])),
            description=item.get("description"),
            endpoints=item.get("endpoints"),
            merge_policy=item.get("merge_policy"),
        )
        rels.append(rel)

    return FactLayer([*facts, *rels])
```

Recommended ingest pipeline:

- generate draft with LLM,
- normalize (`arg_name`, roles, naming consistency),
- construct `Fact`/`Rel`,
- serialize via `to_dict()` for stable canonical storage.

## FactLayer Registry APIs

`FactLayer` is the user-facing alias of `FactSchema`. Under eager-freeze (`Fact`/`Rel` finalized at construction), registry responsibilities are:

- indexing (`schema_id`, name, rel triplet),
- validation (duplicate IDs/names, rel endpoint integrity),
- serialization/deserialization (`to_dict` / `from_dict`).

### Construct and Query

```python
registry = FactLayer([person, company, employment])

# Core lookups
all_predicates = registry.predicates()
only_facts = registry.facts()
only_rels = registry.rels()

person_schema = registry.fact("person")
employment_schema = registry.rel("employment")
person_id = registry.resolve("fact", "person")

# Low-level authority
same_person = registry.get(person_id)

# Precise rel lookup by (name, sub_id, obj_id)
employment_exact = registry.rel_of_ids(
    "employment",
    person.schema_id,
    company.schema_id,
)
```

### Serialization Contract

```python
payload = registry.to_dict()
# shape: {"version": 1, "predicates": [...]}

loaded = FactLayer.from_dict(payload)
```

Guarantees:

- `to_dict()` output is stable-sorted by `(kind, name, schema_id)` for diff-friendly snapshots.
- `from_dict()` treats missing `version` as current version.
- `from_dict()` rejects unsupported versions.
- If a predicate entry includes `schema_id`, it must match recomputed hash.

### Names and Debug Description

```python
name_map = registry.names()
# {"facts": [...], "rels": [...]}

person_desc = registry.describe(person)
# or registry.describe(person.schema_id)
```

`describe(...)` returns concise schema metadata for debugging (`kind`, `name`, `arity`, plus `key_fields/signature` or `endpoints/props`).

### FactView (Subset Registry)

`FactView` mirrors the registry read APIs on a subset of allowed predicates.

```python
view = registry.view([person, employment])  # pass schema objects (schema_ids still accepted for compatibility)

view.allows(person)
view.fact("person")
view.rel("employment")
view.resolve("fact", "person")
view.names()
view.describe(person)
```

Design note:

- Use `FactLayer` as authoritative registry.
- Use `FactView` as allowlist-constrained registry when passing subsets to providers/validators/renderers.

## Instances

`Instance` is the canonical data record type for both fact and rel runtime data.

Design principles:

- one row = one `Instance` (no collection-valued instance),
- single class for both kinds (`fact` / `rel`),
- constructor uses schema object (`Fact`/`Rel`) for strict parsing and validation,
- serialized output stores only identifiers (`schema_id`, entity IDs), not schema object references.
- rel instances are intentionally lightweight: endpoint IDs + relation props, not full endpoint snapshots.

Constructor contract:

```python
from symir.ir.instance import Instance

# schema: PredicateSchema (Fact/Rel)
# terms: list | tuple | dict
# strict=True by default
# resolve_mode: "strict" | "heuristic" (default "strict")
inst = Instance(schema=person, terms=["alice", "darmstadt", 28], prob=0.92)
```

### Fact Terms

```python
# 1) Positional (mapped by Fact.signature order)
alice = Instance(schema=person, terms=["alice", "darmstadt", 28])

# 2) Dict (keys must match signature names in strict mode)
alice2 = Instance(
    schema=person,
    terms={"Name": "alice", "Address": "darmstadt", "Age": 28},
)
```

Fact validation/canonicalization:

- all `key_fields` must be present,
- `entity_id` is derived from `schema_id + ordered key pairs` (ordered by `key_fields`):

```python
entity_id = SHA256(
    schema_id + canonical_json([("Name", "alice")])
)
```

- only `key_fields` affect `entity_id`; non-key props do not.

Engineering note on `key_fields`:

- `key_fields` are identity keys, but uniqueness quality depends on your data/domain.
- strong pattern: stable external IDs (for example `PersonID`, `CompanyID`).
- acceptable fallback: composite business keys (`Name + Address`, etc.).
- high-collision domains should prefer `merge_policy="keep_all"` first, then resolve entities later.

### Rel Terms

Rel supports list/tuple and dict inputs. Endpoint resolution always targets schema endpoint keys.

```python
from symir.ir.fact_schema import InstanceRef

openai = Instance(schema=company, terms=["openai", 10.0])

# A) list/tuple form: [sub_endpoint, obj_endpoint, ...props]
rel_a = Instance(schema=employment, terms=[alice, openai, 2020, "researcher"])

# B) dict form with endpoint refs
rel_b = Instance(
    schema=employment,
    terms={
        "sub_ref": alice,
        "obj_ref": openai,
        "props": {"Since": 2020, "Title": "researcher"},
    },
)

# C) dict form with explicit endpoint keys
rel_c = Instance(
    schema=employment,
    terms={
        "sub_key": {"Name": "alice"},
        "obj_key": {"Company": "openai"},
        "props": {"Since": 2020, "Title": "researcher"},
    },
)

# D) sub_key/obj_key + inline props (also supported)
rel_d = Instance(
    schema=employment,
    terms={
        "sub_key": {"Name": "alice"},
        "obj_key": {"Company": "openai"},
        "Since": 2020,
        "Title": "researcher",
    },
)

# E) dict flattened form (CSV-friendly)
rel_e = Instance(
    schema=employment,
    terms={
        "sub_Name": "alice",
        "obj_Company": "openai",
        "Since": 2020,
        "Title": "researcher",
    },
)

# F) endpoint InstanceRef
alice_ref = InstanceRef(schema_id=person.schema_id, key_values={"Name": "alice"})
openai_ref = InstanceRef(schema_id=company.schema_id, key_values={"Company": "openai"})
rel_f = Instance(schema=employment, terms=[alice_ref, openai_ref, 2020, "researcher"])
```

Endpoint key completeness example (composite keys):

```python
from symir.ir.fact_schema import ArgSpec, Fact, Rel

person2 = Fact(
    "person2",
    [ArgSpec("Name:string", role="key"), ArgSpec("Address:string", role="key")],
)
company2 = Fact("company2", [ArgSpec("Company:string", role="key")])
employment2 = Rel("employment2", sub=person2, obj=company2, props=[ArgSpec("Since:int")])

# OK: both Name and Address are provided for sub endpoint.
ok = Instance(
    schema=employment2,
    terms={
        "sub_key": {"Name": "alice", "Address": "darmstadt"},
        "obj_key": {"Company": "openai"},
        "props": {"Since": 2020},
    },
)

# Error: missing Address in sub endpoint key.
bad = Instance(
    schema=employment2,
    terms={
        "sub_key": {"Name": "alice"},
        "obj_key": {"Company": "openai"},
        "props": {"Since": 2020},
    },
)
```

Notes for dict terms:

- `sub_key` / `obj_key` must provide all endpoint key fields from `Rel.endpoints`.
- `sub_key` / `obj_key` do not contain rel props.
- rel props can be provided either in top-level fields or under `props`.
- if both inline and `props` are used, overlapping prop names raise `SchemaError`.

Support matrix (strict mode):

- `sub_ref` / `obj_ref`: accepts fact `Instance`, `InstanceRef`, or key dict.
- `sub_key` / `obj_key`: accepts key dict only.
- `sub_key` / `obj_key` with direct `Instance` values are not accepted.
- list/tuple endpoints: accepts fact `Instance`, `InstanceRef`, or key dict.

Common error diagnostics:

- `SchemaError: Unknown rel props: [...]`
- prop names in `terms` do not match `Rel.props` arg names.
- check with `[p.arg_name for p in employment.props]`.
- `SchemaError: Missing rel props: [...]`
- strict mode requires all props in `Rel.props`.
- `SchemaError: Rel props duplicated in inline/props: [...]`
- same prop appears both at top level and in `terms["props"]`.
- `SchemaError: Rel dict terms require sub_key and obj_key dicts.`
- one or both endpoint keys were not dict objects in `sub_key` / `obj_key` mode.
- `SchemaError: Rel endpoint missing key fields: [...]`
- endpoint dict/ref did not provide every required endpoint key field.

Rel validation/canonicalization:

- list/tuple form requires `len(terms) >= 2`,
- endpoint values must resolve to all required endpoint key fields,
- rel props must match `Rel.props` names in strict mode,
- `sub_entity_id` and `obj_entity_id` are derived from endpoint schema IDs + key values.
- rel has no standalone `rel_id` field in canonical output.
- same endpoints + different rel props keep the same `sub_entity_id` / `obj_entity_id` by design.

### Rel Readability

`rel` feels "lagged" compared to `fact` by design: it stores relation props and endpoint IDs, then joins endpoint facts when you need full endpoint attributes.

```python
light = rel_b.to_dict()
# {
#   "schema_id": ...,
#   "kind": "rel",
#   "props": {"Since": 2020, "Title": "researcher"},
#   "sub_entity_id": ...,
#   "obj_entity_id": ...,
#   ...
# }

readable = rel_b.to_dict(include_keys=True)
# adds:
#   "sub_key": {"Name": "alice"}
#   "obj_key": {"Company": "openai"}
```

Notes:

- `include_keys=False` (default) is the canonical lightweight export.
- `include_keys=True` is for human-readable export/debug.
- `include_keys=True` requires endpoint key props to be present in memory.
- instances restored with `Instance.from_dict(...)` currently do not recover endpoint key props.
- calling `to_dict(include_keys=True)` on such restored rel instances raises `SchemaError: Rel instance missing endpoint key props; construct with schema/terms or use include_keys=False.`
- one rel `Instance` is always one record; fan-out appears later only when endpoint lookup returns multiple fact versions for the same `entity_id` (for example under `merge_policy="keep_all"`).

Heuristic endpoint mode:

- `resolve_mode="heuristic"` only helps when an endpoint has exactly one key field.
- recommended default remains `resolve_mode="strict"` for deterministic parsing.

### Meta, Prob, and Record IDs

Meta is strict. Allowed keys:

- `source`, `observed_at`, `ingested_at`, `evidence_id`, `trace_id`
- `confidence` (`0.0..1.0`)
- `status` (`asserted|inferred|retracted`)
- `provenance` (dict), `tags` (list[str])

Unknown meta keys raise `SchemaError`.

Merge/identity notes:

- `prob` is per-instance score and is always optional.
- `merge_policy` is schema-level (`Fact`/`Rel`), not stored in `meta`.
- when schema `merge_policy == "keep_all"`, top-level `record_id` is generated.
- `record_id` is computed from `schema_id + primary_ids + evidence_id`.
- if `meta.evidence_id` is missing, fallback evidence hash uses `props`, `meta.source`, `meta.observed_at`, `meta.ingested_at`.
- for rel instances, `primary_ids = [sub_entity_id, obj_entity_id]`.

Current merge execution boundary:

- there is no built-in `merge_instances(...)` executor yet.
- `CSVProvider` and renderers do not merge duplicate instances.
- `max/latest/noisy_or/overwrite` are validated schema options, but no runtime reducer is applied by this package.
- only `keep_all` has concrete runtime effect today (`record_id` generation).

Recommended grouping keys if you implement external merging:

- fact group key: `(schema_id, entity_id)`
- rel group key: `(schema_id, sub_entity_id, obj_entity_id)`

Meta validation examples:

```python
# valid meta
Instance(
    schema=person,
    terms=["alice", "darmstadt", 28],
    meta={
        "source": "csv",
        "observed_at": "2026-02-11T10:00:00Z",
        "confidence": 0.92,
        "status": "asserted",
        "tags": ["demo", "import"],
    },
)

# invalid status (must be asserted|inferred|retracted)
Instance(schema=person, terms=["alice", "darmstadt", 28], meta={"status": "draft"})

# invalid unknown key (`merge_policy` belongs to Fact/Rel schema, not meta)
Instance(
    schema=person,
    terms=["alice", "darmstadt", 28],
    meta={"merge_policy": "latest"},
)
```

Both invalid examples raise `SchemaError`.

### Serialization and Replay

```python
data = rel_b.to_dict(include_keys=True)  # include endpoint keys for readability
loaded = Instance.from_dict(data, registry=schema)  # restores canonical instance fields
```

Canonical output fields:

- common: `schema_id`, `kind`, `props`, `prob`, `meta`
- fact: `entity_id` (+ `record_id` when keep_all)
- rel: `sub_entity_id`, `obj_entity_id` (+ `record_id` when keep_all)

## Load CSV Facts (FactLayer)

```python
from pathlib import Path
from symir.fact_store.provider import CSVProvider, CSVSource

sources = [
    CSVSource(schema=person, file="people.csv", columns=["Name", "address"]),
    CSVSource(schema=company, file="companies.csv", columns=["Company", "Worth"]),
    CSVSource(
        schema=employment,
        file="employment.csv",
        columns=["sub_Name", "obj_Company", "Since", "Title"],
    ),
]

provider = CSVProvider(
    schema=schema,
    base_path=Path("data"),
    sources=sources,
    datatype_cast="coerce",  # none | coerce | strict
)

view = schema.view([person, company, employment])
instances = provider.query(view)
```

Notes:

- `CSVSource.schema`: `Fact`/`Rel` object.
- `CSVSource.file`: CSV filename under `base_path`.
- `CSVSource.columns`: CSV headers mapped by signature order.
- `CSVSource.prob_column`: optional probability column per row.
- `CSVSource.schema` is object-only; do not pass raw `schema_id` as constructor input.
- `columns` are mapped by signature order, not by column/arg name matching.
- for rel predicates, column order must follow: `sub_*` endpoint keys, then `obj_*` endpoint keys, then relation props.
- one CSV row always becomes one canonical `Instance`.
- `CSVProvider` accepts either `FactLayer` or `FactView` as `schema`.
- if constructed with a `FactView`, `query()` can be called without arguments and uses that view by default.
- `CSVProvider` internally indexes sources by `source.schema.schema_id` (stable string key), not by schema object identity.
- if custom wrappers key dicts by schema objects, you may hit `TypeError: unhashable type: 'list'`; use `schema.schema_id` keys instead.

Rel CSV mapping details (important):

```python
from symir.ir.fact_schema import ArgSpec, Fact, Rel

person = Fact("person", [ArgSpec("Name:string", role="key")])
company = Fact("company", [ArgSpec("Company:string", role="key")])
works = Rel("works_at", sub=person, obj=company)  # no props

[arg.name for arg in works.signature]
# -> ["sub_Name", "obj_Company"]

# Column names are arbitrary; only order matters.
# person -> sub_Name, company -> obj_Company
source = CSVSource(schema=works, file="works_at.csv", columns=["person", "company"])
```

- If a rel has props, `columns` must include endpoint keys + all props.
- Example: `["sub_Name", "obj_Company", "Since", "Title"]` or any same-length aliases in the same order.
- If `len(columns) != rel.arity`, `CSVProvider` raises `ProviderError` with arity mismatch.
- `Rel.to_dict()["derived_signature"]` is a descriptive breakdown (includes endpoint attrs); CSV loading still uses `Rel.signature` and `Rel.arity`.

Composite endpoint key example:

```python
person = Fact(
    "person",
    [
        ArgSpec("Name:string", role="key"),
        ArgSpec("Address:string", role="key"),
    ],
)
company = Fact("company", [ArgSpec("Company:string", role="key")])
employment = Rel(
    "employment",
    sub=person,
    obj=company,
    props=[ArgSpec("Since:int")],
)

[arg.name for arg in employment.signature]
# -> ["sub_Name", "sub_Address", "obj_Company", "Since"]

CSVSource(
    schema=employment,
    file="employment.csv",
    columns=["person_name", "person_address", "company_name", "since"],
)
```

If you do not want to change a fact's own `key_fields`, override rel endpoint keys explicitly:

```python
employment = Rel(
    "employment",
    sub=person,
    obj=company,
    props=[ArgSpec("Since:int")],
    endpoints={"sub_key_fields": ["Name", "Address"], "obj_key_fields": ["Company"]},
)
```

`datatype_cast` behavior:

- `none`: keep values as strings.
- `coerce`: best-effort cast for `int`/`float`/`bool`.
- `strict`: cast must succeed; unknown datatype or invalid value raises `ProviderError`.
- bool accepted values: `true/false/1/0/yes/no` (case-insensitive).

Runtime query flow:

- resolve allowed predicates from `view` (or default/all schema),
- optional filter pass (`filt`) on predicates,
- enforce source mapping for each allowed schema,
- parse rows and instantiate `Instance(schema=pred_schema, terms=..., prob=...)`.

Missing source mapping error includes both schema ID and predicate name:

```text
Missing CSV source mapping for schema_id: <id> (name=<predicate_name>).
```

## Build Relations From Facts (RelBuilder)

Use `RelBuilder` when your relation CSV does not already contain `sub_*` / `obj_*` columns
and you need to match existing fact instances.
`CSVProvider.query(...)` does not run this join automatically; use `provider.build_relations(...)`.

```python
from symir.fact_store.rel_builder import RelBuilder
from symir.fact_store.provider import CSVProvider, CSVSource

facts = provider.query(schema.view([person, company]))

builder = RelBuilder(
    rel=employment,
    match_keys=["person", "company"],
    match_props=["Since"],
    key_mode="partial",
    multi="cartesian",
)

rel_source = CSVSource(schema=employment,
    file="employment.csv",
    columns=["person", "company", "Since", "Title"],
    prob_column="prob",
)

rels = provider.build_relations(
    builder=builder,
    facts=facts,
    source=rel_source,
    maps={"person": "person_name", "company": "company_name", "Since": "since", "Title": "title"},
)
```

Notes:

- `columns` are logical row keys consumed by `RelBuilder`.
- `maps` maps logical keys to actual CSV column names.
- `CSVSource` (`file/columns/prob_column`) is IO config only.
- `RelBuilder` (`match_keys/key_mode/match_props/multi`) is matching strategy.
- If `match_keys` is omitted, `RelBuilder` expects `sub_<Key>` / `obj_<Key>` columns by default.
- default `sub_<Key>` / `obj_<Key>` matching is case-sensitive (`sub_Name` != `sub_name`).
- `match_keys=["sub_col", "obj_col"]` is only valid when each endpoint has exactly one key field.
- list-form `match_keys` supports optional `"<row_key>:<key_field>"` spec for single-key endpoints.
- example: `match_keys=["person", "company:cname"]` (when obj single key field is `cname`) maps row key `company` to endpoint key field `cname`.
- For composite endpoint keys, use dict form:

```python
builder = RelBuilder(
    rel=employment,
    match_keys={
        "sub": {"Name": "person_name", "Address": "person_address"},
        "obj": {"Company": "company_name"},
    },
)
```

- in `key_mode="strict"`, every endpoint key field must be present in row mapping.
- in `key_mode="partial"`, missing key fields are allowed and used as partial filters.
- `match_props` affects relation dedup key only; it does not participate in fact endpoint matching.
- `match_props=None` disables builder dedup (all matched rows/pairs are emitted).
- `match_props=["Since"]` or `"all"` enables dedup using endpoints + selected props.
- `multi="error"` fails on multi-match; `multi="cartesian"` emits cartesian combinations.
- `build_relations(...)` flow: `read_rows(...)` loads logical-key rows, stores row probability under `__prob__`, then `RelBuilder` matches facts and creates rel `Instance` records.
- `datatype_cast` from `CSVProvider` is forwarded into `RelBuilder`.

`RelBuilder` is not CSV-only. You can build from in-memory rows directly:

```python
from symir.fact_store.rel_builder import RelBuilder, ROW_PROB_KEY

rows = [
    {"sub_Name": "alice", "obj_Company": "openai", "Since": 2020, "Title": "researcher", ROW_PROB_KEY: 0.9},
]

builder = RelBuilder(rel=employment, key_mode="strict", multi="error")
rels = builder.build(facts=facts, rows=rows, registry=schema, datatype_cast="coerce")
```

Common RelBuilder errors:

- `SchemaError: match_keys list requires single key per endpoint.`
- fix: use dict-form `match_keys` for composite endpoint keys.
- `SchemaError: Missing obj key fields in row: [...]`
- fix: provide missing key columns, switch to `key_mode="partial"`, or simplify endpoint `key_fields`.

## Legacy CSV Mapping (ir.schema)

If you are using the older mapping schema (separate from `Fact/Rel`), use `CsvFactStore`:

```python
from pathlib import Path
from symir.ir.schema import FactSchema
from symir.fact_store.csv_store import CsvFactStore

schema = {
    "nodes": {
        "Person": {"file": "people.csv", "column": "name"},
        "City": {"file": "cities.csv", "column": "name"},
    },
    "relations": {
        "LivesIn": {"file": "lives_in.csv", "columns": ["person", "city"]}
    },
}

store = CsvFactStore(schema=FactSchema.from_dict(schema), base_path=Path("/data"))
facts = store.load_facts()
```

## Rule Schema/IR (new)

This package provides a neutral, serializable rule IR for generation and rendering.

Notes:

- `Rule` head is a predicate schema object (`Fact` / `Rel` / `PredicateSchema`), no extra `Head` wrapper.
- body is `conditions: list[Cond]` (`Cond` is the clause/body unit; old `Body` naming is not used).
- probabilities are attached per condition (`Cond.prob`), not per rule.
- `Ref` is the canonical predicate-reference literal name.

Core syntax:

```python
from symir.rule_ir import (
    ArgSpec, Fact, Rel, FactLayer,
    Var, Const, Ref, Expr, Cond, Rule,
    Call, Unify, If,
)
```

- `Var("X")`: variable term (name only; no datatype argument).
- `Const(value)`: constant term.
- `Ref(schema=<predicate>, terms=[Var|Const, ...], negated=False)`: predicate literal.
- `Expr(expr=<ExprIR>)`: expression literal wrapper.
- `Rule(predicate=<Fact/Rel>, conditions=[Cond(...)])`.

`Ref` schema input contract:

```python
# A) Predicate schema object (preferred, immediate arity/type checks)
Ref(schema=person, terms=[Var("Name"), Const("alice")])

# B) Instance object (terms auto-expanded to all Const values)
from symir.ir.instance import Instance

alice = Instance(schema=person, terms=["alice", 30])
Ref(schema=alice)  # equivalent to terms=[Const("alice"), Const(30)]
```

- `Ref` constructor accepts `PredicateSchema` or `Instance` only.
- `Ref` constructor does not accept raw `schema_id` strings.
- when `schema` is an `Instance`, `terms` must be omitted.
- for rel instances, auto-expansion needs endpoint key props in memory (`to_terms(...)` must succeed).

Fact-head example:

```python
person = Fact("person", [ArgSpec("X:string")])
head = Fact("resident", [ArgSpec("X:string")])

cond = Cond(
    literals=[Ref(schema=person, terms=[Var("X")])],
    prob=0.7,
)
rule = Rule(predicate=head, conditions=[cond])
```

Rel-head example:

```python
from symir.rule_ir import (
    ArgSpec, Fact, Rel, FactLayer, Var, Const, Ref, Expr, Cond, Rule,
    Call, Unify,
    ProbLogRenderer, RenderContext
)

person = Fact("person", [ArgSpec("Name:string"), ArgSpec("Addr:string")])
company = Fact("company", [ArgSpec("Company:string")])
employment = Rel(
    "employment",
    sub=person,
    obj=company,
    props=[ArgSpec("Since:int"), ArgSpec("Title:string")],
)
schema = FactLayer([person, company, employment])

# renderer rel-head convention:
# [Sub, Obj, <prop vars...>]
# endpoint keys remain in body literals.
cond = Cond(
    literals=[
        # structured binding style:
        Expr(expr=Unify(Var("Sub"), Ref(person, [Var("sub_Name"), Var("sub_Addr")]))),
        Expr(expr=Unify(Var("Obj"), Ref(company, [Var("obj_Company")]))),
        # optional non-binding style:
        Ref(schema=person, terms=[Var("sub_Name"), Var("sub_Addr")]),
        Ref(schema=company, terms=[Var("obj_Company")]),
        
        Expr(expr=Unify(Var("Since"), Const(2020))),
        Expr(expr=Unify(Var("Title"), Const("researcher"))),
        # optional structured binding style:

    ],
    prob=0.7,
)
rule = Rule(predicate=employment, conditions=[cond])

text = ProbLogRenderer().render_rule(rule, RenderContext(schema=factlayer, problog_var_mode="sanitize"))
```

Rule conventions (data-template first):

- there is no rule-level `policy` field in `Rule` payload.
- rule payload is canonical predicate schema fields (`Fact`/`Rel`) plus `conditions`.
- convention logic is user-authored in `conditions`; renderer does not auto-insert hidden literals.
- `Rule.from_dict(rule.to_dict())` round-trips this canonical shape.

Rel naming convention (recommended):

- flattened style: use rel signature variable names directly (`sub_*`, `obj_*`, props).
- compound style: add `Sub`/`Obj` variables and explicit `Unify(...)` bindings.
- because `Rel.derived_signature` already prefixes endpoint args, you do not need custom renaming rules.

Flattened style example:

```python
cond = Cond(
    literals=[
        Ref(schema=person, terms=[Var("sub_Name"), Var("sub_Addr")]),
        Ref(schema=company, terms=[Var("obj_Company")]),
    ],
    prob=0.7,
)
```

Compound style example:

```python
cond = Cond(
    literals=[
        Ref(schema=person, terms=[Var("sub_Name"), Var("sub_Addr")]),
        Ref(schema=company, terms=[Var("obj_Company")]),
        Expr(expr=Unify(Var("Sub"), Call("person", [Var("sub_Name"), Var("sub_Addr")]))),
        Expr(expr=Unify(Var("Obj"), Call("company", [Var("obj_Company")]))),
    ],
    prob=0.7,
)
```

Design/validation notes:

- head argument variables come from `predicate.signature` arg names.
- for stable rendering, define head arg names explicitly (for example `ArgSpec("X:string")`).
- `Ref(...)` enforces arity against schema.
- `Const(...)` is type-checked against referenced `ArgSpec.datatype`.
- variable cross-literal type unification is currently not enforced globally.
- for rel-head rendering, `ProbLogRenderer` uses `[Sub, Obj, <props...>]` (not flattened key fields).
- ProbLog variable rendering is configurable:
  - default: `sanitize` (auto converts invalid variable names to uppercase-leading valid tokens),
  - per-rule override: `Rule(..., render_hints={"problog_var_mode": "error|sanitize|prefix|capitalize", "problog_var_prefix": "VAR_"})`,
  - global default override: `RenderContext(..., problog_var_mode="...", problog_var_prefix="...")`.
- `Ref` can appear inside `Expr` (boolean expression context).
- negated `Ref` is supported as literal (`Ref(..., negated=True)`), but not inside expression rendering context.

Serialized rule shape (`Rule.to_dict()`):

```json
{
  "name": "resident",
  "arity": 1,
  "schema_id": "<head_schema_id>",
  "kind": "fact",
  "signature": [{"datatype": "string", "arg_name": "X"}],
  "key_fields": ["X"],
  "conditions": [
    {
      "literals": [
        {"kind": "ref", "schema": "<person_schema_id>", "terms": [{"kind": "var", "name": "X"}], "negated": false}
      ],
      "prob": 0.7
    }
  ]
}
```

Expression ops:

- renderer built-ins: `eq ne lt le gt ge add sub mul div mod`.
- when using strict LLM schemas (`build_pydantic_rule_model` / `build_responses_schema`), `Call.op` must be one of built-ins or registered library expr ops.
- in direct Python rule construction, custom `Call(op, ...)` names are allowed and rendered as `op(args)` fallback.

Renderer status:

- `ProbLogRenderer` and `PrologRenderer` are usable.
- `DatalogRenderer` and `CypherRenderer` are currently stubs and raise `RenderError`.

### LLM Constraint Schemas

Use constraint builders to decode rule **conditions only** from LLM output:

```python
from symir.rule_ir import (
    build_pydantic_rule_model,
    build_responses_schema,
    build_predicate_catalog,
)

model = build_pydantic_rule_model(view, library=None, mode="compact")
json_schema = build_responses_schema(view, library=None, mode="compact")
catalog = build_predicate_catalog(view, library=None)
```

Contract notes:

- decoded payload contains only `conditions`; head/predicate is supplied by application code.
- for ref literals, field name is `schema` (value = predicate `schema_id`).
- `build_predicate_catalog(...)` is prompt context only (not strict decoding schema).
- compact mode uses `args` (`[{name, value}, ...]`), verbose mode uses positional `terms`.

Example payload (compact):

```json
{
  "conditions": [
    {
      "literals": [
        {
          "kind": "ref",
          "schema": "<person_schema_id>",
          "args": [
            {"name": "X", "value": {"kind": "var", "name": "X"}}
          ],
          "negated": false
        }
      ],
      "prob": 0.7
    }
  ]
}
```

Reconstruction pattern:

```python
validated = model.model_validate(payload)  # conditions only
rule = Rule(predicate=head_predicate, conditions=decoded_conditions)
```

There is currently no built-in `RuleLayer` registry; manage rule collections in application code.

### Rule Validation

Validation happens in three layers:

- expression construction (`Ref(...)`) checks arity and `Const` datatype against schema signature.
- rule validation (`RuleValidator`) checks allowed predicate scope, recursion, literal arity, and const type consistency.
- LLM structured decoding (`build_pydantic_rule_model` / `build_responses_schema`) checks payload shape and literal schema IDs.

Construction-time type checks (`Var` / `Const` have no datatype field):

```python
person = Fact("person", [ArgSpec("Name:string"), ArgSpec("Age:int")])

Ref(schema=person, terms=[Var("Name"), Const(30)])      # OK
Ref(schema=person, terms=[Var("Name"), Const("thirty")])  # raises SchemaError
```

Rule-level checks:

```python
from symir.errors import ValidationError
from symir.rule_ir import RuleValidator

validator = RuleValidator(view)
validator.validate(rule)  # raises ValidationError on violations
```

Common `ValidationError` cases:

- ref predicate is not allowed by current `FactView`.
- direct recursion: body `Ref` uses same schema as rule head.
- ref arity mismatch.
- const datatype mismatch against referenced predicate signature.

Strict decode gotchas:

- ref field name is `schema` (not `schema_id`) in decode payload.
- compact mode validates args by order against signature; arg `name` is descriptive.
- strict JSON schema requires numeric `prob` (`0.0..1.0`) in each condition.
- python-side `build_pydantic_rule_model(...)` allows `prob=None` for non-strict/local validation.

Legacy payload compatibility:

- low-level `expr_from_dict(...)` still accepts `schema_id` / `predicate_id` as fallback keys for ref parsing.
- this fallback is deserialization-only; user-side `Ref(...)` construction still requires `schema=<PredicateSchema|Instance>`.

## APIs

```python
from symir.rule_ir import (
    ArgSpec, Fact, Rel, FactLayer, FactView,
    Var, Const, Call, Unify, If, NotExpr, ExprIR,
    Ref, Expr, Cond, Rule, Query,
    FilterAST, PredMatch, And, Or, Not, filter_from_dict,
    RuleValidator, ProbabilityConfig,
    build_pydantic_rule_model, build_responses_schema, build_predicate_catalog,
    DataProvider, CSVProvider, CSVSource,
    Renderer, ProbLogRenderer, PrologRenderer, DatalogRenderer, CypherRenderer, RenderContext,
)
from symir.ir.instance import Instance
```
