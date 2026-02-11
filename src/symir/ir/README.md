# IR Overview

This folder contains core IR definitions for schemas, instances, and rules. The key
runtime concepts are:

- `Fact` / `Rel`: schema definitions (structure only)
- `FactLayer`: registry and validation for schemas
- `Instance`: canonical representation of facts/relations (runtime data)

Below is a focused tutorial on how `Instance` works.

## Instance Tutorial

### 1) Define schemas

```python
from symir.ir.fact_schema import ArgSpec, Fact, Rel, FactLayer

person = Fact(
    "person",
    [ArgSpec("Name:string", role="key"), ArgSpec("address:string")],
    merge_policy="keep_all",
)
city = Fact(
    "city",
    [ArgSpec("Name:string", role="key"), ArgSpec("Country:string")],
)
lives_in = Rel(
    "lives_in",
    sub=person,
    obj=city,
    props=[ArgSpec("since:int")],
    merge_policy="latest",
)

registry = FactLayer([person, city, lives_in])
```

### 2) Create instances (schema vs schema_id)

You can pass a schema object or a schema_id string.

When using schema_id, you must also pass `registry` so the instance can be
validated against the schema.

```python
from symir.ir.instance import Instance

alice = Instance(schema=person, terms=["alice", "darmstadt"])
alice_by_id = Instance(schema=person.schema_id, terms=["alice", "darmstadt"], registry=registry)
```

Instances are strict by default. If you want heuristic endpoint resolution,
set `resolve_mode="heuristic"` explicitly.

### 3) Fact terms

Facts accept `terms` as list/tuple or dict.

```python
# list/tuple: order matches Fact.signature
alice = Instance(schema=person, terms=["alice", "darmstadt"], prob=0.9)

# dict: keys must match Fact.signature
bob = Instance(schema=person, terms={"Name": "bob", "address": "frankfurt"})
```

Canonical facts include:

- `schema_id`
- `entity_id` (computed from `schema_id` + key_fields)
- `props`
- `prob`
- `meta`

### 4) Relation terms

Relations require two endpoints. Endpoints can be:

- a fact `Instance`
- a key dict matching endpoint key fields
- an `InstanceRef`

```python
from symir.ir.fact_schema import InstanceRef

# endpoint by fact instance
rel1 = Instance(schema=lives_in, terms=[alice, {"Name": "germany"}, 2020])

# endpoint by key dict
rel2 = Instance(
    schema=lives_in,
    terms=[{"Name": "alice"}, {"Name": "darmstadt"}, 2021],
)

# endpoint by InstanceRef (schema_id + key values)
ref = InstanceRef(schema_id=person.schema_id, key_values={"Name": "bob"})
rel3 = Instance(schema=lives_in, terms=[ref, {"Name": "seattle"}, 2022])
```

Relation dict inputs support three forms.

```python
# A) sub_ref/obj_ref + props (Instance endpoints)
rel4 = Instance(
    schema=lives_in,
    terms={
        "sub_ref": alice,
        "obj_ref": Instance(schema=city, terms={"Name": "darmstadt", "Country": "germany"}),
        "props": {"since": 2020},
    },
)

# B) sub_key/obj_key + props (dict endpoints)
rel5 = Instance(
    schema=lives_in,
    terms={
        "sub_key": {"Name": "alice"},
        "obj_key": {"Name": "darmstadt"},
        "props": {"since": 2021},  # or inline: "since": 2021
    },
)

# C) flattened dict (CSV-friendly)
rel6 = Instance(
    schema=lives_in,
    terms={
        "Name": "alice",     # sub key
        "obj_Name": "darmstadt",  # obj key (sub_/obj_ prefixes are allowed)
        "since": 2022,
    },
)
```

Notes:

- `sub_key` / `obj_key` must be dicts. If you want to pass Instances, use `sub_ref/obj_ref`
  or list/tuple terms.
- In `sub_ref/obj_ref` form, props can be provided inline or under the `props` key.
- In `sub_key/obj_key` form, props can be inline or under `props`.
- In strict mode, relation properties must match the schema's `props` arg names and all props
  must be present.
- For list/tuple terms, `terms[2:]` must match `Rel.props` order and length.

### 5) Meta and merge policies

Metadata is optional and does not affect entity_id hashing.

```python
instance = Instance(
    schema=person,
    terms=["carol", "berlin"],
    meta={
        "source": "csv",
        "observed_at": "2026-02-10T12:00:00Z",
        "evidence_id": "row:123",
    },
)
```

When a schema sets `merge_policy="keep_all"`, a `record_id` is generated and added to each instance.

Meta validation:

- Meta is strict: unknown keys are rejected.
- `source` / `observed_at` / `ingested_at` / `evidence_id` / `trace_id` must be strings
- `confidence` must be a number in `[0.0, 1.0]`
- `status` must be one of `asserted | inferred | retracted`
- `provenance` must be a dict, `tags` must be a list of strings

Type hints use `Literal` directly on parameters (e.g. `merge_policy`) and `status` values are validated at runtime.

### 6) Canonical output

```python
payload = alice.to_dict()
```

For relations, you can optionally include endpoint key values for readability:

```python
payload = rel5.to_dict(include_keys=True)
```

Produces a canonical dict with stable identifiers:

- Fact: `schema_id`, `entity_id`, `props`, `prob`, `meta`
- Rel: `schema_id`, `sub_entity_id`, `obj_entity_id`, `props`, `prob`, `meta`

Notes on identity:

- Fact `entity_id` is computed from `schema_id` + key_fields values only (non-key props do not affect it).
- Rel `sub_entity_id` / `obj_entity_id` are computed from endpoint key_fields only; relation `props` do not
  affect endpoint identity. If you need per-record uniqueness for relations, set
  `merge_policy="keep_all"` on the relation schema to get a `record_id`.

When `include_keys=True` on `to_dict()`, relation output includes `sub_key` and `obj_key` (if available).

### 7) Strict vs heuristic

By default `Instance` is strict and will refuse ambiguous input. You can enable heuristic
resolution (only for single-key endpoints) explicitly:

```python
rel = Instance(
    schema=lives_in,
    terms=["alice", "darmstadt", 2024],
    resolve_mode="heuristic",
)
```

This only works when each endpoint has exactly one key field.

### 8) Structured refs in expressions

You can use `Ref(...)` inside `Unify` to bind a variable to a structured term. This
renders to `Var = predicate(...)` in Prolog/ProbLog.

```python
from symir.ir.expr_ir import Unify, Var, Ref

expr = Unify(Var("Sub"), Ref(schema_id=person.schema_id, terms=[Var("SubName"), Var("SubAddr")]))
```
