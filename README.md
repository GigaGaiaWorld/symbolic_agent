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

```python
from symir.ir.fact_schema import ArgSpec, Fact, Rel, FactLayer

person = Fact(
    "person",
    [ArgSpec("Name:string", role="key"), ArgSpec("address:string")],
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
)

schema = FactLayer([person, city, lives_in])
```

Notes:

- `merge_policy` is set on `Fact`/`Rel` (schema-level), not on instances.
- `schema_id` is derived from stable schema fields and is used for identity.
- Allowed `merge_policy` values: `max | latest | noisy_or | overwrite | keep_all`.

## Instances

```python
from symir.ir.instance import Instance

alice = Instance(schema=person, terms=["alice", "darmstadt"], prob=0.9)

rel = Instance(
    schema=lives_in,
    terms={
        "sub_ref": alice,
        "obj_key": {"Name": "darmstadt"},
        "props": {"since": 2020},
    },
    meta={
        "source": "csv",
        "observed_at": "2026-02-10T12:00:00Z",
        "status": "asserted",
    },
)

readable = rel.to_dict(include_keys=True)
```

Meta (strict) allowed keys:

- `source`, `observed_at`, `ingested_at`, `evidence_id`, `trace_id`
- `confidence` (0.0–1.0), `status` (`asserted|inferred|retracted`)
- `provenance` (dict), `tags` (list[str])

Unknown meta keys raise `SchemaError`.

## Load CSV Facts (FactLayer)

```python
from pathlib import Path
from symir.fact_store.provider import CSVProvider, CSVSource

sources = [
    CSVSource(predicate_id=person.schema_id, file="people.csv", columns=["Name", "address"]),
    CSVSource(predicate_id=city.schema_id, file="cities.csv", columns=["Name", "Country"]),
    CSVSource(predicate_id=lives_in.schema_id, file="lives_in.csv", columns=["sub_Name", "obj_Name", "since"]),
]

provider = CSVProvider(
    schema=schema,
    base_path=Path("data"),
    sources=sources,
    datatype_cast="coerce",  # none | coerce | strict
)

view = schema.view([p.schema_id for p in schema.predicates()])
instances = provider.query(view)
```

Notes:

- `columns` are mapped **by signature order**, not by arg name.
- For relations, the signature is derived as `sub_*` keys + `obj_*` keys + props.

## Build Relations From Facts (RelBuilder)

Use `RelBuilder` when your relation CSV does not already contain `sub_*` / `obj_*` columns
and you need to match existing fact instances.

```python
from symir.fact_store.rel_builder import RelBuilder
from symir.fact_store.provider import CSVProvider, CSVSource

facts = provider.query(schema.view([person.schema_id, city.schema_id]))

builder = RelBuilder(
    rel=lives_in,
    match_keys=["person", "city"],
    match_props=["since"],
    key_mode="partial",
    multi="cartesian",
)

rel_source = CSVSource(
    predicate_id=lives_in.schema_id,
    file="lives_in.csv",
    columns=["person", "city", "since"],
    prob_column="prob",
)

rels = provider.build_relations(
    builder=builder,
    facts=facts,
    source=rel_source,
    maps={"person": "person_name", "city": "city_name"},
)
```

Notes:

- `columns` are logical row keys consumed by `RelBuilder`.
- `maps` maps logical keys to actual CSV column names.
- If `match_keys` is omitted, `RelBuilder` expects `sub_<Key>` / `obj_<Key>` columns by default.

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

- Rule heads are inferred from the predicate signature; conditions carry the explicit logic.
- Probabilities are attached per condition (clause-level).
- `RefLiteral` references predicates by `schema_id`.

Example:

```python
from symir.rule_ir import (
    ArgSpec, Fact, FactLayer, Var, Ref, Cond, Rule,
    ProbLogRenderer, RenderContext
)
from symir.ir.fact_schema import PredicateSchema

person = Fact("person", [ArgSpec("Name:string")])
schema = FactLayer([person])
view = schema.view([person.schema_id])

head_pred = PredicateSchema("resident", 1, [ArgSpec("X:string")])
cond = Cond(literals=[Ref(schema_id=person.schema_id, terms=[Var("X")])], prob=0.7)
rule = Rule(predicate=head_pred, conditions=[cond])

text = ProbLogRenderer().render_rule(rule, RenderContext(schema=schema))
```

## APIs

```python
from symir.rule_ir import (
    ArgSpec, Fact, Rel, FactLayer, FactView,
    Var, Const, Call, Unify, If, NotExpr, ExprIR,
    RefLiteral, Expr, Cond, Rule, Query,
    FilterAST, PredMatch, And, Or, Not, filter_from_dict,
    RuleValidator, ProbabilityConfig,
    DataProvider, CSVProvider, CSVSource,
    Renderer, ProbLogRenderer, PrologRenderer, DatalogRenderer, CypherRenderer, RenderContext,
)
from symir.ir.instance import Instance
```
