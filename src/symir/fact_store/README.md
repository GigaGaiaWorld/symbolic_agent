# Fact Store

This package provides utilities for loading fact data from external sources into canonical
`Instance` objects.

## CSVProvider (predicate schema based)

`CSVProvider` works with predicate schemas from `symir.ir.fact_schema` (Fact/Rel). It returns
canonical `Instance` objects, one per CSV row.

You can pass either a `FactLayer` or a `FactView` to `CSVProvider`. If you pass a view, it is
stored as the default filter; calling `query()` with no arguments will use that view.

`CSVProvider` also supports optional datatype casting via `datatype_cast`:

- `none` (default): keep all values as strings
- `coerce`: best-effort cast for `int/float/bool`, otherwise keep string
- `strict`: cast must succeed for supported types, unknown datatypes are errors

```python
from pathlib import Path
from symir.ir.fact_schema import ArgSpec, Fact, Rel, FactLayer
from symir.fact_store.provider import CSVProvider, CSVSource

person = Fact(
    "person",
    [ArgSpec("Name:string", role="key"), ArgSpec("address:string")],
)
city = Fact(
    "city",
    [ArgSpec("Name:string", role="key"), ArgSpec("Country:string")],
)
lives_in = Rel("lives_in", sub=person, obj=city, props=[ArgSpec("since:int")])

schema = FactLayer([person, city, lives_in])
sources = [
    CSVSource(predicate_id=person.schema_id, file="people.csv", columns=["Name", "address"]),
    CSVSource(predicate_id=city.schema_id, file="cities.csv", columns=["Name", "Country"]),
    # For rels, columns map to the derived signature: sub_*/obj_* keys + props.
    CSVSource(
        predicate_id=lives_in.schema_id,
        file="lives_in.csv",
        columns=["sub_Name", "obj_Name", "since"],
    ),
]

provider = CSVProvider(
    schema=schema,
    base_path=Path("data"),
    sources=sources,
    datatype_cast="coerce",
)
instances = provider.query(schema.view([p.schema_id for p in schema.predicates()]))

# Or pass a view directly and call query() without arguments.
view = schema.view([person.schema_id, city.schema_id])
provider_from_view = CSVProvider(schema=view, base_path=Path("data"), sources=sources)
instances = provider_from_view.query()
```

Each CSV row becomes one `Instance`. If you need richer provenance (source, trace ids, etc.),
attach metadata at ingestion time.

## RelBuilder (match facts + rows)

Use `RelBuilder` when you need to create relations by matching existing fact instances.
`rows` is a list of dicts. CSV is just one source for those rows.

```python
from symir.fact_store.rel_builder import RelBuilder

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

facts = provider.query(schema.view([person.schema_id, city.schema_id]))
rels = provider.build_relations(
    builder=builder,
    facts=facts,
    source=rel_source,
    maps={"person": "person_name", "city": "city_name"},
)
```

Notes:

- `columns` are logical row keys consumed by `RelBuilder`.
- `maps` maps logical keys to CSV column names.
- If `match_keys` is omitted, the builder expects `sub_<Key>` / `obj_<Key>` columns by default.

## CsvFactStore (mapping schema based)

`CsvFactStore` is a separate loader that uses the lightweight mapping schema in
`symir.ir.schema`. It yields `IRAtom` objects for rule evaluation and is kept for
backwards compatibility with the older CSV mapping format.
