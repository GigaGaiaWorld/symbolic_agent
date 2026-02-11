# symir

Lightweight, IR-first logic representation and mapping. ProbLog is a plugin mapper.

## Components

- `ir/`: Neutral, serializable IR types and fact schema.
- `fact_store/`: CSV-backed fact loading.
- `rules/`: Rule concepts, registry persistence, and constraint schemas (Pydantic + JSON schema).
- `mappers/`: Target language mapping (ProbLog implemented).
- `rule_ir.py`: Unified entrypoint for rule schema/IR (rules, filters, providers, renderers).
- `tests/`: Minimal tests for CSV loading, schema generation, and mapping.

## Fact Schema (JSON/dict)

```python
schema = {
    "nodes": {
        "Person": {"file": "people.csv", "column": "name"},
        "City": {"file": "cities.csv", "column": "name"},
    },
    "relations": {
        "LivesIn": {"file": "lives_in.csv", "columns": ["person", "city"]}
    },
}
```

## Load CSV Facts

```python
from pathlib import Path
from symir.ir.schema import FactSchema
from symir.fact_store.csv_store import CsvFactStore

schema = FactSchema.from_dict(schema)
store = CsvFactStore(schema=schema, base_path=Path("/data"))
facts = store.load_facts()
```

## Rule Registry and Constraint Schemas

```python
from symir.ir.types import IRPredicateRef
from symir.rules.concepts import RuleConcept
from symir.rules.registry import RuleRegistry
from symir.rules.constraint_schemas import (
    build_pydantic_rule_model,
    build_responses_schema,
    build_predicate_catalog,
)

registry = RuleRegistry()
registry.add(
    RuleConcept(
        name="Ancestor",
        arity=2,
        description="Ancestor relation",
        head=IRPredicateRef(name="Ancestor", arity=2, layer="rule"),
        allowed_body_predicates=[
            IRPredicateRef(name="Parent", arity=2, layer="fact"),
            IRPredicateRef(name="Ancestor", arity=2, layer="rule"),
        ],
    )
)

pydantic_model = build_pydantic_rule_model(schema, registry)
json_schema = build_responses_schema(schema, registry)
```

## ProbLog Mapping

```python
from symir.ir.types import IRProgram
from symir.mappers.problog import to_problog

program = IRProgram(facts=facts, rules=[])
problog_text = to_problog(program)
```

## Notes

- Python 3.10+.
- Only standard library + `pydantic`.
- Probabilistic rules are rendered as head annotations with a comment note.
- CSV `prob_column` values may be empty; missing values default to `DEFAULT_PROB_VALUE = 1.0`.

## Rule Schema/IR (new)

This package also provides a **neutral, serializable rule IR** designed for rule generation (LLM or non-LLM).
It is modular and ready for multi-backend rendering (ProbLog implemented, others stubbed).

### Rule shape
`Rule` is defined as `predicate + conditions` (no `HeadSchema`/`Body` wrappers):

```
Rule(predicate=<Fact/Rel>, conditions=[Cond(...), Cond(...)])
```

Probability is attached **per condition/clause** (not on the rule itself). Missing values are resolved by
`ProbabilityConfig` defaults.

### Literals
- `Ref`: references FactView predicates only; supports negation.
- `Expr`: structured ExprIR only (no raw strings).
- `Ref(schema=...)` accepts predicate schema objects or instances; user construction does not take raw `schema_id` strings.

### Negation & restrictions
- Allowed: negation (Ref.negated, ExprIR.Not).
- Forbidden: recursion (direct recursion is blocked), aggregates, cut.

### FactSchema / FactView / Filter AST

`FactSchema` defines canonical predicate schemas with stable `schema_id` (hash). `FactView` is a filtered subset and
is the only set of predicates LLM may reference. Use Filter AST (`And`/`Or`/`Not`/`PredMatch`) or dict sugar.

### Build Rel From Persisted Fact Schemas

To create a new `Rel` schema from persisted data, first restore fact schemas as objects, then build `Rel(...)`.

```python
import json
from symir.ir.fact_schema import FactLayer, Rel, ArgSpec

registry = FactLayer.from_dict(json.load(open("schema.json", "r", encoding="utf-8")))
person = registry.fact("person")
company = registry.fact("company")

works_at = Rel("works_at", sub=person, obj=company, props=[ArgSpec("Since:int")])
registry = FactLayer([*registry.predicates(), works_at])
```

Notes:
- `Rel` construction needs concrete fact schema objects; schema IDs alone are not enough.
- `FactLayer` is rebuilt with a new predicate list when adding schemas.

### DataProvider abstraction
`DataProvider.query(view, filter)` is the extension point. `CSVProvider` implements CSV-backed facts.

### Probability default strategy
Configurable defaults with `ProbabilityConfig`:
- `default_fact_prob`
- `default_rule_prob`
- `missing_prob_policy` (`inject_default` / `warn_and_default` / `error`)

### Example

```python
from symir.rule_ir import (
    ArgSpec, Fact, FactLayer, Var,
    Ref, Cond, Rule, ProbLogRenderer, RenderContext
)

person = Fact("person", [ArgSpec("X:string")])
schema = FactLayer([person])
view = schema.view([person])

head = Fact("resident", [ArgSpec("X:string")])
cond = Cond(literals=[Ref(schema=person, terms=[Var("X")])], prob=0.7)
rule = Rule(predicate=head, conditions=[cond])

text = ProbLogRenderer().render_rule(rule, RenderContext(schema=schema))
```

### LibraryRuntime (Executable Renderer)

`LibrarySpec` only stores serializable metadata. For directly executable backend rendering logic, use `LibraryRuntime`.
```python
from symir.rule_ir import Library, LibrarySpec, LibraryRuntime

lib = Library()
lib.register(LibrarySpec(
    name="member",
    arity=2,
    kind="predicate",
    description="Membership in list",
    signature=["term", "list"],
))

runtime = LibraryRuntime(lib)
runtime.register(
    name="member",
    arity=2,
    kind="predicate",
    backend="problog",
    handler=lambda args: f"member({args[0]}, {args[1]})",
)
```

### LLM Constraint Decoding Schema

If you need to perform structured constraint decoding on an LLM, use:

```python
from symir.rules.constraint_schemas import (
    build_pydantic_rule_model,
    build_responses_schema,
    build_predicate_catalog,
)

# Constraint decoding only generates conditions (head is provided by application code)
# Optional library input to allow library predicates/expressions

model = build_pydantic_rule_model(view, library=None, mode="compact")
responses_schema = build_responses_schema(view, library=None, mode="compact")
catalog = build_predicate_catalog(view, library=None)
```

Notes:
- payload decodes `conditions` only; application supplies rule head (`Fact`/`Rel`) when constructing `Rule`.
- ref literal key is `schema` (predicate `schema_id`), not `schema_id`.
- compact mode validates `args` by order against predicate signature.
- strict JSON schema requires numeric `prob` per condition.


### APIs:
```python
from symir.ir.fact_schema import ArgSpec, PredicateSchema, FactSchema, FactView
from symir.ir.filters import FilterAST, PredMatch, And, Or, Not, filter_from_dict
from symir.ir.expr_ir import Var, Const, Call, Unify, If, NotExpr, ExprIR
from symir.ir.rule_schema import Ref, Expr, Cond, Rule
from symir.rules.validator import RuleValidator
from symir.rules.library import Library, LibrarySpec
from symir.rules.library_runtime import LibraryRuntime
from symir.rules.constraint_schemas import (
    build_pydantic_rule_model,
    build_responses_schema,
    build_predicate_catalog,
)
from symir.fact_store.provider import DataProvider, CSVProvider, CSVSource
from symir.ir.instance import Instance
from symir.mappers.renderers import (
    Renderer,
    ProbLogRenderer,
    PrologRenderer,
    DatalogRenderer,
    CypherRenderer,
    RenderContext,
)
from symir.probability import ProbabilityConfig
```
