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

### Head var-only
`HeadSchema` allows only variables. Any constants must be expressed in the body via `Unify` or comparison calls.

### Bodies = rule branches (clause-level probability)
One head + multiple bodies = multiple clauses:

```
Head :- Body1.
Head :- Body2.
```

Probability is attached **per body/clause** (not on the rule itself). Missing values are resolved by
`ProbabilityConfig` defaults.

### Literals
- `RefLiteral`: references FactView predicates only; supports negation.
- `Expr`: structured ExprIR only (no raw strings).

### Negation & restrictions
- Allowed: negation (RefLiteral.negated, ExprIR.Not).
- Forbidden: recursion (direct recursion is blocked), aggregates, cut.

### FactSchema / FactView / Filter AST

`FactSchema` defines canonical predicate schemas with stable `schema_id` (hash). `FactView` is a filtered subset and
is the only set of predicates LLM may reference. Use Filter AST (`And`/`Or`/`Not`/`PredMatch`) or dict sugar.

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
    ArgSpec, PredicateSchema, FactSchema, Var, HeadSchema,
    RefLiteral, Body, Rule, ProbLogRenderer, RenderContext
)

person = PredicateSchema("Person", 1, [ArgSpec("string")])
schema = FactSchema([person])
view = schema.view([person.schema_id])

head_pred = PredicateSchema("Resident", 1, [ArgSpec("string")])
head = HeadSchema(predicate=head_pred, terms=[Var("X")])
body = Body(literals=[Ref(schema_id=person.schema_id, terms=[Var("X")])], prob=0.7)
rule = Rule(head=head, bodies=[body])

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
)

# Constraint decoding only generates bodies (head is already provided by the system)
# Optional library input to allow library predicates/expressions

model = build_pydantic_rule_model(view, library=None, mode="compact")
responses_schema = build_responses_schema(view, library=None, mode="compact")
catalog = build_predicate_catalog(view, library=None)
```


### APIs:
```python
from symir.ir.fact_schema import ArgSpec, PredicateSchema, FactSchema, FactView
from symir.ir.filters import FilterAST, PredMatch, And, Or, Not, filter_from_dict
from symir.ir.expr_ir import Var, Const, Call, Unify, If, NotExpr, ExprIR
from symir.ir.rule_schema import RefLiteral, Expr, HeadSchema, Body, Rule
from symir.rules.validator import RuleValidator
from symir.rules.library import Library, LibrarySpec
from symir.rules.library_runtime import LibraryRuntime
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
