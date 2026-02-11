"""End-to-end demo for new Fact/Rel schema + rule pipeline.

This script mirrors the updated test.ipynb but in plain Python.
"""

from __future__ import annotations

import json
import pprint as pp
from pathlib import Path

from symir.rule_ir import ArgSpec, Fact, Rel, FactLayer
from symir.ir.filters import filter_from_dict
from symir.ir.expr_ir import Var, Const, Call, Unify, If, NotExpr
from symir.ir.rule_schema import RefLiteral, Expr, HeadSchema, Body, Rule, Query
from symir.ir.fact_schema import cache_predicate_schema, load_predicate_schemas_from_cache
from symir.fact_store.provider import FactInstance, CSVProvider, CSVSource
from symir.probability import ProbabilityConfig, resolve_probability
from symir.rules.library import Library, LibrarySpec
from symir.rules.library_runtime import LibraryRuntime
from symir.rules.constraint_schemas import (
    build_pydantic_rule_model,
    build_responses_schema,
    build_predicate_catalog,
)
from symir.mappers.renderers import (
    ProbLogRenderer,
    PrologRenderer,
    DatalogRenderer,
    CypherRenderer,
    RenderContext,
    RenderError,
)


def section(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def define_schema() -> FactLayer:
    section("1) Define Fact/Rel Schema (Neo4j-friendly)")

    person_name = ArgSpec("Name:string", namespace="person", role="key")
    city_name = ArgSpec("City:string", namespace="geo", role="name")
    company_name = ArgSpec("Company:string", namespace="org", role="name")
    country_name = ArgSpec("Country:string", namespace="geo", role="name")

    # default arg_name: Param, Param2 when omitted
    person_address = ArgSpec("string", namespace="address")
    person_age = ArgSpec("int", namespace="person")

    person = Fact("person", [person_name], description="A person entity")
    city = Fact("city", [city_name], description="A city entity")
    company = Fact("company", [company_name], description="A company entity")
    country = Fact("country", [country_name], description="A country entity")

    # richer fact to show multi-arity + default arg_name
    person_profile = Fact(
        "person_profile",
        [person_name, person_address, person_age],
        description="Person with address and age",
    )

    lives = Rel("lives_in", sub=person, obj=city, description="person lives in city")
    works = Rel("works_at", sub=person, obj=company, description="person works at company")
    located = Rel("located_in", sub=city, obj=country, description="city located in country")
    hq = Rel("company_hq", sub=company, obj=city, description="company HQ city")
    friend = Rel("are_friends", sub=person, obj=person, description="person knows person")

    # relation with properties
    employment = Rel(
        "employment",
        sub=person,
        obj=company,
        props=[ArgSpec("since:int", namespace="time"), ArgSpec("title:string", namespace="org")],
        description="employment relation with attributes",
    )

    schema = FactLayer(
        [person, city, company, country, person_profile, lives, works, located, hq, friend, employment]
    )
    print("SCHEMA:")
    pp.pprint(schema.to_dict())

    print("\nRel signature (employment):")
    pp.pprint([a.to_dict() for a in employment.signature])

    print("\nDefault arg_name examples (person_profile):")
    pp.pprint([a.to_dict() for a in person_profile.signature])

    print("\nSchema IDs:")
    print(person.schema_id, employment.schema_id)

    # cache demo
    cache_predicate_schema(employment)
    loaded = load_predicate_schemas_from_cache()
    print("\nLOADED SCHEMAS (from cache):")
    pp.pprint([s.to_dict() for s in loaded])

    return schema


def build_view(schema: FactLayer) -> None:
    section("2) Build FactView + Filters")

    load_with_filters = False
    if not load_with_filters:
        view = schema.view([pred.schema_id for pred in schema.predicates()])
    else:
        filt = filter_from_dict(
            {
                "and": [
                    {"predicate": {"name": "person"}},
                    {"predicate": {"name": "lives_in"}},
                ]
            }
        )
        view = schema.view_from_filter(filt)

    print("\nVIEW PREDICATES:")
    pp.pprint(view.predicates())

    filter_examples = [
        {"name": "lives_in"},
        {"arity": 2},
        {"name": "lives_in", "arity": 2, "datatype": "string"},
        {"role": "key"},
        {"namespace": "geo"},
        {"match": {"name": "lives_in"}},
        {"match": {"name": "lives_in", "arity": 2}},
        {"match": {"datatype": "string", "namespace": "geo"}},
        {"and": [{"match": {"name": "lives_in"}}, {"match": {"arity": 2}}]},
        {"or": [{"name": "person"}, {"name": "city"}]},
        {"not": {"match": {"namespace": "geo"}}},
        {
            "and": [
                {"match": {"arity": 2}},
                {"not": {"match": {"name": "lives_in"}}},
                {"or": [{"match": {"namespace": "geo"}}, {"match": {"namespace": "org"}}]},
            ]
        },
    ]

    print("\nFILTER EXAMPLES:")
    for idx, example in enumerate(filter_examples, start=1):
        filt = filter_from_dict(example)
        view_from_filter = schema.view_from_filter(filt)
        print(f"\nFilter {idx}: {json.dumps(example, ensure_ascii=False)}")
        print([pred.name for pred in view_from_filter.predicates()])


def manual_facts(schema: FactLayer) -> list[FactInstance]:
    section("3) Manual FactInstances + InstanceRef")

    pred_by_name = {p.name: p for p in schema.predicates()}
    person = pred_by_name["person"]
    city = pred_by_name["city"]
    company = pred_by_name["company"]
    country = pred_by_name["country"]
    person_profile = pred_by_name["person_profile"]
    lives = pred_by_name["lives_in"]
    employment = pred_by_name["employment"]

    # InstanceRef uses key fields (role order: key > id > name > first arg)
    alice_ref = person.build_instance_ref(["alice"])
    openai_ref = company.build_instance_ref(["openai"])
    employment_ref = employment.build_instance_ref(["alice", "openai", 2020, "researcher"])
    print("\nINSTANCEREF EXAMPLES:")
    pp.pprint([alice_ref, openai_ref, employment_ref])

    facts_from_user = [
        FactInstance(predicate_id=person.schema_id, terms=[Const("alice", "string")], prob=0.9),
        FactInstance(predicate_id=person.schema_id, terms=[Const("bob", "string")]),
        FactInstance(predicate_id=city.schema_id, terms=[Const("seattle", "string")]),
        FactInstance(predicate_id=company.schema_id, terms=[Const("openai", "string")]),
        FactInstance(predicate_id=country.schema_id, terms=[Const("usa", "string")]),
        FactInstance(
            predicate_id=person_profile.schema_id,
            terms=[Const("alice", "string"), Const("darmstadt", "string"), Const(33, "int")],
        ),
        FactInstance(
            predicate_id=employment.schema_id,
            terms=[
                Const("alice", "string"),
                Const("openai", "string"),
                Const(2020, "int"),
                Const("researcher", "string"),
            ],
            prob=0.8,
        ),
        FactInstance(
            predicate_id=lives.schema_id,
            terms=[Const("alice", "string"), Const("seattle", "string")],
        ),
    ]

    prob_cfg = ProbabilityConfig(default_fact_prob=1.0, missing_prob_policy="inject_default")
    facts_from_user = [
        FactInstance(
            predicate_id=f.predicate_id,
            terms=f.terms,
            prob=resolve_probability(
                f.prob,
                default_value=prob_cfg.default_fact_prob,
                policy=prob_cfg.missing_prob_policy,
                context="manual fact",
            ),
        )
        for f in facts_from_user
    ]
    print("\nFACTS (manual):")
    pp.pprint(facts_from_user)
    return facts_from_user


def csv_facts(schema: FactLayer) -> list[FactInstance]:
    section("4) CSVProvider Facts (optional)")

    samples_dir = Path("samples")
    if not samples_dir.exists():
        print("samples/ not found; skipping CSVProvider demo.")
        return []

    pred_by_name = {p.name: p for p in schema.predicates()}
    person = pred_by_name["person"]
    city = pred_by_name["city"]
    company = pred_by_name["company"]
    country = pred_by_name["country"]
    lives = pred_by_name["lives_in"]
    works = pred_by_name["works_at"]
    located = pred_by_name["located_in"]
    hq = pred_by_name["company_hq"]
    friend = pred_by_name["are_friends"]

    provider = CSVProvider(
        schema=schema,
        base_path=samples_dir,
        sources=[
            CSVSource(predicate_id=person.schema_id, file="people_rich.csv", columns=["name"]),
            CSVSource(predicate_id=city.schema_id, file="cities_rich.csv", columns=["name"]),
            CSVSource(predicate_id=company.schema_id, file="companies.csv", columns=["name"]),
            CSVSource(predicate_id=country.schema_id, file="countries.csv", columns=["name"]),
            CSVSource(predicate_id=lives.schema_id, file="lives_in_rich.csv", columns=["person", "city"]),
            CSVSource(predicate_id=works.schema_id, file="works_at.csv", columns=["person", "company"]),
            CSVSource(predicate_id=located.schema_id, file="located_in.csv", columns=["city", "country"]),
            CSVSource(predicate_id=hq.schema_id, file="company_hq.csv", columns=["company", "city"]),
            CSVSource(predicate_id=friend.schema_id, file="friends.csv", columns=["person_a", "person_b"]),
        ],
        prob_config=ProbabilityConfig(default_fact_prob=1.0, missing_prob_policy="inject_default"),
    )
    facts_from_csv = provider.query(schema.view([p.schema_id for p in schema.predicates()]))
    print("\nFACTS (csv, first 3):")
    pp.pprint(facts_from_csv[:3])
    return facts_from_csv


def build_rule(schema: FactLayer) -> Rule:
    section("5) Rules + ExprIR")

    pred_by_name = {p.name: p for p in schema.predicates()}
    works = pred_by_name["works_at"]
    hq = pred_by_name["company_hq"]
    lives = pred_by_name["lives_in"]
    friend = pred_by_name["are_friends"]
    located = pred_by_name["located_in"]

    head_pred = Fact(
        "relocation_candidate",
        [ArgSpec("Person:string"), ArgSpec("City:string")],
        description="candidate relation",
    )
    head = HeadSchema(predicate=head_pred, terms=[Var("X"), Var("Y")])

    body1 = Body(
        literals=[
            Ref(schema_id=works.schema_id, terms=[Var("X", "string"), Var("C", "string")]),
            Ref(schema_id=hq.schema_id, terms=[Var("C", "string"), Var("Y", "string")]),
        ],
        prob=0.7,
    )

    expr = If(
        cond=Call("eq", [Var("Y", "string"), Const("seattle", "string")]),
        then=Unify(Var("Flag", "bool"), Const(True, "bool")),
        else_=Unify(Var("Flag", "bool"), Const(False, "bool")),
    )
    body2 = Body(
        literals=[
            Ref(schema_id=lives.schema_id, terms=[Var("X", "string"), Var("Y", "string")]),
            Ref(schema_id=friend.schema_id, terms=[Var("X", "string"), Var("F", "string")]),
            Ref(schema_id=lives.schema_id, terms=[Var("F", "string"), Var("Y", "string")]),
            Ref(schema_id=located.schema_id, terms=[Var("Y", "string"), Var("Country", "string")]),
            Expr(expr=expr),
        ],
        prob=None,
    )

    rule_from_user = Rule(head=head, bodies=[body1, body2])
    print("\nRULE (user):")
    pp.pprint(rule_from_user)
    return rule_from_user


def build_library() -> tuple[Library, LibraryRuntime]:
    section("6) Libraries + Constraint Schemas")

    lib = Library()
    lib.register(
        LibrarySpec(
            name="member",
            arity=2,
            kind="predicate",
            description="Check if an element is a member of a list",
            signature=["term", "list"],
        )
    )
    lib.register(
        LibrarySpec(
            name="is_even",
            arity=1,
            kind="expr",
            description="Check if a number is even",
            signature=["term"],
        )
    )

    runtime = LibraryRuntime(lib)
    runtime.register(
        name="member",
        arity=2,
        kind="predicate",
        backend="problog",
        handler=lambda args: f"member({args[0]}, {args[1]})",
    )

    return lib, runtime


def build_schemas(schema: FactLayer, lib: Library) -> None:
    json_schema = build_responses_schema(schema.view([p.schema_id for p in schema.predicates()]), lib, mode="compact")
    pydantic_model = build_pydantic_rule_model(schema.view([p.schema_id for p in schema.predicates()]), lib, mode="compact")
    catalog = build_predicate_catalog(schema.view([p.schema_id for p in schema.predicates()]), lib)

    print("\n### JSON SCHEMA (responses):")
    pp.pprint(json_schema)
    print("\n### PYDANTIC MODEL:")
    pp.pprint(pydantic_model.schema())
    print("\n### PREDICATE CATALOG (prompt-only):")
    pp.pprint(catalog)


def render_all(schema: FactLayer, facts_from_user: list[FactInstance], facts_from_csv: list[FactInstance], rule: Rule, lib: Library, runtime: LibraryRuntime) -> None:
    section("7) Render Program")

    render_cfg = ProbabilityConfig(default_rule_prob=0.6, missing_prob_policy="inject_default")
    ctx = RenderContext(schema=schema, library=lib, library_runtime=runtime, prob_config=render_cfg)

    all_facts = facts_from_user + facts_from_csv
    all_rules = [rule]
    all_queries = [
        Query(predicate=rule.head.predicate, terms=[Var("X"), Var("Y")]),
    ]

    problog_text = ProbLogRenderer().render_program(all_facts, all_rules, ctx, queries=all_queries)
    print("\nPROBLOG (facts + rules):")
    print(problog_text)

    try:
        _ = PrologRenderer().render_rule(rule, ctx)
        _ = DatalogRenderer().render_rule(rule, ctx)
        _ = CypherRenderer().render_rule(rule, ctx)
    except RenderError:
        print("\nOther renderers not implemented yet.")


if __name__ == "__main__":
    schema = define_schema()
    build_view(schema)
    facts_from_user = manual_facts(schema)
    facts_from_csv = csv_facts(schema)
    rule = build_rule(schema)
    lib, runtime = build_library()
    build_schemas(schema, lib)
    render_all(schema, facts_from_user, facts_from_csv, rule, lib, runtime)
