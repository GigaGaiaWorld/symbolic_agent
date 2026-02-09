"""Unified entrypoint for rule schema/IR and related utilities."""

from __future__ import annotations

from symir.ir.fact_schema import ArgSpec, PredicateSchema, FactSchema, FactView
from symir.ir.filters import FilterAST, PredMatch, And, Or, Not, filter_from_dict
from symir.ir.expr_ir import Var, Const, Call, Unify, If, NotExpr, ExprIR
from symir.ir.rule_schema import RefLiteral, ExprLiteral, HeadSchema, Body, Rule, Query
from symir.rules.validator import RuleValidator
from symir.rules.library import Library, LibrarySpec
from symir.rules.library_runtime import LibraryRuntime
from symir.rules.constraint_schemas import (
    build_pydantic_rule_model,
    build_responses_schema,
    build_predicate_catalog,
)
from symir.fact_store.provider import DataProvider, CSVProvider, CSVSource, FactInstance
from symir.mappers.renderers import (
    Renderer,
    ProbLogRenderer,
    PrologRenderer,
    DatalogRenderer,
    CypherRenderer,
    RenderContext,
)
from symir.probability import ProbabilityConfig

__all__ = [
    "ArgSpec",
    "PredicateSchema",
    "FactSchema",
    "FactView",
    "FilterAST",
    "PredMatch",
    "And",
    "Or",
    "Not",
    "filter_from_dict",
    "Var",
    "Const",
    "Call",
    "Unify",
    "If",
    "NotExpr",
    "ExprIR",
    "RefLiteral",
    "ExprLiteral",
    "HeadSchema",
    "Body",
    "Rule",
    "Query",
    "RuleValidator",
    "Library",
    "LibrarySpec",
    "LibraryRuntime",
    "build_pydantic_rule_model",
    "build_responses_schema",
    "build_predicate_catalog",
    "DataProvider",
    "CSVProvider",
    "CSVSource",
    "FactInstance",
    "Renderer",
    "ProbLogRenderer",
    "PrologRenderer",
    "DatalogRenderer",
    "CypherRenderer",
    "RenderContext",
    "ProbabilityConfig",
]
