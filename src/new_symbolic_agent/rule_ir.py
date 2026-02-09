"""Unified entrypoint for rule schema/IR and related utilities."""

from __future__ import annotations

from new_symbolic_agent.ir.fact_schema import ArgSpec, PredicateSchema, FactSchema, FactView
from new_symbolic_agent.ir.filters import FilterAST, PredMatch, And, Or, Not, filter_from_dict
from new_symbolic_agent.ir.expr_ir import Var, Const, Call, Unify, If, NotExpr, ExprIR
from new_symbolic_agent.ir.rule_schema import RefLiteral, ExprLiteral, HeadSchema, Body, Rule, Query
from new_symbolic_agent.rules.validator import RuleValidator
from new_symbolic_agent.rules.library import Library, LibrarySpec
from new_symbolic_agent.rules.library_runtime import LibraryRuntime
from new_symbolic_agent.rules.constraint_schemas import (
    build_pydantic_rule_model,
    build_responses_schema,
    build_predicate_catalog,
)
from new_symbolic_agent.fact_store.provider import DataProvider, CSVProvider, CSVSource, FactInstance
from new_symbolic_agent.mappers.renderers import (
    Renderer,
    ProbLogRenderer,
    PrologRenderer,
    DatalogRenderer,
    CypherRenderer,
    RenderContext,
)
from new_symbolic_agent.probability import ProbabilityConfig

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
