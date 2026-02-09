"""Renderer interfaces and ProbLog implementation for rule IR."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Callable

from new_symbolic_agent.errors import RenderError
from new_symbolic_agent.ir.rule_schema import Rule, Body, RefLiteral, ExprLiteral, Query
from new_symbolic_agent.ir.expr_ir import Var, Const, Call, Unify, If, NotExpr, ExprIR
from new_symbolic_agent.ir.fact_schema import FactSchema
from new_symbolic_agent.fact_store.provider import FactInstance
from new_symbolic_agent.rules.library import Library
from new_symbolic_agent.rules.library_runtime import LibraryRuntime
from new_symbolic_agent.probability import ProbabilityConfig, resolve_probability


@dataclass(frozen=True)
class RenderContext:
    schema: FactSchema
    library: Optional[Library] = None
    library_runtime: Optional[LibraryRuntime] = None
    prob_config: ProbabilityConfig = ProbabilityConfig()


class Renderer:
    """Base renderer interface."""

    backend: str = "base"

    def render_rule(self, rule: Rule, context: RenderContext) -> str:  # pragma: no cover - interface
        raise NotImplementedError

    def render_facts(  # pragma: no cover - interface
        self, facts: list[FactInstance], context: RenderContext
    ) -> str:
        raise NotImplementedError

    def render_query(  # pragma: no cover - interface
        self, query: Query, context: RenderContext
    ) -> str:
        raise NotImplementedError

    def render_queries(  # pragma: no cover - interface
        self, queries: list[Query], context: RenderContext
    ) -> str:
        raise NotImplementedError

    def render_program(  # pragma: no cover - interface
        self,
        facts: list[FactInstance],
        rules: list[Rule],
        context: RenderContext,
        queries: list[Query] | None = None,
    ) -> str:
        raise NotImplementedError


class ProbLogRenderer(Renderer):
    backend = "problog"

    def render_rule(self, rule: Rule, context: RenderContext) -> str:
        clauses: list[str] = []
        for idx, body in enumerate(rule.bodies):
            prob = resolve_probability(
                body.prob,
                default_value=context.prob_config.default_rule_prob,
                policy=context.prob_config.missing_prob_policy,
                context=f"rule {rule.head.predicate.name} body {idx}",
            )
            head_text = self._render_head(rule, prob)
            body_text = self._render_body(body, context)
            clauses.append(f"{head_text} :- {body_text}.")
        return "\n".join(clauses)

    def render_facts(self, facts: list[FactInstance], context: RenderContext) -> str:
        lines: list[str] = []
        for idx, fact in enumerate(facts):
            prob = resolve_probability(
                fact.prob,
                default_value=context.prob_config.default_fact_prob,
                policy=context.prob_config.missing_prob_policy,
                context=f"fact {idx}",
            )
            pred_name, arity, runtime_handler = self._resolve_predicate(fact.predicate_id, context)
            terms = [self._render_term(t) for t in fact.terms]
            if runtime_handler is not None:
                atom = runtime_handler(terms)
            else:
                atom = f"{pred_name}({', '.join(terms)})" if arity > 0 else pred_name
            prefix = f"{prob}::" if prob is not None else ""
            lines.append(f"{prefix}{atom}.")
        return "\n".join(lines)

    def render_query(self, query: Query, context: RenderContext) -> str:
        if query.predicate_id is not None:
            pred_name, arity, runtime_handler = self._resolve_predicate(query.predicate_id, context)
        else:
            pred_name = query.predicate.name
            arity = query.predicate.arity
            runtime_handler = None
        terms = [self._render_term(t) for t in query.terms]
        if runtime_handler is not None:
            atom = runtime_handler(terms)
        else:
            atom = f"{pred_name}({', '.join(terms)})" if arity > 0 else pred_name
        return f"query({atom})."

    def render_queries(self, queries: list[Query], context: RenderContext) -> str:
        return "\n".join(self.render_query(q, context) for q in queries)

    def render_program(
        self,
        facts: list[FactInstance],
        rules: list[Rule],
        context: RenderContext,
        queries: list[Query] | None = None,
    ) -> str:
        parts: list[str] = []
        if facts:
            parts.append(self.render_facts(facts, context))
        if rules:
            parts.append("\n".join(self.render_rule(rule, context) for rule in rules))
        if queries:
            parts.append(self.render_queries(queries, context))
        return "\n\n".join(part for part in parts if part)

    def _render_head(self, rule: Rule, prob: float) -> str:
        terms = ", ".join(self._render_term(t) for t in rule.head.terms)
        prefix = f"{prob}::" if prob is not None else ""
        if rule.head.predicate.arity == 0:
            return f"{prefix}{rule.head.predicate.name}"
        return f"{prefix}{rule.head.predicate.name}({terms})"

    def _render_body(self, body: Body, context: RenderContext) -> str:
        if not body.literals:
            return "true"
        return ", ".join(self._render_literal(lit, context) for lit in body.literals)

    def _render_literal(self, literal, context: RenderContext) -> str:
        if isinstance(literal, RefLiteral):
            pred_name, arity, runtime_handler = self._resolve_predicate(literal.predicate_id, context)
            terms = [self._render_term(t) for t in literal.terms]
            if runtime_handler is not None:
                atom = runtime_handler(terms)
            else:
                atom = f"{pred_name}({', '.join(terms)})" if arity > 0 else pred_name
            return f"\\+ {atom}" if literal.negated else atom
        if isinstance(literal, ExprLiteral):
            return self._render_expr(literal.expr, context)
        raise RenderError("Unknown literal type.")

    def _render_expr(self, expr: ExprIR, context: RenderContext) -> str:
        if isinstance(expr, Var):
            return expr.name
        if isinstance(expr, Const):
            return self._render_const(expr)
        if isinstance(expr, Unify):
            return f"{self._render_expr(expr.lhs, context)} = {self._render_expr(expr.rhs, context)}"
        if isinstance(expr, Call):
            return self._render_call(expr, context)
        if isinstance(expr, If):
            cond = self._render_expr(expr.cond, context)
            then = self._render_expr(expr.then, context)
            else_ = self._render_expr(expr.else_, context)
            # ProbLog does not support '->' control syntax; use branch-style disjunction.
            # (Cond, Then ; \+ Cond, Else)
            return f"(({cond}, {then}) ; (\\+ ({cond}), {else_}))"
        if isinstance(expr, NotExpr):
            return f"\\+ {self._render_expr(expr.expr, context)}"
        raise RenderError(f"Unsupported ExprIR type: {type(expr)}")

    def _render_call(self, call: Call, context: RenderContext) -> str:
        op = call.op
        args = [self._render_expr(arg, context) for arg in call.args]
        builtin_infix = {
            "eq": "=",
            "ne": "\\=",
            "lt": "<",
            "le": "=<",
            "gt": ">",
            "ge": ">=",
        }
        builtin_arith = {
            "add": "+",
            "sub": "-",
            "mul": "*",
            "div": "/",
            "mod": "mod",
        }
        if op in builtin_infix and len(args) == 2:
            return f"{args[0]} {builtin_infix[op]} {args[1]}"
        if op in builtin_arith and len(args) == 2:
            if op == "mod":
                return f"{args[0]} mod {args[1]}"
            return f"{args[0]} {builtin_arith[op]} {args[1]}"
        if context.library_runtime:
            handler = context.library_runtime.get(op, len(args), "expr", self.backend)
            if handler is not None:
                return handler(args)
        mapping = None
        if context.library:
            mapping = context.library.resolve_mapping(op, len(args), "expr", self.backend)
        if mapping:
            return mapping.format(*args)
        return f"{op}({', '.join(args)})"

    def _render_term(self, term) -> str:
        if isinstance(term, Var):
            return term.name
        if isinstance(term, Const):
            return self._render_const(term)
        raise RenderError("RefLiteral term must be Var/Const.")

    def _render_const(self, const: Const) -> str:
        value = const.value
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float)):
            return str(value)
        if not isinstance(value, str):
            raise RenderError(f"Unsupported const type: {type(value)}")
        if value and value[0].islower() and value.replace("_", "").isalnum():
            return value
        escaped = value.replace("'", "\\'")
        return f"'{escaped}'"

    def _resolve_predicate(
        self, predicate_id: str, context: RenderContext
    ) -> tuple[str, int, Optional[Callable[[list[str]], str]]]:
        try:
            pred = context.schema.get(predicate_id)
            return pred.name, pred.arity, None
        except Exception:
            if context.library:
                spec = context.library.get_predicate_by_id(predicate_id)
                if spec:
                    handler = None
                    if context.library_runtime:
                        handler = context.library_runtime.get(spec.name, spec.arity, "predicate", self.backend)
                    mapped = context.library.resolve_mapping(spec.name, spec.arity, "predicate", self.backend)
                    return (mapped or spec.name), spec.arity, handler
        raise RenderError(f"Unknown predicate_id in renderer: {predicate_id}")


class PrologRenderer(ProbLogRenderer):
    backend = "prolog"


class DatalogRenderer(Renderer):
    backend = "datalog"

    def render_rule(self, rule: Rule, context: RenderContext) -> str:  # pragma: no cover - stub
        raise RenderError("Datalog renderer not implemented yet.")


class CypherRenderer(Renderer):
    backend = "cypher"

    def render_rule(self, rule: Rule, context: RenderContext) -> str:  # pragma: no cover - stub
        raise RenderError("Cypher renderer not implemented yet.")
