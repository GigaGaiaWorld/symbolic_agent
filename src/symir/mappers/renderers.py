"""Renderer interfaces and ProbLog implementation for rule IR."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Callable

from symir.errors import RenderError
from symir.ir.rule_schema import Rule, Cond, Expr, Query
from symir.ir.expr_ir import Var, Const, Call, Unify, If, NotExpr, ExprIR, Ref
from symir.ir.fact_schema import FactSchema
from symir.ir.instance import Instance
from symir.rules.library import Library
from symir.rules.library_runtime import LibraryRuntime
from symir.probability import ProbabilityConfig, resolve_probability


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
        self, facts: list[Instance], context: RenderContext
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
        facts: list[Instance],
        rules: list[Rule],
        context: RenderContext,
        queries: list[Query] | None = None,
    ) -> str:
        raise NotImplementedError


class ProbLogRenderer(Renderer):
    backend = "problog"

    def render_rule(self, rule: Rule, context: RenderContext) -> str:
        clauses: list[str] = []
        for idx, cond in enumerate(rule.conditions):
            prob = resolve_probability(
                cond.prob,
                default_value=context.prob_config.default_rule_prob,
                policy=context.prob_config.missing_prob_policy,
                context=f"rule {rule.predicate.name} condition {idx}",
            )
            head_text = self._render_head(rule.predicate, prob)
            body_text = self._render_body(cond, context)
            clauses.append(f"{head_text} :- {body_text}.")
        return "\n".join(clauses)

    def render_facts(self, facts: list[Instance], context: RenderContext) -> str:
        lines: list[str] = []
        for idx, fact in enumerate(facts):
            prob = resolve_probability(
                fact.prob,
                default_value=context.prob_config.default_fact_prob,
                policy=context.prob_config.missing_prob_policy,
                context=f"fact {idx}",
            )
            pred_name, arity, runtime_handler = self._resolve_predicate(fact.schema_id, context)
            schema = context.schema.get(fact.schema_id)
            terms = [self._render_term(t) for t in fact.to_terms(schema)]
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
        facts: list[Instance],
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

    def _render_head(self, predicate, prob: float) -> str:
        terms = ", ".join(self._render_term(t) for t in self._head_terms(predicate))
        prefix = f"{prob}::" if prob is not None else ""
        if predicate.arity == 0:
            return f"{prefix}{predicate.name}"
        return f"{prefix}{predicate.name}({terms})"

    def _render_body(self, body: Cond, context: RenderContext) -> str:
        if not body.literals:
            return "true"
        return ", ".join(self._render_literal(lit, context) for lit in body.literals)

    def _render_literal(self, literal, context: RenderContext) -> str:
        if isinstance(literal, Ref):
            return self._render_ref(literal, context, negate=literal.negated)
        if isinstance(literal, Expr):
            return self._render_expr(literal.expr, context)
        raise RenderError("Unknown literal type.")

    def _render_expr(self, expr: ExprIR, context: RenderContext) -> str:
        if isinstance(expr, Var):
            return expr.name
        if isinstance(expr, Const):
            return self._render_const(expr)
        if isinstance(expr, Ref):
            if expr.negated:
                raise RenderError("Negated ref not allowed in expression context.")
            return self._render_ref(expr, context, negate=False)
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
        if isinstance(term, (str, int, float, bool)):
            return self._render_const(Const(value=term))
        raise RenderError("Ref term must be Var/Const.")

    def _head_terms(self, predicate) -> list[ExprIR]:
        return [Var(arg.name) for arg in predicate.signature]

    def _render_ref(self, ref: Ref, context: RenderContext, *, negate: bool) -> str:
        pred_name, arity, runtime_handler = self._resolve_predicate(ref.schema, context)
        terms = [self._render_term(t) for t in ref.terms]
        if runtime_handler is not None:
            atom = runtime_handler(terms)
        else:
            atom = f"{pred_name}({', '.join(terms)})" if arity > 0 else pred_name
        return f"\\+ {atom}" if negate else atom

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
