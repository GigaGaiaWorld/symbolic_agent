"""Renderer interfaces and ProbLog implementation for rule IR."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Optional, Callable, Literal

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
    problog_var_mode: Literal["error", "sanitize", "prefix", "capitalize"] = "sanitize"
    problog_var_prefix: str = "VAR_"


@dataclass
class _VarNamePolicy:
    mode: Literal["error", "sanitize", "prefix", "capitalize"]
    prefix: str
    _mapping: dict[str, str] = field(default_factory=dict)
    _used: set[str] = field(default_factory=set)

    _VALID_PATTERN = re.compile(r"^[A-Z][A-Za-z0-9_]*$")
    _SAFE_PATTERN = re.compile(r"[^A-Za-z0-9_]")

    def render(self, name: str) -> str:
        if name in self._mapping:
            return self._mapping[name]
        candidate = self._candidate(name)
        unique = self._dedupe(candidate)
        self._mapping[name] = unique
        self._used.add(unique)
        return unique

    def _candidate(self, name: str) -> str:
        if self.mode == "error":
            if not self._is_valid(name):
                raise RenderError(
                    f"Invalid ProbLog variable name: {name}. "
                    "Use uppercase-leading names or set a non-error variable mode."
                )
            return name
        if self.mode == "capitalize":
            if not name:
                base = "V"
            else:
                base = name[0].upper() + name[1:]
            return self._force_valid(base)
        if self.mode == "prefix":
            token = self._sanitize_token(name)
            return self._force_valid(f"{self.prefix}{token}")
        # sanitize (default)
        base = self._sanitize_token(name)
        return self._force_valid(base)

    def _sanitize_token(self, name: str) -> str:
        cleaned = self._SAFE_PATTERN.sub("_", name)
        cleaned = cleaned.strip("_")
        if not cleaned:
            return "V"
        return cleaned

    def _force_valid(self, token: str) -> str:
        safe = self._SAFE_PATTERN.sub("_", token)
        if not safe:
            safe = "V"
        if safe[0].isdigit():
            safe = f"V_{safe}"
        if not safe[0].isupper():
            safe = safe[0].upper() + safe[1:]
        if not self._is_valid(safe):
            safe = f"V_{self._sanitize_token(safe)}"
            if not safe[0].isupper():
                safe = safe[0].upper() + safe[1:]
        return safe

    def _is_valid(self, name: str) -> bool:
        return bool(self._VALID_PATTERN.match(name))

    def _dedupe(self, base: str) -> str:
        if base not in self._used:
            return base
        idx = 2
        while f"{base}_{idx}" in self._used:
            idx += 1
        return f"{base}_{idx}"


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
        mode, prefix = self._resolve_var_rendering(rule, context)
        clauses: list[str] = []
        for idx, cond in enumerate(rule.conditions):
            var_policy = _VarNamePolicy(mode=mode, prefix=prefix)
            prob = resolve_probability(
                cond.prob,
                default_value=context.prob_config.default_rule_prob,
                policy=context.prob_config.missing_prob_policy,
                context=f"rule {rule.predicate.name} condition {idx}",
            )
            head_text = self._render_head(rule.predicate, prob, var_policy)
            body_text = self._render_body(cond, context, var_policy)
            clauses.append(f"{head_text} :- {body_text}.")
        return "\n".join(clauses)

    def render_facts(self, facts: list[Instance], context: RenderContext) -> str:
        lines: list[str] = []
        var_policy = _VarNamePolicy(
            mode=context.problog_var_mode,
            prefix=context.problog_var_prefix,
        )
        for idx, fact in enumerate(facts):
            prob = resolve_probability(
                fact.prob,
                default_value=context.prob_config.default_fact_prob,
                policy=context.prob_config.missing_prob_policy,
                context=f"fact {idx}",
            )
            pred_name, arity, runtime_handler = self._resolve_predicate(fact.schema_id, context)
            schema = context.schema.get(fact.schema_id)
            terms = [self._render_term(t, var_policy) for t in fact.to_terms(schema)]
            if runtime_handler is not None:
                atom = runtime_handler(terms)
            else:
                atom = f"{pred_name}({', '.join(terms)})" if arity > 0 else pred_name
            prefix = f"{prob}::" if prob is not None else ""
            lines.append(f"{prefix}{atom}.")
        return "\n".join(lines)

    def render_query(self, query: Query, context: RenderContext) -> str:
        var_policy = _VarNamePolicy(
            mode=context.problog_var_mode,
            prefix=context.problog_var_prefix,
        )
        if query.predicate_id is not None:
            pred_name, arity, runtime_handler = self._resolve_predicate(query.predicate_id, context)
        else:
            pred_name = query.predicate.name
            arity = query.predicate.arity
            runtime_handler = None
        terms = [self._render_term(t, var_policy) for t in query.terms]
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

    def _render_head(self, predicate, prob: float, var_policy: _VarNamePolicy) -> str:
        terms = ", ".join(self._render_term(t, var_policy) for t in self._head_terms(predicate))
        prefix = f"{prob}::" if prob is not None else ""
        if predicate.arity == 0:
            return f"{prefix}{predicate.name}"
        return f"{prefix}{predicate.name}({terms})"

    def _render_body(self, body: Cond, context: RenderContext, var_policy: _VarNamePolicy) -> str:
        if not body.literals:
            return "true"
        return ", ".join(self._render_literal(lit, context, var_policy) for lit in body.literals)

    def _render_literal(self, literal, context: RenderContext, var_policy: _VarNamePolicy) -> str:
        if isinstance(literal, Ref):
            return self._render_ref(literal, context, negate=literal.negated, var_policy=var_policy)
        if isinstance(literal, Expr):
            return self._render_expr(literal.expr, context, var_policy)
        raise RenderError("Unknown literal type.")

    def _render_expr(self, expr: ExprIR, context: RenderContext, var_policy: _VarNamePolicy) -> str:
        if isinstance(expr, Var):
            return var_policy.render(expr.name)
        if isinstance(expr, Const):
            return self._render_const(expr)
        if isinstance(expr, Ref):
            if expr.negated:
                raise RenderError("Negated ref not allowed in expression context.")
            return self._render_ref(expr, context, negate=False, var_policy=var_policy)
        if isinstance(expr, Unify):
            return (
                f"{self._render_expr(expr.lhs, context, var_policy)} = "
                f"{self._render_expr(expr.rhs, context, var_policy)}"
            )
        if isinstance(expr, Call):
            return self._render_call(expr, context, var_policy)
        if isinstance(expr, If):
            cond = self._render_expr(expr.cond, context, var_policy)
            then = self._render_expr(expr.then, context, var_policy)
            else_ = self._render_expr(expr.else_, context, var_policy)
            # ProbLog does not support '->' control syntax; use branch-style disjunction.
            # (Cond, Then ; \+ Cond, Else)
            return f"(({cond}, {then}) ; (\\+ ({cond}), {else_}))"
        if isinstance(expr, NotExpr):
            return f"\\+ {self._render_expr(expr.expr, context, var_policy)}"
        raise RenderError(f"Unsupported ExprIR type: {type(expr)}")

    def _render_call(self, call: Call, context: RenderContext, var_policy: _VarNamePolicy) -> str:
        op = call.op
        args = [self._render_expr(arg, context, var_policy) for arg in call.args]
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

    def _render_term(self, term, var_policy: _VarNamePolicy) -> str:
        if isinstance(term, Var):
            return var_policy.render(term.name)
        if isinstance(term, Const):
            return self._render_const(term)
        if isinstance(term, (str, int, float, bool)):
            return self._render_const(Const(value=term))
        raise RenderError("Ref term must be Var/Const.")

    def _head_terms(self, predicate) -> list[ExprIR]:
        if getattr(predicate, "kind", None) == "rel":
            prop_names = [
                arg.name
                for arg in (getattr(predicate, "props", None) or [])
                if getattr(arg, "name", None)
            ]
            return [Var("Sub"), Var("Obj"), *[Var(name) for name in prop_names]]
        return [Var(arg.name) for arg in predicate.signature]

    def _render_ref(
        self,
        ref: Ref,
        context: RenderContext,
        *,
        negate: bool,
        var_policy: _VarNamePolicy,
    ) -> str:
        pred_name, arity, runtime_handler = self._resolve_predicate(ref.schema, context)
        terms = [self._render_term(t, var_policy) for t in ref.terms]
        if runtime_handler is not None:
            atom = runtime_handler(terms)
        else:
            atom = f"{pred_name}({', '.join(terms)})" if arity > 0 else pred_name
        return f"\\+ {atom}" if negate else atom

    def _resolve_var_rendering(
        self, rule: Rule, context: RenderContext
    ) -> tuple[Literal["error", "sanitize", "prefix", "capitalize"], str]:
        mode: str = context.problog_var_mode
        prefix = context.problog_var_prefix
        hints = rule.render_hints or {}
        if "problog_var_mode" in hints:
            mode = str(hints["problog_var_mode"])
        if "problog_var_prefix" in hints:
            prefix = str(hints["problog_var_prefix"])
        problog_hints = hints.get("problog")
        if isinstance(problog_hints, dict):
            if "var_mode" in problog_hints:
                mode = str(problog_hints["var_mode"])
            if "var_prefix" in problog_hints:
                prefix = str(problog_hints["var_prefix"])
        normalized_mode = mode.strip().lower()
        allowed = {"error", "sanitize", "prefix", "capitalize"}
        if normalized_mode not in allowed:
            raise RenderError(
                f"Unknown problog var mode: {mode}. "
                f"Expected one of {sorted(allowed)}."
            )
        if not isinstance(prefix, str) or not prefix:
            prefix = "VAR_"
        return normalized_mode, prefix

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
