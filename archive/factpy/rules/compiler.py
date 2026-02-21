"""Rule compiler: DSL expressions to target-neutral RuleIR clauses."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Iterable

from ..compiler import EntityRef, EntityRefConst, FactPyCompileError
from ..ir import (
    Atom,
    Builtin,
    Clause,
    Const,
    PredicateRuleSpecIR,
    SchemaIR,
    Var,
)
from .dsl import (
    BuiltinExpr,
    FieldConstraint,
    InConstraint,
    LogicVar,
    PredicateCall,
    Rule,
    StructuredEntityExpression,
)


class RuleCompileError(FactPyCompileError):
    """Raised when rule DSL cannot be compiled into valid RuleIR."""


@dataclass(frozen=True)
class _DerivedPredicate:
    name: str
    arity: int


class RuleCompiler:
    def __init__(self, schema_ir: SchemaIR, *, audit_mode: bool = False) -> None:
        self.schema_ir = schema_ir
        self.audit_mode = audit_mode

        self._spec_by_view: dict[str, PredicateRuleSpecIR] = {
            item.view_predicate: item for item in schema_ir.rule_predicates
        }
        self._spec_by_base: dict[str, PredicateRuleSpecIR] = {
            item.base_predicate: item for item in schema_ir.rule_predicates
        }
        self._field_spec_index: dict[tuple[str, str], PredicateRuleSpecIR] = {}
        for mapping in schema_ir.field_mappings:
            spec = self._spec_by_base.get(mapping.base_predicate)
            if spec is not None:
                self._field_spec_index[(mapping.owner_entity, mapping.field_name)] = spec

        self._var_domains: dict[str, str] = {}
        self._auto_var_counter = 0

    def compile(self, rules: Iterable[Rule]) -> tuple[Clause, ...]:
        rule_list = list(rules)
        derived = self._collect_derived_signatures(rule_list)
        clauses: list[Clause] = []
        for idx, rule in enumerate(rule_list):
            clauses.append(self._compile_rule(rule, derived=derived, rule_index=idx))
        return tuple(clauses)

    def compile_rule(self, rule: Rule) -> Clause:
        derived = self._collect_derived_signatures([rule])
        return self._compile_rule(rule, derived=derived, rule_index=0)

    def _collect_derived_signatures(self, rules: list[Rule]) -> dict[str, _DerivedPredicate]:
        out: dict[str, _DerivedPredicate] = {}
        for idx, rule in enumerate(rules):
            if not isinstance(rule.head, PredicateCall):
                raise RuleCompileError(f"Rule[{idx}] head must be a predicate call.")
            pred_name = rule.head.predicate
            arity = len(rule.head.args)
            current = out.get(pred_name)
            if current is None:
                out[pred_name] = _DerivedPredicate(name=pred_name, arity=arity)
                continue
            if current.arity != arity:
                raise RuleCompileError(
                    f"Derived predicate '{pred_name}' has inconsistent head arity: {current.arity} vs {arity}."
                )
        return out

    def _compile_rule(
        self,
        rule: Rule,
        *,
        derived: dict[str, _DerivedPredicate],
        rule_index: int,
    ) -> Clause:
        self._var_domains = {}
        self._auto_var_counter = 0

        head = self._compile_head(rule.head, derived=derived, rule_index=rule_index)
        body_terms: list[Atom | Builtin] = []
        for item_index, body_item in enumerate(rule.body):
            body_terms.extend(
                self._compile_body_item(
                    body_item,
                    derived=derived,
                    rule_index=rule_index,
                    body_index=item_index,
                )
            )
        return Clause(head=head, body=tuple(body_terms))

    def _compile_head(
        self,
        expr: object,
        *,
        derived: dict[str, _DerivedPredicate],
        rule_index: int,
    ) -> Atom:
        if not isinstance(expr, PredicateCall):
            raise RuleCompileError(f"Rule[{rule_index}] head must be PredicateCall.")
        return self._compile_predicate_call(
            expr,
            context="head",
            derived=derived,
            rule_index=rule_index,
            body_index=None,
        )

    def _compile_body_item(
        self,
        expr: object,
        *,
        derived: dict[str, _DerivedPredicate],
        rule_index: int,
        body_index: int,
    ) -> list[Atom | Builtin]:
        if isinstance(expr, FieldConstraint):
            return self._compile_field_constraint(expr, rule_index=rule_index, body_index=body_index)
        if isinstance(expr, BuiltinExpr):
            return [self._compile_builtin(expr, rule_index=rule_index, body_index=body_index)]
        if isinstance(expr, PredicateCall):
            return [
                self._compile_predicate_call(
                    expr,
                    context="body",
                    derived=derived,
                    rule_index=rule_index,
                    body_index=body_index,
                )
            ]
        if isinstance(expr, StructuredEntityExpression):
            return self._compile_structured_entity(
                expr,
                rule_index=rule_index,
                body_index=body_index,
            )
        if isinstance(expr, InConstraint):
            raise RuleCompileError(
                f"Rule[{rule_index}] body[{body_index}] in_() is not supported in Horn core compiler yet."
            )
        raise RuleCompileError(
            f"Rule[{rule_index}] body[{body_index}] has unsupported expression type: {type(expr).__name__}"
        )

    def _compile_field_constraint(
        self,
        constraint: FieldConstraint,
        *,
        rule_index: int,
        body_index: int,
    ) -> list[Atom | Builtin]:
        key = (constraint.path.entity_cls.__name__, constraint.path.field_name)
        spec = self._field_spec_index.get(key)
        if spec is None:
            raise RuleCompileError(
                f"Rule[{rule_index}] body[{body_index}] references unknown field '{key[0]}.{key[1]}'."
            )
        if spec.logical_arity != 2:
            raise RuleCompileError(
                f"Rule[{rule_index}] body[{body_index}] field '{key[0]}.{key[1]}' requires explicit dims; "
                "PathExpression only supports binary view predicates."
            )

        left = self._to_term(
            constraint.path.subject,
            expected_domain=spec.arg_domains[0],
            where=f"Rule[{rule_index}] body[{body_index}] left",
        )

        if constraint.op == "eq":
            right = self._to_term(
                constraint.target,
                expected_domain=spec.arg_domains[1],
                where=f"Rule[{rule_index}] body[{body_index}] right",
            )
            return [Atom(predicate=spec.view_predicate, args=(left, right))]

        if constraint.op == "neq":
            hidden = self._new_auto_var(prefix="v")
            hidden_term = self._to_term(
                hidden,
                expected_domain=spec.arg_domains[1],
                where=f"Rule[{rule_index}] body[{body_index}] hidden",
            )
            target = self._to_term(
                constraint.target,
                expected_domain=spec.arg_domains[1],
                where=f"Rule[{rule_index}] body[{body_index}] right",
            )
            atom = Atom(predicate=spec.view_predicate, args=(left, hidden_term))
            builtin = Builtin(op="neq", left=hidden_term, right=target)
            return [atom, builtin]

        raise RuleCompileError(
            f"Rule[{rule_index}] body[{body_index}] unsupported field constraint op '{constraint.op}'."
        )

    def _compile_structured_entity(
        self,
        expr: StructuredEntityExpression,
        *,
        rule_index: int,
        body_index: int,
    ) -> list[Atom | Builtin]:
        hidden = self._new_auto_var(prefix="e")
        hidden_term = self._to_term(
            hidden,
            expected_domain="entity",
            where=f"Rule[{rule_index}] body[{body_index}] hidden entity",
        )

        out: list[Atom | Builtin] = []
        for field_name, value in expr.field_terms.items():
            key = (expr.entity_cls.__name__, field_name)
            spec = self._field_spec_index.get(key)
            if spec is None:
                raise RuleCompileError(
                    f"Rule[{rule_index}] body[{body_index}] unknown structured field '{key[0]}.{key[1]}'."
                )
            if spec.logical_arity != 2:
                raise RuleCompileError(
                    f"Rule[{rule_index}] body[{body_index}] structured expansion for '{key[0]}.{key[1]}' "
                    "requires explicit dims and is not supported in minimal compiler."
                )

            right = self._to_term(
                value,
                expected_domain=spec.arg_domains[1],
                where=f"Rule[{rule_index}] body[{body_index}] structured value '{field_name}'",
            )
            out.append(Atom(predicate=spec.view_predicate, args=(hidden_term, right)))
        return out

    def _compile_predicate_call(
        self,
        call: PredicateCall,
        *,
        context: str,
        derived: dict[str, _DerivedPredicate],
        rule_index: int,
        body_index: int | None,
    ) -> Atom:
        predicate, spec = self._resolve_predicate(call.predicate, context=context)

        if context == "body" and spec is None and predicate not in derived:
            where = f"Rule[{rule_index}] body[{body_index}]"
            raise RuleCompileError(f"{where} unknown predicate '{call.predicate}'.")

        if spec is not None:
            expected_arity = spec.logical_arity
            if len(call.args) != expected_arity:
                where = f"Rule[{rule_index}] {context}"
                if body_index is not None:
                    where = f"Rule[{rule_index}] body[{body_index}]"
                raise RuleCompileError(
                    f"{where} predicate '{predicate}' arity mismatch: expected {expected_arity}, got {len(call.args)}."
                )
            args = tuple(
                self._to_term(
                    value,
                    expected_domain=spec.arg_domains[idx],
                    where=f"Rule[{rule_index}] {context} arg[{idx}]",
                )
                for idx, value in enumerate(call.args)
            )
            return Atom(predicate=predicate, args=args)

        derived_spec = derived.get(predicate)
        if derived_spec is None:
            where = f"Rule[{rule_index}] {context}"
            if body_index is not None:
                where = f"Rule[{rule_index}] body[{body_index}]"
            raise RuleCompileError(f"{where} unknown predicate '{predicate}'.")
        if len(call.args) != derived_spec.arity:
            raise RuleCompileError(
                f"Rule[{rule_index}] {context} predicate '{predicate}' arity mismatch: "
                f"expected {derived_spec.arity}, got {len(call.args)}."
            )
        args = tuple(
            self._to_term(value, expected_domain=None, where=f"Rule[{rule_index}] {context} arg[{idx}]")
            for idx, value in enumerate(call.args)
        )
        return Atom(predicate=predicate, args=args)

    def _resolve_predicate(self, predicate: str, *, context: str) -> tuple[str, PredicateRuleSpecIR | None]:
        if context == "body" and predicate.startswith(("claim", "meta_")) and not self.audit_mode:
            raise RuleCompileError(
                f"Predicate '{predicate}' is claim/meta layer; enable audit_mode to reference it in rule body."
            )

        spec = self._spec_by_view.get(predicate)
        if spec is not None:
            return predicate, spec

        base_spec = self._spec_by_base.get(predicate)
        if base_spec is not None:
            if context == "body":
                return base_spec.view_predicate, base_spec
            return predicate, base_spec

        return predicate, None

    def _compile_builtin(self, builtin: BuiltinExpr, *, rule_index: int, body_index: int) -> Builtin:
        op = builtin.op.lower()
        if op not in {"eq", "neq", "lt", "le", "gt", "ge"}:
            raise RuleCompileError(f"Rule[{rule_index}] body[{body_index}] unsupported builtin op '{builtin.op}'.")

        left = self._to_term(
            builtin.left,
            expected_domain="value",
            where=f"Rule[{rule_index}] body[{body_index}] builtin left",
        )
        right = self._to_term(
            builtin.right,
            expected_domain="value",
            where=f"Rule[{rule_index}] body[{body_index}] builtin right",
        )
        return Builtin(op=op, left=left, right=right)

    def _to_term(self, value: object, *, expected_domain: str | None, where: str) -> Var | Const:
        if isinstance(value, LogicVar):
            if expected_domain is not None:
                self._bind_var_domain(value.name, expected_domain, where=where)
            return Var(name=value.name)

        if isinstance(value, Var):
            if expected_domain is not None:
                self._bind_var_domain(value.name, expected_domain, where=where)
            return value

        if isinstance(value, EntityRefConst):
            if expected_domain is not None and expected_domain != "entity":
                raise RuleCompileError(f"{where} expects value-domain term, got entity_ref constant.")
            return Const(value=value.ref, type_tag="entity_ref")

        if isinstance(value, EntityRef):
            if expected_domain is not None and expected_domain != "entity":
                raise RuleCompileError(f"{where} expects value-domain term, got entity_ref constant.")
            return Const(value=value.ref, type_tag="entity_ref")

        if isinstance(value, str):
            if value.startswith("tup1:"):
                if expected_domain is not None and expected_domain == "entity":
                    raise RuleCompileError(f"{where} expects entity term, got typed tuple constant.")
                return Const(value=value, type_tag="typed_tuple_v1")
            if expected_domain == "entity":
                raise RuleCompileError(f"{where} expects entity_ref term, got string literal.")
            return Const(value=value, type_tag="string")

        if isinstance(value, bool):
            if expected_domain == "entity":
                raise RuleCompileError(f"{where} expects entity_ref term, got bool literal.")
            return Const(value=value, type_tag="bool")

        if isinstance(value, int):
            if expected_domain == "entity":
                raise RuleCompileError(f"{where} expects entity_ref term, got int literal.")
            return Const(value=value, type_tag="int")

        if isinstance(value, float):
            if expected_domain == "entity":
                raise RuleCompileError(f"{where} expects entity_ref term, got float literal.")
            return Const(value=value, type_tag="float")

        if isinstance(value, date) and not isinstance(value, datetime):
            if expected_domain == "entity":
                raise RuleCompileError(f"{where} expects entity_ref term, got date literal.")
            return Const(value=value.isoformat(), type_tag="date")

        if isinstance(value, datetime):
            if expected_domain == "entity":
                raise RuleCompileError(f"{where} expects entity_ref term, got datetime literal.")
            return Const(value=value.isoformat(), type_tag="datetime")

        if value is None:
            if expected_domain == "entity":
                raise RuleCompileError(f"{where} expects entity_ref term, got null.")
            return Const(value=None, type_tag="null")

        if isinstance(value, PredicateCall):
            raise RuleCompileError(f"{where} predicate call cannot be used as a term.")

        raise RuleCompileError(f"{where} unsupported term type: {type(value).__name__}")

    def _bind_var_domain(self, name: str, domain: str, *, where: str) -> None:
        if domain not in {"entity", "value"}:
            return
        current = self._var_domains.get(name)
        if current is None:
            self._var_domains[name] = domain
            return
        if current != domain:
            raise RuleCompileError(
                f"{where} variable '{name}' domain conflict: expected {domain}, already bound as {current}."
            )

    def _new_auto_var(self, *, prefix: str) -> LogicVar:
        self._auto_var_counter += 1
        return LogicVar(name=f"_{prefix}{self._auto_var_counter}")
