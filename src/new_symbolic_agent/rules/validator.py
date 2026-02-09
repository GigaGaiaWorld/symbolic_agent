"""Validation utilities for rules."""

from __future__ import annotations

from new_symbolic_agent.errors import ValidationError
from new_symbolic_agent.ir.fact_schema import FactView
from new_symbolic_agent.ir.rule_schema import Rule, RefLiteral, ExprLiteral
from new_symbolic_agent.ir.expr_ir import Var, Const
from new_symbolic_agent.rules.library import Library


class RuleValidator:
    """Validate rules against schema/view constraints."""

    def __init__(self, view: FactView, library: Library | None = None) -> None:
        self.view = view
        self.library = library
        self._allowed_predicate_ids = set(view.schema_ids)
        if library is not None:
            self._allowed_predicate_ids.update(library.predicate_ids().keys())

    def validate(self, rule: Rule) -> None:
        self._validate_head(rule)
        self._validate_bodies(rule)

    def _validate_head(self, rule: Rule) -> None:
        for term in rule.head.terms:
            if not isinstance(term, Var):
                raise ValidationError(
                    f"Head contains non-variable term: {term} (predicate {rule.head.predicate.schema_id})"
                )

    def _validate_bodies(self, rule: Rule) -> None:
        head_id = rule.head.predicate.schema_id
        for body_index, body in enumerate(rule.bodies):
            for lit_index, literal in enumerate(body.literals):
                if isinstance(literal, RefLiteral):
                    if literal.predicate_id not in self._allowed_predicate_ids:
                        raise ValidationError(
                            f"RefLiteral predicate not in allowed references: {literal.predicate_id}"
                        )
                    if literal.predicate_id == head_id:
                        raise ValidationError(
                            f"Direct recursion detected: head {head_id} refers to itself in body {body_index}"
                        )
                    for term in literal.terms:
                        if not isinstance(term, (Var, Const)):
                            raise ValidationError(
                                f"RefLiteral term must be Var/Const at body {body_index} literal {lit_index}"
                            )
                elif isinstance(literal, ExprLiteral):
                    continue
                else:  # pragma: no cover - safeguard
                    raise ValidationError("Unknown literal type.")
