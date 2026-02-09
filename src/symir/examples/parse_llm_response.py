"""Parse OpenAI Responses API output into Rule IR (compact/verbose)."""

from __future__ import annotations

import json
from typing import Any

from symir.rules.constraint_schemas import build_pydantic_rule_model
from symir.ir.rule_schema import Rule, Body, RefLiteral, ExprLiteral
from symir.ir.expr_ir import Var, Const, expr_from_dict
from symir.rules.validator import RuleValidator


def _extract_json(resp) -> dict[str, Any]:
    """Extract JSON payload from Responses API result."""

    if getattr(resp, "output_text", None):
        return json.loads(resp.output_text)
    for item in getattr(resp, "output", []):
        for c in getattr(item, "content", []):
            if getattr(c, "type", None) == "output_text":
                return json.loads(c.text)
    raise ValueError("No JSON output found in response.")


def _term_from_value(value: dict[str, Any]):
    if value["kind"] == "var":
        return Var(name=value["name"], datatype=value.get("datatype"))
    if value["kind"] == "const":
        return Const(value=value["value"], datatype=value["datatype"])
    raise ValueError(f"Unknown term kind: {value.get('kind')}")


def resp_to_rule(
    resp,
    *,
    head,
    view,
    library=None,
    mode: str = "compact",
) -> Rule:
    """Parse a Responses API output into Rule IR.

    Args:
        resp: Responses API output object.
        head: HeadSchema (fixed externally).
        view: FactView for validation.
        library: Optional Library (for predicate/expr ops).
        mode: "compact" or "verbose" decoding.
    """

    payload = _extract_json(resp)
    model = build_pydantic_rule_model(view, library=library, mode=mode)
    validated = model.model_validate(payload)

    bodies: list[Body] = []
    for body in validated.bodies:
        literals = []
        for lit in body.literals:
            if lit.kind == "ref":
                if mode == "compact":
                    terms = [_term_from_value(arg.value.model_dump()) for arg in lit.args]
                else:
                    terms = [_term_from_value(term.model_dump()) for term in lit.terms]
                literals.append(
                    RefLiteral(predicate_id=lit.predicate_id, terms=terms, negated=lit.negated)
                )
            elif lit.kind == "expr":
                literals.append(ExprLiteral(expr=expr_from_dict(lit.expr.model_dump())))
            else:
                raise ValueError(f"Unknown literal kind: {lit.kind}")
        bodies.append(Body(literals=literals, prob=body.prob))

    rule = Rule(head=head, bodies=bodies)
    RuleValidator(view, library=library).validate(rule)
    return rule
