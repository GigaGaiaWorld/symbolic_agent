"""ProbLog mapper implementation."""

from __future__ import annotations

from typing import Iterable

from new_symbolic_agent.errors import MappingError
from new_symbolic_agent.ir.types import Const, IRAtom, IRProgram, IRRule, Var
from new_symbolic_agent.mappers.base import Mapper


class ProbLogMapper(Mapper):
    """Map IR programs into ProbLog syntax."""

    def to_language(self, program: IRProgram) -> str:
        return to_problog(program)


def to_problog(program: IRProgram) -> str:
    """Render an IR program into ProbLog syntax."""

    lines: list[str] = []
    if any(rule.prob is not None or rule.head.prob is not None for rule in program.rules):
        lines.append("% NOTE: probabilistic rules are rendered as head annotations (assumed syntax).")
    for fact in program.facts:
        lines.append(_format_atom_line(fact))
    for rule in program.rules:
        lines.extend(_format_rule(rule))
    return "\n".join(lines)


def _format_rule(rule: IRRule) -> list[str]:
    lines: list[str] = []
    if rule.kind == "rule_edge":
        lines.append("% kind: rule_edge")
    head_atom = _format_atom(rule.head, prefer_prob=rule.prob)
    if rule.body:
        body_text = ", ".join(_format_atom(atom) for atom in rule.body)
        lines.append(f"{head_atom} :- {body_text}.")
    else:
        lines.append(f"{head_atom}.")
    return lines


def _format_atom_line(atom: IRAtom) -> str:
    return f"{_format_atom(atom)}."


def _format_atom(atom: IRAtom, prefer_prob: float | None = None) -> str:
    prob = prefer_prob if prefer_prob is not None else atom.prob
    prefix = ""
    if prob is not None:
        prefix = f"{prob}::"
    neg = "\\+ " if atom.negated else ""
    terms = ", ".join(_format_term(t) for t in atom.terms)
    return f"{prefix}{neg}{atom.predicate.name}({terms})" if atom.predicate.arity > 0 else f"{prefix}{neg}{atom.predicate.name}"


def _format_term(term: Var | Const) -> str:
    if isinstance(term, Var):
        return term.name
    return _format_const(term.value)


def _format_const(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if not isinstance(value, str):
        raise MappingError(f"Unsupported constant type: {type(value)}")
    if value and value[0].islower() and value.replace("_", "").isalnum():
        return value
    escaped = value.replace("'", "\\'")
    return f"'{escaped}'"
