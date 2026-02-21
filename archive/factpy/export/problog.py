"""ProbLog exporter for FactPy claim/meta/view programs."""

from __future__ import annotations

from datetime import date, datetime
from typing import Iterable

from ..compiler import Store
from ..er.model import CanonPolicyConfig
from ..ir import Atom, Builtin, Clause, Const, SchemaIR, Var
from ..rules.compiler import RuleCompiler
from ..rules.dsl import Rule
from .policy import PolicyMode, export_policy_artifacts


class ProbLogExporter:
    def render(
        self,
        *,
        store: Store,
        rules: Iterable[Rule] | None = None,
        schema_ir: SchemaIR | None = None,
        policy_mode: PolicyMode = "edb",
        canon_policy: CanonPolicyConfig | None = None,
    ) -> str:
        schema = schema_ir or store.schema_ir
        if schema is None:
            raise ValueError("ProbLogExporter requires schema_ir (or store.schema_ir).")

        rules_to_compile = list(store.rules if rules is None else rules)
        clauses: tuple[Clause, ...] = tuple()
        if rules_to_compile:
            clauses = RuleCompiler(schema).compile(rules_to_compile)

        artifacts = export_policy_artifacts(
            store=store,
            schema_ir=schema,
            policy_mode=policy_mode,
            canon_policy=canon_policy,
        )

        lines: list[str] = []
        lines.append("% EDB")
        for row in sorted(store.facts().get("claim", set())):
            lines.append(
                f"claim({self._fmt_value(row[0])}, {self._fmt_value(row[1])}, {self._fmt_value(row[2])}, {self._fmt_value(row[3])})."
            )

        for aid, idx, value, tag in artifacts.claim_arg_facts:
            lines.append(
                f"claim_arg({self._fmt_value(aid)}, {idx}, {self._fmt_value(value)}, {self._fmt_value(tag)})."
            )

        for item in artifacts.active_facts:
            lines.append(f"active({self._fmt_value(item[0])}).")
        for item in artifacts.chosen_facts:
            lines.append(f"chosen({self._fmt_value(item[0])}).")
        for row in artifacts.canon_chosen_facts:
            args = ", ".join(self._fmt_value(item) for item in row)
            lines.append(f"canon_of_chosen({args}).")

        lines.append("")
        lines.append("% Policy")
        lines.extend(artifacts.policy_rules)

        lines.append("")
        lines.append("% View")
        lines.extend(artifacts.view_rules)

        lines.append("")
        lines.append("% User Rules")
        for clause in clauses:
            lines.append(self._fmt_clause(clause))

        return "\n".join(lines).strip() + "\n"

    def _fmt_clause(self, clause: Clause) -> str:
        head = self._fmt_atom(clause.head)
        if not clause.body:
            return f"{head}."
        body = ", ".join(self._fmt_body_item(item) for item in clause.body)
        return f"{head} :- {body}."

    def _fmt_body_item(self, item: Atom | Builtin) -> str:
        if isinstance(item, Atom):
            return self._fmt_atom(item)
        op_map = {
            "eq": "=",
            "neq": "\\=",
            "lt": "<",
            "le": "=<",
            "gt": ">",
            "ge": ">=",
        }
        op = op_map[item.op]
        return f"{self._fmt_term(item.left)} {op} {self._fmt_term(item.right)}"

    def _fmt_atom(self, atom: Atom) -> str:
        args = ", ".join(self._fmt_term(arg) for arg in atom.args)
        return f"{atom.predicate}({args})"

    def _fmt_term(self, term: Var | Const) -> str:
        if isinstance(term, Var):
            if term.name.startswith("_"):
                suffix = term.name[1:] or "V"
                return f"V_{suffix.upper()}"
            return term.name.upper()
        return self._fmt_value(term.value)

    def _fmt_value(self, value: object) -> str:
        if value is None:
            return "nil"
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, (date, datetime)):
            return self._quote(str(value))
        return self._quote(str(value))

    def _quote(self, text: str) -> str:
        escaped = text.replace("\\", "\\\\").replace("'", "\\'")
        return f"'{escaped}'"


def export(
    *,
    store: Store,
    rules: Iterable[Rule] | None = None,
    schema_ir: SchemaIR | None = None,
    policy_mode: PolicyMode = "edb",
    canon_policy: CanonPolicyConfig | None = None,
) -> str:
    return ProbLogExporter().render(
        store=store,
        rules=rules,
        schema_ir=schema_ir,
        policy_mode=policy_mode,
        canon_policy=canon_policy,
    )
