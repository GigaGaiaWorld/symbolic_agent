"""Convenience export entrypoint."""

from __future__ import annotations

from typing import Iterable, Literal

from ..compiler import Store
from ..er.model import CanonPolicyConfig
from ..ir import SchemaIR
from ..rules.dsl import Rule
from .policy import PolicyMode
from .problog import ProbLogExporter
from .souffle import SouffleExporter


ExportTarget = Literal["souffle", "problog"]


def export(
    *,
    store: Store,
    target: ExportTarget,
    policy_mode: PolicyMode = "edb",
    canon_policy: CanonPolicyConfig | None = None,
    rules: Iterable[Rule] | None = None,
    schema_ir: SchemaIR | None = None,
) -> str:
    if target == "souffle":
        return SouffleExporter().render(
            store=store,
            rules=rules,
            schema_ir=schema_ir,
            policy_mode=policy_mode,
            canon_policy=canon_policy,
        )
    if target == "problog":
        return ProbLogExporter().render(
            store=store,
            rules=rules,
            schema_ir=schema_ir,
            policy_mode=policy_mode,
            canon_policy=canon_policy,
        )
    raise ValueError("target must be 'souffle' or 'problog'.")
