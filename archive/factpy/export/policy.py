"""View/policy generation for claim/meta append-only storage."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Literal

from ..compiler import CanonicalTupleCodec, FactPyCompileError, Store
from ..er.model import CanonPolicyConfig
from ..er.policy import CanonPolicyResolver, extract_mapping_candidates
from ..ir import PredicateRuleSpecIR, SchemaIR


PolicyMode = Literal["edb", "idb"]


@dataclass(frozen=True)
class PolicyArtifacts:
    view_rules: tuple[str, ...]
    policy_rules: tuple[str, ...]
    active_facts: tuple[tuple[str], ...]
    chosen_facts: tuple[tuple[str], ...]
    claim_arg_facts: tuple[tuple[str, int, object, str], ...]
    canon_chosen_facts: tuple[tuple[object, ...], ...] = tuple()


class PolicyCompiler:
    def __init__(self, schema_ir: SchemaIR) -> None:
        self.schema_ir = schema_ir
        self._specs = tuple(schema_ir.rule_predicates)
        self._spec_by_base = {item.base_predicate: item for item in self._specs}

    def compile(
        self,
        *,
        store: Store,
        policy_mode: PolicyMode = "edb",
        canon_policy: CanonPolicyConfig | None = None,
    ) -> PolicyArtifacts:
        if policy_mode not in {"edb", "idb"}:
            raise FactPyCompileError("policy_mode must be 'edb' or 'idb'.")

        canon_cfg = (canon_policy or CanonPolicyConfig()).with_defaults()
        if policy_mode == "idb" and canon_cfg.mode != "error":
            raise FactPyCompileError(
                "policy_mode='idb' for canon mapping requires aggregation/order support. "
                "Use policy_mode='edb' to precompute chosen(A), or set canon_policy.mode='error'."
            )

        active = store.active_assertions()
        chosen = store.chosen_assertions(active_ids=active)

        mapping_all_ids, mapping_chosen_ids, canon_chosen_rows = self._resolve_mapping_choices(
            store=store,
            active_ids=active,
            policy_mode=policy_mode,
            canon_policy=canon_cfg,
        )
        if mapping_all_ids:
            chosen = (chosen - mapping_all_ids) | mapping_chosen_ids

        self._validate_functional_constraints(
            store=store,
            active_ids=active,
            policy_mode=policy_mode,
            chosen_ids=chosen,
        )

        claim_arg = self._decode_claim_args(store)
        view_rules = self._build_view_rules()

        if policy_mode == "edb":
            policy_rules: list[str] = [
                "canon_of_view(M,C) :- canon_of_chosen(M,C)."
            ] if canon_chosen_rows else []
            active_facts = tuple((item,) for item in sorted(active))
            chosen_facts = tuple((item,) for item in sorted(chosen))
        else:
            policy_rules = [
                "active(A) :- claim(A, Pred, S, O).",
                "chosen(A) :- active(A).",
            ]
            active_facts = tuple()
            chosen_facts = tuple()

        return PolicyArtifacts(
            view_rules=tuple(view_rules),
            policy_rules=tuple(policy_rules),
            active_facts=active_facts,
            chosen_facts=chosen_facts,
            claim_arg_facts=tuple(sorted(claim_arg, key=lambda item: (item[0], item[1], item[3], str(item[2])))),
            canon_chosen_facts=tuple(sorted(canon_chosen_rows, key=lambda row: tuple(str(item) for item in row))),
        )

    def _resolve_mapping_choices(
        self,
        *,
        store: Store,
        active_ids: set[str],
        policy_mode: PolicyMode,
        canon_policy: CanonPolicyConfig,
    ) -> tuple[set[str], set[str], set[tuple[object, ...]]]:
        resolver = CanonPolicyResolver(canon_policy)
        all_mapping_ids: set[str] = set()
        chosen_mapping_ids: set[str] = set()
        canon_chosen_rows: set[tuple[object, ...]] = set()

        for spec in self._specs:
            if not spec.is_mapping or spec.mapping_kind != "single_valued":
                continue

            grouped = extract_mapping_candidates(store=store, spec=spec, active_ids=active_ids)
            for key, candidates in grouped.items():
                all_mapping_ids.update(item.assertion_id for item in candidates)
                if len(candidates) > 1 and policy_mode == "idb":
                    raise FactPyCompileError(
                        f"policy_mode='idb' cannot deterministically resolve mapping '{spec.base_predicate}' "
                        f"for key={list(key)}. Use policy_mode='edb'."
                    )
                winner = resolver.resolve_group(
                    mapping_name=spec.base_predicate,
                    key=key,
                    candidates=candidates,
                )
                chosen_mapping_ids.add(winner.assertion_id)

                if (
                    "canon_of" in spec.base_predicate
                    or "canon_of" in spec.view_predicate
                ):
                    row = (*winner.key_terms, *winner.value_terms)
                    canon_chosen_rows.add(row)

        return all_mapping_ids, chosen_mapping_ids, canon_chosen_rows

    def _decode_claim_args(self, store: Store) -> set[tuple[str, int, object, str]]:
        out: set[tuple[str, int, object, str]] = set()
        for claim in store.edb.claims():
            decoded = CanonicalTupleCodec.decode(claim.object_token)
            for idx, term in enumerate(decoded):
                # tag is the canonical typed_tuple_v1 term tag (string/int/entity_ref/...) and
                # must stay stable across exporters and policy checks.
                out.add((claim.assertion_id, idx, term.value, term.tag))
        return out

    def _build_view_rules(self) -> list[str]:
        out: list[str] = []
        for spec in self._specs:
            args = [f"X{idx}" for idx in range(spec.logical_arity)]
            head = f"{spec.view_predicate}({', '.join(args)})"

            body = [
                f"claim(A, \"{spec.base_predicate}\", X0, O)",
                "active(A)",
                "chosen(A)",
            ]
            for idx in range(1, spec.logical_arity):
                body.append(f"claim_arg(A, {idx - 1}, X{idx}, T{idx})")

            out.append(f"{head} :- {', '.join(body)}.")
        return out

    def _validate_functional_constraints(
        self,
        *,
        store: Store,
        active_ids: set[str],
        policy_mode: PolicyMode,
        chosen_ids: set[str],
    ) -> None:
        for spec in self._specs:
            if spec.cardinality != "functional":
                continue
            if spec.is_mapping:
                continue

            groups: dict[tuple[object, ...], list[str]] = defaultdict(list)
            for claim in store.edb.claims(spec.base_predicate):
                if claim.assertion_id not in active_ids:
                    continue
                decoded = CanonicalTupleCodec.decode(claim.object_token)
                group_parts: list[object] = [spec.base_predicate]
                for key_idx in spec.group_key_indexes:
                    if key_idx == spec.subject_position:
                        group_parts.append(claim.subject)
                        continue
                    tuple_idx = key_idx - 1
                    if tuple_idx < 0 or tuple_idx >= len(decoded):
                        raise FactPyCompileError(
                            f"Invalid group_key index for predicate '{spec.base_predicate}': {key_idx}"
                        )
                    group_parts.append(decoded[tuple_idx].value)
                groups[tuple(group_parts)].append(claim.assertion_id)

            conflicting = [key for key, values in groups.items() if len(values) > 1]
            if not conflicting:
                continue

            if policy_mode == "idb":
                raise FactPyCompileError(
                    f"policy_mode='idb' cannot resolve functional conflicts for predicate '{spec.base_predicate}'. "
                    "Use policy_mode='edb' to precompute chosen(A), or configure explicit tie-break strategy."
                )

            # edb mode expects deterministic chosen selection; verify each conflicting group has one chosen winner.
            for key in conflicting:
                chosen_count = sum(1 for aid in groups[key] if aid in chosen_ids)
                if chosen_count != 1:
                    raise FactPyCompileError(
                        f"Functional predicate '{spec.base_predicate}' group={list(key)} has {chosen_count} chosen assertions; expected 1."
                    )


def export_policy_artifacts(
    *,
    store: Store,
    schema_ir: SchemaIR,
    policy_mode: PolicyMode = "edb",
    canon_policy: CanonPolicyConfig | None = None,
) -> PolicyArtifacts:
    return PolicyCompiler(schema_ir).compile(
        store=store,
        policy_mode=policy_mode,
        canon_policy=canon_policy,
    )
