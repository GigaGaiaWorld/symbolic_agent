"""Canon mapping policy resolution and candidate extraction."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

from ..compiler import CanonicalTupleCodec, FactPyCompileError, Store
from ..ir import PredicateRuleSpecIR
from .model import CanonPolicyConfig


@dataclass(frozen=True)
class MappingCandidate:
    assertion_id: str
    predicate: str
    key_terms: tuple[object, ...]
    value_terms: tuple[object, ...]
    meta: dict[str, object]


def extract_mapping_candidates(
    *,
    store: Store,
    spec: PredicateRuleSpecIR,
    active_ids: set[str],
) -> dict[tuple[object, ...], list[MappingCandidate]]:
    if not spec.is_mapping or spec.mapping_kind != "single_valued":
        return {}

    grouped: dict[tuple[object, ...], list[MappingCandidate]] = defaultdict(list)
    for claim in store.edb.claims(spec.base_predicate):
        if claim.assertion_id not in active_ids:
            continue
        decoded = CanonicalTupleCodec.decode(claim.object_token)
        logical_args: list[object] = [claim.subject]
        logical_args.extend(term.value for term in decoded)

        try:
            key_terms = tuple(logical_args[idx] for idx in spec.mapping_key_positions)
            value_terms = tuple(logical_args[idx] for idx in spec.mapping_value_positions)
        except IndexError as exc:
            raise FactPyCompileError(
                f"Mapping predicate '{spec.base_predicate}' has invalid key/value positions in SchemaIR."
            ) from exc

        grouped[key_terms].append(
            MappingCandidate(
                assertion_id=claim.assertion_id,
                predicate=spec.base_predicate,
                key_terms=key_terms,
                value_terms=value_terms,
                meta=store.edb.assertion_meta(claim.assertion_id),
            )
        )
    return grouped


class CanonPolicyResolver:
    """Resolve canon_of mapping conflicts in deterministic way."""

    def __init__(self, config: CanonPolicyConfig | None = None) -> None:
        self.config = (config or CanonPolicyConfig()).with_defaults()

    def resolve_group(
        self,
        *,
        mapping_name: str,
        key: tuple[object, ...],
        candidates: Iterable[MappingCandidate],
    ) -> MappingCandidate:
        rows = list(candidates)
        if not rows:
            raise FactPyCompileError(f"No mapping candidates provided for '{mapping_name}'.")
        if len(rows) == 1:
            return rows[0]

        mode = self.config.mode
        if mode == "error":
            self._raise_conflict_error(mapping_name=mapping_name, key=key, candidates=rows)

        winner = min(rows, key=lambda item: self._ranking_key(item))
        return winner

    def _ranking_key(self, candidate: MappingCandidate) -> tuple[object, ...]:
        mode = self.config.mode
        primary: tuple[object, ...]

        if mode == "prefer_source":
            source = str(candidate.meta.get(self.config.source_key, ""))
            ranking = {name: idx for idx, name in enumerate(self.config.source_priority)}
            primary = (ranking.get(source, len(ranking) + 1), source)
        elif mode == "max_confidence":
            conf = self._as_float(candidate.meta.get(self.config.confidence_key, 0.0))
            primary = (-conf,)
        elif mode == "latest":
            ts = self._as_epoch(candidate.meta.get(self.config.time_key, ""))
            primary = (-ts,)
        elif mode == "min_assertion_id":
            primary = (str(candidate.assertion_id),)
        elif mode == "min_canonical_id":
            canonical = str(candidate.value_terms[0]) if candidate.value_terms else ""
            primary = (canonical,)
        else:  # pragma: no cover - safeguarded by literal type
            primary = (str(candidate.assertion_id),)

        stable = tuple(self._stable_value(candidate, key) for key in self.config.stable_tie_break)
        return (*primary, *stable, str(candidate.assertion_id))

    def _stable_value(self, candidate: MappingCandidate, key: str) -> object:
        if key == "assertion_id":
            return str(candidate.assertion_id)
        if key == "canonical_id":
            return str(candidate.value_terms[0]) if candidate.value_terms else ""
        if key.startswith("meta:"):
            meta_key = key.split(":", 1)[1]
            return str(candidate.meta.get(meta_key, ""))
        return str(candidate.meta.get(key, ""))

    def _raise_conflict_error(
        self,
        *,
        mapping_name: str,
        key: tuple[object, ...],
        candidates: list[MappingCandidate],
    ) -> None:
        payload = []
        for item in candidates:
            payload.append(
                {
                    "assertion_id": item.assertion_id,
                    "value": list(item.value_terms),
                    "source": item.meta.get(self.config.source_key),
                    "confidence": item.meta.get(self.config.confidence_key),
                    "time": item.meta.get(self.config.time_key),
                }
            )
        raise FactPyCompileError(
            f"Mapping '{mapping_name}' conflict on key={list(key)} with {len(candidates)} candidates: {payload}"
        )

    def _as_float(self, value: object) -> float:
        if isinstance(value, bool):
            return float(int(value))
        if isinstance(value, (int, float)):
            return float(value)
        try:
            return float(str(value))
        except Exception:
            return 0.0

    def _as_epoch(self, value: object) -> float:
        if isinstance(value, datetime):
            return value.timestamp()
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value).strip()
        if not text:
            return float("-inf")
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
        except Exception:
            return float("-inf")
