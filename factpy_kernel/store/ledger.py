from __future__ import annotations

from dataclasses import dataclass
from typing import Any


META_KINDS = {"str", "num", "bool", "time", "json"}


@dataclass(frozen=True)
class Claim:
    asrt_id: str
    pred_id: str
    e_ref: str
    rest_terms: list[tuple[str, Any]]


@dataclass(frozen=True)
class ClaimArg:
    asrt_id: str
    idx: int
    val_atom: Any
    tag: str


@dataclass(frozen=True)
class MetaRow:
    asrt_id: str
    key: str
    kind: str
    value: Any


@dataclass(frozen=True)
class Revokes:
    revoker_asrt_id: str
    revoked_asrt_id: str


class Ledger:
    def __init__(self) -> None:
        self._claims: list[Claim] = []
        self._claim_args: list[ClaimArg] = []
        self._meta_rows: list[MetaRow] = []
        self._revokes: list[Revokes] = []
        self._claim_by_asrt_id: dict[str, Claim] = {}
        self._revoker_asrt_ids: set[str] = set()

    @property
    def claims(self) -> list[Claim]:
        return list(self._claims)

    @property
    def claim_args(self) -> list[ClaimArg]:
        return list(self._claim_args)

    @property
    def meta_rows(self) -> list[MetaRow]:
        return list(self._meta_rows)

    @property
    def revokes(self) -> list[Revokes]:
        return list(self._revokes)

    def append_claim(self, claim: Claim) -> None:
        if not isinstance(claim, Claim):
            raise TypeError("claim must be Claim")
        if claim.asrt_id in self._claim_by_asrt_id:
            raise ValueError(f"duplicate asrt_id: {claim.asrt_id}")
        if not claim.asrt_id:
            raise ValueError("asrt_id must be non-empty")
        if not claim.pred_id:
            raise ValueError("pred_id must be non-empty")
        if not claim.e_ref:
            raise ValueError("e_ref must be non-empty")

        normalized_rest_terms = [self._normalize_term(term) for term in claim.rest_terms]
        normalized_claim = Claim(
            asrt_id=claim.asrt_id,
            pred_id=claim.pred_id,
            e_ref=claim.e_ref,
            rest_terms=normalized_rest_terms,
        )
        self._claims.append(normalized_claim)
        self._claim_by_asrt_id[normalized_claim.asrt_id] = normalized_claim

    def append_claim_args(self, rows: list[ClaimArg]) -> None:
        for row in rows:
            if not isinstance(row, ClaimArg):
                raise TypeError("rows must contain ClaimArg")
            if row.asrt_id not in self._claim_by_asrt_id:
                raise ValueError(f"unknown asrt_id for claim_arg: {row.asrt_id}")
            if isinstance(row.idx, bool) or not isinstance(row.idx, int) or row.idx < 0:
                raise ValueError("claim_arg idx must be non-negative int")
            if not isinstance(row.tag, str) or not row.tag:
                raise ValueError("claim_arg tag must be non-empty str")
            self._claim_args.append(row)

    def append_meta(self, rows: list[MetaRow]) -> None:
        for row in rows:
            if not isinstance(row, MetaRow):
                raise TypeError("rows must contain MetaRow")
            if not self._is_known_asrt_id(row.asrt_id):
                raise ValueError(f"unknown asrt_id for meta: {row.asrt_id}")
            if row.kind not in META_KINDS:
                raise ValueError(f"unsupported meta kind: {row.kind}")
            if not isinstance(row.key, str) or not row.key:
                raise ValueError("meta key must be non-empty str")
            self._meta_rows.append(row)

    def append_revokes(self, row: Revokes) -> None:
        if not isinstance(row, Revokes):
            raise TypeError("row must be Revokes")
        if not row.revoker_asrt_id or not row.revoked_asrt_id:
            raise ValueError("revoker_asrt_id and revoked_asrt_id must be non-empty")
        self._revoker_asrt_ids.add(row.revoker_asrt_id)
        self._revokes.append(row)

    def find_claims(
        self, pred_id: str | None = None, e_ref: str | None = None
    ) -> list[Claim]:
        rows = self._claims
        if pred_id is not None:
            rows = [row for row in rows if row.pred_id == pred_id]
        if e_ref is not None:
            rows = [row for row in rows if row.e_ref == e_ref]
        return list(rows)

    def has_active_revocation(self, revoked_asrt_id: str) -> bool:
        return any(row.revoked_asrt_id == revoked_asrt_id for row in self._revokes)

    def get_claim(self, asrt_id: str) -> Claim | None:
        return self._claim_by_asrt_id.get(asrt_id)

    def find_meta(
        self,
        asrt_id: str | None = None,
        key: str | None = None,
        kind: str | None = None,
    ) -> list[MetaRow]:
        rows = self._meta_rows
        if asrt_id is not None:
            rows = [row for row in rows if row.asrt_id == asrt_id]
        if key is not None:
            rows = [row for row in rows if row.key == key]
        if kind is not None:
            rows = [row for row in rows if row.kind == kind]
        return list(rows)

    def find_revoker(self, revoked_asrt_id: str) -> str | None:
        for row in self._revokes:
            if row.revoked_asrt_id == revoked_asrt_id:
                return row.revoker_asrt_id
        return None

    def _is_known_asrt_id(self, asrt_id: str) -> bool:
        return asrt_id in self._claim_by_asrt_id or asrt_id in self._revoker_asrt_ids

    @staticmethod
    def _normalize_term(term: tuple[str, Any]) -> tuple[str, Any]:
        if not isinstance(term, tuple) or len(term) != 2:
            raise ValueError("rest_terms entries must be tuple(tag, value)")
        tag, value = term
        if not isinstance(tag, str) or not tag:
            raise ValueError("rest_terms tag must be non-empty str")
        return tag, value
