from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from factpy_kernel.derivation.candidates import CandidateSet
from factpy_kernel.evidence.write_protocol import WriteProtocolError, set_field
from factpy_kernel.protocol.digests import sha256_token
from factpy_kernel.protocol.tup_v1 import canonical_bytes_tup_v1
from factpy_kernel.store.ledger import Ledger


@dataclass(frozen=True)
class AcceptOptions:
    approved_by: str | None = None
    note: str | None = None
    dry_run: bool = False


@dataclass(frozen=True)
class AcceptResult:
    materialize_id: str
    run_id: str
    accepted_count: int
    skipped_count: int
    written_assertions: list[dict[str, Any]]
    skipped_reason_counts: dict[str, int]


def accept_candidate_set(
    ledger: Ledger,
    candidate_set: CandidateSet,
    options: AcceptOptions,
    derived_rule_id: str,
    derived_rule_version: str,
) -> AcceptResult:
    if not isinstance(ledger, Ledger):
        raise WriteProtocolError("ledger must be Ledger")
    if not isinstance(candidate_set, CandidateSet):
        raise WriteProtocolError("candidate_set must be CandidateSet")
    if not isinstance(options, AcceptOptions):
        raise WriteProtocolError("options must be AcceptOptions")
    if not isinstance(derived_rule_id, str) or not derived_rule_id:
        raise WriteProtocolError("derived_rule_id must be non-empty string")
    if not isinstance(derived_rule_version, str) or not derived_rule_version:
        raise WriteProtocolError("derived_rule_version must be non-empty string")

    payload = candidate_set.payload
    e_ref = payload.get("e_ref") if isinstance(payload, dict) else None
    rest_terms = payload.get("rest_terms") if isinstance(payload, dict) else None

    if not isinstance(e_ref, str) or not e_ref:
        raise WriteProtocolError("candidate payload missing e_ref")
    if not isinstance(rest_terms, list):
        raise WriteProtocolError("candidate payload missing rest_terms")
    if not isinstance(candidate_set.target, str) or not candidate_set.target:
        raise WriteProtocolError("candidate target must be non-empty string")
    if not isinstance(candidate_set.key_tuple_digest, str) or not candidate_set.key_tuple_digest.startswith("sha256:"):
        raise WriteProtocolError("candidate key_tuple_digest must start with sha256:")

    if options.dry_run:
        materialize_id = uuid4().hex
        return AcceptResult(
            materialize_id=materialize_id,
            run_id=candidate_set.run_id,
            accepted_count=1,
            skipped_count=0,
            written_assertions=[
                {
                    "asrt_id": "<dry_run>",
                    "pred_id": candidate_set.target,
                    "key_tuple_digest": candidate_set.key_tuple_digest,
                }
            ],
            skipped_reason_counts={},
        )

    materialize_id = _existing_materialize_id(
        ledger=ledger,
        pred_id=candidate_set.target,
        e_ref=e_ref,
        key_tuple_digest=candidate_set.key_tuple_digest,
    )
    if materialize_id is None:
        materialize_id = uuid4().hex

    cand_key_digest = _compute_cand_key_digest(materialize_id, candidate_set.key_tuple_digest)

    existing_written = _find_existing_written_assertions(
        ledger=ledger,
        pred_id=candidate_set.target,
        e_ref=e_ref,
        key_tuple_digest=candidate_set.key_tuple_digest,
        materialize_id=materialize_id,
        cand_key_digest=cand_key_digest,
    )
    if existing_written:
        return AcceptResult(
            materialize_id=materialize_id,
            run_id=candidate_set.run_id,
            accepted_count=0,
            skipped_count=1,
            written_assertions=existing_written,
            skipped_reason_counts={"duplicate": 1},
        )

    write_meta: dict[str, Any] = {
        "source": "derivation.accept",
        "source_loc": f"{derived_rule_id}:{derived_rule_version}",
        "trace_id": candidate_set.run_id,
        "derived_rule_id": derived_rule_id,
        "derived_rule_version": derived_rule_version,
        "run_id": candidate_set.run_id,
        "materialize_id": materialize_id,
        "key_tuple_digest": candidate_set.key_tuple_digest,
        "cand_key_digest": cand_key_digest,
        "support_digest": candidate_set.support_digest,
        "support_kind": candidate_set.support_kind,
    }
    if options.approved_by is not None:
        write_meta["approved_by"] = options.approved_by
    if options.note is not None:
        write_meta["note"] = options.note

    asrt_id = set_field(
        ledger=ledger,
        pred_id=candidate_set.target,
        e_ref=e_ref,
        rest_terms=rest_terms,
        meta=write_meta,
    )

    return AcceptResult(
        materialize_id=materialize_id,
        run_id=candidate_set.run_id,
        accepted_count=1,
        skipped_count=0,
        written_assertions=[
            {
                "asrt_id": asrt_id,
                "pred_id": candidate_set.target,
                "key_tuple_digest": candidate_set.key_tuple_digest,
            }
        ],
        skipped_reason_counts={},
    )


def _compute_cand_key_digest(materialize_id: str, key_tuple_digest: str) -> str:
    key_terms = [
        ("string", materialize_id),
        ("string", key_tuple_digest),
    ]
    return sha256_token(canonical_bytes_tup_v1(key_terms))


def _existing_materialize_id(
    ledger: Ledger,
    pred_id: str,
    e_ref: str,
    key_tuple_digest: str,
) -> str | None:
    for claim in ledger.find_claims(pred_id=pred_id, e_ref=e_ref):
        if ledger.has_active_revocation(claim.asrt_id):
            continue
        if _meta_value(ledger, claim.asrt_id, "key_tuple_digest") != key_tuple_digest:
            continue
        materialize_id = _meta_value(ledger, claim.asrt_id, "materialize_id")
        if materialize_id:
            return materialize_id
    return None


def _find_existing_written_assertions(
    ledger: Ledger,
    pred_id: str,
    e_ref: str,
    key_tuple_digest: str,
    materialize_id: str,
    cand_key_digest: str,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for claim in ledger.find_claims(pred_id=pred_id, e_ref=e_ref):
        if ledger.has_active_revocation(claim.asrt_id):
            continue

        if _meta_value(ledger, claim.asrt_id, "materialize_id") != materialize_id:
            continue
        if _meta_value(ledger, claim.asrt_id, "cand_key_digest") != cand_key_digest:
            continue
        if _meta_value(ledger, claim.asrt_id, "key_tuple_digest") != key_tuple_digest:
            continue

        rows.append(
            {
                "asrt_id": claim.asrt_id,
                "pred_id": claim.pred_id,
                "key_tuple_digest": key_tuple_digest,
            }
        )
    return rows


def _meta_value(ledger: Ledger, asrt_id: str, key: str) -> str | None:
    for row in ledger.find_meta(asrt_id=asrt_id, key=key):
        if isinstance(row.value, str):
            return row.value
    return None
