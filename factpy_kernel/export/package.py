from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from factpy_kernel.export.pred_norm import normalize_pred_id
from factpy_kernel.export.tsv_v1 import write_tsv
from factpy_kernel.policy.policy_ir import (
    build_policy_ir_v1,
    canonicalize_policy_ir_jcs,
    policy_digest,
)
from factpy_kernel.protocol.digests import sha256_token
from factpy_kernel.protocol.tup_v1 import canonical_bytes_tup_v1
from factpy_kernel.rules.where_compile import (
    compile_where_to_query_dl,
    query_rel_for_where,
)
from factpy_kernel.schema.schema_ir import canonicalize_schema_ir_jcs, schema_digest
from factpy_kernel.store.api import Store
from factpy_kernel.view.souffle_view_gen import generate_view_dl


@dataclass(frozen=True)
class ExportOptions:
    package_kind: str = "inference"
    target_engine: str = "souffle"
    dialect_version: str = "v1"

    def __post_init__(self) -> None:
        if self.package_kind not in {"inference", "audit"}:
            raise ValueError("package_kind must be inference|audit")
        if not isinstance(self.target_engine, str) or not self.target_engine:
            raise ValueError("target_engine must be non-empty string")
        if not isinstance(self.dialect_version, str) or not self.dialect_version:
            raise ValueError("dialect_version must be non-empty string")


def export_package(
    store: Store,
    out_dir: Path,
    options: ExportOptions,
    query: dict[str, Any] | None = None,
) -> Path:
    if not isinstance(store, Store):
        raise TypeError("store must be Store")
    if not isinstance(options, ExportOptions):
        raise TypeError("options must be ExportOptions")

    package_dir = Path(out_dir)
    schema_dir = package_dir / "schema"
    policy_dir = package_dir / "policy"
    facts_dir = package_dir / "facts"
    rules_dir = package_dir / "rules"
    outputs_dir = package_dir / "outputs"
    audit_dir = package_dir / "audit"

    for p in [schema_dir, policy_dir, facts_dir, rules_dir, outputs_dir]:
        p.mkdir(parents=True, exist_ok=True)
    if options.package_kind == "audit":
        audit_dir.mkdir(parents=True, exist_ok=True)

    schema_bytes = canonicalize_schema_ir_jcs(store.schema_ir)
    (schema_dir / "schema_ir.json").write_text(
        schema_bytes.decode("utf-8"), encoding="utf-8", newline="\n"
    )

    policy_ir = build_policy_ir_v1(store.schema_ir)
    policy_ir_bytes = canonicalize_policy_ir_jcs(policy_ir)
    policy_ir_path = policy_dir / "policy_ir.json"
    policy_ir_path.write_text(
        policy_ir_bytes.decode("utf-8"), encoding="utf-8", newline="\n"
    )

    policy_rules_path = policy_dir / "policy_rules.dl"
    policy_rules_path.write_text("", encoding="utf-8", newline="\n")

    view_dl = generate_view_dl(store.schema_ir)
    (rules_dir / "view.dl").write_text(view_dl, encoding="utf-8", newline="\n")

    outputs_map = _outputs_map(store)
    idb_text = ""
    if query is not None:
        if not isinstance(query, dict):
            raise TypeError("query must be dict")
        where = query.get("where")
        if not isinstance(where, list):
            raise ValueError("query.where must be list")
        query_rel = query.get("query_rel")
        if query_rel is None:
            query_rel = query_rel_for_where(where)
        if not isinstance(query_rel, str) or not query_rel:
            raise ValueError("query.query_rel must be non-empty string")
        idb_text = compile_where_to_query_dl(
            schema_ir=store.schema_ir,
            where=where,
            query_rel=query_rel,
        )
        outputs_map["__query__"] = [query_rel]

    (rules_dir / "idb.dl").write_text(idb_text, encoding="utf-8", newline="\n")

    claim_rows, claim_arg_rows, meta_str_rows, meta_time_rows, meta_num_rows, meta_bool_rows, revokes_rows = _build_fact_rows(store)

    claim_path = facts_dir / "claim.facts"
    claim_arg_path = facts_dir / "claim_arg.facts"
    meta_str_path = facts_dir / "meta_str.facts"
    meta_time_path = facts_dir / "meta_time.facts"
    meta_num_path = facts_dir / "meta_num.facts"
    meta_bool_path = facts_dir / "meta_bool.facts"
    revokes_path = facts_dir / "revokes.facts"

    write_tsv(claim_path, claim_rows)
    write_tsv(claim_arg_path, claim_arg_rows)
    write_tsv(meta_str_path, meta_str_rows)
    write_tsv(meta_time_path, meta_time_rows)
    write_tsv(meta_num_path, meta_num_rows)
    write_tsv(meta_bool_path, meta_bool_rows)
    write_tsv(revokes_path, revokes_rows)

    schema_digest_token = schema_digest(store.schema_ir)
    policy_digest_token = policy_digest(policy_ir)

    edb_files = [
        claim_path,
        claim_arg_path,
        meta_str_path,
        meta_time_path,
        meta_num_path,
        meta_bool_path,
        revokes_path,
    ]
    rules_files = [rules_dir / "idb.dl", rules_dir / "view.dl"]

    manifest = {
        "package_kind": options.package_kind,
        "protocol_version": _protocol_version(store.schema_ir),
        "generated_at": time.time_ns(),
        "run_id": _latest_run_id(store),
        "target_engine": options.target_engine,
        "dialect_version": options.dialect_version,
        "policy_mode": "edb",
        "digests": {
            "schema_digest": schema_digest_token,
            "policy_digest": policy_digest_token,
            "edb_digest": _digest_for_paths(edb_files, package_dir),
            "rules_digest": _digest_for_paths(rules_files, package_dir),
        },
        "paths": {
            "schema": "schema/schema_ir.json",
            "policy": "policy/policy_ir.json",
            "facts": {
                "claim": "facts/claim.facts",
                "claim_arg": "facts/claim_arg.facts",
                "meta_str": "facts/meta_str.facts",
                "meta_time": "facts/meta_time.facts",
                "meta_num": "facts/meta_num.facts",
                "meta_bool": "facts/meta_bool.facts",
                "revokes": "facts/revokes.facts",
            },
            "rules": {
                "view": "rules/view.dl",
                "idb": "rules/idb.dl",
            },
            "outputs": "outputs",
            "audit": "audit" if options.package_kind == "audit" else None,
        },
        "entrypoints": _entrypoints(store),
        "outputs_map": outputs_map,
    }

    manifest_path = package_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
        newline="\n",
    )

    return manifest_path


def _build_fact_rows(
    store: Store,
) -> tuple[
    list[list[str]],
    list[list[str]],
    list[list[str]],
    list[list[str]],
    list[list[str]],
    list[list[str]],
    list[list[str]],
]:
    claim_rows: list[list[str]] = []
    for claim in store.ledger.claims:
        tup_digest = sha256_token(canonical_bytes_tup_v1(claim.rest_terms))
        claim_rows.append([claim.asrt_id, claim.pred_id, claim.e_ref, tup_digest])

    claim_arg_rows = [
        [row.asrt_id, str(row.idx), _atom_to_str(row.val_atom), row.tag]
        for row in store.ledger.claim_args
    ]

    meta_str_rows = [
        [row.asrt_id, row.key, _atom_to_str(row.value)]
        for row in store.ledger.meta_rows
        if row.kind == "str"
    ]
    meta_time_rows = [
        [row.asrt_id, row.key, _atom_to_str(row.value)]
        for row in store.ledger.meta_rows
        if row.kind == "time"
    ]
    meta_num_rows = [
        [row.asrt_id, row.key, _atom_to_str(row.value)]
        for row in store.ledger.meta_rows
        if row.kind == "num"
    ]
    meta_bool_rows = [
        [row.asrt_id, row.key, _atom_to_str(row.value)]
        for row in store.ledger.meta_rows
        if row.kind == "bool"
    ]
    revokes_rows = [
        [row.revoker_asrt_id, row.revoked_asrt_id] for row in store.ledger.revokes
    ]

    return (
        sorted(claim_rows),
        sorted(claim_arg_rows),
        sorted(meta_str_rows),
        sorted(meta_time_rows),
        sorted(meta_num_rows),
        sorted(meta_bool_rows),
        sorted(revokes_rows),
    )


def _atom_to_str(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    raise TypeError(f"cannot encode non-atomic TSV value: {type(value).__name__}")


def _digest_for_paths(paths: list[Path], root: Path) -> str:
    hasher = hashlib.sha256()
    for path in sorted(paths, key=lambda p: p.relative_to(root).as_posix()):
        rel = path.relative_to(root).as_posix().encode("utf-8")
        payload = path.read_bytes()
        hasher.update(rel)
        hasher.update(b"\x00")
        hasher.update(payload)
    return f"sha256:{hasher.hexdigest()}"


def _protocol_version(schema_ir: dict) -> dict[str, str]:
    base = {
        "idref_v1": "idref_v1",
        "tup_v1": "tup_v1",
        "export_v1": "export_v1",
    }
    raw = schema_ir.get("protocol_version") if isinstance(schema_ir, dict) else None
    if not isinstance(raw, dict):
        return base
    for key in ("idref_v1", "tup_v1", "export_v1"):
        value = raw.get(key)
        if isinstance(value, str) and value:
            base[key] = value
    return base


def _entrypoints(store: Store) -> list[str]:
    predicates = store.schema_ir.get("predicates") if isinstance(store.schema_ir, dict) else None
    if not isinstance(predicates, list):
        return []
    out: list[str] = []
    for pred in predicates:
        if not isinstance(pred, dict):
            continue
        pred_id = pred.get("pred_id")
        if isinstance(pred_id, str) and pred_id:
            out.append(pred_id)
    return sorted(set(out))


def _outputs_map(store: Store) -> dict[str, list[str]]:
    predicates = store.schema_ir.get("predicates") if isinstance(store.schema_ir, dict) else None
    if not isinstance(predicates, list):
        return {}

    out: dict[str, list[str]] = {}
    for pred in predicates:
        if not isinstance(pred, dict):
            continue
        pred_id = pred.get("pred_id")
        if not isinstance(pred_id, str) or not pred_id:
            continue
        out[pred_id] = [normalize_pred_id(pred_id)]
    return out


def _latest_run_id(store: Store) -> str | None:
    run_rows = [row for row in store.ledger.meta_rows if row.key == "run_id" and row.kind == "str"]
    if not run_rows:
        return None
    return str(run_rows[-1].value)
