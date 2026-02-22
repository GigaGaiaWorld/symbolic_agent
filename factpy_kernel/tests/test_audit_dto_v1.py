from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from factpy_kernel.audit import (
    AuditDTOError,
    AuditQuery,
    build_decision_detail_dto,
    build_run_detail_dto,
    build_run_list_dto,
    load_audit_package,
)
from factpy_kernel.evidence.write_protocol import set_field
from factpy_kernel.export.package import ExportOptions, export_package
from factpy_kernel.store.api import Store
from factpy_kernel.store.ledger import MetaRow


class AuditDTOV1Tests(unittest.TestCase):
    def test_build_run_list_dto(self) -> None:
        store = Store(schema_ir=_mapping_schema(tie_break="latest_by_ingested_at_then_min_assertion_id"))
        asrt_id = set_field(
            store.ledger,
            pred_id="er:canon_of",
            e_ref="idref_v1:Person:auditdto-list",
            rest_terms=[("entity_ref", "idref_v1:Person:c_list")],
            meta={
                "source": "derivation.accept",
                "run_id": "run-auditdto-1",
                "materialize_id": "mat-auditdto-1",
                "derived_rule_id": "rule.accept",
                "derived_rule_version": "v1",
                "key_tuple_digest": "sha256:" + ("1" * 64),
                "cand_key_digest": "sha256:" + ("2" * 64),
                "support_digest": "sha256:" + ("3" * 64),
                "support_kind": "none",
            },
        )
        _set_ingested_at(store, asrt_id, 111)
        query = AuditQuery(_export_and_load(store))

        payload = build_run_list_dto(query)
        self.assertEqual(payload["audit_ui_dto_version"], "audit_ui_dto_v1")
        self.assertEqual(payload["kind"], "run_list")
        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["runs"][0]["run_id"], "run-auditdto-1")
        self.assertEqual(payload["runs"][0]["claim_count"], 1)
        self.assertEqual(payload["runs"][0]["has_failures"], False)
        self.assertEqual(payload["runs"][0]["materialize_ids"], ["mat-auditdto-1"])

    def test_build_run_detail_dto_contains_timeline(self) -> None:
        store = Store(schema_ir=_mapping_schema(tie_break="latest_by_ingested_at_then_min_assertion_id"))
        asrt_id = set_field(
            store.ledger,
            pred_id="er:canon_of",
            e_ref="idref_v1:Person:auditdto-run",
            rest_terms=[("entity_ref", "idref_v1:Person:c_run")],
            meta={
                "source": "derivation.accept",
                "run_id": "run-auditdto-2",
                "materialize_id": "mat-auditdto-2",
                "derived_rule_id": "rule.accept",
                "derived_rule_version": "v1",
                "key_tuple_digest": "sha256:" + ("4" * 64),
                "cand_key_digest": "sha256:" + ("5" * 64),
                "support_digest": "sha256:" + ("6" * 64),
                "support_kind": "none",
            },
        )
        _set_ingested_at(store, asrt_id, 222)
        query = AuditQuery(_export_and_load(store))

        payload = build_run_detail_dto(query, "run-auditdto-2")
        self.assertEqual(payload["kind"], "run_detail")
        self.assertEqual(payload["run_id"], "run-auditdto-2")
        self.assertEqual(payload["stats"]["materialization_count"], 1)
        self.assertEqual(payload["stats"]["candidate_count"], 1)
        self.assertEqual(payload["stats"]["decision_count"], 2)
        self.assertEqual(payload["stats"]["failure_count"], 0)
        self.assertEqual(len(payload["timeline"]), 2)
        self.assertEqual([row["entry_kind"] for row in payload["timeline"]], ["decision", "decision"])
        self.assertEqual(sorted(payload["event_source_counts"].keys()), ["accept", "mapping"])

    def test_build_decision_detail_dto_accept_and_failure_links(self) -> None:
        accept_store = Store(schema_ir=_mapping_schema(tie_break="latest_by_ingested_at_then_min_assertion_id"))
        asrt_id = set_field(
            accept_store.ledger,
            pred_id="er:canon_of",
            e_ref="idref_v1:Person:auditdto-dec-accept",
            rest_terms=[("entity_ref", "idref_v1:Person:c_accept")],
            meta={
                "source": "derivation.accept",
                "run_id": "run-auditdto-3",
                "materialize_id": "mat-auditdto-3",
                "derived_rule_id": "rule.accept",
                "derived_rule_version": "v1",
                "key_tuple_digest": "sha256:" + ("7" * 64),
                "cand_key_digest": "sha256:" + ("8" * 64),
                "support_digest": "sha256:" + ("9" * 64),
                "support_kind": "none",
            },
        )
        _set_ingested_at(accept_store, asrt_id, 333)
        accept_query = AuditQuery(_export_and_load(accept_store))
        accept_decision = accept_query.list_decisions(event_source="accept")[0]["decision_id"]
        accept_payload = build_decision_detail_dto(accept_query, accept_decision)
        self.assertEqual(accept_payload["kind"], "decision_detail")
        self.assertEqual(accept_payload["decision"]["event_kind"], "accept_write")
        self.assertEqual(len(accept_payload["materializations"]), 1)
        self.assertEqual(len(accept_payload["candidates"]), 1)
        self.assertEqual(len(accept_payload["failures"]), 0)
        self.assertEqual(accept_payload["related"]["run_ids"], ["run-auditdto-3"])
        self.assertEqual(accept_payload["related"]["materialize_ids"], ["mat-auditdto-3"])

        conflict_store = Store(schema_ir=_mapping_schema(tie_break=None))
        set_field(
            conflict_store.ledger,
            pred_id="er:canon_of",
            e_ref="idref_v1:Person:auditdto-conflict",
            rest_terms=[("entity_ref", "idref_v1:Person:c1")],
            meta={"source": "hr", "source_loc": "row-1", "run_id": "run-auditdto-4"},
        )
        set_field(
            conflict_store.ledger,
            pred_id="er:canon_of",
            e_ref="idref_v1:Person:auditdto-conflict",
            rest_terms=[("entity_ref", "idref_v1:Person:c2")],
            meta={"source": "crm", "source_loc": "row-2", "run_id": "run-auditdto-4"},
        )
        conflict_query = AuditQuery(_export_and_load(conflict_store))
        failure_decision = conflict_query.list_decisions(event_kind="mapping_conflict")[0]["decision_id"]
        failure_payload = build_decision_detail_dto(conflict_query, failure_decision)
        self.assertEqual(failure_payload["decision"]["event_kind"], "mapping_conflict")
        self.assertEqual(len(failure_payload["materializations"]), 0)
        self.assertEqual(len(failure_payload["candidates"]), 0)
        self.assertEqual(len(failure_payload["failures"]), 1)
        self.assertEqual(failure_payload["related"]["run_ids"], ["run-auditdto-4"])

    def test_build_decision_detail_missing_raises(self) -> None:
        store = Store(schema_ir=_mapping_schema(tie_break="latest_by_ingested_at_then_min_assertion_id"))
        query = AuditQuery(_export_and_load(store))
        with self.assertRaises(AuditDTOError):
            build_decision_detail_dto(query, "missing")


def _export_and_load(store: Store):
    with tempfile.TemporaryDirectory() as tmp:
        pkg_dir = Path(tmp) / "pkg"
        export_package(store, pkg_dir, ExportOptions(package_kind="audit", policy_mode="edb"))
        return load_audit_package(pkg_dir)


def _mapping_schema(tie_break: object) -> dict:
    predicate: dict[str, object] = {
        "pred_id": "er:canon_of",
        "arg_specs": [
            {"name": "mention", "type_domain": "entity_ref"},
            {"name": "canonical", "type_domain": "entity_ref"},
        ],
        "group_key_indexes": [0],
        "cardinality": "functional",
        "is_mapping": True,
        "mapping_kind": "single_valued",
        "mapping_key_positions": [0],
        "mapping_value_positions": [1],
    }
    if tie_break is not None:
        predicate["tie_break"] = tie_break
    return {
        "schema_ir_version": "v1",
        "entities": [
            {
                "entity_type": "Person",
                "identity_fields": [{"name": "source_id", "type_domain": "string"}],
            }
        ],
        "predicates": [predicate],
        "projection": {"entities": [], "predicates": ["er:canon_of"]},
        "protocol_version": {
            "idref_v1": "idref_v1",
            "tup_v1": "tup_v1",
            "export_v1": "export_v1",
        },
        "generated_at": "2026-01-01T00:00:00Z",
    }


def _set_ingested_at(store: Store, asrt_id: str, epoch_nanos: int) -> None:
    replaced = False
    updated: list[MetaRow] = []
    for row in store.ledger._meta_rows:
        if row.asrt_id == asrt_id and row.key == "ingested_at":
            updated.append(MetaRow(asrt_id=row.asrt_id, key=row.key, kind="time", value=epoch_nanos))
            replaced = True
        else:
            updated.append(row)
    if not replaced:
        raise AssertionError(f"missing ingested_at for asrt_id={asrt_id}")
    store.ledger._meta_rows = updated


if __name__ == "__main__":
    unittest.main()
