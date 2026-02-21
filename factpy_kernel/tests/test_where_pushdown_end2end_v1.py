from __future__ import annotations

import unittest

from factpy_kernel.evidence.write_protocol import set_field
from factpy_kernel.runner.runner import find_souffle_binary
from factpy_kernel.store.api import Store


class WherePushdownEnd2EndV1Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.schema_ir = {
            "schema_ir_version": "v1",
            "entities": [
                {
                    "entity_type": "Person",
                    "identity_fields": [
                        {"name": "source_id", "type_domain": "string"},
                    ],
                }
            ],
            "predicates": [
                {
                    "pred_id": "person:country",
                    "arg_specs": [
                        {"name": "E", "type_domain": "entity_ref"},
                        {"name": "country", "type_domain": "string"},
                    ],
                    "group_key_indexes": [0],
                    "cardinality": "functional",
                },
                {
                    "pred_id": "person:rank",
                    "arg_specs": [
                        {"name": "E", "type_domain": "entity_ref"},
                        {"name": "rank", "type_domain": "int"},
                    ],
                    "group_key_indexes": [0],
                    "cardinality": "functional",
                },
                {
                    "pred_id": "person:flag",
                    "arg_specs": [
                        {"name": "E", "type_domain": "entity_ref"},
                        {"name": "flag", "type_domain": "bool"},
                    ],
                    "group_key_indexes": [0],
                    "cardinality": "functional",
                },
                {
                    "pred_id": "person:seen_at",
                    "arg_specs": [
                        {"name": "E", "type_domain": "entity_ref"},
                        {"name": "ts", "type_domain": "time"},
                    ],
                    "group_key_indexes": [0],
                    "cardinality": "functional",
                }
            ],
            "projection": {
                "entities": [],
                "predicates": ["person:country", "person:rank", "person:flag", "person:seen_at"],
            },
            "protocol_version": {
                "idref_v1": "idref_v1",
                "tup_v1": "tup_v1",
                "export_v1": "export_v1",
            },
            "generated_at": "2026-01-01T00:00:00Z",
        }

    def test_where_pushdown_produces_candidates(self) -> None:
        if find_souffle_binary() is None:
            self.skipTest("souffle binary not found")

        store = Store(schema_ir=self.schema_ir)
        e_ref = "idref_v1:Person:pushdown"
        set_field(
            store.ledger,
            pred_id="person:country",
            e_ref=e_ref,
            rest_terms=[("string", "de")],
            meta={"source": "test", "source_loc": "row-1"},
        )

        candidates = store.evaluate_engine(
            derivation_id="drv.where.pushdown",
            version="v1",
            target_pred_id="person:country",
            head_vars=["$E", "$country"],
            where=[
                ("pred", "person:country", ["$E", "$country"]),
                ("eq", "$country", "de"),
            ],
        )

        self.assertTrue(candidates)
        candidate = candidates[0]
        self.assertEqual(candidate.payload["e_ref"], e_ref)
        self.assertEqual(candidate.payload["rest_terms"], [("string", "de")])
        self.assertTrue(candidate.key_tuple_digest.startswith("sha256:"))

    def test_where_pushdown_int_roundtrip(self) -> None:
        if find_souffle_binary() is None:
            self.skipTest("souffle binary not found")

        store = Store(schema_ir=self.schema_ir)
        e_ref = "idref_v1:Person:pushdown-int"
        set_field(
            store.ledger,
            pred_id="person:rank",
            e_ref=e_ref,
            rest_terms=[("int", 3)],
            meta={"source": "test", "source_loc": "row-1"},
        )

        candidates = store.evaluate_engine(
            derivation_id="drv.where.pushdown.int",
            version="v1",
            target_pred_id="person:rank",
            head_vars=["$E", "$rank"],
            where=[
                ("pred", "person:rank", ["$E", "$rank"]),
                ("eq", "$rank", 3),
            ],
        )

        self.assertTrue(candidates)
        candidate = candidates[0]
        self.assertEqual(candidate.payload["e_ref"], e_ref)
        self.assertEqual(candidate.payload["rest_terms"], [("int", 3)])
        self.assertTrue(candidate.key_tuple_digest.startswith("sha256:"))

    def test_where_pushdown_bool_roundtrip(self) -> None:
        if find_souffle_binary() is None:
            self.skipTest("souffle binary not found")

        store = Store(schema_ir=self.schema_ir)
        e_ref = "idref_v1:Person:pushdown-bool"
        set_field(
            store.ledger,
            pred_id="person:flag",
            e_ref=e_ref,
            rest_terms=[("bool", True)],
            meta={"source": "test", "source_loc": "row-1"},
        )

        candidates = store.evaluate_engine(
            derivation_id="drv.where.pushdown.bool",
            version="v1",
            target_pred_id="person:flag",
            head_vars=["$E", "$flag"],
            where=[
                ("pred", "person:flag", ["$E", "$flag"]),
                ("eq", "$flag", True),
            ],
        )

        self.assertTrue(candidates)
        candidate = candidates[0]
        self.assertEqual(candidate.payload["e_ref"], e_ref)
        self.assertEqual(candidate.payload["rest_terms"], [("bool", True)])
        self.assertTrue(candidate.key_tuple_digest.startswith("sha256:"))

    def test_where_pushdown_time_roundtrip(self) -> None:
        if find_souffle_binary() is None:
            self.skipTest("souffle binary not found")

        store = Store(schema_ir=self.schema_ir)
        e_ref = "idref_v1:Person:pushdown-time"
        ts = 1772446272123456789
        set_field(
            store.ledger,
            pred_id="person:seen_at",
            e_ref=e_ref,
            rest_terms=[("time", ts)],
            meta={"source": "test", "source_loc": "row-1"},
        )

        candidates = store.evaluate_engine(
            derivation_id="drv.where.pushdown.time",
            version="v1",
            target_pred_id="person:seen_at",
            head_vars=["$E", "$ts"],
            where=[
                ("pred", "person:seen_at", ["$E", "$ts"]),
                ("eq", "$ts", ts),
            ],
        )

        self.assertTrue(candidates)
        candidate = candidates[0]
        self.assertEqual(candidate.payload["e_ref"], e_ref)
        self.assertEqual(candidate.payload["rest_terms"], [("time", ts)])
        self.assertTrue(candidate.key_tuple_digest.startswith("sha256:"))


if __name__ == "__main__":
    unittest.main()
