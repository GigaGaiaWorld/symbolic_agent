from __future__ import annotations

import unittest

from factpy_kernel.evidence.write_protocol import set_field
from factpy_kernel.runner.runner import find_souffle_binary
from factpy_kernel.store.api import Store


class EvaluateModeParityV1Tests(unittest.TestCase):
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
                }
            ],
            "projection": {
                "entities": [],
                "predicates": ["person:country"],
            },
            "protocol_version": {
                "idref_v1": "idref_v1",
                "tup_v1": "tup_v1",
                "export_v1": "export_v1",
            },
            "generated_at": "2026-01-01T00:00:00Z",
        }

    def test_evaluate_bad_mode_raises(self) -> None:
        store = Store(schema_ir=self.schema_ir)
        with self.assertRaises(ValueError):
            store.evaluate(
                derivation_id="drv.parity",
                version="v1",
                target_pred_id="person:country",
                head_vars=["$E", "$country"],
                where=[("pred", "person:country", ["$E", "$country"])],
                mode="bad",
            )

    def test_evaluate_python_engine_mode_parity(self) -> None:
        if find_souffle_binary() is None:
            self.skipTest("souffle binary not found")

        store = Store(schema_ir=self.schema_ir)
        set_field(
            store.ledger,
            pred_id="person:country",
            e_ref="idref_v1:Person:parity-e1",
            rest_terms=[("string", "de")],
            meta={"source": "test", "source_loc": "row-1"},
        )
        set_field(
            store.ledger,
            pred_id="person:country",
            e_ref="idref_v1:Person:parity-e2",
            rest_terms=[("string", "fr")],
            meta={"source": "test", "source_loc": "row-2"},
        )

        where = [("pred", "person:country", ["$E", "$country"])]

        py = store.evaluate(
            derivation_id="drv.parity",
            version="v1",
            target_pred_id="person:country",
            head_vars=["$E", "$country"],
            where=where,
            mode="python",
        )
        en = store.evaluate(
            derivation_id="drv.parity",
            version="v1",
            target_pred_id="person:country",
            head_vars=["$E", "$country"],
            where=where,
            mode="engine",
        )

        py_payload_set = {
            (cand.payload["e_ref"], tuple(cand.payload["rest_terms"])) for cand in py
        }
        en_payload_set = {
            (cand.payload["e_ref"], tuple(cand.payload["rest_terms"])) for cand in en
        }

        self.assertEqual(py_payload_set, en_payload_set)
        self.assertEqual(
            {cand.key_tuple_digest for cand in py},
            {cand.key_tuple_digest for cand in en},
        )
        self.assertEqual(
            {cand.target for cand in py},
            {"person:country"},
        )
        self.assertEqual(
            {cand.target for cand in en},
            {"person:country"},
        )

    def test_evaluate_python_engine_empty_result_parity(self) -> None:
        if find_souffle_binary() is None:
            self.skipTest("souffle binary not found")

        store = Store(schema_ir=self.schema_ir)
        set_field(
            store.ledger,
            pred_id="person:country",
            e_ref="idref_v1:Person:parity-empty-e1",
            rest_terms=[("string", "de")],
            meta={"source": "test", "source_loc": "row-1"},
        )
        set_field(
            store.ledger,
            pred_id="person:country",
            e_ref="idref_v1:Person:parity-empty-e2",
            rest_terms=[("string", "fr")],
            meta={"source": "test", "source_loc": "row-2"},
        )

        where = [
            ("pred", "person:country", ["$E", "$country"]),
            ("eq", "$country", "zz"),
        ]

        py = store.evaluate(
            derivation_id="drv.parity.empty",
            version="v1",
            target_pred_id="person:country",
            head_vars=["$E", "$country"],
            where=where,
            mode="python",
        )
        en = store.evaluate(
            derivation_id="drv.parity.empty",
            version="v1",
            target_pred_id="person:country",
            head_vars=["$E", "$country"],
            where=where,
            mode="engine",
        )

        self.assertEqual(py, [])
        self.assertEqual(en, [])


if __name__ == "__main__":
    unittest.main()
