from __future__ import annotations

import unittest

from factpy_kernel.derivation.accept import AcceptOptions
from factpy_kernel.store.api import Store


class DerivationAcceptV1Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.schema_ir = {
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
            ]
        }
        self.store = Store(schema_ir=self.schema_ir)
        self.derivation_id = "derive_country"
        self.version = "v1"
        self.e_ref = "idref_v1:Person:irk4tcjz3wzyl4ja6245k5duzqd3vn5dypm4rr5s7glkdulef4ha"

    def test_evaluate_dummy_generates_candidate_set(self) -> None:
        candidate = self.store.evaluate_dummy(
            derivation_id=self.derivation_id,
            version=self.version,
            target="person:country",
            e_ref=self.e_ref,
            rest_terms=[("string", "de")],
            dims_terms=[],
        )

        self.assertEqual(candidate.derivation_id, self.derivation_id)
        self.assertEqual(candidate.derivation_version, self.version)
        self.assertEqual(candidate.target, "person:country")
        self.assertTrue(candidate.key_tuple_digest.startswith("sha256:"))
        self.assertEqual(candidate.payload["e_ref"], self.e_ref)
        self.assertEqual(candidate.payload["rest_terms"], [("string", "de")])

    def test_accept_writes_claim_claim_args_and_meta(self) -> None:
        candidate = self.store.evaluate_dummy(
            derivation_id=self.derivation_id,
            version=self.version,
            target="person:country",
            e_ref=self.e_ref,
            rest_terms=[("string", "de")],
            dims_terms=[],
        )

        result = self.store.accept(
            derivation_id=self.derivation_id,
            version=self.version,
            candidate_set=candidate,
            options=AcceptOptions(approved_by="alice", note="ok"),
        )

        self.assertEqual(result.accepted_count, 1)
        self.assertEqual(len(result.written_assertions), 1)

        claims = self.store.ledger.find_claims(pred_id="person:country", e_ref=self.e_ref)
        self.assertEqual(len(claims), 1)
        asrt_id = claims[0].asrt_id

        claim_args = [row for row in self.store.ledger.claim_args if row.asrt_id == asrt_id]
        self.assertEqual(len(claim_args), 1)

        meta_rows = self.store.ledger.find_meta(asrt_id=asrt_id)
        meta_keys = {row.key for row in meta_rows}
        self.assertIn("materialize_id", meta_keys)
        self.assertIn("run_id", meta_keys)
        self.assertIn("key_tuple_digest", meta_keys)
        self.assertIn("cand_key_digest", meta_keys)

    def test_repeat_accept_is_noop(self) -> None:
        candidate = self.store.evaluate_dummy(
            derivation_id=self.derivation_id,
            version=self.version,
            target="person:country",
            e_ref=self.e_ref,
            rest_terms=[("string", "de")],
            dims_terms=[],
        )

        first = self.store.accept(
            derivation_id=self.derivation_id,
            version=self.version,
            candidate_set=candidate,
            options=AcceptOptions(),
        )
        second = self.store.accept(
            derivation_id=self.derivation_id,
            version=self.version,
            candidate_set=candidate,
            options=AcceptOptions(),
        )

        claims = self.store.ledger.find_claims(pred_id="person:country", e_ref=self.e_ref)
        self.assertEqual(len(claims), 1)
        self.assertEqual(second.accepted_count, 0)
        self.assertEqual(second.skipped_count, 1)
        self.assertEqual(second.materialize_id, first.materialize_id)

    def test_dry_run_does_not_write_ledger(self) -> None:
        candidate = self.store.evaluate_dummy(
            derivation_id=self.derivation_id,
            version=self.version,
            target="person:country",
            e_ref=self.e_ref,
            rest_terms=[("string", "fr")],
            dims_terms=[],
        )

        before = len(self.store.ledger.find_claims(pred_id="person:country", e_ref=self.e_ref))
        result = self.store.accept(
            derivation_id=self.derivation_id,
            version=self.version,
            candidate_set=candidate,
            options=AcceptOptions(dry_run=True),
        )
        after = len(self.store.ledger.find_claims(pred_id="person:country", e_ref=self.e_ref))

        self.assertEqual(before, after)
        self.assertEqual(result.accepted_count, 1)
        self.assertEqual(result.skipped_count, 0)
        self.assertTrue(bool(result.materialize_id))
        self.assertEqual(len(result.written_assertions), 1)


if __name__ == "__main__":
    unittest.main()
