from __future__ import annotations

import unittest

from factpy_kernel.authoring import AuthoringDerivationCompileError, compile_authoring_derivation_v1


class AuthoringDerivationCompileV1Tests(unittest.TestCase):
    def test_compile_authoring_derivation_aliases(self) -> None:
        payload = compile_authoring_derivation_v1(
            {
                "name": "drv.country",
                "target": "person:country",
                "select": ["E", "country"],
                "body": [("pred", "person:country", ["$E", "$country"])],
                "mode": "python",
                "temporal_view": "record",
            }
        )
        self.assertEqual(payload["derivation_id"], "drv.country")
        self.assertEqual(payload["target_pred_id"], "person:country")
        self.assertEqual(payload["head_vars"], ["$E", "$country"])
        self.assertEqual(payload["where"], [("pred", "person:country", ["$E", "$country"])])

    def test_reject_invalid_mode_with_path(self) -> None:
        with self.assertRaises(AuthoringDerivationCompileError) as ctx:
            compile_authoring_derivation_v1(
                {
                    "derivation_id": "drv.bad",
                    "target_pred_id": "person:country",
                    "head_vars": ["$E", "$C"],
                    "where": [("pred", "person:country", ["$E", "$C"])],
                    "mode": "bad",
                }
            )
        self.assertEqual(ctx.exception.path, "$.mode")

    def test_reject_head_vars_conflict_path(self) -> None:
        with self.assertRaises(AuthoringDerivationCompileError) as ctx:
            compile_authoring_derivation_v1(
                {
                    "derivation_id": "drv.bad",
                    "target_pred_id": "person:country",
                    "head_vars": ["$E", "$C"],
                    "select": ["$E", "$X"],
                    "where": [("pred", "person:country", ["$E", "$C"])],
                }
            )
        self.assertEqual(ctx.exception.path, "$.select")


if __name__ == "__main__":
    unittest.main()
