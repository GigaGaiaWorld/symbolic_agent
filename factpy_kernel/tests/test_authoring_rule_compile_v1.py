from __future__ import annotations

import unittest

from factpy_kernel.authoring import AuthoringRuleCompileError, compile_authoring_rule_v1


class AuthoringRuleCompileV1Tests(unittest.TestCase):
    def test_compile_authoring_rule_aliases(self) -> None:
        payload = compile_authoring_rule_v1(
            {
                "name": "rules.country_rows",
                "version": "v1",
                "select": ["E", "$C"],
                "body": [("pred", "person:country", ["$E", "$C"])],
                "public": True,
            }
        )
        self.assertEqual(payload["rule_id"], "rules.country_rows")
        self.assertEqual(payload["version"], "v1")
        self.assertEqual(payload["select_vars"], ["$E", "$C"])
        self.assertEqual(payload["where"], [("pred", "person:country", ["$E", "$C"])])
        self.assertTrue(payload["expose"])

    def test_reject_conflicting_select_aliases_with_path(self) -> None:
        with self.assertRaises(AuthoringRuleCompileError) as ctx:
            compile_authoring_rule_v1(
                {
                    "rule_id": "rules.bad",
                    "version": "v1",
                    "select_vars": ["$E"],
                    "select": ["$X"],
                    "where": [("pred", "person:country", ["$E", "$C"])],
                }
            )
        self.assertEqual(ctx.exception.path, "$.select")

    def test_reject_invalid_select_item_path(self) -> None:
        with self.assertRaises(AuthoringRuleCompileError) as ctx:
            compile_authoring_rule_v1(
                {
                    "rule_id": "rules.bad",
                    "where": [("pred", "person:country", ["$E", "$C"])],
                    "select": ["bad-name"],
                }
            )
        self.assertEqual(ctx.exception.path, "$.select[0]")


if __name__ == "__main__":
    unittest.main()
