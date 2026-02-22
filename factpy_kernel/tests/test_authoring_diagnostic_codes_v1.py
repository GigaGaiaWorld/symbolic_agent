from __future__ import annotations

import re
import unittest
from pathlib import Path

from factpy_kernel.authoring.diagnostic_codes import (
    AUTHORING_DIAGNOSTIC_CODES_V1,
    AUTHORING_DIAGNOSTIC_CODES_V1_SET,
    AUTHORING_ERROR_CODES_V1,
    AUTHORING_WARNING_CODES_V1,
    CODE_AUTHORING_DERIVATION_COMPILE_ERROR,
    CODE_AUTHORING_RULE_COMPILE_ERROR,
    CODE_AUTHORING_SCHEMA_COMPILE_ERROR,
    CODE_DERIVATION_PREVIEW_ERROR,
    CODE_EMPTY_PREDICATES,
    CODE_PREVIEW_TRUNCATED,
    CODE_REGISTRY_RULE_ERROR,
    CODE_RULE_COMPILE_ERROR,
    CODE_RULE_SPEC_ERROR,
    CODE_SCHEMA_VALIDATION_ERROR,
    CODE_SOUFFLE_BINARY_MISSING,
    CODE_TEMPORAL_CURRENT_NO_PRED_REFS,
    CODE_TEMPORAL_CURRENT_NO_TEMPORAL_SCHEMA_PREDICATES,
    CODE_TEMPORAL_CURRENT_NO_TEMPORAL_WHERE_PREDICATES,
)


class AuthoringDiagnosticCodesV1Tests(unittest.TestCase):
    def test_codes_are_unique_and_partitioned(self) -> None:
        self.assertEqual(len(AUTHORING_DIAGNOSTIC_CODES_V1), len(AUTHORING_DIAGNOSTIC_CODES_V1_SET))
        self.assertEqual(
            AUTHORING_DIAGNOSTIC_CODES_V1_SET,
            set(AUTHORING_ERROR_CODES_V1).union(AUTHORING_WARNING_CODES_V1),
        )
        self.assertTrue(set(AUTHORING_ERROR_CODES_V1).isdisjoint(set(AUTHORING_WARNING_CODES_V1)))

    def test_registry_contains_current_preflight_codes(self) -> None:
        expected = {
            CODE_AUTHORING_SCHEMA_COMPILE_ERROR,
            CODE_SCHEMA_VALIDATION_ERROR,
            CODE_EMPTY_PREDICATES,
            CODE_REGISTRY_RULE_ERROR,
            CODE_RULE_SPEC_ERROR,
            CODE_RULE_COMPILE_ERROR,
            CODE_AUTHORING_RULE_COMPILE_ERROR,
            CODE_DERIVATION_PREVIEW_ERROR,
            CODE_AUTHORING_DERIVATION_COMPILE_ERROR,
            CODE_PREVIEW_TRUNCATED,
            CODE_SOUFFLE_BINARY_MISSING,
            CODE_TEMPORAL_CURRENT_NO_PRED_REFS,
            CODE_TEMPORAL_CURRENT_NO_TEMPORAL_SCHEMA_PREDICATES,
            CODE_TEMPORAL_CURRENT_NO_TEMPORAL_WHERE_PREDICATES,
        }
        self.assertEqual(AUTHORING_DIAGNOSTIC_CODES_V1_SET, expected)

    def test_preflight_uses_registry_constants_for_code_fields(self) -> None:
        preflight_py = Path(__file__).resolve().parents[1] / "authoring" / "preflight.py"
        text = preflight_py.read_text(encoding="utf-8")
        self.assertIsNone(re.search(r'code\\s*=\\s*["\\\']', text))


if __name__ == "__main__":
    unittest.main()

