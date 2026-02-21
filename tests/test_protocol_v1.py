from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from symir.protocol.digests import sha256_token
from symir.protocol.idref_v1 import encode_idref_v1
from symir.protocol.tup_v1 import TUP_V1_PREFIX, canonical_bytes_tup_v1, claim_args_from_rest_terms


def test_idref_v1_matches_doc_vector() -> None:
    identity_fields = [
        ("source_system", "string", "HR"),
        ("source_id", "string", "123"),
    ]
    token = encode_idref_v1("Person", identity_fields)
    assert (
        token
        == "idref_v1:Person:irk4tcjz3wzyl4ja6245k5duzqd3vn5dypm4rr5s7glkdulef4ha"
    )


def test_tup_v1_example_a_claim_args_and_digest() -> None:
    entity_ref = "idref_v1:Person:irk4tcjz3wzyl4ja6245k5duzqd3vn5dypm4rr5s7glkdulef4ha"
    rest_terms = [("entity_ref", entity_ref), ("string", "de"), ("int", 3)]
    claim_args = claim_args_from_rest_terms(rest_terms)
    assert claim_args == [
        (0, entity_ref, "entity_ref"),
        (1, "de", "string"),
        (2, 3, "int"),
    ]
    canonical = canonical_bytes_tup_v1(rest_terms)
    assert canonical.startswith(TUP_V1_PREFIX)
    assert (
        sha256_token(canonical)
        == "sha256:5a02dcfdd201f292bd4175e543657b17f1d549b42cb2864a9208b54bdafe793e"
    )


def test_tup_v1_example_b_time_and_bytes_val_atom() -> None:
    rest_terms = [("bytes", b"\x00\xff\x10"), ("time", 1772446272123456789)]
    claim_args = claim_args_from_rest_terms(rest_terms)
    assert claim_args == [
        (0, "AP8Q", "bytes"),
        (1, 1772446272123456789, "time"),
    ]


def test_reject_invalid_entity_ref_and_invalid_float64_hex() -> None:
    with pytest.raises(ValueError):
        claim_args_from_rest_terms([("entity_ref", "Person__abc123")])

    with pytest.raises(ValueError):
        canonical_bytes_tup_v1([("float64", "0x1234")])
