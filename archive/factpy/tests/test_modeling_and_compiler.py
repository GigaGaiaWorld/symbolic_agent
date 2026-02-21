import unittest
from datetime import date

from factpy import (
    CanonicalTupleCodec,
    Entity,
    EntityRefConst,
    FactPyCompileError,
    Field,
    Identity,
    SchemaCompiler,
    Store,
    ValidTimeValue,
    VersionedValue,
    batch,
)


class Company(Entity):
    source_system: str = Identity()
    source_id: str = Identity()
    sector: str = Field(cardinality="functional")


class Person(Entity):
    """
    自然人实体。docstring 可提取为 schema 元数据。
    """

    source_system: str = Identity()
    source_id: str = Identity()

    name: str = Field(cardinality="multi", description="姓名（允许历史/别名并存）")
    age: int = Field(name="has_age", cardinality="functional")
    phone: str = Field(cardinality="multi", aliases=["mobile", "handy"])
    works_at: Company = Field(cardinality="multi")
    name_by_lang: str = Field(cardinality="functional", fact_key=["lang"])
    lang: str = Field(cardinality="functional")
    salary: int = Field(cardinality="temporal", temporal_mode="valid_time")
    level: str = Field(cardinality="temporal", temporal_mode="versioned")

    class Meta:
        owner = "HR_Platform"
        security_level = "PII_Sensitive"


class TestModelingAndCompiler(unittest.TestCase):
    def test_schema_compiler_emits_expected_ir(self) -> None:
        schema_ir = SchemaCompiler().compile([Person, Company])
        entity_names = {item.name for item in schema_ir.entities}
        self.assertEqual(entity_names, {"Person", "Company"})

        person_ir = next(item for item in schema_ir.entities if item.name == "Person")
        self.assertEqual(person_ir.metadata.get("owner"), "HR_Platform")
        self.assertTrue(any(spec.name == "source_system" for spec in person_ir.identity_fields))
        self.assertTrue(any(spec.field_name == "salary" for spec in person_ir.field_specs))

        mapping = {
            (item.owner_entity, item.field_name): item for item in schema_ir.field_mappings
        }
        self.assertEqual(mapping[("Person", "name")].cardinality, "multi")
        self.assertEqual(mapping[("Person", "age")].base_predicate, "has_age")
        self.assertEqual(mapping[("Person", "name_by_lang")].fact_key_dims, ("lang",))
        self.assertEqual(mapping[("Person", "salary")].temporal_mode, "valid_time")
        self.assertEqual(mapping[("Person", "level")].temporal_mode, "versioned")
        self.assertEqual(mapping[("Person", "name")].subject_position, 0)

        self.assertEqual(schema_ir.term_encoding.name, "typed_tuple_v1")
        self.assertTrue(schema_ir.term_encoding.deterministic)
        self.assertTrue(schema_ir.term_encoding.reversible)

    def test_canonical_tuple_roundtrip_reversible(self) -> None:
        token = CanonicalTupleCodec.encode(
            [
                EntityRefConst("E_person"),
                "E_person",
                42,
                3.14,
                True,
                date(2026, 2, 19),
            ]
        )
        decoded = CanonicalTupleCodec.decode(token)

        self.assertEqual(decoded[0].tag, "entity_ref")
        self.assertEqual(decoded[0].value, "E_person")
        self.assertEqual(decoded[1].tag, "string")
        self.assertEqual(decoded[1].value, "E_person")
        self.assertEqual(decoded[2].tag, "int")
        self.assertEqual(decoded[2].value, 42)
        self.assertEqual(decoded[3].tag, "float")
        self.assertAlmostEqual(decoded[3].value, 3.14)
        self.assertEqual(decoded[4].tag, "bool")
        self.assertTrue(decoded[4].value)
        self.assertEqual(decoded[5].tag, "date")
        self.assertEqual(decoded[5].value, date(2026, 2, 19))

        token2 = CanonicalTupleCodec.encode(
            [EntityRefConst("E_person"), "E_person", 42, 3.14, True, date(2026, 2, 19)]
        )
        self.assertEqual(token, token2)

    def test_append_only_with_view_projection(self) -> None:
        store = Store()
        alice = Person(source_system="hr", source_id="p1")
        alice.age.set(30)
        first = alice.save(store=store)

        alice.age.set(31)
        second = alice.save(store=store)

        self.assertEqual(len(first), 1)
        self.assertEqual(len(second), 1)

        claim_rows = [row for row in store.facts()["claim"] if row[1] == "has_age"]
        self.assertEqual(len(claim_rows), 2)

        view_rows = store.view("has_age")
        self.assertEqual(len(view_rows), 1)
        only = next(iter(view_rows))
        self.assertEqual(only[1], 31)

    def test_multi_add_remove_append_only(self) -> None:
        store = Store()
        alice = Person(source_system="hr", source_id="p2")

        alice.name.add("Alice")
        alice.name.add("A. Smith")
        alice.save(store=store)

        self.assertEqual(len(store.view("person:name")), 2)

        alice.name.remove("Alice")
        alice.save(store=store)

        claim_rows = [row for row in store.facts()["claim"] if row[1] == "person:name"]
        self.assertEqual(len(claim_rows), 3)
        view_values = {row[1] for row in store.view("person:name")}
        self.assertEqual(view_values, {"A. Smith"})

    def test_meta_schema_strict_and_batch_merge(self) -> None:
        store = Store()
        alice = Person(source_system="hr", source_id="p3")
        alice.age.set(30)

        with self.assertRaises(FactPyCompileError):
            alice.save(store=store, meta={"unknown_key": "x"})

        with batch(store=store, meta={"trace_id": "t1", "source": "HR"}):
            alice.age.set(31, meta={"field_source": "manual"})
            ids = alice.save(store=store, meta={"doc_id": "row-1"})

        self.assertEqual(len(ids), 2)

        meta_rows = store.facts().get("meta_str", set())
        keys = {(row[0], row[1], row[2]) for row in meta_rows}
        for aid in ids:
            self.assertIn((aid, "trace_id", "t1"), keys)
            self.assertIn((aid, "source", "HR"), keys)
            self.assertIn((aid, "doc_id", "row-1"), keys)
        self.assertTrue(any(row[1] == "field_source" and row[2] == "manual" for row in keys))

    def test_temporal_valid_time_and_versioned(self) -> None:
        store = Store()
        alice = Person(source_system="hr", source_id="p4")

        alice.salary.add(ValidTimeValue(value=100, start=2020, end=2022))
        alice.salary.add(ValidTimeValue(value=110, start=2021, end=2023))
        alice.salary.add(ValidTimeValue(value=120, start=2022, end=None))

        alice.level.add(VersionedValue(value="L3", version=1))
        alice.level.add(VersionedValue(value="L4", version=2))

        alice.save(store=store)

        salary_owner_claims = [row for row in store.facts()["claim"] if row[1] == "person:salary:owner"]
        self.assertEqual(len(salary_owner_claims), 3)

        person_ref = next(iter(store.view("person:salary:owner")))[1]
        valid_current = store.current_valid_time(
            owner_entity="Person",
            field_name="salary",
            entity_ref=person_ref,
            now=2022,
        )
        self.assertEqual(len(valid_current), 2)

        versioned_current = store.current_versioned(
            owner_entity="Person",
            field_name="level",
            entity_ref=person_ref,
        )
        self.assertEqual(len(versioned_current), 1)

        current_claims = [row for row in store.facts()["claim"] if row[1] == "person:level:current"]
        self.assertGreaterEqual(len(current_claims), 3)

        current_view = store.view("person:level:current")
        true_rows = [row for row in current_view if row[1] is True]
        false_rows = [row for row in current_view if row[1] is False]
        self.assertEqual(len(true_rows), 1)
        self.assertGreaterEqual(len(false_rows), 1)

    def test_temporal_dim_key_enforced(self) -> None:
        store = Store()
        alice = Person(source_system="hr", source_id="p5")
        alice.level.add({"value": "L3", "version": 1, "dims": {"source": "hr"}})

        with self.assertRaises(FactPyCompileError):
            alice.save(store=store)

    def test_low_level_emit(self) -> None:
        store = Store()
        aid = store.emit(
            pred="custom:edge",
            s="E1",
            o=[EntityRefConst("E2"), "friend"],
            meta={"trace_id": "t-custom"},
        )

        claims = [row for row in store.facts()["claim"] if row[0] == aid]
        self.assertEqual(len(claims), 1)

        view = store.view("custom:edge")
        self.assertEqual(view, {("E1", "E2", "friend")})

    def test_batch_collect_error_mode(self) -> None:
        store = Store()
        alice = Person(source_system="hr", source_id="p6")
        alice.age.set(30)

        with batch(store=store, on_error="collect") as tx:
            result = alice.save(store=store, meta={"bad_key": "x"})

        self.assertEqual(result, [])
        self.assertEqual(len(tx.errors), 1)
        self.assertEqual(len(store.facts().get("claim", set())), 0)


if __name__ == "__main__":
    unittest.main()
