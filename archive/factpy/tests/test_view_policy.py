import unittest

from factpy import (
    Entity,
    FactPyCompileError,
    Field,
    Identity,
    SchemaCompiler,
    Store,
    export_policy_artifacts,
)


class Company(Entity):
    source_system: str = Identity()
    source_id: str = Identity()
    sector: str = Field(cardinality="functional")


class Person(Entity):
    source_system: str = Identity()
    source_id: str = Identity()
    name: str = Field(cardinality="multi")
    works_at: Company = Field(cardinality="multi")


class CanonicalPerson(Entity):
    uid: str = Identity()
    name: str = Field(cardinality="functional")


class PersonMention(Entity):
    batch_id: str = Identity()
    row_num: int = Identity()
    canon_of: CanonicalPerson = Field(cardinality="functional")


class TestViewPolicy(unittest.TestCase):
    def test_policy_mode_edb_and_idb(self) -> None:
        schema = SchemaCompiler().compile([Person, Company])
        store = Store()

        company = Company(source_system="hr", source_id="c1")
        company.sector.set("Tech")
        company.save(store=store)

        alice = Person(source_system="hr", source_id="p1")
        alice.name.add("Alice")
        alice.works_at.add(company)
        alice.save(store=store)

        edb_artifacts = export_policy_artifacts(store=store, schema_ir=schema, policy_mode="edb")
        idb_artifacts = export_policy_artifacts(store=store, schema_ir=schema, policy_mode="idb")

        self.assertGreater(len(edb_artifacts.active_facts), 0)
        self.assertGreater(len(edb_artifacts.chosen_facts), 0)
        self.assertEqual(len(idb_artifacts.active_facts), 0)
        self.assertEqual(len(idb_artifacts.chosen_facts), 0)
        self.assertTrue(any(item.startswith("active(") for item in idb_artifacts.policy_rules))

        view_text = "\n".join(edb_artifacts.view_rules)
        self.assertIn("claim_arg", view_text)
        self.assertIn("person_name_view", view_text)
        self.assertIn("person_works_at_view", view_text)

    def test_canon_of_functional_violation_errors(self) -> None:
        schema = SchemaCompiler().compile([PersonMention, CanonicalPerson])
        store = Store()

        c1 = CanonicalPerson(uid="cp1")
        c1.name.set("Alice")
        c1.save(store=store)

        c2 = CanonicalPerson(uid="cp2")
        c2.name.set("Alice B")
        c2.save(store=store)

        mention = PersonMention(batch_id="b1", row_num=1)
        mention.canon_of.set(c1)
        mention.save(store=store)
        mention.canon_of.set(c2)
        mention.save(store=store)

        with self.assertRaises(FactPyCompileError) as ctx:
            export_policy_artifacts(store=store, schema_ir=schema, policy_mode="edb")
        self.assertIn("canon_of", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
