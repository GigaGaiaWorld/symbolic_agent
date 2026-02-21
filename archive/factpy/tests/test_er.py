import unittest

from factpy import (
    CanonPolicyConfig,
    CanonicalMixin,
    CanonicalTupleCodec,
    ERCompiler,
    Entity,
    FactPyCompileError,
    Field,
    Identity,
    MentionMixin,
    SchemaCompiler,
    Store,
    export_policy_artifacts,
)


class CanonicalPerson(CanonicalMixin, Entity):
    uid: str = Identity(default_factory="uuid4")
    name: str = Field(cardinality="functional")


class CanonicalPersonExternal(CanonicalMixin, Entity):
    external_id: str = Identity()
    name: str = Field(cardinality="functional")


class PersonMention(MentionMixin, Entity):
    batch_id: str = Identity()
    row_num: int = Identity()
    raw_name: str = Field(cardinality="functional")
    raw_dob: str = Field(cardinality="functional")
    canon_of: CanonicalPerson = Field(cardinality="functional")


class Employee(Entity):
    source_system: str = Identity()
    source_id: str = Identity()
    age: int = Field(cardinality="functional")


class TestER(unittest.TestCase):
    def _make_conflicted_store(
        self,
        *,
        meta1: dict[str, object],
        meta2: dict[str, object],
    ) -> tuple[Store, object, str, str, str]:
        schema = SchemaCompiler().compile([PersonMention, CanonicalPerson])
        store = Store()

        c1 = CanonicalPerson()
        c1.name.set("Alice canonical A")
        c1.save(store=store)

        c2 = CanonicalPerson()
        c2.name.set("Alice canonical B")
        c2.save(store=store)

        canonical_rows = store.view("canonicalperson:name")
        self.assertEqual(len(canonical_rows), 2)
        ref_by_name = {row[1]: row[0] for row in canonical_rows}
        c1_ref = ref_by_name["Alice canonical A"]
        c2_ref = ref_by_name["Alice canonical B"]

        mention = PersonMention(batch_id="b1", row_num=1)
        mention.raw_name.set("Alice")
        mention.raw_dob.set("1990-01-01")
        mention.save(store=store)

        mention.canon_of.set(c1, meta=meta1)
        mention.save(store=store)

        mention.canon_of.set(c2, meta=meta2)
        mention.save(store=store)

        mention_ref = next(iter(store.view("personmention:raw_name")))[0]
        return store, schema, mention_ref, c1_ref, c2_ref

    def test_mention_identity_is_record_address(self) -> None:
        store = Store()

        m = PersonMention(batch_id="batchA", row_num=7)
        m.raw_name.set("Alice")
        m.raw_dob.set("1990-01-01")
        m.save(store=store)

        m.raw_name.set("Alice B")
        m.save(store=store)

        rows = [row for row in store.facts().get("claim", set()) if row[1] == "personmention:raw_name"]
        subjects = {row[2] for row in rows}
        self.assertEqual(len(subjects), 1)

    def test_canonical_identity_is_synthetic_or_external(self) -> None:
        store = Store()

        c = CanonicalPerson()
        c.name.set("Alice")
        c.save(store=store)
        c.name.set("Alice Updated")
        c.save(store=store)

        c_rows = [row for row in store.facts().get("claim", set()) if row[1] == "canonicalperson:name"]
        c_subjects = {row[2] for row in c_rows}
        self.assertEqual(len(c_subjects), 1)

        e = CanonicalPersonExternal(external_id="ext-1")
        e.name.set("Ext Alice")
        e.save(store=store)
        e.name.set("Ext Alice Updated")
        e.save(store=store)

        e_rows = [row for row in store.facts().get("claim", set()) if row[1] == "canonicalpersonexternal:name"]
        e_subjects = {row[2] for row in e_rows}
        self.assertEqual(len(e_subjects), 1)

    def test_canon_of_default_error_on_conflict(self) -> None:
        store, schema, _, _, _ = self._make_conflicted_store(
            meta1={"source": "S1", "confidence": 0.5, "ingested_at": "2026-02-19T10:00:00+00:00"},
            meta2={"source": "S2", "confidence": 0.8, "ingested_at": "2026-02-19T11:00:00+00:00"},
        )

        with self.assertRaises(FactPyCompileError):
            export_policy_artifacts(store=store, schema_ir=schema, policy_mode="edb")

    def test_canon_policy_prefer_source(self) -> None:
        store, schema, mention_ref, c1_ref, c2_ref = self._make_conflicted_store(
            meta1={"source": "S1", "confidence": 0.9, "ingested_at": "2026-02-19T10:00:00+00:00"},
            meta2={"source": "S2", "confidence": 0.1, "ingested_at": "2026-02-19T09:00:00+00:00"},
        )
        _ = c1_ref

        artifacts = export_policy_artifacts(
            store=store,
            schema_ir=schema,
            policy_mode="edb",
            canon_policy=CanonPolicyConfig(mode="prefer_source", source_priority=("S2", "S1")),
        )
        self.assertEqual(set(artifacts.canon_chosen_facts), {(mention_ref, c2_ref)})

    def test_canon_policy_max_confidence(self) -> None:
        store, schema, mention_ref, c1_ref, c2_ref = self._make_conflicted_store(
            meta1={"source": "S1", "confidence": 0.3, "ingested_at": "2026-02-19T10:00:00+00:00"},
            meta2={"source": "S2", "confidence": 0.7, "ingested_at": "2026-02-19T09:00:00+00:00"},
        )
        _ = c1_ref

        artifacts = export_policy_artifacts(
            store=store,
            schema_ir=schema,
            policy_mode="edb",
            canon_policy=CanonPolicyConfig(mode="max_confidence"),
        )
        self.assertEqual(set(artifacts.canon_chosen_facts), {(mention_ref, c2_ref)})

    def test_canon_policy_latest(self) -> None:
        store, schema, mention_ref, c1_ref, c2_ref = self._make_conflicted_store(
            meta1={"source": "S1", "confidence": 0.8, "ingested_at": "2026-02-19T08:00:00+00:00"},
            meta2={"source": "S2", "confidence": 0.8, "ingested_at": "2026-02-19T12:00:00+00:00"},
        )
        _ = c1_ref

        artifacts = export_policy_artifacts(
            store=store,
            schema_ir=schema,
            policy_mode="edb",
            canon_policy=CanonPolicyConfig(mode="latest"),
        )
        self.assertEqual(set(artifacts.canon_chosen_facts), {(mention_ref, c2_ref)})

    def test_canon_policy_determinism(self) -> None:
        store, schema, _, _, _ = self._make_conflicted_store(
            meta1={"source": "S1", "confidence": 0.5, "ingested_at": "2026-02-19T10:00:00+00:00"},
            meta2={"source": "S1", "confidence": 0.5, "ingested_at": "2026-02-19T10:00:00+00:00"},
        )

        cfg = CanonPolicyConfig(mode="max_confidence", stable_tie_break=("assertion_id",))
        a1 = export_policy_artifacts(store=store, schema_ir=schema, policy_mode="edb", canon_policy=cfg)
        a2 = export_policy_artifacts(store=store, schema_ir=schema, policy_mode="edb", canon_policy=cfg)

        self.assertEqual(a1.canon_chosen_facts, a2.canon_chosen_facts)
        self.assertEqual(a1.chosen_facts, a2.chosen_facts)

    def test_bridge_key_to_mention_roundtrip(self) -> None:
        store = Store()
        mention = PersonMention(batch_id="batchB", row_num=8)
        mention.raw_name.set("Bob")
        mention.raw_dob.set("1988-08-08")
        mention.save(store=store)

        mention_ref = next(iter(store.view("personmention:raw_name")))[0]

        er = ERCompiler()
        er.emit_key_to_mention(
            store=store,
            source_key=("HR", "row-8"),
            mention=mention,
            meta={"source": "HR"},
        )

        bridge_rows = store.view("er:key_to_mention")
        self.assertEqual(len(bridge_rows), 1)
        source_key_token, mention_value = next(iter(bridge_rows))
        self.assertEqual(mention_value, mention_ref)

        decoded = CanonicalTupleCodec.decode(source_key_token)
        self.assertEqual(tuple(item.value for item in decoded), ("HR", "row-8"))

    def test_er_does_not_affect_non_mapping_predicates(self) -> None:
        store = Store()
        schema = SchemaCompiler().compile([Employee, PersonMention, CanonicalPerson])

        emp = Employee(source_system="HR", source_id="u1")
        emp.age.set(20)
        emp.save(store=store)
        emp.age.set(25)
        emp.save(store=store)

        c1 = CanonicalPerson()
        c1.name.set("X")
        c1.save(store=store)
        c2 = CanonicalPerson()
        c2.name.set("Y")
        c2.save(store=store)

        m = PersonMention(batch_id="b1", row_num=9)
        m.raw_name.set("Alice")
        m.raw_dob.set("1990-01-01")
        m.save(store=store)
        m.canon_of.set(c1, meta={"source": "S1"})
        m.save(store=store)
        m.canon_of.set(c2, meta={"source": "S2"})
        m.save(store=store)

        artifacts = export_policy_artifacts(
            store=store,
            schema_ir=schema,
            policy_mode="edb",
            canon_policy=CanonPolicyConfig(mode="prefer_source", source_priority=("S2", "S1")),
        )

        age_view = store.view("employee:age")
        self.assertEqual({row[1] for row in age_view}, {25})

        age_claim_ids = {
            row[0] for row in store.facts().get("claim", set()) if row[1] == "employee:age"
        }
        chosen_ids = {row[0] for row in artifacts.chosen_facts}
        self.assertEqual(len(age_claim_ids & chosen_ids), 1)

    def test_policy_mode_idb_rejects_without_support(self) -> None:
        store, schema, _, _, _ = self._make_conflicted_store(
            meta1={"source": "S1", "confidence": 0.5, "ingested_at": "2026-02-19T10:00:00+00:00"},
            meta2={"source": "S2", "confidence": 0.8, "ingested_at": "2026-02-19T11:00:00+00:00"},
        )

        with self.assertRaises(FactPyCompileError) as ctx:
            export_policy_artifacts(
                store=store,
                schema_ir=schema,
                policy_mode="idb",
                canon_policy=CanonPolicyConfig(mode="prefer_source", source_priority=("S2", "S1")),
            )
        msg = str(ctx.exception)
        self.assertIn("policy_mode='idb'", msg)
        self.assertIn("edb", msg)


if __name__ == "__main__":
    unittest.main()
