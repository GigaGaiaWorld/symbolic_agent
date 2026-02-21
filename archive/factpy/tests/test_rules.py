import unittest

from factpy import (
    Entity,
    Field,
    Identity,
    PredicateCall,
    Rule,
    RuleCompileError,
    RuleCompiler,
    SchemaCompiler,
    vars,
)


class Person(Entity):
    source_system: str = Identity()
    source_id: str = Identity()
    name: str = Field(cardinality="functional")


class Company(Entity):
    source_system: str = Identity()
    source_id: str = Identity()
    sector: str = Field(cardinality="functional")


class Employment(Entity):
    uid: str = Identity()
    employee: Person = Field(cardinality="functional")
    employer: Company = Field(cardinality="functional")
    since: int = Field(cardinality="functional")


class TestRules(unittest.TestCase):
    def setUp(self) -> None:
        self.schema = SchemaCompiler().compile([Person, Company, Employment])
        self.compiler = RuleCompiler(self.schema)

    def test_rule_compiles_to_view_predicates(self) -> None:
        with vars() as (c,):
            rule = Rule(
                head=Company.is_tech(c),
                body=[Company(c).sector == "Tech"],
            )

        clause = self.compiler.compile_rule(rule)
        self.assertEqual(clause.body[0].predicate, "company_sector_view")

    def test_reification_expansion_with_explicit_e(self) -> None:
        with vars() as (p, c, e, s):
            rule = Rule(
                head=Person.is_senior(p),
                body=[
                    Employment(e).employee == p,
                    Employment(e).employer == c,
                    Employment(e).since == s,
                ],
            )

        clause = self.compiler.compile_rule(rule)
        preds = [item.predicate for item in clause.body]
        self.assertIn("employment_employee_view", preds)
        self.assertIn("employment_employer_view", preds)
        self.assertIn("employment_since_view", preds)

    def test_reification_expansion_with_structured_constructor(self) -> None:
        with vars() as (p, c, s):
            rule = Rule(
                head=Person.is_senior(p),
                body=[Employment(employee=p, employer=c, since=s)],
            )

        clause = self.compiler.compile_rule(rule)
        atoms = [item for item in clause.body]
        self.assertGreaterEqual(len(atoms), 3)

        preds = {item.predicate for item in atoms}
        self.assertIn("employment_employee_view", preds)
        self.assertIn("employment_employer_view", preds)
        self.assertIn("employment_since_view", preds)

        first_args = {item.args[0].name for item in atoms}
        self.assertEqual(len(first_args), 1)

    def test_schema_validation_unknown_predicate_fails(self) -> None:
        with vars() as (p,):
            rule = Rule(
                head=Person.is_senior(p),
                body=[PredicateCall(predicate="not_exists", args=(p,))],
            )

        with self.assertRaises(RuleCompileError) as ctx:
            self.compiler.compile_rule(rule)
        self.assertIn("not_exists", str(ctx.exception))

    def test_builtin_type_restriction(self) -> None:
        with vars() as (c,):
            rule = Rule(
                head=Company.is_tech(c),
                body=[
                    Company(c).sector == "Tech",
                    c < 10,
                ],
            )

        with self.assertRaises(RuleCompileError):
            self.compiler.compile_rule(rule)


if __name__ == "__main__":
    unittest.main()
