import unittest

from symir.errors import SchemaError, ValidationError
from symir.ir.fact_schema import ArgSpec, PredicateSchema, FactSchema
from symir.ir.expr_ir import Var, Const, Call, Unify, If, expr_from_dict
from symir.ir.rule_schema import RefLiteral, ExprLiteral, HeadSchema, Body, Rule, Query
from symir.rules.validator import RuleValidator
from symir.mappers.renderers import ProbLogRenderer, RenderContext
from symir.probability import ProbabilityConfig


class TestLLMRules(unittest.TestCase):
    def _schema(self) -> FactSchema:
        person = PredicateSchema(
            name="Person",
            arity=1,
            signature=[ArgSpec(datatype="string")],
        )
        lives = PredicateSchema(
            name="LivesIn",
            arity=2,
            signature=[ArgSpec(datatype="string"), ArgSpec(datatype="string")],
        )
        return FactSchema([person, lives])

    def test_head_var_only(self) -> None:
        head_pred = PredicateSchema(
            name="Resident",
            arity=1,
            signature=[ArgSpec(datatype="string")],
        )
        with self.assertRaises(SchemaError):
            HeadSchema(predicate=head_pred, terms=[Const(value="alice", datatype="string")])

    def test_multiple_bodies_render(self) -> None:
        schema = self._schema()
        view = schema.view([p.schema_id for p in schema.predicates()])
        head_pred = PredicateSchema(
            name="Resident",
            arity=1,
            signature=[ArgSpec(datatype="string")],
        )
        head = HeadSchema(predicate=head_pred, terms=[Var("X")])
        person_id = schema.predicates()[0].schema_id
        body1 = Body(literals=[RefLiteral(predicate_id=person_id, terms=[Var("X")])], prob=0.5)
        body2 = Body(literals=[RefLiteral(predicate_id=person_id, terms=[Var("X")], negated=True)], prob=0.5)
        rule = Rule(head=head, bodies=[body1, body2])

        RuleValidator(view).validate(rule)
        renderer = ProbLogRenderer()
        text = renderer.render_rule(rule, RenderContext(schema=schema))
        self.assertEqual(len(text.splitlines()), 2)

    def test_ref_literal_not_in_view(self) -> None:
        schema = self._schema()
        view = schema.view([])
        head_pred = PredicateSchema(
            name="Resident",
            arity=1,
            signature=[ArgSpec(datatype="string")],
        )
        head = HeadSchema(predicate=head_pred, terms=[Var("X")])
        person_id = schema.predicates()[0].schema_id
        body = Body(literals=[RefLiteral(predicate_id=person_id, terms=[Var("X")])], prob=0.5)
        rule = Rule(head=head, bodies=[body])
        with self.assertRaises(ValidationError):
            RuleValidator(view).validate(rule)

    def test_negated_ref_literal_render(self) -> None:
        schema = self._schema()
        view = schema.view([p.schema_id for p in schema.predicates()])
        head_pred = PredicateSchema(
            name="Resident",
            arity=1,
            signature=[ArgSpec(datatype="string")],
        )
        head = HeadSchema(predicate=head_pred, terms=[Var("X")])
        person_id = schema.predicates()[0].schema_id
        body = Body(literals=[RefLiteral(predicate_id=person_id, terms=[Var("X")], negated=True)], prob=0.5)
        rule = Rule(head=head, bodies=[body])
        RuleValidator(view).validate(rule)
        text = ProbLogRenderer().render_rule(rule, RenderContext(schema=schema))
        self.assertIn("\\+ Person(X)", text)

    def test_expr_serialization_and_render(self) -> None:
        expr = If(
            cond=Call("gt", [Var("X"), Const(0, "int")]),
            then=Unify(Var("Y"), Call("add", [Var("X"), Const(1, "int")])) ,
            else_=Unify(Var("Y"), Const(0, "int")),
        )
        expr_dict = expr.to_dict()
        expr2 = expr_from_dict(expr_dict)
        self.assertEqual(expr, expr2)

        schema = self._schema()
        view = schema.view([p.schema_id for p in schema.predicates()])
        head_pred = PredicateSchema(
            name="Calc",
            arity=2,
            signature=[ArgSpec(datatype="int"), ArgSpec(datatype="int")],
        )
        head = HeadSchema(predicate=head_pred, terms=[Var("X"), Var("Y")])
        body = Body(literals=[ExprLiteral(expr=expr2)], prob=0.8)
        rule = Rule(head=head, bodies=[body])
        RuleValidator(view).validate(rule)
        text = ProbLogRenderer().render_rule(rule, RenderContext(schema=schema))
        self.assertIn("((X > 0, Y = X + 1) ; (\\+ (X > 0), Y = 0))", text)

    def test_missing_probability_default(self) -> None:
        schema = self._schema()
        view = schema.view([p.schema_id for p in schema.predicates()])
        head_pred = PredicateSchema(
            name="Resident",
            arity=1,
            signature=[ArgSpec(datatype="string")],
        )
        head = HeadSchema(predicate=head_pred, terms=[Var("X")])
        person_id = schema.predicates()[0].schema_id
        body = Body(literals=[RefLiteral(predicate_id=person_id, terms=[Var("X")])], prob=None)
        rule = Rule(head=head, bodies=[body])
        RuleValidator(view).validate(rule)
        config = ProbabilityConfig(default_rule_prob=0.7, missing_prob_policy="inject_default")
        text = ProbLogRenderer().render_rule(rule, RenderContext(schema=schema, prob_config=config))
        self.assertIn("0.7::Resident(X)", text)

    def test_query_render(self) -> None:
        schema = self._schema()
        head_pred = PredicateSchema(
            name="Resident",
            arity=1,
            signature=[ArgSpec(datatype="string")],
        )
        query = Query(predicate=head_pred, terms=[Var("X")])
        text = ProbLogRenderer().render_query(query, RenderContext(schema=schema))
        self.assertEqual("query(Resident(X)).", text)

        person_id = schema.predicates()[0].schema_id
        query_fact = Query(predicate_id=person_id, terms=[Const("alice", "string")])
        text2 = ProbLogRenderer().render_query(query_fact, RenderContext(schema=schema))
        self.assertEqual("query(Person(alice)).", text2)


if __name__ == "__main__":
    unittest.main()
