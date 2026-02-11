import unittest

from symir.errors import ValidationError
from symir.ir.fact_schema import ArgSpec, PredicateSchema, FactSchema
from symir.ir.expr_ir import Var, Const, Call, Unify, If, expr_from_dict, Ref
from symir.ir.rule_schema import Expr, Cond, Rule, Query
from symir.rules.validator import RuleValidator
from symir.mappers.renderers import ProbLogRenderer, RenderContext
from symir.probability import ProbabilityConfig


class TestLLMRules(unittest.TestCase):
    def _schema(self) -> FactSchema:
        person = PredicateSchema(
            name="Person",
            arity=1,
            signature=[ArgSpec(spec="string")],
        )
        lives = PredicateSchema(
            name="LivesIn",
            arity=2,
            signature=[ArgSpec(spec="string"), ArgSpec(spec="string")],
        )
        return FactSchema([person, lives])

    def test_rule_roundtrip(self) -> None:
        head_pred = PredicateSchema(
            name="Resident",
            arity=1,
            signature=[ArgSpec(spec="X:string")],
        )
        body_pred = PredicateSchema(
            name="Person",
            arity=1,
            signature=[ArgSpec(spec="X:string")],
        )
        body = Cond(literals=[Ref(schema=body_pred, terms=[Var("X")])], prob=0.5)
        rule = Rule(predicate=head_pred, conditions=[body])
        loaded = Rule.from_dict(rule.to_dict())
        self.assertEqual(rule.predicate.schema_id, loaded.predicate.schema_id)
        self.assertEqual(len(loaded.conditions), 1)

    def test_multiple_conditions_render(self) -> None:
        schema = self._schema()
        view = schema.view(schema.predicates())
        person = schema.predicates()[0]
        body1 = Cond(literals=[Ref(schema=person, terms=[Var("X")])], prob=0.5)
        body2 = Cond(literals=[Ref(schema=person, terms=[Var("X")], negated=True)], prob=0.5)
        head_pred = PredicateSchema(
            name="Resident",
            arity=1,
            signature=[ArgSpec(spec="X:string")],
        )
        rule = Rule(predicate=head_pred, conditions=[body1, body2])

        RuleValidator(view).validate(rule)
        renderer = ProbLogRenderer()
        text = renderer.render_rule(rule, RenderContext(schema=schema))
        self.assertEqual(len(text.splitlines()), 2)

    def test_ref_not_in_view(self) -> None:
        schema = self._schema()
        view = schema.view([])
        person = schema.predicates()[0]
        body = Cond(literals=[Ref(schema=person, terms=[Var("X")])], prob=0.5)
        head_pred = PredicateSchema(
            name="Resident",
            arity=1,
            signature=[ArgSpec(spec="X:string")],
        )
        rule = Rule(predicate=head_pred, conditions=[body])
        with self.assertRaises(ValidationError):
            RuleValidator(view).validate(rule)

    def test_negated_ref_render(self) -> None:
        schema = self._schema()
        view = schema.view(schema.predicates())
        person = schema.predicates()[0]
        body = Cond(literals=[Ref(schema=person, terms=[Var("X")], negated=True)], prob=0.5)
        head_pred = PredicateSchema(
            name="Resident",
            arity=1,
            signature=[ArgSpec(spec="string")],
        )
        rule = Rule(predicate=head_pred, conditions=[body])
        RuleValidator(view).validate(rule)
        text = ProbLogRenderer().render_rule(rule, RenderContext(schema=schema))
        self.assertIn("\\+ Person(X)", text)

    def test_expr_serialization_and_render(self) -> None:
        expr = If(
            cond=Call("gt", [Var("X"), Const(0)]),
            then=Unify(Var("Y"), Call("add", [Var("X"), Const(1)])) ,
            else_=Unify(Var("Y"), Const(0)),
        )
        expr_dict = expr.to_dict()
        expr2 = expr_from_dict(expr_dict)
        self.assertEqual(expr, expr2)

        schema = self._schema()
        view = schema.view(schema.predicates())
        head_pred = PredicateSchema(
            name="Calc",
            arity=2,
            signature=[ArgSpec(spec="X:int"), ArgSpec(spec="Y:int")],
        )
        body = Cond(literals=[Expr(expr=expr2)], prob=0.8)
        rule = Rule(predicate=head_pred, conditions=[body])
        RuleValidator(view).validate(rule)
        text = ProbLogRenderer().render_rule(rule, RenderContext(schema=schema))
        self.assertIn("((X > 0, Y = X + 1) ; (\\+ (X > 0), Y = 0))", text)

    def test_missing_probability_default(self) -> None:
        schema = self._schema()
        view = schema.view(schema.predicates())
        person = schema.predicates()[0]
        body = Cond(literals=[Ref(schema=person, terms=[Var("X")])], prob=None)
        head_pred = PredicateSchema(
            name="Resident",
            arity=1,
            signature=[ArgSpec(spec="X:string")],
        )
        rule = Rule(predicate=head_pred, conditions=[body])
        RuleValidator(view).validate(rule)
        config = ProbabilityConfig(default_rule_prob=0.7, missing_prob_policy="inject_default")
        text = ProbLogRenderer().render_rule(rule, RenderContext(schema=schema, prob_config=config))
        self.assertIn("0.7::Resident(X)", text)

    def test_query_render(self) -> None:
        schema = self._schema()
        head_pred = PredicateSchema(
            name="Resident",
            arity=1,
            signature=[ArgSpec(spec="string")],
        )
        query = Query(predicate=head_pred, terms=[Var("X")])
        text = ProbLogRenderer().render_query(query, RenderContext(schema=schema))
        self.assertEqual("query(Resident(X)).", text)

        person_id = schema.predicates()[0].schema_id
        query_fact = Query(predicate_id=person_id, terms=[Const("alice")])
        text2 = ProbLogRenderer().render_query(query_fact, RenderContext(schema=schema))
        self.assertEqual("query(Person(alice)).", text2)


if __name__ == "__main__":
    unittest.main()
