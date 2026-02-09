import unittest

from symir.ir.types import (
    Const,
    IRAtom,
    IRPredicateRef,
    IRProgram,
    IRRule,
    Var,
)
from symir.mappers.problog import to_problog

class TestProbLogMapper(unittest.TestCase):
    def test_mapper_output(self) -> None:
        fact_pred = IRPredicateRef(name="Person", arity=1, layer="fact")
        rule_pred = IRPredicateRef(name="Known", arity=1, layer="rule")
        fact = IRAtom(predicate=fact_pred, terms=[Const("alice")], prob=0.7)
        head = IRAtom(predicate=rule_pred, terms=[Var("X")])
        body = [IRAtom(predicate=fact_pred, terms=[Var("X")])]
        rule = IRRule(head=head, body=body, kind="rule_edge")
        program = IRProgram(facts=[fact], rules=[rule])

        output = to_problog(program)
        self.assertIn("0.7::Person(alice).", output)
        self.assertIn("% kind: rule_edge", output)
        self.assertIn("Known(X) :- Person(X).", output)


if __name__ == "__main__":
    unittest.main()
