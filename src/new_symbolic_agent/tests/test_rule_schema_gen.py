import json
import unittest

from new_symbolic_agent.ir.fact_schema import ArgSpec, PredicateSchema, FactSchema
from new_symbolic_agent.rules.constraint_schemas import (
    build_pydantic_rule_model,
    build_responses_schema,
    build_predicate_catalog,
)


class TestRuleSchemaGen(unittest.TestCase):
    def test_pydantic_and_json_schema(self) -> None:
        parent = PredicateSchema(
            name="Parent",
            arity=2,
            signature=[ArgSpec(datatype="string"), ArgSpec(datatype="string")],
        )
        schema = FactSchema([parent])
        view = schema.view([parent.schema_id])
        model = build_pydantic_rule_model(view, mode="compact")
        instance = model.model_validate(
            {
                "bodies": [
                    {
                        "literals": [
                            {
                                "kind": "ref",
                                "predicate_id": parent.schema_id,
                                "args": [
                                    {"name": "p", "value": {"kind": "var", "name": "X"}},
                                    {"name": "c", "value": {"kind": "var", "name": "Y"}},
                                ],
                            }
                        ],
                        "prob": 0.6,
                    }
                ],
            }
        )
        self.assertEqual(len(instance.bodies), 1)

        json_schema = build_responses_schema(view, mode="compact")
        json_blob = json.dumps(json_schema)
        self.assertIn(parent.schema_id, json_blob)

        catalog = build_predicate_catalog(view)
        self.assertIn(parent.schema_id, catalog)


if __name__ == "__main__":
    unittest.main()
