"""Data provider abstraction and CSV implementation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional
import csv

from symir.errors import ProviderError
from symir.ir.fact_schema import FactSchema, FactView, PredicateSchema
from symir.ir.filters import FilterAST, apply_filter
from symir.ir.expr_ir import Const
from symir.probability import ProbabilityConfig, resolve_probability


@dataclass(frozen=True)
class FactInstance:
    predicate_id: str
    terms: list[Const]
    prob: Optional[float] = None


class DataProvider:
    """Abstract data provider interface."""

    def __init__(self, schema: FactSchema, prob_config: Optional[ProbabilityConfig] = None) -> None:
        self.schema = schema
        self.prob_config = prob_config or ProbabilityConfig()

    def query(self, view: FactView, filt: Optional[FilterAST] = None) -> list[FactInstance]:  # pragma: no cover - interface
        raise NotImplementedError


@dataclass(frozen=True)
class CSVSource:
    predicate_id: str
    file: str
    columns: list[str]
    prob_column: Optional[str] = None


class CSVProvider(DataProvider):
    """CSV-backed data provider."""

    def __init__(
        self,
        schema: FactSchema,
        base_path: Path,
        sources: list[CSVSource],
        prob_config: Optional[ProbabilityConfig] = None,
    ) -> None:
        if not isinstance(schema, FactSchema):
            raise ProviderError(
                "CSVProvider requires FactSchema from ir.fact_schema (predicate schema), "
                "not the CSV mapping schema. Use symir.ir.fact_schema.FactSchema."
            )
        super().__init__(schema=schema, prob_config=prob_config)
        self.base_path = base_path
        self.sources = {source.predicate_id: source for source in sources}

    def query(self, view: FactView, filt: Optional[FilterAST] = None) -> list[FactInstance]:
        allowed_ids = set(view.schema_ids)
        if filt is not None:
            filtered = apply_filter(self.schema.predicates(), filt)
            allowed_ids = allowed_ids.intersection({p.schema_id for p in filtered})
        facts: list[FactInstance] = []
        for schema_id in allowed_ids:
            if schema_id not in self.sources:
                raise ProviderError(f"Missing CSV source mapping for schema_id: {schema_id}")
            pred_schema = self.schema.get(schema_id)
            if pred_schema is None:
                raise ProviderError(f"Unknown predicate schema_id: {schema_id}")
            source = self.sources[schema_id]
            facts.extend(self._load_source(pred_schema, source))
        return facts

    def _load_source(self, pred_schema: PredicateSchema, source: CSVSource) -> list[FactInstance]:
        path = (self.base_path / source.file).resolve()
        if not path.exists():
            raise ProviderError(f"CSV file not found: {path}")
        if len(source.columns) != pred_schema.arity:
            raise ProviderError(
                f"CSV column mapping arity mismatch for {pred_schema.name}: expected {pred_schema.arity}"
            )
        results: list[FactInstance] = []
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames is None:
                raise ProviderError(f"CSV file has no header: {path}")
            raw_fieldnames = list(reader.fieldnames)
            normalized_fieldnames = [name.strip() for name in raw_fieldnames]
            fieldname_map = dict(zip(raw_fieldnames, normalized_fieldnames))
            missing = [c for c in source.columns if c not in normalized_fieldnames]
            if missing:
                raise ProviderError(f"CSV file {path} missing columns: {missing}")
            prob_available = source.prob_column in normalized_fieldnames if source.prob_column else False
            for idx, row in enumerate(reader, start=2):
                normalized_row = {
                    fieldname_map[key]: (value.strip() if isinstance(value, str) else value)
                    for key, value in row.items()
                }
                try:
                    terms = []
                    for col, arg_spec in zip(source.columns, pred_schema.signature):
                        value = self._coerce_value(normalized_row.get(col), path, idx, col)
                        terms.append(Const(value=value, datatype=arg_spec.datatype))
                    prob = None
                    if source.prob_column:
                        prob_raw = normalized_row.get(source.prob_column) if prob_available else None
                        prob = resolve_probability(
                            self._maybe_float(prob_raw),
                            default_value=self.prob_config.default_fact_prob,
                            policy=self.prob_config.missing_prob_policy,
                            context=f"fact {pred_schema.name} row {idx}",
                        )
                    else:
                        prob = resolve_probability(
                            None,
                            default_value=self.prob_config.default_fact_prob,
                            policy=self.prob_config.missing_prob_policy,
                            context=f"fact {pred_schema.name} row {idx}",
                        )
                    results.append(FactInstance(predicate_id=pred_schema.schema_id, terms=terms, prob=prob))
                except ProviderError:
                    raise
                except Exception as exc:
                    raise ProviderError(f"Error in {path} row {idx}: {exc}") from exc
        return results

    def _coerce_value(self, raw: Optional[str], path: Path, row: int, col: str) -> str:
        if raw is None:
            raise ProviderError(f"Missing value in {path} row {row} column {col}")
        value = raw.strip() if isinstance(raw, str) else str(raw)
        if value == "":
            raise ProviderError(f"Empty value in {path} row {row} column {col}")
        return value

    def _maybe_float(self, raw: Optional[str]) -> Optional[float]:
        if raw is None:
            return None
        if isinstance(raw, str) and raw.strip() == "":
            return None
        try:
            return float(raw)
        except (TypeError, ValueError):
            raise ProviderError(f"Invalid probability value: {raw}")
