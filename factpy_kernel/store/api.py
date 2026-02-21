from __future__ import annotations

import base64
import binascii
import json
import re
import tempfile
import warnings
from pathlib import Path
from typing import Any
from uuid import uuid4

from factpy_kernel.derivation.accept import AcceptOptions, AcceptResult, accept_candidate_set
from factpy_kernel.derivation.candidates import CandidateSet, make_candidate
from factpy_kernel.evidence.write_protocol import now_epoch_nanos
from factpy_kernel.export.tsv_v1 import tsv_cell_v1_decode
from factpy_kernel.policy.active import is_active
from factpy_kernel.policy.chosen import compute_chosen_for_predicate
from factpy_kernel.protocol.digests import sha256_token
from factpy_kernel.protocol.tup_v1 import CANONICAL_TAGS, canonical_bytes_tup_v1
from factpy_kernel.rules.where_compile import extract_where_variables
from factpy_kernel.rules.where_eval import WhereValidationError, evaluate_where
from factpy_kernel.store.ledger import Ledger
from factpy_kernel.view.projector import build_args_for_claim, project_view_facts


_BYTES_B64URL_RE = re.compile(r"^[A-Za-z0-9_-]*$")


class Store:
    def __init__(self, schema_ir: dict, ledger: Ledger | None = None) -> None:
        if not isinstance(schema_ir, dict):
            raise ValueError("schema_ir must be dict")
        self.schema_ir = schema_ir
        self.ledger = ledger if ledger is not None else Ledger()

    def evaluate(
        self,
        derivation_id: str,
        version: str,
        target_pred_id: str,
        head_vars: list[Any],
        where: list[Any],
        mode: str = "python",
    ) -> list[CandidateSet]:
        if mode == "engine":
            return self.evaluate_engine(
                derivation_id=derivation_id,
                version=version,
                target_pred_id=target_pred_id,
                head_vars=head_vars,
                where=where,
            )
        if mode != "python":
            raise ValueError("mode must be 'python' or 'engine'")

        schema_pred = self._find_schema_pred(target_pred_id)
        if schema_pred is None:
            raise WhereValidationError(f"target predicate not found: {target_pred_id}")

        arg_specs = schema_pred.get("arg_specs")
        if not isinstance(arg_specs, list) or not arg_specs:
            raise WhereValidationError("target predicate arg_specs must be non-empty list")

        if not isinstance(head_vars, list) or len(head_vars) != len(arg_specs):
            raise WhereValidationError("head_vars length must match target arg_specs")
        if "$E" not in head_vars:
            raise WhereValidationError("head_vars must include $E")

        view_facts = project_view_facts(self.ledger, self.schema_ir)
        bindings = evaluate_where(view_facts, where)
        if not bindings:
            return []

        return self._candidates_from_bindings(
            derivation_id=derivation_id,
            version=version,
            target_pred_id=target_pred_id,
            arg_specs=arg_specs,
            head_vars=head_vars,
            schema_pred=schema_pred,
            bindings=bindings,
        )

    def evaluate_engine(
        self,
        derivation_id: str,
        version: str,
        target_pred_id: str,
        head_vars: list[Any],
        where: list[Any],
    ) -> list[CandidateSet]:
        """Internal/legacy entrypoint; prefer evaluate(mode='engine')."""
        schema_pred = self._find_schema_pred(target_pred_id)
        if schema_pred is None:
            raise WhereValidationError(f"target predicate not found: {target_pred_id}")

        arg_specs = schema_pred.get("arg_specs")
        if not isinstance(arg_specs, list) or not arg_specs:
            raise WhereValidationError("target predicate arg_specs must be non-empty list")

        if not isinstance(head_vars, list) or len(head_vars) != len(arg_specs):
            raise WhereValidationError("head_vars length must match target arg_specs")
        if "$E" not in head_vars:
            raise WhereValidationError("head_vars must include $E")

        where_variables = extract_where_variables(where)
        missing_vars = [
            value
            for value in head_vars
            if isinstance(value, str) and value.startswith("$") and value not in where_variables
        ]
        if missing_vars:
            raise WhereValidationError(f"head_vars reference unbound where variables: {missing_vars}")

        from factpy_kernel.export.package import ExportOptions, export_package
        from factpy_kernel.rules.where_compile import query_rel_for_where
        from factpy_kernel.runner.runner import run_package

        query_rel = query_rel_for_where(where)
        bindings: list[dict[str, Any]]
        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = Path(tmpdir) / "engine_eval_pkg"
            manifest_path = export_package(
                self,
                out_dir,
                ExportOptions(),
                query={"where": where, "query_rel": query_rel},
            )
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            outputs_map = manifest.get("outputs_map", {})
            query_outputs = outputs_map.get("__query__") if isinstance(outputs_map, dict) else None
            if query_outputs != [query_rel]:
                raise WhereValidationError("query outputs_map is missing or invalid")

            run_package(out_dir, ["__query__"], engine="souffle")
            out_path = out_dir / "outputs" / f"{query_rel}.out.facts"
            bindings = self._read_query_bindings(out_path, where_variables)

        if not bindings:
            return []

        return self._candidates_from_bindings(
            derivation_id=derivation_id,
            version=version,
            target_pred_id=target_pred_id,
            arg_specs=arg_specs,
            head_vars=head_vars,
            schema_pred=schema_pred,
            bindings=bindings,
        )

    def evaluate_dummy(
        self,
        derivation_id: str,
        version: str,
        target: str,
        e_ref: str,
        rest_terms: list[tuple[str, Any]],
        dims_terms: list[tuple[str, Any]],
    ) -> CandidateSet:
        warnings.warn(
            "evaluate_dummy is deprecated; use evaluate()",
            DeprecationWarning,
            stacklevel=2,
        )

        run_id = uuid4().hex
        key_terms = [("string", target), ("entity_ref", e_ref), *dims_terms]
        payload = {
            "e_ref": e_ref,
            "rest_terms": list(rest_terms),
        }
        tup_digest = sha256_token(canonical_bytes_tup_v1(rest_terms))
        return make_candidate(
            derivation_id=derivation_id,
            derivation_version=version,
            run_id=run_id,
            target=target,
            key_terms=key_terms,
            payload=payload,
            support_digest=f"sha256:{'0' * 64}",
            support_kind="none",
            generated_at=now_epoch_nanos(),
            tup_digest=tup_digest,
            state="generated",
        )

    def accept(
        self,
        derivation_id: str,
        version: str,
        candidate_set: CandidateSet,
        options: AcceptOptions,
    ) -> AcceptResult:
        if candidate_set.derivation_id != derivation_id:
            raise ValueError("derivation_id mismatch")
        if candidate_set.derivation_version != version:
            raise ValueError("derivation_version mismatch")
        return accept_candidate_set(
            ledger=self.ledger,
            candidate_set=candidate_set,
            options=options,
            derived_rule_id=derivation_id,
            derived_rule_version=version,
        )

    def explain_fact(self, pred_id: str, e_ref: str, *val_atoms: Any) -> dict[str, Any]:
        schema_pred = self._find_schema_pred(pred_id)

        active_claims = [
            claim
            for claim in self.ledger.find_claims(pred_id=pred_id, e_ref=e_ref)
            if is_active(self.ledger, claim.asrt_id)
        ]

        claim_rows: list[dict[str, Any]] = []
        for claim in active_claims:
            args = build_args_for_claim(self.ledger, claim)
            if val_atoms and tuple(args[1:]) != tuple(val_atoms):
                continue
            claim_rows.append(
                {
                    "asrt_id": claim.asrt_id,
                    "args": args,
                    "meta": self._meta_subset(claim.asrt_id),
                }
            )

        chosen_asrt_id: str | None = None
        if schema_pred is not None and claim_rows:
            chosen_map = compute_chosen_for_predicate(self.ledger, schema_pred)
            chosen_ids = set(chosen_map.values())
            matching = [row["asrt_id"] for row in claim_rows if row["asrt_id"] in chosen_ids]
            if matching:
                chosen_asrt_id = sorted(matching)[0]

        return {
            "pred_id": pred_id,
            "e_ref": e_ref,
            "active_claims": claim_rows,
            "chosen_asrt_id": chosen_asrt_id,
        }

    def conflicts(self, pred_id: str, e_ref: str) -> dict[str, Any]:
        schema_pred = self._find_schema_pred(pred_id)
        active_claims = [
            claim
            for claim in self.ledger.find_claims(pred_id=pred_id, e_ref=e_ref)
            if is_active(self.ledger, claim.asrt_id)
        ]
        active_asrt_ids = [claim.asrt_id for claim in active_claims]

        chosen_asrt_id: str | None = None
        if schema_pred is not None and active_asrt_ids:
            chosen_map = compute_chosen_for_predicate(self.ledger, schema_pred)
            chosen_ids = set(chosen_map.values())
            overlap = sorted([asrt_id for asrt_id in active_asrt_ids if asrt_id in chosen_ids])
            if overlap:
                chosen_asrt_id = overlap[0]

        return {
            "pred_id": pred_id,
            "e_ref": e_ref,
            "active_asrt_ids": active_asrt_ids,
            "chosen_asrt_id": chosen_asrt_id,
        }

    def _candidates_from_bindings(
        self,
        *,
        derivation_id: str,
        version: str,
        target_pred_id: str,
        arg_specs: list[dict[str, Any]],
        head_vars: list[Any],
        schema_pred: dict[str, Any],
        bindings: list[dict[str, Any]],
    ) -> list[CandidateSet]:
        run_id = uuid4().hex
        group_key_indexes = self._read_group_key_indexes(schema_pred, len(arg_specs))

        candidates: list[CandidateSet] = []
        for binding in bindings:
            tagged_args = self._build_tagged_args(arg_specs, head_vars, binding)

            e_tag, e_ref = tagged_args[0]
            if e_tag != "entity_ref":
                raise WhereValidationError("target arg0 must be entity_ref")

            rest_terms = [(tag, value) for tag, value in tagged_args[1:]]
            try:
                canonical_bytes_tup_v1(rest_terms)
            except ValueError as exc:
                raise WhereValidationError(f"invalid rest_terms for target payload: {exc}") from exc

            dims_terms: list[tuple[str, Any]] = []
            for idx in group_key_indexes:
                if idx == 0:
                    continue
                dims_terms.append(tagged_args[idx])

            key_terms = [("string", target_pred_id), ("entity_ref", e_ref), *dims_terms]
            tup_digest = sha256_token(canonical_bytes_tup_v1(rest_terms))

            candidate = make_candidate(
                derivation_id=derivation_id,
                derivation_version=version,
                run_id=run_id,
                target=target_pred_id,
                key_terms=key_terms,
                payload={
                    "e_ref": e_ref,
                    "rest_terms": rest_terms,
                },
                support_digest=f"sha256:{'0' * 64}",
                support_kind="none",
                generated_at=now_epoch_nanos(),
                tup_digest=tup_digest,
                state="generated",
            )
            candidates.append(candidate)

        unique: dict[tuple[Any, ...], CandidateSet] = {}
        for candidate in candidates:
            payload = candidate.payload
            rest_terms = payload.get("rest_terms", [])
            key = (
                candidate.key_tuple_digest,
                payload.get("e_ref"),
                tuple((tag, self._hashable_value(value)) for tag, value in rest_terms),
            )
            if key not in unique:
                unique[key] = candidate

        return sorted(unique.values(), key=lambda cand: (cand.key_tuple_digest, cand.tup_digest or ""))

    @staticmethod
    def _read_query_bindings(out_path: Path, variables: list[str]) -> list[dict[str, Any]]:
        if not out_path.exists():
            raise WhereValidationError(f"missing query output file: {out_path}")

        rows: list[dict[str, Any]] = []
        seen: set[tuple[tuple[str, Any], ...]] = set()
        with out_path.open("r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.rstrip("\n")
                if line == "":
                    continue
                cells = [tsv_cell_v1_decode(cell) for cell in line.split("\t")]
                if len(cells) != len(variables):
                    raise WhereValidationError(
                        f"query output arity mismatch: expected {len(variables)}, got {len(cells)}"
                    )
                binding = {var: value for var, value in zip(variables, cells)}
                key = tuple((var, binding[var]) for var in variables)
                if key in seen:
                    continue
                seen.add(key)
                rows.append(binding)

        rows.sort(key=lambda row: tuple((key, row[key]) for key in sorted(row)))
        return rows

    def _find_schema_pred(self, pred_id: str) -> dict[str, Any] | None:
        predicates = self.schema_ir.get("predicates")
        if not isinstance(predicates, list):
            return None
        for schema_pred in predicates:
            if not isinstance(schema_pred, dict):
                continue
            if schema_pred.get("pred_id") == pred_id:
                return schema_pred
        return None

    def _meta_subset(self, asrt_id: str) -> dict[str, Any]:
        wanted = {"ingested_at", "source", "run_id", "materialize_id", "cand_key_digest"}
        out: dict[str, Any] = {}
        for row in self.ledger.find_meta(asrt_id=asrt_id):
            if row.key in wanted and row.key not in out:
                out[row.key] = row.value
        return out

    @staticmethod
    def _read_group_key_indexes(schema_pred: dict, arg_count: int) -> list[int]:
        group_key_indexes = schema_pred.get("group_key_indexes")
        if not isinstance(group_key_indexes, list):
            raise WhereValidationError("group_key_indexes must be list")

        last = -1
        out: list[int] = []
        for idx in group_key_indexes:
            if isinstance(idx, bool) or not isinstance(idx, int):
                raise WhereValidationError("group_key_indexes values must be int")
            if idx < 0 or idx >= arg_count:
                raise WhereValidationError("group_key_indexes contains out-of-range value")
            if idx <= last:
                raise WhereValidationError("group_key_indexes must be strictly ascending")
            out.append(idx)
            last = idx
        return out

    def _build_tagged_args(
        self,
        arg_specs: list[dict[str, Any]],
        head_vars: list[Any],
        binding: dict[str, Any],
    ) -> list[tuple[str, Any]]:
        tagged_args: list[tuple[str, Any]] = []

        for idx, (arg_spec, head_ref) in enumerate(zip(arg_specs, head_vars)):
            if not isinstance(arg_spec, dict):
                raise WhereValidationError(f"arg_specs[{idx}] must be object")
            tag = arg_spec.get("type_domain")
            if tag not in CANONICAL_TAGS:
                raise WhereValidationError(f"arg_specs[{idx}].type_domain invalid: {tag}")

            value = self._resolve_head_ref(head_ref, binding)
            coerced = self._coerce_value_for_tag(tag, value)
            tagged_args.append((tag, coerced))

        return tagged_args

    @staticmethod
    def _resolve_head_ref(head_ref: Any, binding: dict[str, Any]) -> Any:
        if isinstance(head_ref, str) and head_ref.startswith("$"):
            if head_ref not in binding:
                raise WhereValidationError(f"unbound head variable: {head_ref}")
            return binding[head_ref]
        return head_ref

    @staticmethod
    def _coerce_value_for_tag(tag: str, value: Any) -> Any:
        if tag == "entity_ref":
            if not isinstance(value, str) or not value.startswith("idref_v1:"):
                raise WhereValidationError("entity_ref value must be idref_v1 token")
            candidate = value
        elif tag == "string":
            if not isinstance(value, str):
                raise WhereValidationError("string value must be str")
            candidate = value
        elif tag == "int":
            if isinstance(value, str):
                if not re.fullmatch(r"-?\d+", value):
                    raise WhereValidationError("int string must be decimal integer")
                candidate = int(value)
            elif isinstance(value, bool) or not isinstance(value, int):
                raise WhereValidationError("int value must be int")
            else:
                candidate = value
        elif tag == "float64":
            if isinstance(value, bool) or not isinstance(value, (float, str)):
                raise WhereValidationError("float64 value must be float or 0x<16hex> string")
            candidate = value
        elif tag == "bool":
            if isinstance(value, str):
                if value == "true":
                    candidate = True
                elif value == "false":
                    candidate = False
                else:
                    raise WhereValidationError("bool string must be 'true' or 'false'")
            elif not isinstance(value, bool):
                raise WhereValidationError("bool value must be bool")
            else:
                candidate = value
        elif tag == "bytes":
            candidate = Store._coerce_bytes(value)
        elif tag == "time":
            if isinstance(value, str):
                if not re.fullmatch(r"-?\d+", value):
                    raise WhereValidationError("time string must be epoch-nanos decimal int")
                candidate = int(value)
            elif isinstance(value, bool) or not isinstance(value, int):
                raise WhereValidationError("time value must be epoch-nanos int")
            else:
                candidate = value
        elif tag == "uuid":
            if not isinstance(value, str):
                raise WhereValidationError("uuid value must be canonical string")
            candidate = value
        else:
            raise WhereValidationError(f"unsupported tag: {tag}")

        try:
            canonical_bytes_tup_v1([(tag, candidate)])
        except ValueError as exc:
            raise WhereValidationError(f"invalid {tag} value: {exc}") from exc
        return candidate

    @staticmethod
    def _coerce_bytes(value: Any) -> bytes:
        if isinstance(value, bytes):
            return value
        if isinstance(value, bytearray):
            return bytes(value)
        if isinstance(value, memoryview):
            return value.tobytes()
        if isinstance(value, str):
            if not _BYTES_B64URL_RE.fullmatch(value):
                raise WhereValidationError("bytes string must be base64url no-pad")
            padded = value + ("=" * ((4 - len(value) % 4) % 4))
            try:
                return base64.urlsafe_b64decode(padded.encode("ascii"))
            except (binascii.Error, UnicodeEncodeError) as exc:
                raise WhereValidationError("bytes string must be valid base64url") from exc
        raise WhereValidationError("bytes value must be bytes-like or base64url string")

    @staticmethod
    def _hashable_value(value: Any) -> Any:
        if isinstance(value, (str, int, bool, bytes)):
            return value
        if isinstance(value, bytearray):
            return bytes(value)
        if isinstance(value, memoryview):
            return value.tobytes()
        return str(value)
