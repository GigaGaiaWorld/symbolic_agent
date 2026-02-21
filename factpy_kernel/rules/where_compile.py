from __future__ import annotations

import hashlib
import json
from typing import Any

from factpy_kernel.export.pred_norm import normalize_pred_id
from factpy_kernel.rules.where_eval import WhereValidationError


def compile_where_to_query_dl(*, schema_ir: dict, where: list[Any], query_rel: str) -> str:
    if not isinstance(schema_ir, dict):
        raise WhereValidationError("schema_ir must be dict")
    if not isinstance(query_rel, str) or not query_rel:
        raise WhereValidationError("query_rel must be non-empty string")

    pred_arities = _schema_pred_arities(schema_ir)
    bodies = _normalize_where_subset(where)
    variables = extract_where_variables(where)
    if not variables:
        raise WhereValidationError("where must contain at least one variable")

    var_symbols = {var: f"C{i}" for i, var in enumerate(variables)}
    head_vars = [var_symbols[var] for var in variables]
    decl_cols = [f"{var_symbols[var]}:symbol" for var in variables]

    in_rel_values: dict[str, tuple[str, ...]] = {}
    rule_lines: list[str] = []

    for body in bodies:
        bound_vars: set[str] = set()
        body_terms: list[str] = []
        for atom in body:
            body_terms.append(
                _compile_atom(
                    atom=atom,
                    pred_arities=pred_arities,
                    var_symbols=var_symbols,
                    bound_vars=bound_vars,
                    in_rel_values=in_rel_values,
                )
            )
        rule_lines.append(f'{query_rel}({", ".join(head_vars)}) :- {", ".join(body_terms)}.')

    lines: list[str] = []
    for rel_name in sorted(in_rel_values):
        values = in_rel_values[rel_name]
        lines.append(f'.decl {rel_name}(V:symbol)')
        for text in values:
            lines.append(f'{rel_name}({_text_to_symbol(text)}).')
        lines.append("")

    lines.extend(
        [
            f'.decl {query_rel}({", ".join(decl_cols)})',
            f'.output {query_rel}',
            "",
            *rule_lines,
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def extract_where_variables(where: list[Any]) -> list[str]:
    bodies = _normalize_where_subset(where)
    found: set[str] = set()
    for body in bodies:
        for atom in body:
            kind = atom[0]
            if kind == "pred":
                _, _, terms = atom
                for term in terms:
                    if _is_var(term):
                        found.add(term)
            elif kind == "eq":
                _, lhs, rhs = atom
                if _is_var(lhs):
                    found.add(lhs)
                if _is_var(rhs):
                    found.add(rhs)
            elif kind == "in":
                _, var, _ = atom
                if _is_var(var):
                    found.add(var)
    return sorted(found)


def query_rel_for_where(where: list[Any]) -> str:
    # Protocol behavior (hard contract):
    # 1) Internal query results must be addressed by outputs_map["__query__"].
    # 2) Query relation name is fixed to query__<sha256-prefix-8>.
    # 3) Any change here is an incompatible change and must bump protocol/version
    #    (e.g. where_v2 / export_v2 / policy_v2).
    digest = hashlib.sha256(canonical_where_json_bytes(where)).hexdigest()
    return f"query__{digest[:8]}"


def canonical_where_json_bytes(where: list[Any]) -> bytes:
    return json.dumps(
        where,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _compile_atom(
    *,
    atom: tuple[Any, ...],
    pred_arities: dict[str, int],
    var_symbols: dict[str, str],
    bound_vars: set[str],
    in_rel_values: dict[str, tuple[str, ...]],
) -> str:
    kind = atom[0]

    if kind == "pred":
        _, pred_id, terms = atom
        if pred_id not in pred_arities:
            raise WhereValidationError(f"unknown predicate in where: {pred_id}")
        if len(terms) != pred_arities[pred_id]:
            raise WhereValidationError(
                f"arity mismatch for predicate {pred_id}: expected {pred_arities[pred_id]}, got {len(terms)}"
            )

        args: list[str] = []
        for term in terms:
            if _is_var(term):
                args.append(var_symbols[term])
                bound_vars.add(term)
            else:
                args.append(_literal_to_symbol(term))
        return f'{normalize_pred_id(pred_id)}({", ".join(args)})'

    if kind == "eq":
        _, lhs, rhs = atom
        lhs_is_var = _is_var(lhs)
        rhs_is_var = _is_var(rhs)

        if lhs_is_var and rhs_is_var:
            lhs_bound = lhs in bound_vars
            rhs_bound = rhs in bound_vars
            if not lhs_bound and not rhs_bound:
                raise WhereValidationError("eq requires at least one bound/constant side")
            if lhs_bound and not rhs_bound:
                bound_vars.add(rhs)
            if rhs_bound and not lhs_bound:
                bound_vars.add(lhs)
        elif lhs_is_var and not rhs_is_var:
            bound_vars.add(lhs)
        elif rhs_is_var and not lhs_is_var:
            bound_vars.add(rhs)
        else:
            raise WhereValidationError("eq requires at least one variable side")

        lhs_expr = var_symbols[lhs] if lhs_is_var else _literal_to_symbol(lhs)
        rhs_expr = var_symbols[rhs] if rhs_is_var else _literal_to_symbol(rhs)
        return f"{lhs_expr} = {rhs_expr}"

    if kind == "in":
        _, var, values = atom
        if var not in bound_vars:
            raise WhereValidationError(f"in variable must be bound before filter: {var}")

        canonical_values = _canonicalize_in_values(values)
        rel_name = _in_rel_name(canonical_values)
        existing = in_rel_values.get(rel_name)
        if existing is None:
            in_rel_values[rel_name] = canonical_values
        elif existing != canonical_values:
            raise WhereValidationError("in relation name collision detected")

        return f"{rel_name}({var_symbols[var]})"

    raise WhereValidationError(f"unsupported atom kind: {kind}")


def _schema_pred_arities(schema_ir: dict) -> dict[str, int]:
    predicates = schema_ir.get("predicates")
    if not isinstance(predicates, list):
        raise WhereValidationError("schema_ir.predicates must be list")

    arities: dict[str, int] = {}
    for predicate in predicates:
        if not isinstance(predicate, dict):
            continue
        pred_id = predicate.get("pred_id")
        arg_specs = predicate.get("arg_specs")
        if not isinstance(pred_id, str) or not pred_id:
            continue
        if not isinstance(arg_specs, list):
            continue
        arities[pred_id] = len(arg_specs)
    return arities


def _normalize_where_subset(where: Any) -> list[list[tuple[Any, ...]]]:
    if not isinstance(where, list) or not where:
        raise WhereValidationError("where must be non-empty list")

    if all(_is_atom(item) for item in where):
        body = [_validate_atom_subset(item) for item in where]
        if not body:
            raise WhereValidationError("where body must not be empty")
        return [body]

    if all(isinstance(item, list) for item in where):
        bodies: list[list[tuple[Any, ...]]] = []
        for branch in where:
            if not branch:
                raise WhereValidationError("where OR branch must not be empty")
            if not all(_is_atom(atom) for atom in branch):
                raise WhereValidationError("where supports at most 2 list levels")
            bodies.append([_validate_atom_subset(atom) for atom in branch])
        return bodies

    raise WhereValidationError("where must be one-level AND or two-level OR-of-AND")


def _validate_atom_subset(atom: Any) -> tuple[Any, ...]:
    if not _is_atom(atom):
        raise WhereValidationError("invalid atom structure")

    kind = atom[0]
    if kind == "pred":
        if len(atom) != 3:
            raise WhereValidationError("pred atom must be ('pred', pred_id, [terms...])")
        _, pred_id, terms = atom
        if not isinstance(pred_id, str) or not pred_id:
            raise WhereValidationError("pred_id must be non-empty string")
        if not isinstance(terms, list) or not terms:
            raise WhereValidationError("pred terms must be non-empty list")
        for term in terms:
            if not _is_var(term) and not _is_literal(term):
                raise WhereValidationError("pred terms must be variables or literals")
        return atom

    if kind == "eq":
        if len(atom) != 3:
            raise WhereValidationError("eq atom must be ('eq', lhs, rhs)")
        _, lhs, rhs = atom
        lhs_is_var = _is_var(lhs)
        rhs_is_var = _is_var(rhs)
        if not lhs_is_var and not rhs_is_var:
            raise WhereValidationError("eq must be var=literal or var=var")
        for side in (lhs, rhs):
            if not _is_var(side) and not _is_literal(side):
                raise WhereValidationError("eq sides must be variables or literals")
        return atom

    if kind == "in":
        if len(atom) != 3:
            raise WhereValidationError("in atom must be ('in', var, [values...])")
        _, var, values = atom
        if not _is_var(var):
            raise WhereValidationError("in atom first argument must be variable")
        if not isinstance(values, list) or not values:
            raise WhereValidationError("in values must be non-empty list")
        for value in values:
            if not _is_literal(value):
                raise WhereValidationError("in values must be literals")
        return atom

    raise WhereValidationError(f"unsupported atom kind: {kind}")


def _canonicalize_in_values(values: list[Any]) -> tuple[str, ...]:
    canonical = sorted({_literal_to_text(value) for value in values})
    if not canonical:
        raise WhereValidationError("in values must be non-empty")
    return tuple(canonical)


def _in_rel_name(values: tuple[str, ...]) -> str:
    payload = json.dumps(list(values), ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    digest = hashlib.sha256(payload).hexdigest()
    return f"__in_{digest[:12]}"


def _literal_to_symbol(value: Any) -> str:
    return _text_to_symbol(_literal_to_text(value))


def _literal_to_text(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str):
        return value
    raise WhereValidationError("unsupported literal type")


def _text_to_symbol(text: str) -> str:
    escaped = text.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
    return f'"{escaped}"'


def _is_var(value: Any) -> bool:
    return isinstance(value, str) and len(value) > 1 and value.startswith("$")


def _is_literal(value: Any) -> bool:
    if isinstance(value, bool):
        return True
    if isinstance(value, int):
        return True
    if isinstance(value, str) and not value.startswith("$"):
        return True
    return False


def _is_atom(value: Any) -> bool:
    return isinstance(value, tuple) and len(value) >= 1 and isinstance(value[0], str)
