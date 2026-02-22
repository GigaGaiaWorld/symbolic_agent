from __future__ import annotations

import re
from typing import Any


_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class AuthoringDerivationCompileError(Exception):
    def __init__(self, message: str, *, path: str | None = None) -> None:
        super().__init__(message)
        self.path = path


def compile_authoring_derivation_v1(authoring_derivation: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(authoring_derivation, dict):
        raise _compile_error("authoring_derivation must be object", path="$")

    derivation_id = authoring_derivation.get("derivation_id", authoring_derivation.get("name"))
    if not isinstance(derivation_id, str) or not derivation_id:
        raise _compile_error("derivation_id (or name) must be non-empty string", path="$.derivation_id")

    version = authoring_derivation.get("version", "v1")
    if not isinstance(version, str) or not version:
        raise _compile_error("version must be non-empty string", path="$.version")

    target_pred_id = authoring_derivation.get("target_pred_id", authoring_derivation.get("target"))
    if not isinstance(target_pred_id, str) or not target_pred_id:
        raise _compile_error("target_pred_id (or target) must be non-empty string", path="$.target_pred_id")

    head_vars = _compile_head_vars(authoring_derivation)
    where = _compile_where(authoring_derivation)
    mode = _compile_mode(authoring_derivation)
    temporal_view = _compile_temporal_view(authoring_derivation)

    return {
        "derivation_id": derivation_id,
        "version": version,
        "target_pred_id": target_pred_id,
        "head_vars": head_vars,
        "where": where,
        "mode": mode,
        "temporal_view": temporal_view,
    }


def _compile_head_vars(payload: dict[str, Any]) -> list[Any]:
    has_head = "head_vars" in payload
    has_select = "select" in payload
    if not has_head and not has_select:
        raise _compile_error("head_vars (or select) is required", path="$.head_vars")
    if has_head and has_select and payload["head_vars"] != payload["select"]:
        raise _compile_error("head_vars and select conflict", path="$.select")
    raw = payload["head_vars"] if has_head else payload["select"]
    path = "$.head_vars" if has_head else "$.select"
    if not isinstance(raw, list) or not raw:
        raise _compile_error("head_vars must be non-empty list", path=path)

    out: list[Any] = []
    for idx, item in enumerate(raw):
        item_path = f"{path}[{idx}]"
        if isinstance(item, str):
            if item.startswith("$"):
                if not _IDENT_RE.fullmatch(item[1:]):
                    raise _compile_error("variable must be '$' + identifier", path=item_path)
                out.append(item)
                continue
            # bare identifier is interpreted as variable alias; other strings are literals
            if _IDENT_RE.fullmatch(item):
                out.append(f"${item}")
            else:
                out.append(item)
            continue
        if isinstance(item, (int, bool)):
            out.append(item)
            continue
        raise _compile_error("head var/literal must be str|int|bool", path=item_path)
    return out


def _compile_where(payload: dict[str, Any]) -> list[Any]:
    has_where = "where" in payload
    has_body = "body" in payload
    if not has_where and not has_body:
        raise _compile_error("where (or body) is required", path="$.where")
    if has_where and has_body and payload["where"] != payload["body"]:
        raise _compile_error("where and body conflict", path="$.body")
    raw = payload["where"] if has_where else payload["body"]
    if not isinstance(raw, list) or not raw:
        raise _compile_error("where must be non-empty list", path="$.where" if has_where else "$.body")
    return raw


def _compile_mode(payload: dict[str, Any]) -> str:
    mode = payload.get("mode", "python")
    if mode not in {"python", "engine"}:
        raise _compile_error("mode must be 'python' or 'engine'", path="$.mode")
    return str(mode)


def _compile_temporal_view(payload: dict[str, Any]) -> str:
    temporal_view = payload.get("temporal_view", "record")
    if temporal_view not in {"record", "current"}:
        raise _compile_error("temporal_view must be 'record' or 'current'", path="$.temporal_view")
    return str(temporal_view)


def _compile_error(message: str, *, path: str) -> AuthoringDerivationCompileError:
    return AuthoringDerivationCompileError(message, path=path)
