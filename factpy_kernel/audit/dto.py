from __future__ import annotations

from typing import Any

from .query import AuditQuery, AuditQueryError


class AuditDTOError(Exception):
    pass


def build_run_list_dto(query: AuditQuery) -> dict[str, Any]:
    _ensure_query(query)
    runs = query.list_runs()
    items = [_run_summary_item(row) for row in runs]
    return {
        "audit_ui_dto_version": "audit_ui_dto_v1",
        "kind": "run_list",
        "count": len(items),
        "runs": items,
    }


def build_run_detail_dto(query: AuditQuery, run_id: str) -> dict[str, Any]:
    _ensure_query(query)
    try:
        bundle = query.get_run_bundle(run_id)
    except AuditQueryError as exc:
        raise AuditDTOError(str(exc)) from exc

    run = dict(bundle["run"])
    decisions = [dict(row) for row in bundle["decisions"]]
    materializations = [dict(row) for row in bundle["materializations"]]
    candidates = [dict(row) for row in bundle["candidates"]]
    failures = [dict(row) for row in bundle["failures"]]
    timeline = _build_timeline(decisions, failures)

    return {
        "audit_ui_dto_version": "audit_ui_dto_v1",
        "kind": "run_detail",
        "run_id": run_id,
        "run": run,
        "stats": {
            "materialization_count": len(materializations),
            "candidate_count": len(candidates),
            "decision_count": len(decisions),
            "failure_count": len(failures),
            "has_failures": bool(run.get("has_failures")),
        },
        "decision_ids": [row.get("decision_id") for row in decisions if isinstance(row.get("decision_id"), str)],
        "materialize_ids": [row.get("materialize_id") for row in materializations if isinstance(row.get("materialize_id"), str)],
        "decisions": decisions,
        "materializations": materializations,
        "candidates": candidates,
        "failures": failures,
        "timeline": timeline,
        "event_source_counts": dict(run.get("event_source_counts") or {}),
        "event_kind_counts": dict(run.get("event_kind_counts") or {}),
    }


def build_decision_detail_dto(query: AuditQuery, decision_id: str) -> dict[str, Any]:
    _ensure_query(query)
    try:
        decision = query.get_decision(decision_id)
    except AuditQueryError as exc:
        raise AuditDTOError(str(exc)) from exc
    if decision is None:
        raise AuditDTOError(f"decision not found: {decision_id}")

    failures = [row for row in query.list_failures() if row.get("decision_id") == decision_id]
    materialize_ids = _sorted_strings(decision.get("materialize_ids"))
    run_ids = _sorted_strings(decision.get("run_ids"))
    if isinstance(decision.get("materialize_id"), str):
        materialize_ids = sorted({*materialize_ids, decision["materialize_id"]})
    if isinstance(decision.get("run_id"), str):
        run_ids = sorted({*run_ids, decision["run_id"]})

    materializations: list[dict[str, Any]] = []
    for materialize_id in materialize_ids:
        materializations.extend(query.list_materializations(materialize_id=materialize_id))
    materializations = _dedupe_rows(materializations, keys=("materialize_id", "asrt_id"))

    candidates = _dedupe_rows(
        [row for row in query.list_candidates() if row.get("decision_id") == decision_id],
        keys=("candidate_id", "asrt_id", "decision_id"),
    )
    runs = [row for row in query.list_runs() if row.get("run_id") in set(run_ids)]
    runs = sorted(runs, key=lambda row: str(row.get("run_id", "")))

    return {
        "audit_ui_dto_version": "audit_ui_dto_v1",
        "kind": "decision_detail",
        "decision_id": decision_id,
        "decision": dict(decision),
        "runs": runs,
        "materializations": materializations,
        "candidates": candidates,
        "failures": [dict(row) for row in failures],
        "related": {
            "run_ids": run_ids,
            "materialize_ids": materialize_ids,
        },
    }


def _ensure_query(query: AuditQuery) -> None:
    if not isinstance(query, AuditQuery):
        raise AuditDTOError("query must be AuditQuery")


def _run_summary_item(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "run_id": row.get("run_id"),
        "claim_count": row.get("claim_count", 0),
        "decision_count": row.get("decision_count", 0),
        "error_count": row.get("error_count", 0),
        "has_failures": bool(row.get("has_failures")),
        "event_ts_min": row.get("event_ts_min"),
        "event_ts_max": row.get("event_ts_max"),
        "materialize_ids": _sorted_strings(row.get("materialize_ids")),
        "pred_ids": _sorted_strings(row.get("pred_ids")),
    }


def _build_timeline(
    decisions: list[dict[str, Any]],
    failures: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for row in decisions:
        entries.append(
            {
                "entry_kind": "decision",
                "decision_id": row.get("decision_id"),
                "event_source": row.get("event_source"),
                "event_kind": row.get("event_kind"),
                "event_ts": row.get("event_ts"),
            }
        )
    for row in failures:
        entries.append(
            {
                "entry_kind": "failure",
                "decision_id": row.get("decision_id"),
                "event_source": row.get("event_source"),
                "event_kind": row.get("event_kind"),
                "event_ts": row.get("event_ts"),
                "error_class": row.get("error_class"),
                "message": row.get("message"),
            }
        )
    return sorted(entries, key=_timeline_sort_key)


def _timeline_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    event_ts = row.get("event_ts")
    event_ts_key = event_ts if isinstance(event_ts, int) and not isinstance(event_ts, bool) else -1
    return (
        event_ts_key,
        str(row.get("decision_id", "")),
        str(row.get("entry_kind", "")),
    )


def _sorted_strings(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return sorted([item for item in value if isinstance(item, str)])


def _dedupe_rows(rows: list[dict[str, Any]], *, keys: tuple[str, ...]) -> list[dict[str, Any]]:
    seen: set[tuple[str, ...]] = set()
    out: list[dict[str, Any]] = []
    for row in rows:
        key = tuple(str(row.get(name, "")) for name in keys)
        if key in seen:
            continue
        seen.add(key)
        out.append(dict(row))
    return sorted(out, key=lambda row: tuple(str(row.get(name, "")) for name in keys))
