from __future__ import annotations

import hashlib
import json
import logging
import os
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor

from webapp.audit_openrouter import call_openrouter_audit

logger = logging.getLogger(__name__)

SERVER_BASE_DIR = Path("/home/kv145/traffic-analytics")
LOCAL_BASE_DIR = Path(__file__).resolve().parents[1]
BASE_DIR = SERVER_BASE_DIR if SERVER_BASE_DIR.exists() else LOCAL_BASE_DIR
ENV_PATH = BASE_DIR / ".env"
ENV_AI_PATH = BASE_DIR / ".env_ai"


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or "=" not in line:
                continue
            if line.startswith("export "):
                line = line[len("export ") :].strip()
            k, v = line.split("=", 1)
            if k.strip() and os.getenv(k.strip()) is None:
                os.environ[k.strip()] = v.strip().strip('"').strip("'")


def _load_env() -> None:
    _load_env_file(ENV_PATH)
    if not ENV_PATH.exists():
        _load_env_file(ENV_AI_PATH)


_load_env()


ALLOWED_TRANSITIONS: dict[str, set[str]] = {
    "pending": {"running", "failed"},
    "running": {"reviewed", "requires_fix", "approved", "failed"},
    "reviewed": {"requires_fix", "approved", "failed"},
    "requires_fix": {"approved", "failed"},
    "approved": set(),
    "failed": {"pending"},
}


JSON_FIELDS = {
    "top_risks_json",
    "required_fixes_json",
    "strengths_json",
    "changed_modules_json",
    "response_json",
    "raw_response_json",
}

REVIEW_DECISIONS = {
    "approve": "approved",
    "reject": "rejected",
    "needs_fix": "needs_fix",
}


def _db():
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        dbname=os.getenv("PGDATABASE", "traffic_analytics"),
        user=os.getenv("PGUSER", "traffic_admin"),
        password=os.getenv("PGPASSWORD"),
        cursor_factory=RealDictCursor,
    )


def _fetch_one(sql: str, params: tuple = ()) -> dict[str, Any] | None:
    conn = _db()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        conn.close()


def _fetch_all(sql: str, params: tuple = ()) -> list[dict[str, Any]]:
    conn = _db()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def _execute(sql: str, params: tuple = ()) -> None:
    conn = _db()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
    finally:
        conn.close()


def _validate_transition(current: str, target: str) -> None:
    if current == target:
        return
    allowed = ALLOWED_TRANSITIONS.get(current, set())
    if target not in allowed:
        raise ValueError(f"invalid status transition: {current} -> {target}")


def _normalize_text(value: str) -> str:
    lines = [" ".join((line or "").strip().split()) for line in str(value or "").splitlines()]
    return "\n".join([x for x in lines if x]).strip()


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _cache_ttl_hours() -> int:
    try:
        raw = int(str(os.getenv("AUDIT_CACHE_TTL_HOURS", "24")).strip() or "24")
        return max(1, min(raw, 168))
    except Exception:
        return 24


def _prompt_version() -> str:
    return str(os.getenv("AUDIT_PROMPT_VERSION", "v1")).strip() or "v1"


def _fetch_project_context(project_id: str) -> dict[str, Any]:
    try:
        row = _fetch_one(
            """
            select
                summary_markdown,
                architecture_summary,
                open_issues_summary_json,
                last_audit_summary_json,
                active_branch,
                updated_at
            from project_contexts
            where project_id = %s
            order by updated_at desc
            limit 1
            """,
            (project_id,),
        )
        return row or {}
    except Exception:
        return {}


def _build_stage_report_text(row: dict[str, Any]) -> str:
    source_report_path = str(row.get("source_report_path") or "").strip()
    report_text = ""
    if source_report_path:
        p = Path(source_report_path)
        if not p.is_absolute():
            p = (BASE_DIR / p).resolve()
        if p.exists():
            report_text = p.read_text(encoding="utf-8")[:20000]

    if report_text:
        return _normalize_text(report_text)

    fallback_context = {
        "project_id": row.get("project_id"),
        "branch": row.get("branch"),
        "stage": row.get("stage"),
        "audit_level": row.get("audit_level"),
        "changed_modules": row.get("changed_modules_json") or [],
    }
    return _normalize_text(json.dumps(fallback_context, ensure_ascii=False, sort_keys=True))


def _build_context_hash(*, row: dict[str, Any], project_context: dict[str, Any]) -> str:
    payload = {
        "project_id": row.get("project_id"),
        "branch": row.get("branch"),
        "stage": row.get("stage"),
        "audit_level": row.get("audit_level"),
        "changed_modules": row.get("changed_modules_json") or [],
        "project_context": project_context or {},
    }
    return _sha256(_normalize_text(json.dumps(payload, ensure_ascii=False, sort_keys=True)))


def _build_cache_key(
    *,
    project_id: str,
    audit_level: str,
    prompt_version: str,
    input_hash: str,
    context_hash: str,
    branch: str,
) -> str:
    material = "|".join(
        [
            str(project_id or "").strip(),
            str(audit_level or "").strip(),
            str(prompt_version or "").strip(),
            str(input_hash or "").strip(),
            str(context_hash or "").strip(),
            str(branch or "").strip(),
        ]
    )
    return _sha256(material)


def _build_prompt(*, row: dict[str, Any], stage_report_text: str, project_context: dict[str, Any], prompt_version: str) -> str:
    context_text = _normalize_text(json.dumps(project_context or {}, ensure_ascii=False, sort_keys=True))
    return (
        f"Audit prompt version: {prompt_version}. "
        "Проведи архитектурный и production audit. "
        "Верни строго JSON объект с полями: "
        "scores, overall_score, architecture_score, code_hygiene_score, scalability_score, "
        "production_readiness_score, verdict, can_proceed, top_risks, required_fixes, strengths, changed_modules, report_markdown.\n\n"
        f"PROJECT_ID: {row.get('project_id')}\n"
        f"BRANCH: {row.get('branch')}\n"
        f"STAGE: {row.get('stage')}\n"
        f"AUDIT_LEVEL: {row.get('audit_level')}\n\n"
        f"PROJECT_CONTEXT:\n{context_text}\n\n"
        f"STAGE_REPORT:\n{stage_report_text}"
    )


def _cache_get(cache_key: str) -> dict[str, Any] | None:
    return _fetch_one(
        """
        select *
        from audit_cache
        where cache_key = %s
          and (expires_at is null or expires_at > now())
        limit 1
        """,
        (cache_key,),
    )


def _cache_touch(cache_id: int) -> None:
    _execute("update audit_cache set hit_count = coalesce(hit_count, 0) + 1 where id = %s", (cache_id,))


def _cache_put(
    *,
    cache_key: str,
    project_id: str,
    audit_level: str,
    prompt_version: str,
    input_hash: str,
    context_hash: str,
    response_json: dict[str, Any],
    response_markdown: str,
    overall_score: float | None,
    verdict: str,
) -> None:
    ttl = _cache_ttl_hours()
    _execute(
        """
        insert into audit_cache(
            cache_key,
            project_id,
            audit_level,
            prompt_version,
            input_hash,
            context_hash,
            response_json,
            response_markdown,
            overall_score,
            verdict,
            expires_at,
            hit_count
        )
        values (%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s, now() + (%s || ' hours')::interval, 0)
        on conflict (cache_key)
        do update set
            response_json = excluded.response_json,
            response_markdown = excluded.response_markdown,
            overall_score = excluded.overall_score,
            verdict = excluded.verdict,
            project_id = excluded.project_id,
            audit_level = excluded.audit_level,
            prompt_version = excluded.prompt_version,
            input_hash = excluded.input_hash,
            context_hash = excluded.context_hash,
            expires_at = excluded.expires_at,
            created_at = now()
        """,
        (
            cache_key,
            project_id,
            audit_level,
            prompt_version,
            input_hash,
            context_hash,
            json.dumps(response_json or {}),
            response_markdown,
            overall_score,
            verdict,
            str(ttl),
        ),
    )


def _derive_success_status(normalized: dict[str, Any]) -> str:
    required_fixes = normalized.get("required_fixes_json") or []
    verdict = str(normalized.get("verdict") or "").strip().lower()
    can_proceed = bool(normalized.get("can_proceed"))

    if required_fixes:
        return "requires_fix"
    if can_proceed and ("approve" in verdict or "pass" in verdict or "ok" in verdict):
        return "approved"
    return "reviewed"


def _extract_issue_items(normalized: dict[str, Any]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []

    for idx, item in enumerate(normalized.get("required_fixes_json") or []):
        if not isinstance(item, dict):
            item = {"description": str(item)}
        issues.append(
            {
                "severity": str(item.get("severity") or "high").strip().lower() or "high",
                "category": str(item.get("category") or "required_fix").strip() or "required_fix",
                "title": str(item.get("title") or item.get("summary") or f"required_fix_{idx+1}").strip(),
                "description": str(item.get("description") or item.get("details") or "").strip(),
                "recommended_action": str(item.get("recommended_action") or item.get("action") or "").strip(),
            }
        )

    for idx, item in enumerate(normalized.get("top_risks_json") or []):
        if not isinstance(item, dict):
            item = {"description": str(item)}
        issues.append(
            {
                "severity": str(item.get("severity") or "medium").strip().lower() or "medium",
                "category": str(item.get("category") or "risk").strip() or "risk",
                "title": str(item.get("title") or item.get("risk") or f"risk_{idx+1}").strip(),
                "description": str(item.get("description") or item.get("details") or "").strip(),
                "recommended_action": str(item.get("recommended_action") or item.get("mitigation") or "").strip(),
            }
        )

    return [x for x in issues if x.get("title")]


def _derive_run_decision(row: dict[str, Any]) -> str:
    if bool(row.get("cache_hit")) or str(row.get("source") or "").strip().lower() == "cache":
        return "use_cache"
    level = str(row.get("audit_level") or "").strip().lower()
    if level == "mini":
        return "partial"
    if level == "full":
        return "full"
    return "full"


def _derive_run_status(row: dict[str, Any]) -> str:
    status = str(row.get("status") or "").strip().lower()
    if status == "failed":
        return "failed"
    if status == "requires_fix":
        return "warning"
    if status in {"approved", "reviewed"}:
        return "success"
    return "warning"


def _build_summary(row: dict[str, Any]) -> str:
    report = str(row.get("report_markdown") or "").strip()
    if report:
        first = report.splitlines()[0].strip()
        if first:
            return first[:240]
    verdict = str(row.get("verdict") or "").strip()
    score = row.get("overall_score")
    if verdict and score is not None:
        return f"verdict={verdict}; score={score}"
    if verdict:
        return f"verdict={verdict}"
    return f"status={row.get('status')}"


def _top_issues_for_run(audit_run_id: int, row: dict[str, Any], limit: int = 5) -> list[dict[str, Any]]:
    safe_limit = max(1, min(limit, 10))
    try:
        rows = _fetch_all(
            """
            select severity, category, title, description
            from audit_issues
            where audit_run_id = %s
            order by id asc
            limit %s
            """,
            (audit_run_id, safe_limit),
        )
        if rows:
            return rows
    except Exception:
        pass

    fallback: list[dict[str, Any]] = []
    for item in (row.get("required_fixes_json") or [])[:safe_limit]:
        if isinstance(item, dict):
            fallback.append(
                {
                    "severity": str(item.get("severity") or "high"),
                    "category": str(item.get("category") or "required_fix"),
                    "title": str(item.get("title") or item.get("summary") or "required_fix"),
                    "description": str(item.get("description") or item.get("details") or ""),
                }
            )
        else:
            fallback.append({"severity": "high", "category": "required_fix", "title": str(item), "description": ""})

    for item in (row.get("top_risks_json") or [])[: max(0, safe_limit - len(fallback))]:
        if isinstance(item, dict):
            fallback.append(
                {
                    "severity": str(item.get("severity") or "medium"),
                    "category": str(item.get("category") or "risk"),
                    "title": str(item.get("title") or item.get("risk") or "risk"),
                    "description": str(item.get("description") or item.get("details") or ""),
                }
            )
        else:
            fallback.append({"severity": "medium", "category": "risk", "title": str(item), "description": ""})
    return fallback[:safe_limit]


def build_audit_notification(row: dict[str, Any]) -> dict[str, Any]:
    audit_run_id = int(row.get("id") or 0)
    payload = {
        "project_id": row.get("project_id"),
        "audit_run_id": audit_run_id,
        "decision": _derive_run_decision(row),
        "source": row.get("source"),
        "cache_hit": bool(row.get("cache_hit")),
        "attempt_count": int(row.get("attempt_count") or 0),
        "status": _derive_run_status(row),
        "summary": _build_summary(row),
        "top_issues": _top_issues_for_run(audit_run_id=audit_run_id, row=row, limit=5) if audit_run_id > 0 else [],
        "url": f"/audits/{audit_run_id}" if audit_run_id > 0 else None,
    }
    logger.info("audit_notification %s", json.dumps(payload, ensure_ascii=False))
    return payload


def _replace_audit_issues(audit_run_id: int, normalized: dict[str, Any]) -> None:
    issues = _extract_issue_items(normalized)
    try:
        _execute("delete from audit_issues where audit_run_id = %s", (audit_run_id,))
    except Exception:
        return

    for item in issues:
        _execute(
            """
            insert into audit_issues(
                audit_run_id,
                severity,
                category,
                title,
                description,
                recommended_action,
                status
            )
            values (%s,%s,%s,%s,%s,%s,'new')
            """,
            (
                audit_run_id,
                item.get("severity"),
                item.get("category"),
                item.get("title"),
                item.get("description"),
                item.get("recommended_action"),
            ),
        )


def get_pending_audit_runs(limit: int = 10) -> list[dict[str, Any]]:
    safe_limit = max(1, min(limit, 100))
    return _fetch_all(
        """
        select *
        from audit_runs
        where status = 'pending'
        order by created_at asc
        limit %s
        """,
        (safe_limit,),
    )


def get_audit_run(audit_run_id: int) -> dict[str, Any] | None:
    return _fetch_one("select * from audit_runs where id = %s", (audit_run_id,))


def list_audit_runs(limit: int = 50, status: str | None = None, project_id: str | None = None, branch: str | None = None) -> list[dict[str, Any]]:
    safe_limit = max(1, min(limit, 500))
    where: list[str] = []
    params: list[Any] = []

    if status:
        where.append("status = %s")
        params.append(status)
    if project_id:
        where.append("project_id = %s")
        params.append(project_id)
    if branch:
        where.append("branch = %s")
        params.append(branch)

    where_sql = f"where {' and '.join(where)}" if where else ""
    return _fetch_all(
        f"""
        select
            id,
            project_id,
            branch,
            stage,
            audit_level,
            status,
            source,
            cache_hit,
            cache_key,
            attempt_count,
            last_error,
            retryable,
            review_status,
            reviewed_at,
            reviewed_by,
            review_comment,
            overall_score,
            verdict,
            can_proceed,
            created_at,
            updated_at
        from audit_runs
        {where_sql}
        order by created_at desc
        limit %s
        """,
        tuple(params + [safe_limit]),
    )


def create_audit_run(
    *,
    project_id: str,
    branch: str,
    stage: str,
    audit_level: str,
    source_report_path: str | None = None,
    changed_modules_json: list[Any] | None = None,
) -> dict[str, Any]:
    conn = _db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into audit_runs(
                    project_id,
                    branch,
                    stage,
                    audit_level,
                    status,
                    source,
                    cache_hit,
                    source_report_path,
                    changed_modules_json,
                    prompt_version
                )
                values (%s,%s,%s,%s,'pending','openrouter',false,%s,%s::jsonb,%s)
                returning *
                """,
                (
                    project_id,
                    branch,
                    stage,
                    audit_level,
                    source_report_path,
                    json.dumps(changed_modules_json or []),
                    _prompt_version(),
                ),
            )
            row = cur.fetchone()
        conn.commit()
        return dict(row) if row else {}
    finally:
        conn.close()


def review_audit_run(
    *,
    audit_run_id: int,
    decision: str,
    comment: str | None = None,
    reviewed_by: str = "webapp",
) -> dict[str, Any]:
    row = get_audit_run(audit_run_id)
    if not row:
        raise ValueError(f"audit_run not found: {audit_run_id}")

    normalized_decision = str(decision or "").strip().lower()
    if normalized_decision not in REVIEW_DECISIONS:
        raise ValueError("decision must be approve|reject|needs_fix")

    review_status = REVIEW_DECISIONS[normalized_decision]
    _execute(
        """
        update audit_runs
        set
            review_status = %s,
            reviewed_at = now(),
            reviewed_by = %s,
            review_comment = %s,
            updated_at = now()
        where id = %s
        """,
        (
            review_status,
            (reviewed_by or "webapp").strip()[:120],
            (comment or "").strip()[:2000] or None,
            audit_run_id,
        ),
    )
    updated = get_audit_run(audit_run_id)
    return updated or {}


def patch_audit_run_status(audit_run_id: int, target_status: str, *, allow_missing: bool = False, **fields: Any) -> dict[str, Any]:
    existing = get_audit_run(audit_run_id)
    if not existing:
        if allow_missing:
            return {}
        raise ValueError(f"audit_run not found: {audit_run_id}")

    current = str(existing.get("status") or "").strip().lower()
    target = str(target_status or "").strip().lower()
    if not target:
        raise ValueError("target status is required")
    _validate_transition(current, target)

    sets = ["status = %s", "updated_at = now()"]
    params: list[Any] = [target]

    allowed_fields = {
        "overall_score",
        "architecture_score",
        "code_hygiene_score",
        "scalability_score",
        "production_readiness_score",
        "verdict",
        "can_proceed",
        "top_risks_json",
        "required_fixes_json",
        "strengths_json",
        "changed_modules_json",
        "report_markdown",
        "response_json",
        "raw_response_json",
        "attempt_count",
        "last_error",
        "last_error_type",
        "last_error_text",
        "latency_ms",
        "last_attempt_at",
        "retryable_failure",
        "retryable",
        "transport_status_code",
        "external_status",
        "source",
        "cache_hit",
        "cache_key",
        "prompt_version",
        "input_hash",
        "context_hash",
    }

    for key, value in fields.items():
        if key not in allowed_fields:
            continue
        if key in JSON_FIELDS:
            sets.append(f"{key} = %s::jsonb")
            params.append(json.dumps(value if value is not None else {}))
        else:
            sets.append(f"{key} = %s")
            params.append(value)

    params.append(audit_run_id)
    _execute(f"update audit_runs set {', '.join(sets)} where id = %s", tuple(params))
    updated = get_audit_run(audit_run_id)
    return updated or {}


def _external_channel_snapshot_for_audit(row: dict[str, Any]) -> dict[str, Any]:
    status = str(row.get("status") or "unknown").lower()
    retryable = bool(row.get("retryable") if row.get("retryable") is not None else row.get("retryable_failure"))
    if status == "failed":
        return {
            "channel": "openrouter",
            "status": "warning",
            "non_blocking": True,
            "retryable": retryable,
            "last_error": row.get("last_error") or row.get("last_error_text"),
            "audit_run_id": row.get("id"),
            "updated_at": row.get("updated_at"),
            "source": row.get("source"),
            "cache_hit": bool(row.get("cache_hit")),
        }
    if status in {"approved", "reviewed", "requires_fix"}:
        return {
            "channel": "openrouter",
            "status": "ok",
            "non_blocking": True,
            "retryable": False,
            "last_error": None,
            "audit_run_id": row.get("id"),
            "updated_at": row.get("updated_at"),
            "source": row.get("source"),
            "cache_hit": bool(row.get("cache_hit")),
        }
    if status in {"running", "pending"}:
        return {
            "channel": "openrouter",
            "status": "running",
            "non_blocking": True,
            "retryable": True,
            "last_error": None,
            "audit_run_id": row.get("id"),
            "updated_at": row.get("updated_at"),
            "source": row.get("source"),
            "cache_hit": bool(row.get("cache_hit")),
        }
    return {
        "channel": "openrouter",
        "status": "warning",
        "non_blocking": True,
        "retryable": retryable,
        "last_error": row.get("last_error") or row.get("last_error_text"),
        "audit_run_id": row.get("id"),
        "updated_at": row.get("updated_at"),
        "source": row.get("source"),
        "cache_hit": bool(row.get("cache_hit")),
    }


def update_checkpoints_external_channel(project_id: str, branch: str, audit_row: dict[str, Any]) -> dict[str, Any]:
    snapshot = _external_channel_snapshot_for_audit(audit_row)
    try:
        checkpoints = _fetch_all(
            """
            select id, status, coalesce(audit_status_json, '{}'::jsonb) as audit_status_json, can_merge
            from checkpoint_runs
            where project_id = %s and branch = %s and status in ('running','passed','warning')
            order by created_at desc
            limit 100
            """,
            (project_id, branch),
        )
    except Exception as exc:  # noqa: BLE001
        return {
            "updated_checkpoints": 0,
            "external_audit_channel": snapshot,
            "warning": f"checkpoint integration skipped: {exc}",
        }

    updated = 0
    for cp in checkpoints:
        current_status = str(cp.get("status") or "running").lower()
        next_status = current_status
        if snapshot.get("status") == "warning" and current_status in {"running", "passed"}:
            next_status = "warning"

        status_json = cp.get("audit_status_json") or {}
        if not isinstance(status_json, dict):
            status_json = {}
        status_json["external_audit_channel"] = snapshot

        try:
            _execute(
                """
                update checkpoint_runs
                set
                    status = %s,
                    audit_status_json = %s::jsonb,
                    can_merge = coalesce(can_merge, true)
                where id = %s
                """,
                (next_status, json.dumps(status_json), cp["id"]),
            )
            updated += 1
        except Exception as exc:  # noqa: BLE001
            return {
                "updated_checkpoints": updated,
                "external_audit_channel": snapshot,
                "warning": f"checkpoint update partially applied: {exc}",
            }

    return {
        "updated_checkpoints": updated,
        "external_audit_channel": snapshot,
    }


def process_pending_audit_runs(*, limit: int = 1, timeout_sec: int = 45, max_retries: int = 3) -> dict[str, Any]:
    safe_limit = max(1, min(limit, 50))
    runs = get_pending_audit_runs(limit=safe_limit)
    processed: list[dict[str, Any]] = []
    prompt_version = _prompt_version()

    for row in runs:
        audit_run_id = int(row["id"])

        stage_report_text = _build_stage_report_text(row)
        input_hash = _sha256(stage_report_text)
        project_context = _fetch_project_context(str(row.get("project_id") or ""))
        context_hash = _build_context_hash(row=row, project_context=project_context)
        cache_key = _build_cache_key(
            project_id=str(row.get("project_id") or ""),
            audit_level=str(row.get("audit_level") or ""),
            prompt_version=prompt_version,
            input_hash=input_hash,
            context_hash=context_hash,
            branch=str(row.get("branch") or ""),
        )

        running_row = patch_audit_run_status(
            audit_run_id,
            "running",
            source="openrouter",
            cache_hit=False,
            cache_key=cache_key,
            prompt_version=prompt_version,
            input_hash=input_hash,
            context_hash=context_hash,
            external_status="running",
            last_error=None,
            last_error_type=None,
            last_error_text=None,
        )

        cache_row = _cache_get(cache_key)
        if cache_row:
            _cache_touch(int(cache_row["id"]))
            cached_response = cache_row.get("response_json") if isinstance(cache_row.get("response_json"), dict) else {}
            normalized = cached_response if isinstance(cached_response, dict) else {}
            final_status = _derive_success_status(normalized)
            updated = patch_audit_run_status(
                audit_run_id,
                final_status,
                source="cache",
                cache_hit=True,
                cache_key=cache_key,
                prompt_version=prompt_version,
                input_hash=input_hash,
                context_hash=context_hash,
                overall_score=cache_row.get("overall_score") if cache_row.get("overall_score") is not None else normalized.get("overall_score"),
                architecture_score=normalized.get("architecture_score"),
                code_hygiene_score=normalized.get("code_hygiene_score"),
                scalability_score=normalized.get("scalability_score"),
                production_readiness_score=normalized.get("production_readiness_score"),
                verdict=cache_row.get("verdict") or normalized.get("verdict"),
                can_proceed=normalized.get("can_proceed"),
                top_risks_json=normalized.get("top_risks_json") or [],
                required_fixes_json=normalized.get("required_fixes_json") or [],
                strengths_json=normalized.get("strengths_json") or [],
                changed_modules_json=normalized.get("changed_modules_json") or running_row.get("changed_modules_json") or [],
                report_markdown=cache_row.get("response_markdown") or normalized.get("report_markdown") or "",
                response_json=normalized,
                raw_response_json=None,
                attempt_count=0,
                last_error=None,
                last_error_type=None,
                last_error_text=None,
                retryable=False,
                retryable_failure=False,
                transport_status_code=None,
                latency_ms=None,
                external_status="cached",
            )
            _replace_audit_issues(audit_run_id, normalized)
        else:
            prompt = _build_prompt(
                row=running_row,
                stage_report_text=stage_report_text,
                project_context=project_context,
                prompt_version=prompt_version,
            )
            outcome = call_openrouter_audit(
                prompt=prompt,
                timeout_sec=timeout_sec,
                max_retries=max_retries,
            )

            if outcome.ok and outcome.data:
                normalized = outcome.data
                final_status = _derive_success_status(normalized)
                updated = patch_audit_run_status(
                    audit_run_id,
                    final_status,
                    source="openrouter",
                    cache_hit=False,
                    cache_key=cache_key,
                    prompt_version=prompt_version,
                    input_hash=input_hash,
                    context_hash=context_hash,
                    overall_score=normalized.get("overall_score"),
                    architecture_score=normalized.get("architecture_score"),
                    code_hygiene_score=normalized.get("code_hygiene_score"),
                    scalability_score=normalized.get("scalability_score"),
                    production_readiness_score=normalized.get("production_readiness_score"),
                    verdict=normalized.get("verdict"),
                    can_proceed=normalized.get("can_proceed"),
                    top_risks_json=normalized.get("top_risks_json") or [],
                    required_fixes_json=normalized.get("required_fixes_json") or [],
                    strengths_json=normalized.get("strengths_json") or [],
                    changed_modules_json=(
                        normalized.get("changed_modules_json")
                        or running_row.get("changed_modules_json")
                        or []
                    ),
                    report_markdown=normalized.get("report_markdown") or "",
                    response_json=normalized.get("response_json") or normalized,
                    raw_response_json={
                        "raw_response": outcome.raw_response or {},
                        "attempt_log": outcome.attempt_log,
                    },
                    attempt_count=outcome.attempt_count,
                    last_error=None,
                    last_error_type=None,
                    last_error_text=None,
                    latency_ms=outcome.latency_ms,
                    last_attempt_at=outcome.last_attempt_at,
                    retryable=False,
                    retryable_failure=False,
                    transport_status_code=outcome.transport_status_code,
                    external_status=outcome.external_status,
                )
                _replace_audit_issues(audit_run_id, normalized)
                _cache_put(
                    cache_key=cache_key,
                    project_id=str(running_row.get("project_id") or ""),
                    audit_level=str(running_row.get("audit_level") or ""),
                    prompt_version=prompt_version,
                    input_hash=input_hash,
                    context_hash=context_hash,
                    response_json=normalized,
                    response_markdown=normalized.get("report_markdown") or "",
                    overall_score=normalized.get("overall_score"),
                    verdict=str(normalized.get("verdict") or ""),
                )
            else:
                error_payload = {
                    "error_class": outcome.error_class,
                    "error": outcome.last_error,
                    "attempt_log": outcome.attempt_log,
                }
                updated = patch_audit_run_status(
                    audit_run_id,
                    "failed",
                    source="openrouter",
                    cache_hit=False,
                    cache_key=cache_key,
                    prompt_version=prompt_version,
                    input_hash=input_hash,
                    context_hash=context_hash,
                    attempt_count=outcome.attempt_count,
                    last_error=outcome.last_error,
                    last_error_type=outcome.error_class,
                    last_error_text=outcome.last_error,
                    latency_ms=outcome.latency_ms,
                    last_attempt_at=outcome.last_attempt_at,
                    retryable=bool(outcome.retryable),
                    retryable_failure=bool(outcome.retryable),
                    transport_status_code=outcome.transport_status_code,
                    external_status=outcome.external_status,
                    response_json=error_payload,
                    raw_response_json=outcome.raw_response or {},
                )

        cp_status = update_checkpoints_external_channel(
            project_id=str(updated.get("project_id") or ""),
            branch=str(updated.get("branch") or ""),
            audit_row=updated,
        )
        processed.append(
            {
                "audit_run_id": updated.get("id"),
                "status": updated.get("status"),
                "source": updated.get("source"),
                "cache_hit": bool(updated.get("cache_hit")),
                "cache_key": updated.get("cache_key"),
                "attempt_count": updated.get("attempt_count"),
                "last_error": updated.get("last_error") or updated.get("last_error_text"),
                "retryable": bool(updated.get("retryable") if updated.get("retryable") is not None else updated.get("retryable_failure")),
                "notification": build_audit_notification(updated),
                "checkpoint": cp_status,
            }
        )

    return {
        "ok": True,
        "requested_limit": safe_limit,
        "processed_count": len(processed),
        "items": processed,
    }


def get_checkpoint_external_channel_status(checkpoint_id: int) -> dict[str, Any]:
    row = _fetch_one(
        "select id, status, can_merge, coalesce(audit_status_json, '{}'::jsonb) as audit_status_json from checkpoint_runs where id = %s",
        (checkpoint_id,),
    )
    if not row:
        raise ValueError(f"checkpoint not found: {checkpoint_id}")

    status_json = row.get("audit_status_json") or {}
    if not isinstance(status_json, dict):
        status_json = {}

    external = status_json.get("external_audit_channel")
    if not isinstance(external, dict):
        external = {
            "channel": "openrouter",
            "status": "unknown",
            "non_blocking": True,
            "retryable": True,
            "last_error": None,
        }

    return {
        "checkpoint_id": row.get("id"),
        "checkpoint_status": row.get("status"),
        "can_merge": bool(row.get("can_merge")) if row.get("can_merge") is not None else None,
        "external_audit_channel": external,
    }
