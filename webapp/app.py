import os
import json
import subprocess
import sys
from pathlib import Path
from datetime import date
from typing import Any
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
from src.scoring.service import (
    bootstrap_scoring_activation_direct,
    generate_scoring_ad_template_banners,
    get_scoring_ad_templates,
    get_scoring_activation_plan,
    get_scoring_activation_reaction,
    get_scoring_audience_report,
    get_scoring_audience_export,
    get_scoring_audiences_cohorts,
    get_scoring_attribution_quality,
    get_scoring_creative_plan,
    get_scoring_summary,
    get_scoring_timeseries,
    get_scoring_visitor,
    get_scoring_visitors,
    rebuild_scoring_v1,
    sync_scoring_activation_to_direct,
)
from src.scoring.feature_sync import debug_unknown_attribution_examples, probe_metrica_source_queries
from src.scoring.report import send_scoring_report
from src.sync_alfacrm_serm import sync_alfacrm_from_serm
from webapp.audit_openrouter import openrouter_health
from webapp.audit_service import (
    build_audit_notification,
    create_audit_run,
    get_checkpoint_external_channel_status,
    get_audit_run,
    list_audit_runs,
    process_pending_audit_runs,
    review_audit_run,
)

SERVER_BASE_DIR = Path("/home/kv145/traffic-analytics")
LOCAL_BASE_DIR = Path(__file__).resolve().parents[1]
BASE_DIR = SERVER_BASE_DIR if SERVER_BASE_DIR.exists() else LOCAL_BASE_DIR
ENV_PATH = BASE_DIR / ".env"
ENV_AI_PATH = BASE_DIR / ".env_ai"
TEMPLATES_DIR = BASE_DIR / "webapp" / "templates"
STATIC_DIR = BASE_DIR / "webapp" / "static"


def _git_output(args: list[str]) -> str:
    try:
        out = subprocess.check_output(
            ["git", *args],
            cwd=str(BASE_DIR),
            stderr=subprocess.DEVNULL,
            text=True,
        )
        return out.strip()
    except Exception:
        return ""


def _normalize_repo_url(raw: str) -> str:
    value = (raw or "").strip()
    if not value:
        return ""
    if value.startswith("git@github.com:"):
        value = "https://github.com/" + value.split("git@github.com:", 1)[1]
    if value.endswith(".git"):
        value = value[:-4]
    return value


def _build_git_meta() -> dict[str, Any]:
    repo_url = _normalize_repo_url(os.getenv("GITHUB_REPO_URL", "") or _git_output(["config", "--get", "remote.origin.url"]))
    branch = (os.getenv("GIT_BRANCH", "") or _git_output(["rev-parse", "--abbrev-ref", "HEAD"]) or "").strip()
    commit_full = (os.getenv("GIT_COMMIT", "") or _git_output(["rev-parse", "HEAD"]) or "").strip()
    commit_short = _git_output(["rev-parse", "--short", "HEAD"]) if commit_full else ""
    if not commit_short and commit_full:
        commit_short = commit_full[:8]

    branch_url = f"{repo_url}/tree/{branch}" if repo_url and branch else ""
    commit_url = f"{repo_url}/commit/{commit_full}" if repo_url and commit_full else ""
    latest_changes_url = f"{repo_url}/commits/{branch}" if repo_url and branch else (f"{repo_url}/commits" if repo_url else "")

    return {
        "branch": branch or "unknown",
        "commit_short": commit_short or "unknown",
        "commit_full": commit_full or "",
        "repo_url": repo_url,
        "branch_url": branch_url,
        "commit_url": commit_url,
        "latest_changes_url": latest_changes_url,
    }

def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or "=" not in line:
                continue
            if line.startswith("export "):
                line = line[len("export "):].strip()
            k, v = line.split("=", 1)
            os.environ[k.strip()] = v.strip().strip('"').strip("'")


def load_env():
    # Priority: canonical .env, then local dev .env_ai fallback.
    _load_env_file(ENV_PATH)
    if not ENV_PATH.exists():
        _load_env_file(ENV_AI_PATH)

load_env()

APP_BASE_URL = os.getenv("APP_BASE_URL", "https://ai.aidaplus.ru")
WEBAPP_URL = os.getenv("WEBAPP_URL", APP_BASE_URL)
WEBAPP_PATH = os.getenv("WEBAPP_PATH", "/webapp")


app = FastAPI(title="Direct AI WebApp")
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

def db():
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        dbname=os.getenv("PGDATABASE", "traffic_analytics"),
        user=os.getenv("PGUSER", "traffic_admin"),
        password=os.getenv("PGPASSWORD"),
        cursor_factory=RealDictCursor,
    )

def fetch_all(sql: str, params: tuple = ()) -> list[dict[str, Any]]:
    conn = db()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()

def fetch_one(sql: str, params: tuple = ()) -> dict[str, Any] | None:
    conn = db()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        conn.close()

def execute(sql: str, params: tuple = ()) -> None:
    conn = db()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
    finally:
        conn.close()

def log_decision(entity_type: str, entity_key: str, action: str, status: str, details: str):
    execute("""
        insert into ui_decision_log(entity_type, entity_key, action, status, details, actor)
        values (%s,%s,%s,%s,%s,'webapp')
    """, (entity_type, entity_key, action, status, details))


CRM_SCHEMA_EXPECTED: dict[str, dict[str, Any]] = {
    "stg_alfacrm_customers_daily": {
        "kind": "BASE TABLE",
        "columns": [
            ("report_date", "date"),
            ("segment", "text"),
            ("customer_id", "bigint"),
            ("customer_name", "text"),
            ("phone_normalized", "text"),
            ("email_normalized", "text"),
            ("telegram_username", "text"),
            ("is_study", "smallint"),
            ("removed", "smallint"),
            ("source_file", "text"),
            ("payload_json", "jsonb"),
            ("loaded_at", "timestamp"),
        ],
    },
    "stg_alfacrm_communications_daily": {
        "kind": "BASE TABLE",
        "columns": [
            ("report_date", "date"),
            ("row_key", "text"),
            ("communication_id", "bigint"),
            ("customer_id", "bigint"),
            ("communication_type", "text"),
            ("created_at", "text"),
            ("source_file", "text"),
            ("payload_json", "jsonb"),
            ("loaded_at", "timestamp"),
        ],
    },
    "etl_alfacrm_file_loads": {
        "kind": "BASE TABLE",
        "columns": [
            ("load_id", "bigint"),
            ("loaded_at", "timestamp"),
            ("report_date", "date"),
            ("source_file", "text"),
            ("file_hash", "text"),
            ("customers_rows", "integer"),
            ("communications_rows", "integer"),
            ("note", "text"),
        ],
    },
    "vw_alfacrm_customers_latest": {
        "kind": "VIEW",
        "columns": [
            ("report_date", "date"),
            ("segment", "text"),
            ("customer_id", "bigint"),
            ("customer_name", "text"),
            ("phone_normalized", "text"),
            ("email_normalized", "text"),
            ("telegram_username", "text"),
            ("is_study", "smallint"),
            ("removed", "smallint"),
            ("source_file", "text"),
            ("loaded_at", "timestamp"),
        ],
    },
    "vw_alfacrm_communications_latest": {
        "kind": "VIEW",
        "columns": [
            ("report_date", "date"),
            ("row_key", "text"),
            ("communication_id", "bigint"),
            ("customer_id", "bigint"),
            ("communication_type", "text"),
            ("created_at", "text"),
            ("source_file", "text"),
            ("loaded_at", "timestamp"),
            ("payload_json", "jsonb"),
        ],
    },
}


def _normalize_db_type(value: str) -> str:
    v = (value or "").strip().lower()
    if v.startswith("timestamp"):
        return "timestamp"
    if v == "character varying":
        return "text"
    return v


def _crm_schema_diff() -> dict[str, Any]:
    objects = list(CRM_SCHEMA_EXPECTED.keys())
    conn = None
    try:
        conn = db()
        with conn.cursor() as cur:
            cur.execute(
                """
                select table_name, table_type
                from information_schema.tables
                where table_schema = 'public' and table_name = any(%s)
                """,
                (objects,),
            )
            kinds = {r["table_name"]: r["table_type"] for r in cur.fetchall()}

            cur.execute(
                """
                select table_name, column_name, data_type
                from information_schema.columns
                where table_schema = 'public' and table_name = any(%s)
                order by table_name, ordinal_position
                """,
                (objects,),
            )
            columns_raw = cur.fetchall()

        columns_by_object: dict[str, list[tuple[str, str]]] = {}
        for row in columns_raw:
            table_name = row["table_name"]
            columns_by_object.setdefault(table_name, []).append(
                (row["column_name"], _normalize_db_type(row["data_type"]))
            )

        missing_objects: list[str] = []
        kind_mismatches: list[dict[str, str]] = []
        column_diffs: list[dict[str, Any]] = []

        for obj, spec in CRM_SCHEMA_EXPECTED.items():
            expected_kind = spec["kind"]
            actual_kind = kinds.get(obj)
            if not actual_kind:
                missing_objects.append(obj)
                continue
            if actual_kind != expected_kind:
                kind_mismatches.append(
                    {"object": obj, "expected_kind": expected_kind, "actual_kind": actual_kind}
                )

            expected_cols = spec["columns"]
            expected_map = {name: _normalize_db_type(dtype) for name, dtype in expected_cols}
            actual_cols = columns_by_object.get(obj, [])
            actual_map = {name: _normalize_db_type(dtype) for name, dtype in actual_cols}

            missing_cols = [name for name in expected_map.keys() if name not in actual_map]
            unexpected_cols = [name for name in actual_map.keys() if name not in expected_map]
            type_mismatches = [
                {
                    "column": name,
                    "expected_type": expected_map[name],
                    "actual_type": actual_map.get(name, ""),
                }
                for name in expected_map.keys()
                if name in actual_map and expected_map[name] != actual_map[name]
            ]

            if missing_cols or unexpected_cols or type_mismatches:
                column_diffs.append(
                    {
                        "object": obj,
                        "missing_columns": missing_cols,
                        "unexpected_columns": unexpected_cols,
                        "type_mismatches": type_mismatches,
                    }
                )

        ok = not missing_objects and not kind_mismatches and not column_diffs
        return {
            "ok": ok,
            "missing_objects": missing_objects,
            "kind_mismatches": kind_mismatches,
            "column_diffs": column_diffs,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "error": str(exc),
            "missing_objects": [],
            "kind_mismatches": [],
            "column_diffs": [],
        }
    finally:
        if conn is not None:
            conn.close()


def _ensure_crm_schema_ready() -> None:
    diff = _crm_schema_diff()
    if diff.get("ok"):
        return
    if diff.get("error"):
        raise HTTPException(status_code=500, detail={"message": "crm schema check failed", **diff})
    raise HTTPException(
        status_code=409,
        detail={
            "message": "crm schema mismatch; apply/fix sql/045_alfacrm_crm_ingest.sql before using CRM API",
            **diff,
        },
    )


def _parse_report_date_or_none(value: str | None) -> str | None:
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text).isoformat()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f"invalid report_date '{value}': {exc}")


@app.get("/api/config")
def api_config():
    return {
        "app_base_url": APP_BASE_URL,
        "webapp_url": WEBAPP_URL,
        "webapp_path": WEBAPP_PATH,
    }


@app.get("/api/system/version")
def api_system_version():
    return {"ok": True, **_build_git_meta()}

@app.get("/health")
def health():
    return {"ok": True}


@app.get("/api/audits/health/openrouter")
def api_audits_health_openrouter(timeout_sec: int = 15):
    safe_timeout = max(3, min(timeout_sec, 120))
    return openrouter_health(timeout_sec=safe_timeout)


class AuditRunCreateIn(BaseModel):
    project_id: str = "traffic-analytics"
    branch: str = "main"
    stage: str = "manual"
    audit_level: str = "mini"
    source_report_path: str | None = None
    changed_modules_json: list[str] | None = None


@app.post("/api/audits/runs/create")
def api_audits_create_run(payload: AuditRunCreateIn):
    level = (payload.audit_level or "").strip().lower()
    if level not in {"mini", "full"}:
        raise HTTPException(status_code=400, detail="audit_level must be mini|full")
    if not (payload.project_id or "").strip():
        raise HTTPException(status_code=400, detail="project_id is required")
    if not (payload.branch or "").strip():
        raise HTTPException(status_code=400, detail="branch is required")
    if not (payload.stage or "").strip():
        raise HTTPException(status_code=400, detail="stage is required")

    try:
        row = create_audit_run(
            project_id=payload.project_id.strip(),
            branch=payload.branch.strip(),
            stage=payload.stage.strip(),
            audit_level=level,
            source_report_path=(payload.source_report_path or "").strip() or None,
            changed_modules_json=payload.changed_modules_json or [],
        )
        return {"ok": True, "item": row}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"failed to create audit run: {exc}")


@app.get("/api/audits/runs")
def api_audits_runs(
    limit: int = 50,
    status: str | None = None,
    project_id: str | None = None,
    branch: str | None = None,
):
    safe_limit = max(1, min(limit, 500))
    try:
        raw_items = list_audit_runs(
            limit=safe_limit,
            status=(status or "").strip() or None,
            project_id=(project_id or "").strip() or None,
            branch=(branch or "").strip() or None,
        )
        items = [{**row, "notification": build_audit_notification(row)} for row in raw_items]
        return {"ok": True, "items": items, "count": len(items)}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"failed to list audit runs: {exc}")


@app.get("/api/audits/runs/{audit_run_id}")
def api_audits_run_detail(audit_run_id: int):
    if audit_run_id <= 0:
        raise HTTPException(status_code=400, detail="audit_run_id must be positive")
    row = get_audit_run(audit_run_id)
    if not row:
        raise HTTPException(status_code=404, detail="audit_run not found")
    return {"ok": True, "item": row, "notification": build_audit_notification(row)}


class AuditWorkerRunIn(BaseModel):
    limit: int = 1
    timeout_sec: int = 45
    max_retries: int = 3


@app.post("/api/audits/worker/openrouter/run")
def api_audits_worker_openrouter_run(payload: AuditWorkerRunIn | None = None):
    body = payload or AuditWorkerRunIn()
    safe_limit = max(1, min(body.limit, 50))
    safe_timeout = max(5, min(body.timeout_sec, 180))
    safe_retries = max(1, min(body.max_retries, 6))
    try:
        return process_pending_audit_runs(
            limit=safe_limit,
            timeout_sec=safe_timeout,
            max_retries=safe_retries,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"audit worker failed: {exc}")


class AuditRunReviewIn(BaseModel):
    decision: str
    comment: str | None = None


@app.post("/api/audits/runs/{audit_run_id}/review")
def api_audits_run_review(audit_run_id: int, payload: AuditRunReviewIn):
    if audit_run_id <= 0:
        raise HTTPException(status_code=400, detail="audit_run_id must be positive")
    try:
        updated = review_audit_run(
            audit_run_id=audit_run_id,
            decision=payload.decision,
            comment=payload.comment,
            reviewed_by="webapp",
        )
        return {"ok": True, "item": updated, "notification": build_audit_notification(updated)}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"failed to review audit run: {exc}")


@app.get("/api/audits/checkpoints/{checkpoint_id}/external-status")
def api_audits_checkpoint_external_status(checkpoint_id: int):
    if checkpoint_id <= 0:
        raise HTTPException(status_code=400, detail="checkpoint_id must be positive")
    try:
        return get_checkpoint_external_channel_status(checkpoint_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"failed to load checkpoint status: {exc}")


@app.get("/admin", response_class=HTMLResponse)
def admin_page():
    html = (TEMPLATES_DIR / "admin.html").read_text(encoding="utf-8")
    response = HTMLResponse(html)
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.get("/admin/scoring", response_class=HTMLResponse)
@app.get("/admin/scoring/", response_class=HTMLResponse)
def admin_scoring_page():
    return admin_page()

@app.get("/admin/scoring/creatives", response_class=HTMLResponse)
@app.get("/admin/scoring/creatives/", response_class=HTMLResponse)
def admin_scoring_creatives_page():
    return admin_page()


@app.get("/admin/scoring/templates", response_class=HTMLResponse)
@app.get("/admin/scoring/templates/", response_class=HTMLResponse)
def admin_scoring_templates_page():
    return admin_page()


@app.get("/admin/audits", response_class=HTMLResponse)
@app.get("/admin/audits/", response_class=HTMLResponse)
def admin_audits_page():
    return admin_page()


@app.get("/audits/{audit_run_id}", response_class=HTMLResponse)
def audit_run_page(audit_run_id: int):
    if audit_run_id <= 0:
        raise HTTPException(status_code=400, detail="audit_run_id must be positive")
    html = (TEMPLATES_DIR / "audit_run.html").read_text(encoding="utf-8")
    return HTMLResponse(html.replace("__AUDIT_RUN_ID__", str(audit_run_id)))


@app.get("/webapp", response_class=HTMLResponse)
def webapp():
    html = (TEMPLATES_DIR / "index.html").read_text(encoding="utf-8")
    response = HTMLResponse(html)
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

@app.get("/api/summary")
def api_summary():
    row = fetch_one("""
        with open_creatives as (
            select count(*) as cnt
            from mart_ai_creative_candidates c
            where coalesce(c.decision,'PENDING') = 'PENDING'
              and not exists (
                  select 1
                  from snoozed_items s
                  where s.entity_type = 'creative'
                    and s.entity_key = c.ad_id::text
                    and s.snoozed_until > now()
              )
        ),
        structure_cnt as (
            select count(*) as cnt from mart_group_builder
        ),
        forecast_cnt as (
            select count(*) as cnt
            from mart_ai_creative_forecast_review
            where review_date = current_date
        ),
        pending_actions as (
            select
                coalesce(
                    (select count(*) from mart_ai_ab_test_actions where status = 'PENDING'), 0
                )
                +
                coalesce(
                    (select count(*) from mart_negative_actions where status = 'PENDING'), 0
                )
                +
                coalesce(
                    (select count(*) from mart_structure_actions where status = 'PENDING'), 0
                ) as cnt
        ),
        approved_cnt as (
            select count(*) as cnt
            from mart_ai_creative_candidates
            where decision in ('APPROVED','EXECUTED')
        )
        select
            (select cnt from open_creatives) as open_creatives,
            (select cnt from structure_cnt) as structure_items,
            (select cnt from forecast_cnt) as forecast_items,
            (select cnt from approved_cnt) as approved_actions,
            (select cnt from pending_actions) as pending_actions
    """)
    return row or {}

@app.get("/api/creative-tasks")
def api_creative_tasks():
    return fetch_all("""
        select
            c.created_at,
            c.campaign_name,
            c.ad_id,
            c.ad_group_id,
            c.original_title,
            c.original_title_2,
            c.original_body_text,
            c.sample_queries,
            c.score,
            round(coalesce(c.ctr,0) * 100, 2) as ctr_pct,
            c.cpc,
            c.ai_title_1,
            c.ai_title_2,
            c.ai_body_1,
            c.ai_title_1_b,
            c.ai_title_2_b,
            c.ai_body_2,
            c.ai_title_1_c,
            c.ai_title_2_c,
            c.ai_body_3,
            c.predicted_ctr_pct,
            c.predicted_cpc,
            c.predicted_relevance,
            c.prediction_confidence,
            c.prediction_reason,
            coalesce(c.decision, 'PENDING') as decision,
            s.snoozed_until
        from mart_ai_creative_candidates c
        left join lateral (
            select snoozed_until
            from snoozed_items s
            where s.entity_type = 'creative'
              and s.entity_key = c.ad_id::text
              and s.snoozed_until > now()
            order by s.created_at desc
            limit 1
        ) s on true
        where coalesce(c.decision, 'PENDING') in ('PENDING','APPROVED','IGNORED','SNOOZED')
        order by c.created_at desc
        limit 100
    """)

@app.get("/api/structure")
def api_structure():
    return fetch_all("""
        select
            g.created_at,
            g.campaign_name,
            g.ad_group_id,
            g.queries,
            g.recommendation,
            a.action_type,
            a.status as action_status
        from mart_group_builder g
        left join lateral (
            select action_type, status
            from mart_structure_actions a
            where a.campaign_name = g.campaign_name
              and a.ad_group_id = g.ad_group_id
            order by a.created_at desc
            limit 1
        ) a on true
        order by g.created_at desc
        limit 100
    """)

@app.get("/api/negatives")
def api_negatives():
    safe = fetch_all("""
        select
            campaign_name,
            safe_negative_keywords_copy_paste as words,
            keywords_count
        from vw_campaign_negative_keywords_ai_safe_copy_paste
        order by campaign_name
    """)
    blocked = fetch_all("""
        select
            campaign_name,
            blocked_negative_keywords_copy_paste as words,
            keywords_count
        from vw_campaign_negative_keywords_ai_blocked_copy_paste
        order by campaign_name
    """)
    actions = fetch_all("""
        select
            created_at,
            campaign_name,
            words_text,
            keywords_count,
            action_type,
            status,
            api_response
        from mart_negative_actions
        order by created_at desc
        limit 50
    """)
    return {"safe": safe, "blocked": blocked, "actions": actions}

@app.get("/api/forecast-review")
def api_forecast_review():
    return fetch_all("""
        select
            review_date,
            campaign_name,
            ad_group_id,
            ad_id,
            variant,
            predicted_ctr_pct,
            predicted_cpc,
            predicted_relevance,
            actual_ctr_pct,
            actual_cpc,
            actual_relevance,
            forecast_status,
            comment,
            created_at
        from mart_ai_creative_forecast_review
        order by created_at desc
        limit 100
    """)

@app.get("/api/action-log")
def api_action_log():
    return fetch_all("""
        select
            created_at,
            entity_type,
            entity_key,
            action,
            status,
            details,
            actor
        from ui_decision_log
        order by created_at desc
        limit 200
    """)


@app.get("/api/scoring/summary")
def api_scoring_summary():
    return get_scoring_summary()


@app.get("/api/scoring/timeseries")
def api_scoring_timeseries(days: int = 90):
    safe_days = max(1, min(days, 365))
    return get_scoring_timeseries(days=safe_days)


@app.get("/api/scoring/audience")
def api_scoring_audience(days: int = 90):
    safe_days = max(1, min(days, 90))
    return get_scoring_audience_report(days=safe_days)

@app.get("/api/scoring/attribution-quality")
def api_scoring_attribution_quality(days: int = 90):
    safe_days = max(1, min(days, 365))
    return get_scoring_attribution_quality(days=safe_days)

@app.get("/api/scoring/creative-plan")
def api_scoring_creative_plan(days: int = 90, limit_per_segment: int = 5):
    safe_days = max(1, min(days, 365))
    safe_limit = max(1, min(limit_per_segment, 20))
    return get_scoring_creative_plan(days=safe_days, limit_per_segment=safe_limit)


@app.get("/api/scoring/audiences/cohorts")
def api_scoring_audiences_cohorts(days: int = 90):
    safe_days = max(1, min(days, 365))
    return get_scoring_audiences_cohorts(days=safe_days)


@app.get("/api/scoring/audiences/export")
def api_scoring_audiences_export(
    days: int = 90,
    segment: str | None = None,
    os_root: str | None = None,
    source: str | None = None,
    min_score: float | None = None,
    limit: int = 5000,
    numeric_only: bool = True,
):
    safe_days = max(1, min(days, 365))
    safe_limit = max(1, min(limit, 50000))
    segment_norm = segment.strip().lower() if segment else None
    if segment_norm and segment_norm not in ("hot", "warm", "cold"):
        raise HTTPException(status_code=400, detail="segment must be one of: hot, warm, cold")
    os_norm = os_root.strip().lower() if os_root else None
    source_norm = source.strip().lower() if source else None
    return get_scoring_audience_export(
        days=safe_days,
        segment=segment_norm,
        os_root=os_norm,
        source=source_norm,
        min_score=min_score,
        limit=safe_limit,
        numeric_only=bool(numeric_only),
    )


@app.get("/api/scoring/activation/plan")
def api_scoring_activation_plan(
    days: int = 90,
    min_audience_size: int = 100,
    export_limit: int = 5000,
):
    safe_days = max(1, min(days, 365))
    safe_min = max(1, min(min_audience_size, 100000))
    safe_export_limit = max(1, min(export_limit, 50000))
    return get_scoring_activation_plan(
        days=safe_days,
        min_audience_size=safe_min,
        export_limit=safe_export_limit,
    )


@app.get("/api/scoring/activation/reaction")
def api_scoring_activation_reaction(days: int = 30, limit: int = 50):
    safe_days = max(1, min(days, 365))
    safe_limit = max(1, min(limit, 200))
    return get_scoring_activation_reaction(days=safe_days, limit=safe_limit)


@app.get("/api/scoring/ad-templates")
def api_scoring_ad_templates(
    days: int = 90,
    min_audience_size: int = 1,
    include_small: bool = True,
    variants: int = 3,
):
    safe_days = max(1, min(days, 365))
    safe_min = max(1, min(min_audience_size, 100000))
    safe_variants = max(1, min(variants, 5))
    return get_scoring_ad_templates(
        days=safe_days,
        min_audience_size=safe_min,
        include_small=bool(include_small),
        variants=safe_variants,
    )


class ScoringAdBannerGenerateIn(BaseModel):
    cohort_name: str
    variant_key: str | None = None
    days: int = 90
    min_audience_size: int = 1
    include_small: bool = True
    variants: int = 3
    images_per_variant: int = 1
    size: str = "1536x1024"
    quality: str = "medium"
    output_format: str = "png"


@app.post("/api/scoring/ad-templates/generate-banners")
def api_scoring_generate_banners(payload: ScoringAdBannerGenerateIn):
    safe_days = max(1, min(payload.days, 365))
    safe_min = max(1, min(payload.min_audience_size, 100000))
    safe_variants = max(1, min(payload.variants, 5))
    safe_images_per_variant = max(1, min(payload.images_per_variant, 3))
    safe_size = (payload.size or "1536x1024").strip()
    safe_quality = (payload.quality or "medium").strip().lower()
    safe_output_format = (payload.output_format or "png").strip().lower()

    result = generate_scoring_ad_template_banners(
        cohort_name=(payload.cohort_name or "").strip(),
        variant_key=(payload.variant_key or "").strip() or None,
        days=safe_days,
        min_audience_size=safe_min,
        include_small=bool(payload.include_small),
        variants=safe_variants,
        images_per_variant=safe_images_per_variant,
        size=safe_size,
        quality=safe_quality,
        output_format=safe_output_format,
    )
    if not result.get("ok", False):
        detail = result.get("error") or "banner generation failed"
        detail_l = detail.lower()
        if "status=401" in detail_l or "unauthorized" in detail_l:
            status = 401
        elif "status=403" in detail_l or "forbidden" in detail_l:
            status = 403
        elif "missing" in detail_l or "not found" in detail_l:
            status = 400
        else:
            status = 500
        raise HTTPException(status_code=status, detail=detail)
    return result


class ScoringActivationSyncIn(BaseModel):
    days: int = 90
    min_audience_size: int = 100
    export_limit: int = 5000
    dry_run: bool = True


@app.post("/api/scoring/activation/direct-sync")
def api_scoring_activation_direct_sync(payload: ScoringActivationSyncIn | None = None):
    body = payload or ScoringActivationSyncIn()
    safe_days = max(1, min(body.days, 365))
    safe_min = max(1, min(body.min_audience_size, 100000))
    safe_export_limit = max(1, min(body.export_limit, 50000))

    result = sync_scoring_activation_to_direct(
        days=safe_days,
        min_audience_size=safe_min,
        export_limit=safe_export_limit,
        dry_run=bool(body.dry_run),
    )
    if not result.get("ok", True) and result.get("ready", True):
        raise HTTPException(status_code=500, detail=result.get("error", "direct sync failed"))
    return result


class ScoringActivationBootstrapIn(BaseModel):
    days: int = 90
    min_audience_size: int = 100
    export_limit: int = 5000
    campaign_id: int | None = None
    region_ids: list[int] | None = None
    apply: bool = False
    include_small: bool = False
    env_path: str = "/home/kv145/traffic-analytics/.env"


@app.post("/api/scoring/activation/bootstrap-direct")
def api_scoring_activation_bootstrap_direct(payload: ScoringActivationBootstrapIn | None = None):
    body = payload or ScoringActivationBootstrapIn()
    safe_days = max(1, min(body.days, 365))
    safe_min = max(1, min(body.min_audience_size, 100000))
    safe_export_limit = max(1, min(body.export_limit, 50000))
    safe_regions = [int(x) for x in (body.region_ids or [0]) if isinstance(x, int) or str(x).lstrip("-").isdigit()]
    if not safe_regions:
        safe_regions = [0]
    result = bootstrap_scoring_activation_direct(
        days=safe_days,
        min_audience_size=safe_min,
        export_limit=safe_export_limit,
        campaign_id=body.campaign_id,
        region_ids=safe_regions,
        apply=bool(body.apply),
        env_path=body.env_path or "/home/kv145/traffic-analytics/.env",
        include_small=bool(body.include_small),
    )
    if not result.get("ok", True):
        raise HTTPException(status_code=500, detail=result.get("error", "bootstrap direct failed"))
    return result


@app.get("/api/scoring/visitors")
def api_scoring_visitors(limit: int = 100, segment: str | None = None, source: str | None = None):
    segment_norm = segment.strip().lower() if segment else None
    if segment_norm and segment_norm not in ("hot", "warm", "cold"):
        raise HTTPException(status_code=400, detail="segment must be one of: hot, warm, cold")
    return get_scoring_visitors(limit=limit, segment=segment_norm, source=source)


class ScoringRebuildIn(BaseModel):
    limit: int | None = None
    use_fallback: bool = True
    send_report: bool = False
    sync_features: bool = True
    features_days: int = 90
    features_limit: int = 50000


@app.post("/api/scoring/rebuild")
def api_scoring_rebuild(payload: ScoringRebuildIn | None = None):
    body = payload or ScoringRebuildIn()
    if body.limit is not None and body.limit <= 0:
        raise HTTPException(status_code=400, detail="limit must be positive")
    if body.features_days <= 0:
        raise HTTPException(status_code=400, detail="features_days must be positive")
    if body.features_limit <= 0:
        raise HTTPException(status_code=400, detail="features_limit must be positive")

    result = rebuild_scoring_v1(
        limit=body.limit,
        use_fallback=body.use_fallback,
        sync_features=body.sync_features,
        features_days=body.features_days,
        features_limit=body.features_limit,
    )
    if not result.get("ok", True):
        raise HTTPException(status_code=500, detail=result.get("error", "scoring rebuild failed"))

    if body.send_report:
        try:
            summary = get_scoring_summary()
            top_hot = get_scoring_visitors(limit=5, segment="hot").get("items", [])
            report_status = send_scoring_report(summary=summary, top_visitors=top_hot)
            result["report"] = report_status
            if not report_status.get("ok", False):
                print(f"[scoring-report] {report_status.get('error')}")
        except Exception as exc:  # noqa: BLE001
            result["report"] = {"ok": False, "sent": False, "error": str(exc)}
            print(f"[scoring-report] {exc}")

    return result


@app.get("/api/scoring/debug/unknown-attribution")
def api_scoring_debug_unknown_attribution(limit: int = 20, days: int = 30):
    safe_limit = max(1, min(limit, 20))
    safe_days = max(1, min(days, 365))
    try:
        return debug_unknown_attribution_examples(days=safe_days, limit=safe_limit)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"failed to collect unknown attribution debug: {exc}")


@app.get("/api/scoring/debug/metrica-source-probe")
def api_scoring_debug_metrica_source_probe(days: int = 7, sample_limit: int = 20):
    safe_days = max(1, min(days, 30))
    safe_limit = max(1, min(sample_limit, 20))
    try:
        return probe_metrica_source_queries(days=safe_days, sample_limit=safe_limit)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"failed metrica source probe: {exc}")


@app.get("/api/scoring/visitor/{visitor_id}")
def api_scoring_visitor(visitor_id: str):
    row = get_scoring_visitor(visitor_id)
    if not row:
        raise HTTPException(status_code=404, detail="visitor not found")
    row["source_mode"] = row.get("source_mode") or row.get("data_source") or ""
    row["score_metadata"] = {
        "raw_score": row.get("raw_score"),
        "normalized_score": row.get("normalized_score"),
        "segment": row.get("segment"),
        "scoring_version": row.get("scoring_version"),
        "scored_at": row.get("scored_at"),
    }
    return row


@app.get("/api/crm/load-status")
def api_crm_load_status(limit: int = 20):
    _ensure_crm_schema_ready()
    safe_limit = max(1, min(limit, 200))
    rows = fetch_all(
        """
        select
            report_date,
            source_file,
            customers_rows as users_rows,
            communications_rows,
            note,
            loaded_at
        from etl_alfacrm_file_loads
        order by loaded_at desc
        limit %s
        """,
        (safe_limit,),
    )
    return {"ready": True, "items": rows, "count": len(rows), "limit": safe_limit}


@app.get("/api/crm/users")
def api_crm_users(
    report_date: str | None = None,
    segment: str | None = None,
    limit: int = 100,
    offset: int = 0,
    q: str | None = None,
):
    _ensure_crm_schema_ready()
    safe_limit = max(1, min(limit, 1000))
    safe_offset = max(0, min(offset, 100000))
    report_date_norm = _parse_report_date_or_none(report_date)
    segment_norm = (segment or "").strip()
    if segment_norm.lower() == "all":
        segment_norm = ""
    search_q = (q or "").strip()

    source_rel = "stg_alfacrm_customers_daily" if report_date_norm else "vw_alfacrm_customers_latest"
    where_parts: list[str] = []
    params: list[Any] = []

    if report_date_norm:
        where_parts.append("report_date = %s")
        params.append(report_date_norm)
    if segment_norm:
        where_parts.append("segment = %s")
        params.append(segment_norm)
    if search_q:
        where_parts.append(
            """
            (
                customer_name ilike %s
                or phone_normalized ilike %s
                or email_normalized ilike %s
                or telegram_username ilike %s
                or customer_id::text ilike %s
            )
            """
        )
        like = f"%{search_q}%"
        params.extend([like, like, like, like, like])

    where_sql = f"where {' and '.join(where_parts)}" if where_parts else ""

    total = fetch_one(f"select count(*) as cnt from {source_rel} {where_sql}", tuple(params)) or {"cnt": 0}
    rows = fetch_all(
        f"""
        select
            report_date,
            segment,
            customer_id,
            customer_name,
            phone_normalized,
            email_normalized,
            telegram_username,
            is_study,
            removed,
            payload_json->>'paid_till' as paid_till,
            payload_json->>'paid_count' as paid_count,
            payload_json->>'paid_lesson_count' as paid_lesson_count,
            payload_json->>'balance' as balance,
            payload_json->>'study_status_id' as study_status_id,
            payload_json->>'lead_status_id' as lead_status_id,
            source_file,
            loaded_at
        from {source_rel}
        {where_sql}
        order by report_date desc, loaded_at desc, customer_id desc
        limit %s offset %s
        """,
        tuple(params + [safe_limit, safe_offset]),
    )
    return {
        "ready": True,
        "items": rows,
        "count": int(total.get("cnt", 0) or 0),
        "limit": safe_limit,
        "offset": safe_offset,
        "source": source_rel,
        "report_date": report_date_norm,
        "segment": segment_norm or None,
        "q": search_q,
    }


@app.get("/api/crm/communications")
def api_crm_communications(
    report_date: str | None = None,
    customer_id: int | None = None,
    limit: int = 100,
    offset: int = 0,
):
    _ensure_crm_schema_ready()
    safe_limit = max(1, min(limit, 1000))
    safe_offset = max(0, min(offset, 100000))
    report_date_norm = _parse_report_date_or_none(report_date)

    source_rel = "stg_alfacrm_communications_daily" if report_date_norm else "vw_alfacrm_communications_latest"
    where_parts: list[str] = []
    params: list[Any] = []

    if report_date_norm:
        where_parts.append("report_date = %s")
        params.append(report_date_norm)
    if customer_id is not None:
        where_parts.append("customer_id = %s")
        params.append(customer_id)

    where_sql = f"where {' and '.join(where_parts)}" if where_parts else ""

    total = fetch_one(f"select count(*) as cnt from {source_rel} {where_sql}", tuple(params)) or {"cnt": 0}
    rows = fetch_all(
        f"""
        select
            report_date,
            row_key,
            communication_id,
            customer_id,
            communication_type,
            created_at,
            source_file,
            loaded_at,
            payload_json
        from {source_rel}
        {where_sql}
        order by report_date desc, loaded_at desc, communication_id desc nulls last
        limit %s offset %s
        """,
        tuple(params + [safe_limit, safe_offset]),
    )
    return {
        "ready": True,
        "items": rows,
        "count": int(total.get("cnt", 0) or 0),
        "limit": safe_limit,
        "offset": safe_offset,
        "source": source_rel,
        "report_date": report_date_norm,
        "customer_id": customer_id,
    }


class CrmLoadFileIn(BaseModel):
    xlsx_path: str
    report_date: str | None = None
    skip_communications: bool = False


class CrmDirectSyncIn(BaseModel):
    report_date: str | None = None
    updates_only: bool = True
    include_communications: bool = True
    include_lessons: bool = False
    include_extra: bool = False
    timeout_sec: int = 1800


@app.post("/api/crm/load-file")
def api_crm_load_file(payload: CrmLoadFileIn):
    _ensure_crm_schema_ready()
    xlsx_input = (payload.xlsx_path or "").strip()
    if not xlsx_input:
        raise HTTPException(status_code=400, detail="xlsx_path is required")

    xlsx_path = Path(xlsx_input).expanduser()
    if not xlsx_path.is_absolute():
        xlsx_path = (BASE_DIR / xlsx_path).resolve()
    if not xlsx_path.exists():
        raise HTTPException(status_code=400, detail=f"xlsx file not found: {xlsx_path}")

    report_date_norm = _parse_report_date_or_none(payload.report_date) or date.today().isoformat()

    cmd = [
        sys.executable,
        "-m",
        "src.load_alfacrm_crm_xlsx",
        "--xlsx",
        str(xlsx_path),
        "--report-date",
        report_date_norm,
        "--schema-sql",
        "",
    ]
    if payload.skip_communications:
        cmd.append("--skip-communications")

    try:
        proc = subprocess.run(
            cmd,
            cwd=str(BASE_DIR),
            capture_output=True,
            text=True,
            timeout=1800,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise HTTPException(status_code=504, detail=f"crm load timeout: {exc}")
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"failed to start crm loader: {exc}")

    stdout = (proc.stdout or "").strip()
    stderr = (proc.stderr or "").strip()
    if proc.returncode != 0:
        raise HTTPException(
            status_code=500,
            detail={
                "message": "crm loader failed",
                "returncode": proc.returncode,
                "stderr_tail": stderr[-2000:],
                "stdout_tail": stdout[-2000:],
            },
        )

    parsed: dict[str, Any] = {}
    for line in reversed(stdout.splitlines()):
        txt = line.strip()
        if not txt:
            continue
        try:
            maybe = json.loads(txt)
            if isinstance(maybe, dict):
                parsed = maybe
            break
        except Exception:
            continue

    result = {
        "ok": True,
        "xlsx_path": str(xlsx_path),
        "report_date": report_date_norm,
        "skip_communications": bool(payload.skip_communications),
    }
    if parsed:
        result.update(parsed)
    elif stdout:
        result["stdout_tail"] = stdout[-2000:]
    return result


@app.post("/api/crm/direct-sync")
def api_crm_direct_sync(payload: CrmDirectSyncIn | None = None):
    _ensure_crm_schema_ready()
    body = payload or CrmDirectSyncIn()
    report_date_norm = _parse_report_date_or_none(body.report_date) or date.today().isoformat()
    timeout_sec = max(60, min(int(body.timeout_sec or 1800), 7200))

    try:
        result = sync_alfacrm_from_serm(
            report_date=report_date_norm,
            updates_only=bool(body.updates_only),
            include_communications=bool(body.include_communications),
            include_lessons=bool(body.include_lessons),
            include_extra=bool(body.include_extra),
            timeout_sec=timeout_sec,
        )
    except Exception as exc:  # noqa: BLE001
        message = str(exc)
        lowered = message.lower()
        if "not configured" in lowered or "output not found" in lowered:
            raise HTTPException(status_code=400, detail=message)
        raise HTTPException(status_code=500, detail=message)

    return {
        "ok": True,
        "mode": "direct_sync",
        "report_date": report_date_norm,
        "updates_only": bool(body.updates_only),
        "include_communications": bool(body.include_communications),
        "include_lessons": bool(body.include_lessons),
        "include_extra": bool(body.include_extra),
        "timeout_sec": timeout_sec,
        **result,
    }


@app.get("/api/diagnostic")
def api_diagnostic():
    report_path = "/tmp/direct_ai_diagnostic_report.txt"
    cmd = ["/home/kv145/traffic-analytics/src/ai_diagnostic.sh"]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=90)
    content = ""
    try:
        content = Path(report_path).read_text(encoding="utf-8")
    except Exception:
        content = proc.stdout + "\n" + proc.stderr

    return {
        "ok": proc.returncode == 0,
        "returncode": proc.returncode,
        "report_path": report_path,
        "content": content[-25000:]
    }

@app.get("/api/full-dashboard")
def api_full_dashboard():
    return {
        "summary": api_summary(),
        "creative_tasks": api_creative_tasks(),
        "structure": api_structure(),
        "negatives": api_negatives(),
        "forecast_review": api_forecast_review(),
        "action_log": api_action_log(),
    }

class QueueABTestIn(BaseModel):
    ad_id: int
    variant: str

@app.post("/api/queue-ab-test")
def api_queue_ab_test(payload: QueueABTestIn):
    variant = payload.variant.upper().strip()
    if variant not in ("A", "B", "C"):
        raise HTTPException(status_code=400, detail="variant must be A, B or C")

    row = fetch_one("""
        select
            campaign_name,
            ad_id,
            ad_group_id,
            original_title,
            original_title_2,
            original_body_text,
            ai_title_1,
            ai_title_2,
            ai_body_1,
            ai_title_1_b,
            ai_title_2_b,
            ai_body_2,
            ai_title_1_c,
            ai_title_2_c,
            ai_body_3
        from mart_ai_creative_candidates
        where ad_id = %s
        order by created_at desc
        limit 1
    """, (payload.ad_id,))
    if not row:
        raise HTTPException(status_code=404, detail="creative candidate not found")

    if variant == "A":
        new_title, new_title_2, new_body = row["ai_title_1"], row["ai_title_2"], row["ai_body_1"]
    elif variant == "B":
        new_title, new_title_2, new_body = row["ai_title_1_b"], row["ai_title_2_b"], row["ai_body_2"]
    else:
        new_title, new_title_2, new_body = row["ai_title_1_c"], row["ai_title_2_c"], row["ai_body_3"]

    execute("""
        insert into mart_ai_ab_test_actions(
            campaign_name,
            ad_id,
            ad_group_id,
            source_title,
            source_title_2,
            source_body_text,
            new_title,
            new_title_2,
            new_body_text,
            action_type,
            status,
            api_response,
            requested_by
        )
        values (%s,%s,%s,%s,%s,%s,%s,%s,%s,'CREATE_AB_TEST','PENDING','queued from webapp','webapp')
    """, (
        row["campaign_name"],
        row["ad_id"],
        row["ad_group_id"],
        row["original_title"],
        row["original_title_2"],
        row["original_body_text"],
        new_title,
        new_title_2,
        new_body,
    ))

    execute("""
        update mart_ai_creative_candidates
        set decision = 'APPROVED'
        where ad_id = %s
    """, (payload.ad_id,))

    log_decision("creative", str(payload.ad_id), f"QUEUE_AB_TEST_{variant}", "PENDING",
                 json.dumps({"variant": variant, "new_title": new_title, "new_title_2": new_title_2}, ensure_ascii=False))

    return {"ok": True, "message": f"A/B test queued for ad_id={payload.ad_id}, variant={variant}"}

class IgnoreIn(BaseModel):
    ad_id: int

@app.post("/api/ignore-creative")
def api_ignore_creative(payload: IgnoreIn):
    execute("""
        update mart_ai_creative_candidates
        set decision = 'IGNORED'
        where ad_id = %s
    """, (payload.ad_id,))
    log_decision("creative", str(payload.ad_id), "IGNORE", "DONE", "ignored from webapp")
    return {"ok": True, "message": f"ad_id={payload.ad_id} ignored"}

class SnoozeIn(BaseModel):
    entity_type: str
    entity_key: str
    days: int = 1
    reason: str = "manual snooze"

@app.post("/api/snooze")
def api_snooze(payload: SnoozeIn):
    execute("""
        insert into snoozed_items(entity_type, entity_key, snoozed_until, reason, actor)
        values (%s,%s,now() + (%s || ' day')::interval,%s,'webapp')
    """, (payload.entity_type, payload.entity_key, payload.days, payload.reason))
    if payload.entity_type == "creative":
        execute("""
            update mart_ai_creative_candidates
            set decision = 'SNOOZED'
            where ad_id::text = %s
        """, (payload.entity_key,))
    log_decision(payload.entity_type, payload.entity_key, "SNOOZE", "DONE",
                 json.dumps({"days": payload.days, "reason": payload.reason}, ensure_ascii=False))
    return {"ok": True, "message": f"{payload.entity_type}:{payload.entity_key} snoozed for {payload.days} day(s)"}

class ApplyNegativesIn(BaseModel):
    campaign_name: str

@app.post("/api/apply-safe-negatives")
def api_apply_safe_negatives(payload: ApplyNegativesIn):
    row = fetch_one("""
        select
            campaign_name,
            safe_negative_keywords_copy_paste as words,
            keywords_count
        from vw_campaign_negative_keywords_ai_safe_copy_paste
        where campaign_name = %s
    """, (payload.campaign_name,))
    if not row:
        raise HTTPException(status_code=404, detail="safe negatives not found for campaign")

    execute("""
        insert into mart_negative_actions(
            campaign_name,
            words_text,
            keywords_count,
            action_type,
            status,
            api_response,
            requested_by
        )
        values (%s,%s,%s,'APPLY_SAFE_NEGATIVES','PENDING','queued from webapp','webapp')
    """, (row["campaign_name"], row["words"], row["keywords_count"]))

    log_decision("negative", payload.campaign_name, "APPLY_SAFE_NEGATIVES", "PENDING",
                 json.dumps({"keywords_count": row["keywords_count"], "words": row["words"]}, ensure_ascii=False))

    return {"ok": True, "message": f"safe negatives queued for {payload.campaign_name}"}

class StructureActionIn(BaseModel):
    campaign_name: str
    ad_group_id: int
    action: str
    reason: str = ""

@app.post("/api/structure-action")
def api_structure_action(payload: StructureActionIn):
    action = payload.action.upper().strip()
    if action not in ("APPLY_SPLIT", "IGNORE", "SNOOZE"):
        raise HTTPException(status_code=400, detail="action must be APPLY_SPLIT, IGNORE or SNOOZE")

    row = fetch_one("""
        select recommendation
        from mart_group_builder
        where campaign_name = %s
          and ad_group_id = %s
        order by created_at desc
        limit 1
    """, (payload.campaign_name, payload.ad_group_id))
    recommendation = row["recommendation"] if row else ""

    if action == "SNOOZE":
        execute("""
            insert into snoozed_items(entity_type, entity_key, snoozed_until, reason, actor)
            values ('structure', %s, now() + interval '1 day', %s, 'webapp')
        """, (f"{payload.campaign_name}:{payload.ad_group_id}", payload.reason or "structure snoozed"))
        log_decision("structure", f"{payload.campaign_name}:{payload.ad_group_id}", "SNOOZE", "DONE", payload.reason or "")
        return {"ok": True, "message": "structure snoozed"}

    execute("""
        insert into mart_structure_actions(
            campaign_name,
            ad_group_id,
            recommendation,
            action_type,
            status,
            api_response,
            requested_by
        )
        values (%s,%s,%s,%s,'PENDING','queued from webapp','webapp')
    """, (payload.campaign_name, payload.ad_group_id, recommendation, action))

    log_decision("structure", f"{payload.campaign_name}:{payload.ad_group_id}", action, "PENDING", payload.reason or "")
    return {"ok": True, "message": f"{action} queued"}

@app.get("/api/context/{code}")
def api_context(code: str):

    conn=db()
    try:
        with conn.cursor() as cur:

            cur.execute("""
            select *
            from ai_context_registry
            where context_code=%s
            """,(code,))

            row=cur.fetchone()

            if not row:
                return {"error":"context_not_found"}

            return row

    finally:
        conn.close()

@app.get("/creatives",response_class=HTMLResponse)
def page_creatives():
    html=open("webapp/templates/pages/creatives.html").read()
    return HTMLResponse(html)
