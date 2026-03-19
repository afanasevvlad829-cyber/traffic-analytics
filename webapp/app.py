import os
import json
import subprocess
from pathlib import Path
from typing import Any
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
from src.scoring.service import (
    get_scoring_summary,
    get_scoring_visitor,
    get_scoring_visitors,
    rebuild_scoring_v1,
)
from src.scoring.report import send_scoring_report

BASE_DIR = Path("/home/kv145/traffic-analytics")
ENV_PATH = BASE_DIR / ".env"
TEMPLATES_DIR = BASE_DIR / "webapp" / "templates"
STATIC_DIR = BASE_DIR / "webapp" / "static"

def load_env():
    if not ENV_PATH.exists():
        return
    with open(ENV_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or "=" not in line:
                continue
            if line.startswith("export "):
                line = line[len("export "):].strip()
            k, v = line.split("=", 1)
            os.environ[k.strip()] = v.strip().strip('"').strip("'")

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


@app.get("/api/config")
def api_config():
    return {
        "app_base_url": APP_BASE_URL,
        "webapp_url": WEBAPP_URL,
        "webapp_path": WEBAPP_PATH,
    }

@app.get("/health")
def health():
    return {"ok": True}


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


@app.post("/api/scoring/rebuild")
def api_scoring_rebuild(payload: ScoringRebuildIn | None = None):
    body = payload or ScoringRebuildIn()
    if body.limit is not None and body.limit <= 0:
        raise HTTPException(status_code=400, detail="limit must be positive")

    result = rebuild_scoring_v1(limit=body.limit, use_fallback=body.use_fallback)
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
