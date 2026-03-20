from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
import ssl
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def api_get(
    base_url: str,
    path: str,
    params: dict[str, Any] | None = None,
    ssl_context: ssl.SSLContext | None = None,
) -> dict[str, Any]:
    url = base_url.rstrip("/") + path
    if params:
        url += "?" + urlencode(params)
    req = Request(url=url, method="GET", headers={"Content-Type": "application/json"})
    try:
        with urlopen(req, timeout=60, context=ssl_context) as resp:
            payload = resp.read().decode("utf-8")
            return json.loads(payload) if payload else {}
    except HTTPError as exc:  # noqa: PERF203
        body = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"GET {url} failed: HTTP {exc.code} {body}") from exc
    except URLError as exc:
        raise RuntimeError(f"GET {url} failed: {exc}") from exc


def api_post(
    base_url: str,
    path: str,
    payload: dict[str, Any],
    ssl_context: ssl.SSLContext | None = None,
) -> dict[str, Any]:
    url = base_url.rstrip("/") + path
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = Request(
        url=url,
        data=data,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urlopen(req, timeout=120, context=ssl_context) as resp:
            text = resp.read().decode("utf-8")
            return json.loads(text) if text else {}
    except HTTPError as exc:  # noqa: PERF203
        body = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"POST {url} failed: HTTP {exc.code} {body}") from exc
    except URLError as exc:
        raise RuntimeError(f"POST {url} failed: {exc}") from exc


def _normalize_source(value: str | None) -> str:
    val = str(value or "").strip().lower()
    return val or "unknown"


@dataclass
class SourceStats:
    total: int
    unknown: int
    unknown_pct: float
    top_sources: list[dict[str, Any]]


def _api_sample_stats(base_url: str, ssl_context: ssl.SSLContext | None = None) -> SourceStats:
    payload = api_get(base_url, "/api/scoring/visitors", {"limit": 1000}, ssl_context=ssl_context)
    items = payload.get("items") or []
    counts: dict[str, int] = {}
    for row in items:
        source = _normalize_source(row.get("traffic_source"))
        counts[source] = int(counts.get(source, 0) or 0) + 1

    total = len(items)
    unknown = int(counts.get("unknown", 0) or 0)
    unknown_pct = round((100.0 * unknown / total), 2) if total else 0.0
    top_sources = [
        {"traffic_source": source, "count": count}
        for source, count in sorted(counts.items(), key=lambda x: x[1], reverse=True)[:15]
    ]
    return SourceStats(total=total, unknown=unknown, unknown_pct=unknown_pct, top_sources=top_sources)


def _db_query_stats(table_name: str) -> SourceStats:
    from src.db import get_connection

    norm_expr = "coalesce(nullif(lower(traffic_source), ''), 'unknown')"

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                select
                    count(*)::int as total_visitors,
                    count(*) filter (where {norm_expr} = 'unknown')::int as unknown_visitors
                from {table_name}
                """
            )
            row = cur.fetchone() or (0, 0)
            total = int(row[0] or 0)
            unknown = int(row[1] or 0)
            unknown_pct = round((100.0 * unknown / total), 2) if total else 0.0

            cur.execute(
                f"""
                select {norm_expr} as traffic_source, count(*)::int as cnt
                from {table_name}
                group by 1
                order by 2 desc
                limit 15
                """
            )
            top_sources = [
                {"traffic_source": str(source), "count": int(cnt)}
                for source, cnt in (cur.fetchall() or [])
            ]
    finally:
        conn.close()

    return SourceStats(total=total, unknown=unknown, unknown_pct=unknown_pct, top_sources=top_sources)


def _db_sample_rows(table_name: str) -> list[dict[str, Any]]:
    from src.db import get_connection

    order_expr = "scored_at desc nulls last, last_seen_at desc nulls last, visitor_id"
    if table_name == "stg_metrica_visitors_features":
        order_expr = "loaded_at desc nulls last, last_seen_at desc nulls last, visitor_id"

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                select
                    visitor_id,
                    coalesce(nullif(lower(traffic_source), ''), 'unknown') as traffic_source,
                    sessions_count,
                    pageviews
                from {table_name}
                order by {order_expr}
                limit 5
                """
            )
            rows = cur.fetchall() or []
            return [
                {
                    "visitor_id": str(item[0]),
                    "traffic_source": str(item[1]),
                    "sessions_count": int(item[2] or 0),
                    "pageviews": int(item[3] or 0),
                }
                for item in rows
            ]
    finally:
        conn.close()


def _db_available() -> bool:
    try:
        from src.db import get_connection

        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("select 1")
                cur.fetchone()
            return True
        finally:
            conn.close()
    except Exception:  # noqa: BLE001
        return False


def run_audit(
    base_url: str,
    features_days: int,
    features_limit: int,
    ssl_context: ssl.SSLContext | None = None,
) -> dict[str, Any]:
    before_api_sample = _api_sample_stats(base_url, ssl_context=ssl_context)
    db_available = _db_available()

    before_staging = _db_query_stats("stg_metrica_visitors_features") if db_available else None
    before_mart = _db_query_stats("mart_visitor_scoring") if db_available else None

    rebuild = api_post(
        base_url,
        "/api/scoring/rebuild",
        {
            "sync_features": True,
            "use_fallback": False,
            "features_days": features_days,
            "features_limit": features_limit,
            "replace_features": True,
        },
        ssl_context=ssl_context,
    )
    if not rebuild.get("ok"):
        raise RuntimeError(f"rebuild failed: {rebuild}")

    summary = api_get(base_url, "/api/scoring/summary", ssl_context=ssl_context)
    after_api_sample = _api_sample_stats(base_url, ssl_context=ssl_context)

    after_staging = _db_query_stats("stg_metrica_visitors_features") if db_available else None
    after_mart = _db_query_stats("mart_visitor_scoring") if db_available else None

    sample_rows = (
        _db_sample_rows("mart_visitor_scoring")
        if db_available
        else [
            {
                "visitor_id": str(row.get("visitor_id")),
                "traffic_source": _normalize_source(row.get("traffic_source")),
                "sessions_count": int(row.get("sessions_count") or 0),
                "pageviews": int(row.get("pageviews") or 0),
            }
            for row in (
                api_get(base_url, "/api/scoring/visitors", {"limit": 5}, ssl_context=ssl_context).get("items") or []
            )
        ]
    )

    return {
        "rebuild": rebuild,
        "summary": summary,
        "db_available": db_available,
        "before": {
            "api_sample_mart": before_api_sample.__dict__,
            "staging": before_staging.__dict__ if before_staging else None,
            "mart": before_mart.__dict__ if before_mart else None,
        },
        "after": {
            "api_sample_mart": after_api_sample.__dict__,
            "staging": after_staging.__dict__ if after_staging else None,
            "mart": after_mart.__dict__ if after_mart else None,
        },
        "sample_rows": sample_rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Scoring attribution post-deploy audit")
    parser.add_argument("--base-url", default="https://ai.aidaplus.ru", help="webapp base URL")
    parser.add_argument("--features-days", type=int, default=30, help="feature sync lookback days")
    parser.add_argument("--features-limit", type=int, default=50000, help="feature sync row cap")
    parser.add_argument(
        "--insecure",
        action="store_true",
        help="disable SSL certificate verification for API calls (debug only)",
    )
    args = parser.parse_args()
    ssl_context = ssl._create_unverified_context() if args.insecure else None

    try:
        report = run_audit(
            base_url=args.base_url,
            features_days=args.features_days,
            features_limit=args.features_limit,
            ssl_context=ssl_context,
        )
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False, indent=2))
        sys.exit(1)

    print(json.dumps({"ok": True, **report}, ensure_ascii=False, default=str, indent=2))


if __name__ == "__main__":
    main()
