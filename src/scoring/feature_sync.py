from __future__ import annotations

import hashlib
from datetime import date, datetime, time, timedelta
from typing import Any
from urllib.parse import parse_qs, urlparse

import requests

from src.db import db_cursor
from src.extract_metrica import METRICA_URL
from src.settings import Settings


def _safe_dim(dims: list[dict[str, Any]], idx: int) -> str:
    if len(dims) <= idx:
        return ""
    value = dims[idx] or {}
    name = value.get("name")
    return str(name or "").strip()


def _safe_metric(metrics: list[Any], idx: int) -> float:
    if len(metrics) <= idx:
        return 0.0
    raw = metrics[idx]
    try:
        return float(raw or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _bool_keyword(value: str, keywords: list[str]) -> bool:
    low = value.lower()
    return any(k in low for k in keywords)


def _is_invalid_client_id(client_id: str) -> bool:
    low = client_id.strip().lower()
    return low in {"", "(not set)", "not set", "none", "undefined", "не определено"}


def _fallback_visitor_id(*parts: str) -> str:
    stable_key = "|".join((p or "").strip().lower() for p in parts)
    digest = hashlib.sha1(stable_key.encode("utf-8")).hexdigest()[:20]
    return f"metrica_fallback_{digest}"


def _to_int(value: float) -> int:
    if value <= 0:
        return 0
    return int(round(value))


def _guess_traffic_source(utm_source: str, utm_medium: str) -> str:
    source = (utm_source or "").strip().lower()
    medium = (utm_medium or "").strip().lower()
    if medium in {"cpc", "ppc", "paid", "cpm", "cpv", "display"}:
        return "paid"
    if medium in {"organic", "seo"}:
        return "organic"
    if medium in {"social", "smm"}:
        return "social"
    if not source and not medium:
        return "unknown"
    if medium in {"none", "direct"}:
        return "direct"
    return "referral"


def _extract_url_signals(raw_url: str) -> dict[str, Any]:
    parsed = urlparse(raw_url or "")
    query = parse_qs(parsed.query or "")
    path = (parsed.path or "").lower()

    def _q(name: str) -> str:
        values = query.get(name) or []
        return str(values[0] if values else "").strip()

    utm_source = _q("utm_source")
    utm_medium = _q("utm_medium")
    text_blob = " ".join([path, raw_url or ""]).lower()

    return {
        "utm_source": utm_source,
        "utm_medium": utm_medium,
        "visited_price_page": _bool_keyword(text_blob, ["price", "pricing", "цена", "стоим"]),
        "visited_program_page": _bool_keyword(text_blob, ["program", "программ", "schedule", "camp-program"]),
        "visited_booking_page": _bool_keyword(text_blob, ["book", "booking", "reserve", "брон", "запис"]),
        "clicked_booking_button": _bool_keyword(text_blob, ["book=1", "booking_click", "utm_content=book", "cta=book"]),
    }


def _upsert_feature_rows(rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0

    upserted = 0
    with db_cursor() as (_, cur):
        for row in rows:
            cur.execute(
                """
                insert into stg_metrica_visitors_features (
                    visitor_id,
                    session_id,
                    first_seen_at,
                    last_seen_at,
                    sessions_count,
                    total_time_sec,
                    pageviews,
                    visited_price_page,
                    visited_program_page,
                    visited_booking_page,
                    clicked_booking_button,
                    scroll_70,
                    return_visitor,
                    traffic_source,
                    utm_source,
                    utm_medium,
                    device_type,
                    is_bounce,
                    loaded_at
                )
                values (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now()
                )
                on conflict (visitor_id)
                do update set
                    session_id = excluded.session_id,
                    first_seen_at = excluded.first_seen_at,
                    last_seen_at = excluded.last_seen_at,
                    sessions_count = excluded.sessions_count,
                    total_time_sec = excluded.total_time_sec,
                    pageviews = excluded.pageviews,
                    visited_price_page = excluded.visited_price_page,
                    visited_program_page = excluded.visited_program_page,
                    visited_booking_page = excluded.visited_booking_page,
                    clicked_booking_button = excluded.clicked_booking_button,
                    scroll_70 = excluded.scroll_70,
                    return_visitor = excluded.return_visitor,
                    traffic_source = excluded.traffic_source,
                    utm_source = excluded.utm_source,
                    utm_medium = excluded.utm_medium,
                    device_type = excluded.device_type,
                    is_bounce = excluded.is_bounce,
                    loaded_at = now()
                """,
                (
                    row["visitor_id"],
                    None,
                    row["first_seen_at"],
                    row["last_seen_at"],
                    row["sessions_count"],
                    row["total_time_sec"],
                    row["pageviews"],
                    row["visited_price_page"],
                    row["visited_program_page"],
                    row["visited_booking_page"],
                    row["clicked_booking_button"],
                    row["scroll_70"],
                    row["return_visitor"],
                    row["traffic_source"],
                    row["utm_source"],
                    row["utm_medium"],
                    row["device_type"],
                    row["is_bounce"],
                ),
            )
            upserted += 1
    return upserted


def _clear_staging_features() -> int:
    with db_cursor() as (_, cur):
        cur.execute("select count(*)::int as cnt from stg_metrica_visitors_features")
        row = cur.fetchone()
        before = int((row[0] if row else 0) or 0)
        cur.execute("delete from stg_metrica_visitors_features")
    return before


def _load_client_page_signals(
    token: str,
    counter_id: str,
    date_from: date,
    date_to: date,
    max_rows: int,
    page_limit: int,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """
    Пытаемся получить более реалистичные page signals через clientID + startURL.
    Если API-срез недоступен, вызывающий код просто продолжит без этой детализации.
    """
    headers = {"Authorization": f"OAuth {token}"}
    offset = 1
    fetched = 0
    safe_max_rows = max(1, min(max_rows, 200000))
    safe_page_limit = max(1, min(page_limit, 100000))
    signals: dict[str, dict[str, Any]] = {}
    total_rows: int | None = None
    pages = 0
    query_info = {
        "dimensions": ["ym:s:clientID", "ym:s:startURL"],
        "metrics": ["ym:s:visits"],
        "filters": "",
    }

    while fetched < safe_max_rows:
        page_size = min(safe_page_limit, safe_max_rows - fetched)
        params = {
            "ids": counter_id,
            "date1": date_from.isoformat(),
            "date2": date_to.isoformat(),
            "dimensions": "ym:s:clientID,ym:s:startURL",
            "metrics": "ym:s:visits",
            "accuracy": "full",
            "limit": page_size,
            "offset": offset,
        }
        response = requests.get(METRICA_URL, params=params, headers=headers, timeout=90)
        response.raise_for_status()
        payload = response.json()
        rows = payload.get("data") or []
        if not rows:
            break

        total_rows = int(payload.get("total_rows") or 0) or total_rows
        for item in rows:
            dims = item.get("dimensions") or []
            client_id = _safe_dim(dims, 0)
            start_url_raw = _safe_dim(dims, 1)
            start_url = start_url_raw.lower()
            visits = max(0.0, _safe_metric(item.get("metrics") or [], 0))

            if _is_invalid_client_id(client_id):
                continue

            row = signals.setdefault(
                client_id,
                {
                    "sessions_count": 0,
                    "pageviews": 0,
                    "utm_source": "",
                    "utm_medium": "",
                    "traffic_source": "unknown",
                    "device_type": "",
                    "visited_price_page": False,
                    "visited_program_page": False,
                    "visited_booking_page": False,
                    "clicked_booking_button": False,
                },
            )
            row["sessions_count"] += visits
            row["pageviews"] += visits
            if not start_url:
                continue

            url_signals = _extract_url_signals(start_url_raw)
            row["visited_price_page"] = bool(row["visited_price_page"]) or bool(url_signals["visited_price_page"])
            row["visited_program_page"] = bool(row["visited_program_page"]) or bool(url_signals["visited_program_page"])
            row["visited_booking_page"] = bool(row["visited_booking_page"]) or bool(url_signals["visited_booking_page"])
            row["clicked_booking_button"] = bool(row["clicked_booking_button"]) or bool(url_signals["clicked_booking_button"])

            if not row["utm_source"] and url_signals["utm_source"]:
                row["utm_source"] = str(url_signals["utm_source"])
            if not row["utm_medium"] and url_signals["utm_medium"]:
                row["utm_medium"] = str(url_signals["utm_medium"])
            row["traffic_source"] = _guess_traffic_source(str(row["utm_source"]), str(row["utm_medium"]))

        pages += 1
        fetched += len(rows)
        offset += len(rows)

        if len(rows) < page_size:
            break
        if total_rows and fetched >= min(total_rows, safe_max_rows):
            break

    return (
        signals,
        {
            "ok": True,
            "source": "metrica_stat_api",
            "query": query_info,
            "fetched": fetched,
            "pages": pages,
            "total_rows_reported": total_rows,
            "clients": len(signals),
        },
    )


def _build_rows_from_page_signals(
    page_signals: dict[str, dict[str, Any]],
    first_seen_at: datetime,
    last_seen_at: datetime,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for client_id, signal in page_signals.items():
        sessions_count = max(1, _to_int(float(signal.get("sessions_count") or 0.0)))
        pageviews = max(sessions_count, _to_int(float(signal.get("pageviews") or 0.0)))
        total_time_sec = max(30, sessions_count * 75)
        return_visitor = sessions_count > 1
        is_bounce = sessions_count <= 1 and pageviews <= 1
        scroll_70 = (
            bool(signal.get("visited_program_page"))
            or bool(signal.get("visited_price_page"))
            or bool(signal.get("visited_booking_page"))
            or pageviews >= 4
            or sessions_count > 1
        )

        rows.append(
            {
                "visitor_id": client_id,
                "first_seen_at": first_seen_at,
                "last_seen_at": last_seen_at,
                "sessions_count": sessions_count,
                "total_time_sec": total_time_sec,
                "pageviews": pageviews,
                "visited_price_page": bool(signal.get("visited_price_page")),
                "visited_program_page": bool(signal.get("visited_program_page")),
                "visited_booking_page": bool(signal.get("visited_booking_page")),
                "clicked_booking_button": bool(signal.get("clicked_booking_button")),
                "scroll_70": scroll_70,
                "return_visitor": return_visitor,
                "traffic_source": str(signal.get("traffic_source") or "unknown"),
                "utm_source": str(signal.get("utm_source") or ""),
                "utm_medium": str(signal.get("utm_medium") or ""),
                "device_type": str(signal.get("device_type") or ""),
                "is_bounce": is_bounce,
            }
        )
    return rows


def build_scoring_features(
    days: int = 30,
    max_rows: int = 50000,
    page_limit: int = 10000,
    replace: bool = True,
) -> dict[str, Any]:
    """
    Синхронизация visitor-level фичей в stg_metrica_visitors_features.

    Данные берутся из стандартного Metrica stat API, чтобы не создавать
    отдельную интеграцию с нуля и переиспользовать текущий auth-контур.
    """
    token = (Settings.METRICA_TOKEN or "").strip()
    counter_id = (Settings.METRICA_COUNTER_ID or "").strip()
    if not token or not counter_id:
        return {
            "ok": False,
            "skipped": True,
            "reason": "METRICA_TOKEN or METRICA_COUNTER_ID is missing",
            "fetched": 0,
            "upserted": 0,
        }

    safe_days = max(1, min(int(days or 30), 365))
    safe_max_rows = max(1, min(int(max_rows or 50000), 200000))
    safe_page_limit = max(1, min(int(page_limit or 10000), 100000))

    date_to = date.today() - timedelta(days=1)
    date_from = date_to - timedelta(days=safe_days - 1)
    first_seen_at = datetime.combine(date_from, time.min)
    last_seen_at = datetime.combine(date_to, time.max)

    headers = {"Authorization": f"OAuth {token}"}
    offset = 1
    fetched = 0
    upserted = 0
    skipped_no_id = 0
    pages = 0
    total_rows: int | None = None
    page_signals: dict[str, dict[str, Any]] = {}
    page_signal_status: dict[str, Any] = {"ok": False, "reason": "not_started"}
    cleared_rows = 0
    primary_query = {
        "dimensions": [
            "ym:s:clientID",
            "ym:s:lastTrafficSource",
            "ym:s:lastAdvEngine",
            "ym:s:lastUTMSource",
            "ym:s:lastUTMMedium",
            "ym:s:deviceCategory",
            "ym:s:lastUTMCampaign",
        ],
        "metrics": [
            "ym:s:visits",
            "ym:s:pageDepth",
            "ym:s:avgVisitDurationSeconds",
            "ym:s:bounceRate",
        ],
        "filters": "",
    }

    if replace:
        try:
            cleared_rows = _clear_staging_features()
        except Exception as exc:  # noqa: BLE001
            return {
                "ok": False,
                "skipped": False,
                "reason": f"failed to clear stg_metrica_visitors_features: {exc}",
                "fetched": 0,
                "upserted": 0,
                "primary_query": primary_query,
            }

    try:
        page_signals, page_signal_status = _load_client_page_signals(
            token=token,
            counter_id=counter_id,
            date_from=date_from,
            date_to=date_to,
            max_rows=safe_max_rows,
            page_limit=safe_page_limit,
        )
    except Exception as exc:  # noqa: BLE001
        page_signals = {}
        page_signal_status = {"ok": False, "reason": str(exc)}

    while fetched < safe_max_rows:
        page_size = min(safe_page_limit, safe_max_rows - fetched)
        params = {
            "ids": counter_id,
            "date1": date_from.isoformat(),
            "date2": date_to.isoformat(),
            "dimensions": ",".join(primary_query["dimensions"]),
            "metrics": ",".join(primary_query["metrics"]),
            "accuracy": "full",
            "limit": page_size,
            "offset": offset,
        }

        response = requests.get(METRICA_URL, params=params, headers=headers, timeout=90)
        response.raise_for_status()
        payload = response.json()
        rows = payload.get("data") or []
        if not rows:
            break

        total_rows = int(payload.get("total_rows") or 0) or total_rows
        page_rows: list[dict[str, Any]] = []

        for item in rows:
            dims = item.get("dimensions") or []
            metrics = item.get("metrics") or []

            client_id = _safe_dim(dims, 0)
            traffic_source = _safe_dim(dims, 1)
            source_engine = _safe_dim(dims, 2)
            utm_source = _safe_dim(dims, 3)
            utm_medium = _safe_dim(dims, 4)
            device_type = _safe_dim(dims, 5)
            utm_campaign = _safe_dim(dims, 6)

            sessions_count = _to_int(_safe_metric(metrics, 0))
            page_depth = _safe_metric(metrics, 1)
            avg_duration = _safe_metric(metrics, 2)
            bounce_rate = _safe_metric(metrics, 3)

            pageviews = _to_int(sessions_count * max(page_depth, 0.0))
            total_time_sec = _to_int(sessions_count * max(avg_duration, 0.0))
            return_visitor = sessions_count > 1
            scroll_70 = page_depth >= 2.5 or total_time_sec >= 90
            is_bounce = (sessions_count <= 1 and bounce_rate >= 90.0) or (pageviews <= 1 and total_time_sec < 20)

            text_blob = " ".join(
                [
                    traffic_source,
                    source_engine,
                    utm_source,
                    utm_medium,
                    utm_campaign,
                ]
            ).lower()
            signal = page_signals.get(client_id) or {}

            visitor_id = client_id
            if _is_invalid_client_id(visitor_id):
                if not text_blob.strip():
                    skipped_no_id += 1
                    continue
                visitor_id = _fallback_visitor_id(
                    traffic_source,
                    source_engine,
                    utm_source,
                    utm_medium,
                    utm_campaign,
                    str(offset),
                )

            page_rows.append(
                {
                    "visitor_id": visitor_id,
                    "first_seen_at": first_seen_at,
                    "last_seen_at": last_seen_at,
                    "sessions_count": sessions_count,
                    "total_time_sec": total_time_sec,
                    "pageviews": pageviews,
                    "visited_price_page": bool(signal.get("visited_price_page")) or _bool_keyword(
                        text_blob, ["price", "цена", "стоим"]
                    ),
                    "visited_program_page": bool(signal.get("visited_program_page")) or _bool_keyword(
                        text_blob, ["program", "програм", "schedule"]
                    ),
                    "visited_booking_page": bool(signal.get("visited_booking_page")) or _bool_keyword(
                        text_blob, ["book", "booking", "брон", "запис"]
                    ),
                    "clicked_booking_button": bool(signal.get("clicked_booking_button")) or _bool_keyword(
                        text_blob, ["booking_click", "book_click", "apply", "registration"]
                    ),
                    "scroll_70": scroll_70,
                    "return_visitor": return_visitor,
                    "traffic_source": traffic_source or "unknown",
                    "utm_source": utm_source or source_engine,
                    "utm_medium": utm_medium,
                    "device_type": device_type,
                    "is_bounce": is_bounce,
                }
            )

        upserted += _upsert_feature_rows(page_rows)
        pages += 1
        fetched += len(rows)
        offset += len(rows)

        if len(rows) < page_size:
            break
        if total_rows and fetched >= min(total_rows, safe_max_rows):
            break

    source_mode = "primary_query"
    fallback_rows_count = 0
    if upserted == 0 and page_signals:
        fallback_rows = _build_rows_from_page_signals(
            page_signals=page_signals,
            first_seen_at=first_seen_at,
            last_seen_at=last_seen_at,
        )
        fallback_rows_count = len(fallback_rows)
        upserted = _upsert_feature_rows(fallback_rows)
        source_mode = "page_signals_fallback"

    return {
        "ok": True,
        "skipped": False,
        "source": "metrica_stat_api",
        "source_mode": source_mode,
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "fetched": fetched,
        "upserted": upserted,
        "replace_mode": bool(replace),
        "cleared_rows": int(cleared_rows),
        "pages": pages,
        "skipped_no_id": skipped_no_id,
        "total_rows_reported": total_rows,
        "primary_query": primary_query,
        "primary_query_status": {
            "fetched": fetched,
            "pages": pages,
            "total_rows_reported": total_rows,
            "empty_reason": (
                "primary visitor query returned 0 rows; using page_signals fallback"
                if fetched == 0 and len(page_signals) > 0
                else ""
            ),
        },
        "page_signal_status": page_signal_status,
        "page_signals_clients": len(page_signals),
        "page_signals_rows_built": fallback_rows_count,
    }
