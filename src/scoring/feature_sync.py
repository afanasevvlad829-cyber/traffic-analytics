from __future__ import annotations

import hashlib
from datetime import date, datetime, time, timedelta
from typing import Any
from urllib.parse import parse_qs, urlparse

import requests

from src.db import db_cursor
from src.extract_metrica import METRICA_URL
from src.settings import Settings

FEATURE_SYNC_VERSION = "feature_sync_v2026_03_20"
ATTRIBUTION_LOGIC_VERSION = "attribution_v2026_03_20"


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


def _norm_token(value: str) -> str:
    return (value or "").strip().lower().replace("-", "_").replace(" ", "_")


def _legacy_guess_traffic_source(utm_source: str, utm_medium: str) -> str:
    source = _norm_token(utm_source)
    medium = _norm_token(utm_medium)
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


def _source_rank(source: str) -> int:
    ranks = {
        "yandex_direct": 100,
        "vk_ads": 95,
        "google_ads": 92,
        "paid_ads": 90,
        "email": 70,
        "messenger": 65,
        "social": 60,
        "organic": 55,
        "referral": 45,
        "direct": 35,
        "unknown": 0,
    }
    return ranks.get(_norm_token(source), 10)


def _derive_traffic_source(utm_source: str, utm_medium: str, raw_url: str, query_keys: set[str]) -> str:
    source = _norm_token(utm_source)
    medium = _norm_token(utm_medium)
    url_low = (raw_url or "").lower()
    paid_mediums = {
        "cpc",
        "ppc",
        "paid",
        "cpm",
        "cpv",
        "display",
        "banner",
        "paid_social",
        "paidsearch",
        "context",
    }
    social_sources = {"vk", "vkontakte", "ok", "odnoklassniki", "facebook", "instagram", "meta", "tiktok", "youtube"}
    messenger_sources = {"telegram", "tg", "whatsapp", "viber"}
    search_sources = {"yandex", "google", "bing", "duckduckgo", "mail", "mail_ru", "rambler"}

    has_yclid = "yclid" in query_keys or "ymclid" in query_keys
    has_vkclid = "vkclid" in query_keys
    has_gclid = "gclid" in query_keys

    if has_yclid or "yandex_direct" in source or source in {"yandex", "ya"}:
        if medium in paid_mediums or has_yclid or "direct" in url_low or "utm_campaign" in query_keys:
            return "yandex_direct"

    if has_vkclid or source in {"vk", "vkontakte"}:
        if medium in paid_mediums or has_vkclid:
            return "vk_ads"
        return "social"

    if has_gclid or source in {"google_ads", "adwords"}:
        return "google_ads"

    if medium in {"email", "e_mail", "newsletter", "mail"} or source in {"email", "newsletter"}:
        return "email"

    if medium in {"messenger", "chat"} or source in messenger_sources:
        return "messenger"

    if medium in {"social", "smm", "social_network"} or source in social_sources:
        return "social"

    if medium in {"organic", "seo"} or (source in search_sources and medium not in paid_mediums and not has_gclid):
        return "organic"

    if medium in paid_mediums:
        if "yandex" in source:
            return "yandex_direct"
        if source in {"vk", "vkontakte"}:
            return "vk_ads"
        return "paid_ads"

    if source in {"direct", "(direct)", "none", "(none)"} or medium in {"direct", "none", "(none)"}:
        return "direct"

    if source:
        return "referral"

    # Если нет ни UTM, ни явных маркеров, для startURL это чаще всего прямой заход.
    return "direct"


def _derive_traffic_source_from_hints(
    *,
    traffic_source: str,
    source_engine: str,
    utm_source: str,
    utm_medium: str,
    utm_campaign: str,
) -> str:
    traffic = _norm_token(traffic_source)
    engine = _norm_token(source_engine)
    source = _norm_token(utm_source)
    medium = _norm_token(utm_medium)
    campaign = _norm_token(utm_campaign)

    if traffic in {"organic", "organic_search"}:
        return "organic"
    if traffic in {"direct", "internal"}:
        return "direct"
    if traffic in {"social_network", "social"}:
        return "social"
    if traffic in {"ad", "ad_engine", "cpc"}:
        if engine in {"yandex_direct", "yandex"}:
            return "yandex_direct"
        if engine in {"vk_ads", "vk"}:
            return "vk_ads"
        return "paid_ads"
    if traffic in {"referral", "recommendation"}:
        return "referral"
    if traffic in {"email"}:
        return "email"
    if traffic in {"messenger"}:
        return "messenger"

    # UTM fallback (priority)
    if source or medium:
        return _derive_traffic_source(
            utm_source=utm_source,
            utm_medium=utm_medium,
            raw_url=" ".join([traffic_source, source_engine, utm_campaign]),
            query_keys=set(),
        )

    # campaign/source-engine fallback for known ad systems
    if "yandex" in engine or "yandex" in campaign:
        return "yandex_direct"
    if engine in {"vk", "vk_ads", "vkontakte"} or "vk" in campaign:
        return "vk_ads"
    if engine in {"google_ads", "adwords"} or "google" in campaign:
        return "google_ads"

    return "unknown"


def _extract_url_signals(raw_url: str) -> dict[str, Any]:
    parsed = urlparse(raw_url or "")
    query = parse_qs(parsed.query or "")
    path = (parsed.path or "").lower()
    query_keys = {str(k or "").strip().lower() for k in query.keys()}

    def _q(name: str) -> str:
        values = query.get(name) or []
        return str(values[0] if values else "").strip()

    utm_source = _q("utm_source") or _q("source") or _q("src") or _q("from")
    utm_medium = _q("utm_medium") or _q("medium") or _q("utm_channel")
    text_blob = " ".join([path, raw_url or ""]).lower()
    traffic_source = _derive_traffic_source(utm_source=utm_source, utm_medium=utm_medium, raw_url=raw_url, query_keys=query_keys)

    return {
        "utm_source": utm_source,
        "utm_medium": utm_medium,
        "traffic_source": traffic_source,
        "has_yclid": "yclid" in query_keys or "ymclid" in query_keys,
        "has_vkclid": "vkclid" in query_keys,
        "has_gclid": "gclid" in query_keys,
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
    extended_dimensions = [
        "ym:s:clientID",
        "ym:s:startURL",
        "ym:s:lastTrafficSource",
        "ym:s:lastAdvEngine",
        "ym:s:lastUTMSource",
        "ym:s:lastUTMMedium",
    ]
    basic_dimensions = ["ym:s:clientID", "ym:s:startURL"]
    selected_dimensions = extended_dimensions.copy()
    selected_query_name = "extended_with_source_utm"
    safe_fallback_triggered = False
    query_info = {"dimensions": selected_dimensions.copy(), "metrics": ["ym:s:visits"], "filters": ""}
    raw_samples: list[dict[str, Any]] = []

    while fetched < safe_max_rows:
        page_size = min(safe_page_limit, safe_max_rows - fetched)
        params = {
            "ids": counter_id,
            "date1": date_from.isoformat(),
            "date2": date_to.isoformat(),
            "dimensions": ",".join(selected_dimensions),
            "metrics": "ym:s:visits",
            "accuracy": "full",
            "limit": page_size,
            "offset": offset,
        }
        try:
            response = requests.get(METRICA_URL, params=params, headers=headers, timeout=90)
            response.raise_for_status()
        except Exception:
            can_fallback_to_basic = selected_dimensions != basic_dimensions and fetched == 0 and pages == 0
            if not can_fallback_to_basic:
                raise
            selected_dimensions = basic_dimensions.copy()
            selected_query_name = "fallback_current"
            safe_fallback_triggered = True
            query_info["dimensions"] = selected_dimensions.copy()
            continue

        payload = response.json()
        rows = payload.get("data") or []
        if not rows:
            can_fallback_to_basic = selected_dimensions != basic_dimensions and fetched == 0 and pages == 0
            if can_fallback_to_basic:
                selected_dimensions = basic_dimensions.copy()
                selected_query_name = "fallback_current"
                safe_fallback_triggered = True
                query_info["dimensions"] = selected_dimensions.copy()
                continue
            break

        total_rows = int(payload.get("total_rows") or 0) or total_rows
        for item in rows:
            dims = item.get("dimensions") or []
            client_id = _safe_dim(dims, 0)
            start_url_raw = _safe_dim(dims, 1)
            start_url = start_url_raw.lower()
            raw_traffic_source = _safe_dim(dims, 2)
            raw_source_engine = _safe_dim(dims, 3)
            raw_utm_source = _safe_dim(dims, 4)
            raw_utm_medium = _safe_dim(dims, 5)
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
                    "source_rank": _source_rank("unknown"),
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
            candidate_source = str(url_signals.get("traffic_source") or "unknown")
            candidate_rank = _source_rank(candidate_source)
            raw_candidate_source = _derive_traffic_source_from_hints(
                traffic_source=raw_traffic_source,
                source_engine=raw_source_engine,
                utm_source=raw_utm_source,
                utm_medium=raw_utm_medium,
                utm_campaign="",
            )
            raw_candidate_rank = _source_rank(raw_candidate_source)
            row["visited_price_page"] = bool(row["visited_price_page"]) or bool(url_signals["visited_price_page"])
            row["visited_program_page"] = bool(row["visited_program_page"]) or bool(url_signals["visited_program_page"])
            row["visited_booking_page"] = bool(row["visited_booking_page"]) or bool(url_signals["visited_booking_page"])
            row["clicked_booking_button"] = bool(row["clicked_booking_button"]) or bool(url_signals["clicked_booking_button"])

            if not row["utm_source"] and raw_utm_source:
                row["utm_source"] = str(raw_utm_source)
            if not row["utm_medium"] and raw_utm_medium:
                row["utm_medium"] = str(raw_utm_medium)
            if not row["utm_source"] and url_signals["utm_source"]:
                row["utm_source"] = str(url_signals["utm_source"])
            if not row["utm_medium"] and url_signals["utm_medium"]:
                row["utm_medium"] = str(url_signals["utm_medium"])

            if raw_candidate_rank > int(row.get("source_rank") or 0):
                row["traffic_source"] = raw_candidate_source
                row["source_rank"] = raw_candidate_rank
            if candidate_rank > int(row.get("source_rank") or 0):
                row["traffic_source"] = candidate_source
                row["source_rank"] = candidate_rank

            if len(raw_samples) < 20:
                raw_samples.append(
                    {
                        "visitor_id": client_id,
                        "raw_start_url": start_url_raw,
                        "lastTrafficSource": raw_traffic_source,
                        "lastAdvEngine": raw_source_engine,
                        "lastUTMSource": raw_utm_source,
                        "lastUTMMedium": raw_utm_medium,
                    }
                )

        pages += 1
        fetched += len(rows)
        offset += len(rows)

        if len(rows) < page_size:
            break
        if total_rows and fetched >= min(total_rows, safe_max_rows):
            break

    unknown_before = 0
    unknown_after = 0
    dist: dict[str, int] = {}
    for signal in signals.values():
        src = str(signal.get("traffic_source") or "unknown")
        legacy = _legacy_guess_traffic_source(
            str(signal.get("utm_source") or ""),
            str(signal.get("utm_medium") or ""),
        )
        if legacy == "unknown":
            unknown_before += 1
        if src == "unknown":
            unknown_after += 1
        dist[src] = int(dist.get(src, 0) or 0) + 1
        signal.pop("source_rank", None)

    top_sources = [
        {"traffic_source": key, "count": value}
        for key, value in sorted(dist.items(), key=lambda x: x[1], reverse=True)[:10]
    ]

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
            "selected_query_name": selected_query_name,
            "selected_dimensions": selected_dimensions,
            "safe_fallback_triggered": safe_fallback_triggered,
            "raw_sample_rows": raw_samples,
            "attribution_stats": {
                "unknown_before": unknown_before,
                "unknown_after": unknown_after,
                "top_traffic_sources": top_sources,
            },
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
            "feature_sync_version": FEATURE_SYNC_VERSION,
            "attribution_logic_version": ATTRIBUTION_LOGIC_VERSION,
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

    primary_rows_all: list[dict[str, Any]] = []
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
            resolved_source = _derive_traffic_source_from_hints(
                traffic_source=traffic_source,
                source_engine=source_engine,
                utm_source=utm_source,
                utm_medium=utm_medium,
                utm_campaign=utm_campaign,
            )

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
                    "traffic_source": resolved_source,
                    "utm_source": utm_source or source_engine,
                    "utm_medium": utm_medium,
                    "device_type": device_type,
                    "is_bounce": is_bounce,
                }
            )

        primary_rows_all.extend(page_rows)
        pages += 1
        fetched += len(rows)
        offset += len(rows)

        if len(rows) < page_size:
            break
        if total_rows and fetched >= min(total_rows, safe_max_rows):
            break

    selected_query_name = str((page_signal_status.get("selected_query_name") or "") if isinstance(page_signal_status, dict) else "")
    selected_dimensions = (
        page_signal_status.get("selected_dimensions")
        if isinstance(page_signal_status, dict)
        else []
    ) or []
    safe_fallback_triggered = bool(
        (page_signal_status.get("safe_fallback_triggered") or False) if isinstance(page_signal_status, dict) else False
    )

    source_mode = "primary_query"
    fallback_rows_count = 0
    rows_to_upsert: list[dict[str, Any]] = []
    if primary_rows_all:
        rows_to_upsert = primary_rows_all
        source_mode = "primary_query"
    elif page_signals:
        fallback_rows = _build_rows_from_page_signals(
            page_signals=page_signals,
            first_seen_at=first_seen_at,
            last_seen_at=last_seen_at,
        )
        fallback_rows_count = len(fallback_rows)
        rows_to_upsert = fallback_rows
        source_mode = "fallback_current"

    skipped_replace_due_to_empty_fetch = False
    staging_preserved = False
    if not rows_to_upsert:
        if replace:
            skipped_replace_due_to_empty_fetch = True
            staging_preserved = True
        return {
            "ok": True,
            "skipped": False,
            "source": "metrica_stat_api",
            "feature_sync_version": FEATURE_SYNC_VERSION,
            "attribution_logic_version": ATTRIBUTION_LOGIC_VERSION,
            "source_mode": "empty",
            "selected_query_name": selected_query_name or ("fallback_current" if safe_fallback_triggered else "none"),
            "selected_dimensions": selected_dimensions,
            "safe_fallback_triggered": safe_fallback_triggered,
            "skipped_replace_due_to_empty_fetch": skipped_replace_due_to_empty_fetch,
            "staging_preserved": staging_preserved,
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "fetched": fetched,
            "upserted": 0,
            "replace_mode": bool(replace),
            "cleared_rows": 0,
            "pages": pages,
            "skipped_no_id": skipped_no_id,
            "total_rows_reported": total_rows,
            "primary_query": primary_query,
            "primary_query_status": {
                "fetched": fetched,
                "pages": pages,
                "total_rows_reported": total_rows,
                "empty_reason": (
                    "all candidate queries returned 0 rows; keeping existing staging untouched"
                ),
            },
            "page_signal_status": page_signal_status,
            "page_signals_clients": len(page_signals),
            "page_signals_rows_built": fallback_rows_count,
        }

    if replace:
        try:
            cleared_rows = _clear_staging_features()
        except Exception as exc:  # noqa: BLE001
            return {
                "ok": False,
                "skipped": False,
                "reason": f"failed to clear stg_metrica_visitors_features: {exc}",
                "feature_sync_version": FEATURE_SYNC_VERSION,
                "attribution_logic_version": ATTRIBUTION_LOGIC_VERSION,
                "fetched": fetched,
                "upserted": 0,
                "primary_query": primary_query,
                "selected_query_name": selected_query_name or ("fallback_current" if safe_fallback_triggered else "none"),
                "selected_dimensions": selected_dimensions,
                "safe_fallback_triggered": safe_fallback_triggered,
                "skipped_replace_due_to_empty_fetch": False,
                "staging_preserved": True,
            }
    upserted = _upsert_feature_rows(rows_to_upsert)

    return {
        "ok": True,
        "skipped": False,
        "source": "metrica_stat_api",
        "feature_sync_version": FEATURE_SYNC_VERSION,
        "attribution_logic_version": ATTRIBUTION_LOGIC_VERSION,
        "source_mode": source_mode,
        "selected_query_name": selected_query_name or ("fallback_current" if source_mode == "fallback_current" else "primary_query"),
        "selected_dimensions": selected_dimensions,
        "safe_fallback_triggered": safe_fallback_triggered,
        "skipped_replace_due_to_empty_fetch": False,
        "staging_preserved": False,
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


def debug_unknown_attribution_examples(
    days: int = 30,
    max_rows: int = 50000,
    page_limit: int = 10000,
    limit: int = 20,
) -> dict[str, Any]:
    token = (Settings.METRICA_TOKEN or "").strip()
    counter_id = (Settings.METRICA_COUNTER_ID or "").strip()
    if not token or not counter_id:
        return {
            "ok": False,
            "reason": "METRICA_TOKEN or METRICA_COUNTER_ID is missing",
            "feature_sync_version": FEATURE_SYNC_VERSION,
            "attribution_logic_version": ATTRIBUTION_LOGIC_VERSION,
            "items": [],
        }

    safe_limit = max(1, min(int(limit or 20), 20))
    safe_days = max(1, min(int(days or 30), 365))
    safe_max_rows = max(1, min(int(max_rows or 50000), 200000))
    safe_page_limit = max(1, min(int(page_limit or 10000), 100000))
    date_to = date.today() - timedelta(days=1)
    date_from = date_to - timedelta(days=safe_days - 1)

    headers = {"Authorization": f"OAuth {token}"}
    offset = 1
    fetched = 0
    pages = 0
    items: list[dict[str, Any]] = []

    while fetched < safe_max_rows and len(items) < safe_limit:
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

        for row in rows:
            dims = row.get("dimensions") or []
            visitor_id = _safe_dim(dims, 0)
            raw_url = _safe_dim(dims, 1)
            if _is_invalid_client_id(visitor_id):
                continue
            signals = _extract_url_signals(raw_url)
            derived = str(signals.get("traffic_source") or "unknown")
            if derived != "unknown":
                continue
            items.append(
                {
                    "visitor_id": visitor_id,
                    "raw_start_url": raw_url,
                    "utm_source": signals.get("utm_source"),
                    "utm_medium": signals.get("utm_medium"),
                    "has_yclid": bool(signals.get("has_yclid")),
                    "has_vkclid": bool(signals.get("has_vkclid")),
                    "has_gclid": bool(signals.get("has_gclid")),
                    "derived_traffic_source": derived,
                }
            )
            if len(items) >= safe_limit:
                break

        pages += 1
        fetched += len(rows)
        offset += len(rows)
        if len(rows) < page_size:
            break

    return {
        "ok": True,
        "feature_sync_version": FEATURE_SYNC_VERSION,
        "attribution_logic_version": ATTRIBUTION_LOGIC_VERSION,
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "source": "metrica_stat_api",
        "items": items,
        "count": len(items),
        "fetched_rows": fetched,
        "pages": pages,
    }


def probe_metrica_source_queries(days: int = 7, sample_limit: int = 20) -> dict[str, Any]:
    token = (Settings.METRICA_TOKEN or "").strip()
    counter_id = (Settings.METRICA_COUNTER_ID or "").strip()
    if not token or not counter_id:
        return {
            "ok": False,
            "reason": "METRICA_TOKEN or METRICA_COUNTER_ID is missing",
            "feature_sync_version": FEATURE_SYNC_VERSION,
            "attribution_logic_version": ATTRIBUTION_LOGIC_VERSION,
            "probes": [],
        }

    safe_days = max(1, min(int(days or 7), 30))
    safe_limit = max(1, min(int(sample_limit or 20), 20))
    date_to = date.today() - timedelta(days=1)
    date_from = date_to - timedelta(days=safe_days - 1)
    headers = {"Authorization": f"OAuth {token}"}

    probes = [
        {
            "name": "primary_current",
            "dimensions": [
                "ym:s:clientID",
                "ym:s:lastTrafficSource",
                "ym:s:lastAdvEngine",
                "ym:s:lastUTMSource",
                "ym:s:lastUTMMedium",
                "ym:s:deviceCategory",
                "ym:s:lastUTMCampaign",
            ],
            "metrics": ["ym:s:visits", "ym:s:pageDepth"],
        },
        {
            "name": "fallback_current",
            "dimensions": ["ym:s:clientID", "ym:s:startURL"],
            "metrics": ["ym:s:visits"],
        },
        {
            "name": "client_starturl_with_last_source",
            "dimensions": [
                "ym:s:clientID",
                "ym:s:startURL",
                "ym:s:lastTrafficSource",
                "ym:s:lastAdvEngine",
                "ym:s:lastUTMSource",
                "ym:s:lastUTMMedium",
            ],
            "metrics": ["ym:s:visits"],
        },
        {
            "name": "client_starturl_with_visit_source",
            "dimensions": [
                "ym:s:clientID",
                "ym:s:startURL",
                "ym:s:trafficSource",
                "ym:s:sourceEngine",
                "ym:s:UTMSource",
                "ym:s:UTMMedium",
            ],
            "metrics": ["ym:s:visits"],
        },
    ]

    out: list[dict[str, Any]] = []
    for probe in probes:
        params = {
            "ids": counter_id,
            "date1": date_from.isoformat(),
            "date2": date_to.isoformat(),
            "dimensions": ",".join(probe["dimensions"]),
            "metrics": ",".join(probe["metrics"]),
            "accuracy": "full",
            "limit": safe_limit,
            "offset": 1,
        }
        try:
            response = requests.get(METRICA_URL, params=params, headers=headers, timeout=90)
            response.raise_for_status()
            payload = response.json()
            rows = payload.get("data") or []
            sample_rows = []
            for item in rows[:safe_limit]:
                dims = [str((d or {}).get("name") or "") for d in (item.get("dimensions") or [])]
                metrics = item.get("metrics") or []
                sample_rows.append({"dimensions": dims, "metrics": metrics})
            out.append(
                {
                    "name": probe["name"],
                    "dimensions": probe["dimensions"],
                    "metrics": probe["metrics"],
                    "ok": True,
                    "total_rows_reported": int(payload.get("total_rows") or 0),
                    "sample_rows": sample_rows,
                }
            )
        except Exception as exc:  # noqa: BLE001
            out.append(
                {
                    "name": probe["name"],
                    "dimensions": probe["dimensions"],
                    "metrics": probe["metrics"],
                    "ok": False,
                    "error": str(exc),
                    "sample_rows": [],
                }
            )

    return {
        "ok": True,
        "feature_sync_version": FEATURE_SYNC_VERSION,
        "attribution_logic_version": ATTRIBUTION_LOGIC_VERSION,
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "probes": out,
    }
