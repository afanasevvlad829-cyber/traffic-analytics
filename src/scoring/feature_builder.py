from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from psycopg2.extras import RealDictCursor

from src.db import get_connection

UNDEFINED_TABLE_PGCODE = "42P01"


@dataclass
class VisitorFeatures:
    visitor_id: str
    session_id: str | None
    first_seen_at: datetime | None
    last_seen_at: datetime | None
    sessions_count: int
    total_time_sec: int
    pageviews: int
    visited_price_page: bool
    visited_program_page: bool
    visited_booking_page: bool
    clicked_booking_button: bool
    scroll_70: bool
    return_visitor: bool
    traffic_source: str
    utm_source: str
    utm_medium: str
    device_type: str
    is_bounce: bool
    data_source: str


class FeatureBuilder:
    """
    Строит признаки для Scoring v1.

    Приоритет источников:
    1) stg_metrica_visitors_features (visitor-level staging)
    2) fallback из stg_metrica_source_daily (если staging пуст)
    """

    def __init__(self, use_fallback: bool = True, fallback_days: int = 30) -> None:
        self.use_fallback = use_fallback
        self.fallback_days = fallback_days
        self.source_mode = "empty"

    def build(self, limit: int | None = None) -> list[VisitorFeatures]:
        staging_rows = self._load_staging_rows(limit=limit)
        if staging_rows:
            self.source_mode = "staging"
            return [self._from_staging_row(row) for row in staging_rows]

        if not self.use_fallback:
            self.source_mode = "empty"
            return []

        fallback_rows = self._load_fallback_rows(limit=limit)
        if not fallback_rows:
            self.source_mode = "empty"
            return []

        self.source_mode = "fallback"
        return [self._from_fallback_row(row) for row in fallback_rows]

    def _load_staging_rows(self, limit: int | None = None) -> list[dict[str, Any]]:
        sql = """
            select
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
                is_bounce
            from stg_metrica_visitors_features
            order by coalesce(last_seen_at, first_seen_at, loaded_at) desc
        """
        params: list[Any] = []
        if limit and limit > 0:
            sql += "\nlimit %s"
            params.append(limit)

        conn = get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, tuple(params))
                return [dict(row) for row in cur.fetchall()]
        except Exception as exc:  # noqa: BLE001
            if getattr(exc, "pgcode", None) == UNDEFINED_TABLE_PGCODE:
                return []
            raise
        finally:
            conn.close()

    def _load_fallback_rows(self, limit: int | None = None) -> list[dict[str, Any]]:
        sql = """
            select
                min(m.date)::timestamp as first_seen_at,
                max(m.date)::timestamp as last_seen_at,
                coalesce(m.traffic_source, 'unknown') as traffic_source,
                coalesce(m.source_engine, '') as source_engine,
                coalesce(m.source_medium, '') as source_medium,
                coalesce(m.campaign_name, '') as campaign_name,
                sum(coalesce(m.sessions, 0))::int as sessions_count,
                sum(coalesce(m.users, 0))::int as users_count
            from stg_metrica_source_daily m
            where m.date >= current_date - (%s || ' day')::interval
            group by
                coalesce(m.traffic_source, 'unknown'),
                coalesce(m.source_engine, ''),
                coalesce(m.source_medium, ''),
                coalesce(m.campaign_name, '')
            order by max(m.date) desc
        """
        params: list[Any] = [max(1, int(self.fallback_days))]
        if limit and limit > 0:
            sql += "\nlimit %s"
            params.append(limit)

        conn = get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, tuple(params))
                return [dict(row) for row in cur.fetchall()]
        except Exception as exc:  # noqa: BLE001
            if getattr(exc, "pgcode", None) == UNDEFINED_TABLE_PGCODE:
                return []
            raise
        finally:
            conn.close()

    @staticmethod
    def _from_staging_row(row: dict[str, Any]) -> VisitorFeatures:
        return VisitorFeatures(
            visitor_id=str(row.get("visitor_id") or ""),
            session_id=row.get("session_id"),
            first_seen_at=row.get("first_seen_at"),
            last_seen_at=row.get("last_seen_at"),
            sessions_count=int(row.get("sessions_count") or 0),
            total_time_sec=int(row.get("total_time_sec") or 0),
            pageviews=int(row.get("pageviews") or 0),
            visited_price_page=bool(row.get("visited_price_page")),
            visited_program_page=bool(row.get("visited_program_page")),
            visited_booking_page=bool(row.get("visited_booking_page")),
            clicked_booking_button=bool(row.get("clicked_booking_button")),
            scroll_70=bool(row.get("scroll_70")),
            return_visitor=bool(row.get("return_visitor")),
            traffic_source=str(row.get("traffic_source") or ""),
            utm_source=str(row.get("utm_source") or ""),
            utm_medium=str(row.get("utm_medium") or ""),
            device_type=str(row.get("device_type") or ""),
            is_bounce=bool(row.get("is_bounce")),
            data_source="stg_metrica_visitors_features",
        )

    def _from_fallback_row(self, row: dict[str, Any]) -> VisitorFeatures:
        traffic_source = str(row.get("traffic_source") or "")
        utm_source = str(row.get("source_engine") or "")
        utm_medium = str(row.get("source_medium") or "")
        campaign_name = str(row.get("campaign_name") or "")
        sessions_count = max(0, int(row.get("sessions_count") or 0))
        users_count = max(0, int(row.get("users_count") or 0))

        visitor_id = self._build_fallback_visitor_id(
            traffic_source=traffic_source,
            utm_source=utm_source,
            utm_medium=utm_medium,
            campaign_name=campaign_name,
        )

        text_blob = " ".join([traffic_source, utm_source, utm_medium, campaign_name]).lower()

        visited_price_page = self._contains_any(text_blob, ["price", "цена", "стоим"])
        visited_program_page = self._contains_any(text_blob, ["program", "програм", "course", "курс"])
        visited_booking_page = self._contains_any(text_blob, ["book", "booking", "брон", "запис"])

        pageviews = max(1, sessions_count * 2)
        total_time_sec = sessions_count * 75
        return_visitor = sessions_count > max(users_count, 1)
        is_bounce = sessions_count <= 1

        return VisitorFeatures(
            visitor_id=visitor_id,
            session_id=None,
            first_seen_at=row.get("first_seen_at"),
            last_seen_at=row.get("last_seen_at"),
            sessions_count=sessions_count,
            total_time_sec=total_time_sec,
            pageviews=pageviews,
            visited_price_page=visited_price_page,
            visited_program_page=visited_program_page,
            visited_booking_page=visited_booking_page,
            clicked_booking_button=False,
            scroll_70=pageviews >= 4,
            return_visitor=return_visitor,
            traffic_source=traffic_source,
            utm_source=utm_source,
            utm_medium=utm_medium,
            device_type="",
            is_bounce=is_bounce,
            data_source="stg_metrica_source_daily_fallback",
        )

    @staticmethod
    def _contains_any(value: str, needles: list[str]) -> bool:
        return any(needle in value for needle in needles)

    @staticmethod
    def _build_fallback_visitor_id(
        traffic_source: str,
        utm_source: str,
        utm_medium: str,
        campaign_name: str,
    ) -> str:
        stable_key = "|".join([
            traffic_source.strip().lower(),
            utm_source.strip().lower(),
            utm_medium.strip().lower(),
            campaign_name.strip().lower(),
        ])
        digest = hashlib.sha1(stable_key.encode("utf-8")).hexdigest()[:20]
        return f"fallback_{digest}"
