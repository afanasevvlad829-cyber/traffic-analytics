from __future__ import annotations

from typing import Any

from psycopg2.extras import Json, RealDictCursor

from src.db import db_cursor, get_connection
from src.scoring.feature_builder import FeatureBuilder, VisitorFeatures
from src.scoring.feature_sync import build_scoring_features
from src.scoring.scorer import RuleBasedScorer

UNDEFINED_TABLE_PGCODE = "42P01"
UNDEFINED_COLUMN_PGCODE = "42703"


class ScoringService:
    def __init__(self) -> None:
        self.scorer = RuleBasedScorer()

    def rebuild(
        self,
        limit: int | None = None,
        use_fallback: bool = True,
        sync_features: bool = True,
        features_days: int = 30,
        features_limit: int = 50000,
    ) -> dict[str, Any]:
        feature_sync_result: dict[str, Any] | None = None
        if sync_features:
            try:
                feature_sync_result = build_scoring_features(
                    days=features_days,
                    max_rows=features_limit,
                )
            except Exception as exc:  # noqa: BLE001
                feature_sync_result = {"ok": False, "skipped": False, "error": str(exc)}

            if feature_sync_result and not feature_sync_result.get("ok", False):
                if not feature_sync_result.get("skipped", False) and self._count_staging_rows() == 0:
                    return {
                        "ok": False,
                        "ready": False,
                        "error": "failed to sync real metrica visitor features",
                        "feature_sync": feature_sync_result,
                    }

        builder = FeatureBuilder(use_fallback=use_fallback)

        try:
            features = builder.build(limit=limit)
        except Exception as exc:  # noqa: BLE001
            if self._is_undefined_table_error(exc):
                return {
                    "ok": False,
                    "ready": False,
                    "error": "scoring schema is missing, run sql/040_scoring_v1.sql and sql/042_scoring_v1_explainable_upgrade.sql",
                }
            raise

        if not features:
            return {
                "ok": True,
                "ready": True,
                "processed": 0,
                "upserted": 0,
                "source_mode": builder.source_mode,
                "scoring_version": self.scorer.scoring_version,
                "feature_sync": feature_sync_result,
            }

        upserted = 0
        with db_cursor() as (_, cur):
            for feature in features:
                if not feature.visitor_id:
                    continue
                score = self.scorer.score(feature)
                self._upsert_scoring_row(cur, feature, score)
                upserted += 1

        return {
            "ok": True,
            "ready": True,
            "processed": len(features),
            "upserted": upserted,
            "source_mode": builder.source_mode,
            "scoring_version": self.scorer.scoring_version,
            "feature_sync": feature_sync_result,
        }

    def get_summary(self) -> dict[str, Any]:
        sql = """
            select
                count(*)::int as total_visitors_scored,
                count(*) filter (where segment = 'hot')::int as hot_count,
                count(*) filter (where segment = 'warm')::int as warm_count,
                count(*) filter (where segment = 'cold')::int as cold_count,
                round(coalesce(avg(normalized_score), 0)::numeric, 4) as avg_score,
                max(scored_at) as latest_scored_at
            from mart_visitor_scoring
        """
        conn = get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql)
                row = cur.fetchone() or {}
                return {
                    "ready": True,
                    "total_visitors_scored": int(row.get("total_visitors_scored") or 0),
                    "hot_count": int(row.get("hot_count") or 0),
                    "warm_count": int(row.get("warm_count") or 0),
                    "cold_count": int(row.get("cold_count") or 0),
                    "avg_score": float(row.get("avg_score") or 0),
                    "latest_scored_at": row.get("latest_scored_at"),
                }
        except Exception as exc:  # noqa: BLE001
            if self._is_undefined_table_error(exc):
                return {
                    "ready": False,
                    "total_visitors_scored": 0,
                    "hot_count": 0,
                    "warm_count": 0,
                    "cold_count": 0,
                    "avg_score": 0,
                    "latest_scored_at": None,
                    "error": "scoring schema is missing, run sql/040_scoring_v1.sql and sql/042_scoring_v1_explainable_upgrade.sql",
                }
            raise
        finally:
            conn.close()

    def get_visitors(
        self,
        limit: int = 100,
        segment: str | None = None,
        source: str | None = None,
    ) -> dict[str, Any]:
        safe_limit = max(1, min(limit, 1000))

        where = ["1=1"]
        params: list[Any] = []

        if segment:
            where.append("segment = %s")
            params.append(segment.lower())

        if source:
            where.append("coalesce(traffic_source, '') ilike %s")
            params.append(f"%{source}%")

        sql = f"""
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
                raw_score,
                normalized_score,
                normalized_score as score,
                segment,
                explanation_json,
                human_explanation,
                short_reason,
                coalesce(recommended_action, recommendation) as recommended_action,
                coalesce(data_source, '') as data_source,
                coalesce(data_source, '') as source_mode,
                scoring_version,
                scored_at
            from mart_visitor_scoring
            where {' and '.join(where)}
            order by scored_at desc
            limit %s
        """
        params.append(safe_limit)

        conn = get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, tuple(params))
                items = [dict(row) for row in cur.fetchall()]
                return {
                    "ready": True,
                    "items": items,
                    "limit": safe_limit,
                    "count": len(items),
                }
        except Exception as exc:  # noqa: BLE001
            if self._is_undefined_table_error(exc):
                return {
                    "ready": False,
                    "items": [],
                    "limit": safe_limit,
                    "count": 0,
                    "error": "scoring schema is missing, run sql/040_scoring_v1.sql and sql/042_scoring_v1_explainable_upgrade.sql",
                }
            raise
        finally:
            conn.close()

    def get_visitor(self, visitor_id: str) -> dict[str, Any] | None:
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
                raw_score,
                normalized_score,
                normalized_score as score,
                segment,
                explanation_json,
                human_explanation,
                short_reason,
                coalesce(recommended_action, recommendation) as recommended_action,
                coalesce(data_source, '') as data_source,
                coalesce(data_source, '') as source_mode,
                scoring_version,
                scored_at
            from mart_visitor_scoring
            where visitor_id = %s
            limit 1
        """

        conn = get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, (visitor_id,))
                row = cur.fetchone()
                return dict(row) if row else None
        finally:
            conn.close()

    def get_timeseries(self, days: int = 30) -> dict[str, Any]:
        safe_days = max(1, min(int(days or 30), 365))
        check_sql = """
            select count(*)::int as cnt
            from mart_visitor_scoring
            where scored_at::date >= current_date - (%s::int - 1)
        """
        sql = """
            with date_axis as (
                select generate_series(
                    current_date - (%s::int - 1),
                    current_date,
                    interval '1 day'
                )::date as day
            ),
            seg as (
                select
                    scored_at::date as day,
                    count(*) filter (where segment = 'hot')::int as hot,
                    count(*) filter (where segment = 'warm')::int as warm,
                    count(*) filter (where segment = 'cold')::int as cold
                from mart_visitor_scoring
                where scored_at::date >= current_date - (%s::int - 1)
                group by scored_at::date
            )
            select
                to_char(a.day, 'YYYY-MM-DD') as date,
                coalesce(s.hot, 0)::int as hot,
                coalesce(s.warm, 0)::int as warm,
                coalesce(s.cold, 0)::int as cold
            from date_axis a
            left join seg s on s.day = a.day
            order by a.day asc
        """
        conn = get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(check_sql, (safe_days,))
                cnt_row = cur.fetchone() or {}
                if int(cnt_row.get("cnt") or 0) == 0:
                    return {
                        "ready": True,
                        "days": safe_days,
                        "dates": [],
                        "hot": [],
                        "warm": [],
                        "cold": [],
                    }
                cur.execute(sql, (safe_days, safe_days))
                rows = [dict(row) for row in cur.fetchall()]
                return {
                    "ready": True,
                    "days": safe_days,
                    "dates": [str(row.get("date") or "") for row in rows],
                    "hot": [int(row.get("hot") or 0) for row in rows],
                    "warm": [int(row.get("warm") or 0) for row in rows],
                    "cold": [int(row.get("cold") or 0) for row in rows],
                }
        except Exception as exc:  # noqa: BLE001
            if self._is_undefined_table_error(exc):
                return {
                    "ready": False,
                    "days": safe_days,
                    "dates": [],
                    "hot": [],
                    "warm": [],
                    "cold": [],
                    "error": "scoring schema is missing, run sql/040_scoring_v1.sql and sql/042_scoring_v1_explainable_upgrade.sql",
                }
            raise
        finally:
            conn.close()

    @staticmethod
    def _upsert_scoring_row(cur, features: VisitorFeatures, score) -> None:
        cur.execute(
            """
            insert into mart_visitor_scoring (
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
                raw_score,
                normalized_score,
                segment,
                explanation_json,
                human_explanation,
                short_reason,
                recommendation,
                recommended_action,
                data_source,
                scoring_version,
                scored_at
            ) values (
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now()
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
                raw_score = excluded.raw_score,
                normalized_score = excluded.normalized_score,
                segment = excluded.segment,
                explanation_json = excluded.explanation_json,
                human_explanation = excluded.human_explanation,
                short_reason = excluded.short_reason,
                recommendation = excluded.recommendation,
                recommended_action = excluded.recommended_action,
                data_source = excluded.data_source,
                scoring_version = excluded.scoring_version,
                scored_at = now()
            """,
            (
                features.visitor_id,
                features.session_id,
                features.first_seen_at,
                features.last_seen_at,
                features.sessions_count,
                features.total_time_sec,
                features.pageviews,
                features.visited_price_page,
                features.visited_program_page,
                features.visited_booking_page,
                features.clicked_booking_button,
                features.scroll_70,
                features.return_visitor,
                features.traffic_source,
                features.utm_source,
                features.utm_medium,
                features.device_type,
                score.raw_score,
                score.normalized_score,
                score.segment,
                Json(score.explanation),
                score.human_explanation,
                score.short_reason,
                score.recommended_action,
                score.recommended_action,
                features.data_source,
                score.scoring_version,
            ),
        )

    @staticmethod
    def _is_undefined_table_error(exc: Exception) -> bool:
        return getattr(exc, "pgcode", None) in (UNDEFINED_TABLE_PGCODE, UNDEFINED_COLUMN_PGCODE)

    @staticmethod
    def _count_staging_rows() -> int:
        conn = get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("select count(*)::int as cnt from stg_metrica_visitors_features")
                row = cur.fetchone() or {}
                return int(row.get("cnt") or 0)
        except Exception:
            return 0
        finally:
            conn.close()


_service = ScoringService()


def rebuild_scoring_v1(
    limit: int | None = None,
    use_fallback: bool = True,
    sync_features: bool = True,
    features_days: int = 30,
    features_limit: int = 50000,
) -> dict[str, Any]:
    return _service.rebuild(
        limit=limit,
        use_fallback=use_fallback,
        sync_features=sync_features,
        features_days=features_days,
        features_limit=features_limit,
    )


def get_scoring_summary() -> dict[str, Any]:
    return _service.get_summary()


def get_scoring_visitors(
    limit: int = 100,
    segment: str | None = None,
    source: str | None = None,
) -> dict[str, Any]:
    return _service.get_visitors(limit=limit, segment=segment, source=source)


def get_scoring_visitor(visitor_id: str) -> dict[str, Any] | None:
    return _service.get_visitor(visitor_id)


def get_scoring_timeseries(days: int = 30) -> dict[str, Any]:
    return _service.get_timeseries(days=days)
