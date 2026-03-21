from __future__ import annotations

import json
import os
import subprocess
from typing import Any

import requests
from psycopg2.extras import Json, RealDictCursor

from src.db import db_cursor, get_connection
from src.scoring.banner_generator import generate_template_banners
from src.scoring.direct_bootstrap import bootstrap_direct_entities
from src.scoring.feature_builder import FeatureBuilder, VisitorFeatures
from src.scoring.creative_playbook import (
    build_creative_plan_row,
    build_creative_variants,
    build_kpi_hypothesis,
)
from src.scoring.direct_sync import sync_audience_targets
from src.scoring.feature_sync import build_scoring_features
from src.scoring.scorer import RuleBasedScorer
from src.settings import Settings

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
                **self._runtime_debug(feature_sync_result=feature_sync_result, source_mode=builder.source_mode),
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
            **self._runtime_debug(feature_sync_result=feature_sync_result, source_mode=builder.source_mode),
        }

    def _runtime_debug(self, feature_sync_result: dict[str, Any] | None, source_mode: str) -> dict[str, Any]:
        page_signal_status = ((feature_sync_result or {}).get("page_signal_status") or {}) if feature_sync_result else {}
        return {
            "feature_sync_version": (feature_sync_result or {}).get("feature_sync_version"),
            "attribution_logic_version": (feature_sync_result or {}).get("attribution_logic_version"),
            "git_commit": self._git_commit(),
            "source_mode": source_mode,
            "has_attribution_stats": isinstance(page_signal_status.get("attribution_stats"), dict),
            "selected_query_name": (feature_sync_result or {}).get("selected_query_name"),
            "selected_dimensions": (feature_sync_result or {}).get("selected_dimensions"),
            "safe_fallback_triggered": bool((feature_sync_result or {}).get("safe_fallback_triggered", False)),
            "skipped_replace_due_to_empty_fetch": bool(
                (feature_sync_result or {}).get("skipped_replace_due_to_empty_fetch", False)
            ),
        }

    @staticmethod
    def _git_commit() -> str:
        try:
            return (
                subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], text=True, timeout=2).strip()
                or "unknown"
            )
        except Exception:  # noqa: BLE001
            return "unknown"

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
                os_root,
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
                os_root,
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

    def get_timeseries(self, days: int = 90) -> dict[str, Any]:
        safe_days = max(1, min(int(days or 90), 365))
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

    def get_audience_report(self, days: int = 90) -> dict[str, Any]:
        safe_days = max(1, min(int(days or 90), 90))
        gender_age = self._fetch_metrica_gender_age(days=safe_days)
        source_mix = self._fetch_scoring_source_mix(days=safe_days, limit=12)
        device_mix = self._fetch_metrica_device_mix(days=safe_days, limit=12)
        mobile_os_mix = self._fetch_metrica_mobile_os_mix(days=safe_days, limit=12)
        if not device_mix:
            device_mix = self._fetch_scoring_device_mix(days=safe_days, limit=12)

        ready = bool(gender_age.get("ready", False) or source_mix or device_mix or mobile_os_mix)
        return {
            "ready": ready,
            "days": safe_days,
            "gender_age": gender_age.get("items", []),
            "gender_age_error": gender_age.get("error"),
            "source_mix": source_mix,
            "device_mix": device_mix,
            "mobile_os_mix": mobile_os_mix,
            "note": (
                "Пол/возраст — агрегаты Метрики (не персональные признаки visitor). "
                "Для visitor-level scoring используйте поведенческие сигналы."
            ),
        }

    def get_attribution_quality(self, days: int = 90) -> dict[str, Any]:
        safe_days = max(1, min(int(days or 90), 365))
        conn = get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    with base as (
                        select coalesce(nullif(traffic_source, ''), 'unknown') as source
                        from mart_visitor_scoring
                        where scored_at::date >= current_date - (%s::int - 1)
                    )
                    select
                        count(*)::int as total,
                        count(*) filter (where source = 'direct')::int as direct_cnt,
                        count(*) filter (where source = 'unknown')::int as unknown_cnt
                    from base
                    """,
                    (safe_days,),
                )
                row = cur.fetchone() or {}
                total = int(row.get("total") or 0)
                direct_cnt = int(row.get("direct_cnt") or 0)
                unknown_cnt = int(row.get("unknown_cnt") or 0)
                direct_pct = round((direct_cnt / total) * 100, 2) if total else 0.0
                unknown_pct = round((unknown_cnt / total) * 100, 2) if total else 0.0

                cur.execute(
                    """
                    select
                        coalesce(nullif(traffic_source, ''), 'unknown') as source,
                        count(*)::int as visitors
                    from mart_visitor_scoring
                    where scored_at::date >= current_date - (%s::int - 1)
                    group by 1
                    order by visitors desc
                    limit 7
                    """,
                    (safe_days,),
                )
                top_sources = [dict(r) for r in cur.fetchall()]

                if total == 0:
                    status = "empty"
                elif unknown_pct > 20 or direct_pct > 85:
                    status = "low"
                elif unknown_pct > 8 or direct_pct > 65:
                    status = "medium"
                else:
                    status = "high"

                return {
                    "ready": True,
                    "days": safe_days,
                    "status": status,
                    "total": total,
                    "direct_cnt": direct_cnt,
                    "unknown_cnt": unknown_cnt,
                    "direct_pct": direct_pct,
                    "unknown_pct": unknown_pct,
                    "top_sources": top_sources,
                }
        except Exception as exc:  # noqa: BLE001
            return {
                "ready": False,
                "days": safe_days,
                "status": "error",
                "error": str(exc),
                "total": 0,
                "direct_cnt": 0,
                "unknown_cnt": 0,
                "direct_pct": 0.0,
                "unknown_pct": 0.0,
                "top_sources": [],
            }
        finally:
            conn.close()

    def get_creative_plan(self, days: int = 90, limit_per_segment: int = 5) -> dict[str, Any]:
        safe_days = max(1, min(int(days or 90), 365))
        safe_limit = max(1, min(int(limit_per_segment or 5), 20))
        conn = get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    with ranked as (
                        select
                            visitor_id,
                            segment,
                            short_reason,
                            coalesce(nullif(traffic_source, ''), 'unknown') as traffic_source,
                            coalesce(nullif(os_root, ''), 'unknown') as os_root,
                            normalized_score,
                            scored_at,
                            row_number() over (partition by segment order by normalized_score desc, scored_at desc) as rn
                        from mart_visitor_scoring
                        where scored_at::date >= current_date - (%s::int - 1)
                    )
                    select
                        visitor_id,
                        segment,
                        short_reason,
                        traffic_source,
                        os_root,
                        normalized_score,
                        scored_at
                    from ranked
                    where rn <= %s
                    order by segment, normalized_score desc, scored_at desc
                    """,
                    (safe_days, safe_limit),
                )
                rows = [dict(r) for r in cur.fetchall()]

            items: list[dict[str, Any]] = []
            for row in rows:
                plan = build_creative_plan_row(
                    segment=str(row.get("segment") or ""),
                    short_reason=str(row.get("short_reason") or ""),
                    traffic_source=str(row.get("traffic_source") or ""),
                )
                items.append(
                    {
                        **row,
                        **plan,
                    }
                )

            return {
                "ready": True,
                "days": safe_days,
                "limit_per_segment": safe_limit,
                "items": items,
                "count": len(items),
            }
        except Exception as exc:  # noqa: BLE001
            return {
                "ready": False,
                "days": safe_days,
                "limit_per_segment": safe_limit,
                "items": [],
                "count": 0,
                "error": str(exc),
            }
        finally:
            conn.close()

    def get_audiences_cohorts(self, days: int = 90) -> dict[str, Any]:
        safe_days = max(1, min(int(days or 90), 365))
        conn = get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    select
                        segment,
                        coalesce(nullif(os_root, ''), 'unknown') as os_root,
                        coalesce(nullif(traffic_source, ''), 'unknown') as traffic_source,
                        count(*)::int as visitors
                    from mart_visitor_scoring
                    where scored_at::date >= current_date - (%s::int - 1)
                    group by 1,2,3
                    order by visitors desc
                    limit 100
                    """,
                    (safe_days,),
                )
                matrix = [dict(r) for r in cur.fetchall()]

                cohorts_spec = [
                    ("hot_all_7d", "hot", None, 7),
                    ("hot_android_7d", "hot", "android", 7),
                    ("hot_ios_7d", "hot", "ios", 7),
                    ("warm_all_14d", "warm", None, 14),
                    ("warm_android_14d", "warm", "android", 14),
                    ("warm_ios_14d", "warm", "ios", 14),
                    ("cold_all_30d", "cold", None, 30),
                ]
                cohorts: list[dict[str, Any]] = []
                for name, segment, os_root, window_days in cohorts_spec:
                    cur.execute(
                        """
                        select count(*)::int as cnt
                        from mart_visitor_scoring
                        where segment = %s
                          and scored_at::date >= current_date - (%s::int - 1)
                          and (%s::text is null or coalesce(nullif(os_root, ''), 'unknown') = %s::text)
                        """,
                        (segment, min(window_days, safe_days), os_root, os_root),
                    )
                    cnt = int((cur.fetchone() or {}).get("cnt") or 0)
                    cohorts.append(
                        {
                            "cohort_name": name,
                            "segment": segment,
                            "os_root": os_root or "all",
                            "window_days": min(window_days, safe_days),
                            "visitors": cnt,
                            "direct_tag": name,
                        }
                    )

            return {
                "ready": True,
                "days": safe_days,
                "cohorts": cohorts,
                "matrix": matrix,
            }
        except Exception as exc:  # noqa: BLE001
            return {
                "ready": False,
                "days": safe_days,
                "cohorts": [],
                "matrix": [],
                "error": str(exc),
            }
        finally:
            conn.close()

    def get_audience_export(
        self,
        *,
        days: int = 90,
        segment: str | None = None,
        os_root: str | None = None,
        source: str | None = None,
        min_score: float | None = None,
        limit: int = 5000,
        numeric_only: bool = True,
    ) -> dict[str, Any]:
        safe_days = max(1, min(int(days or 90), 365))
        safe_limit = max(1, min(int(limit or 5000), 50000))
        where = ["scored_at::date >= current_date - (%s::int - 1)"]
        params: list[Any] = [safe_days]

        if segment:
            where.append("segment = %s")
            params.append(segment.lower())
        if os_root:
            where.append("coalesce(nullif(os_root, ''), 'unknown') = %s")
            params.append(os_root.lower())
        if source:
            where.append("coalesce(nullif(traffic_source, ''), 'unknown') = %s")
            params.append(source.lower())
        if min_score is not None:
            where.append("normalized_score >= %s")
            params.append(float(min_score))
        if numeric_only:
            where.append("visitor_id ~ '^[0-9]+$'")

        sql = f"""
            select
                visitor_id,
                segment,
                normalized_score,
                short_reason,
                coalesce(nullif(traffic_source, ''), 'unknown') as traffic_source,
                coalesce(nullif(os_root, ''), 'unknown') as os_root,
                coalesce(nullif(device_type, ''), 'unknown') as device_type,
                scored_at
            from mart_visitor_scoring
            where {' and '.join(where)}
            order by normalized_score desc, scored_at desc
            limit %s
        """
        params.append(safe_limit)

        conn = get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, tuple(params))
                items = [dict(r) for r in cur.fetchall()]
                return {
                    "ready": True,
                    "days": safe_days,
                    "segment": segment,
                    "os_root": os_root,
                    "source": source,
                    "min_score": min_score,
                    "numeric_only": numeric_only,
                    "limit": safe_limit,
                    "count": len(items),
                    "items": items,
                }
        except Exception as exc:  # noqa: BLE001
            return {
                "ready": False,
                "days": safe_days,
                "segment": segment,
                "os_root": os_root,
                "source": source,
                "min_score": min_score,
                "numeric_only": numeric_only,
                "limit": safe_limit,
                "count": 0,
                "items": [],
                "error": str(exc),
            }
        finally:
            conn.close()

    def get_activation_plan(
        self,
        *,
        days: int = 90,
        min_audience_size: int = 100,
        export_limit: int = 5000,
    ) -> dict[str, Any]:
        safe_days = max(1, min(int(days or 90), 365))
        safe_min = max(1, min(int(min_audience_size or 100), 100000))
        safe_limit = max(1, min(int(export_limit or 5000), 50000))

        cohorts_payload = self.get_audiences_cohorts(days=safe_days)
        if not cohorts_payload.get("ready", False):
            return {
                "ready": False,
                "days": safe_days,
                "min_audience_size": safe_min,
                "export_limit": safe_limit,
                "cohorts": [],
                "count": 0,
                "eligible_count": 0,
                "error": cohorts_payload.get("error", "failed to build cohorts"),
            }

        cohorts = cohorts_payload.get("cohorts", []) or []
        items: list[dict[str, Any]] = []
        eligible = 0

        for c in cohorts:
            segment = str(c.get("segment") or "").strip().lower()
            if segment not in {"hot", "warm", "cold"}:
                continue
            os_root = str(c.get("os_root") or "all").strip().lower()
            window_days = int(c.get("window_days") or safe_days)
            direct_tag = str(c.get("direct_tag") or c.get("cohort_name") or "").strip()
            window = max(1, min(window_days, safe_days))
            os_filter = None if os_root in {"", "all"} else os_root

            export = self.get_audience_export(
                days=window,
                segment=segment,
                os_root=os_filter,
                limit=safe_limit,
                numeric_only=True,
            )
            export_items = export.get("items", []) or []
            audience_size = int(export.get("count") or len(export_items))
            is_eligible = audience_size >= safe_min
            if is_eligible:
                eligible += 1

            source_hint = str((export_items[0].get("traffic_source") if export_items else "") or "unknown")
            reason_hint = str((export_items[0].get("short_reason") if export_items else "") or "")
            creative = build_creative_plan_row(
                segment=segment,
                short_reason=reason_hint,
                traffic_source=source_hint,
            )

            items.append(
                {
                    "cohort_name": str(c.get("cohort_name") or ""),
                    "segment": segment,
                    "os_root": os_root or "all",
                    "window_days": window,
                    "direct_tag": direct_tag,
                    "audience_size": audience_size,
                    "eligible": is_eligible,
                    "min_required": safe_min,
                    "source_hint": source_hint,
                    "short_reason_hint": reason_hint,
                    "sample_visitor_ids": [str(i.get("visitor_id")) for i in export_items[:20] if i.get("visitor_id")],
                    "creative": creative,
                }
            )

        return {
            "ready": True,
            "days": safe_days,
            "min_audience_size": safe_min,
            "export_limit": safe_limit,
            "cohorts": items,
            "count": len(items),
            "eligible_count": eligible,
        }

    def sync_activation_to_direct(
        self,
        *,
        days: int = 90,
        min_audience_size: int = 100,
        export_limit: int = 5000,
        dry_run: bool = True,
    ) -> dict[str, Any]:
        plan = self.get_activation_plan(
            days=days,
            min_audience_size=min_audience_size,
            export_limit=export_limit,
        )
        if not plan.get("ready", False):
            return {
                "ok": False,
                "ready": False,
                "dry_run": bool(dry_run),
                "error": plan.get("error", "activation plan is not ready"),
                "plan": plan,
            }

        eligible_cohorts = [c for c in (plan.get("cohorts") or []) if bool(c.get("eligible", False))]
        sync = sync_audience_targets(cohorts=eligible_cohorts, dry_run=bool(dry_run))
        return {
            "ok": bool(sync.get("ok", False)),
            "ready": True,
            "dry_run": bool(sync.get("dry_run", dry_run)),
            "days": int(plan.get("days") or days),
            "min_audience_size": int(plan.get("min_audience_size") or min_audience_size),
            "eligible_count": len(eligible_cohorts),
            "plan_count": int(plan.get("count") or 0),
            "sync": sync,
        }

    def get_activation_reaction(self, *, days: int = 30, limit: int = 50) -> dict[str, Any]:
        safe_days = max(1, min(int(days or 30), 365))
        safe_limit = max(1, min(int(limit or 50), 200))

        conn = get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    with base as (
                        select
                            date,
                            campaign_name,
                            impressions,
                            clicks,
                            cost,
                            lower((regexp_match(campaign_name, '(scoring_[a-z0-9_]+)'))[1]) as direct_tag
                        from stg_direct_campaign_daily
                        where date >= current_date - (%s::int - 1)
                    )
                    select
                        direct_tag,
                        sum(impressions)::bigint as impressions,
                        sum(clicks)::bigint as clicks,
                        round(sum(cost)::numeric, 2) as cost,
                        round(
                            case when sum(impressions) > 0
                                then (sum(clicks)::numeric / sum(impressions)::numeric) * 100
                                else 0
                            end
                        , 2) as ctr_pct,
                        round(
                            case when sum(clicks) > 0
                                then sum(cost)::numeric / sum(clicks)::numeric
                                else 0
                            end
                        , 2) as avg_cpc
                    from base
                    where direct_tag is not null
                    group by direct_tag
                    order by clicks desc, impressions desc
                    limit %s
                    """,
                    (safe_days, safe_limit),
                )
                items = [dict(r) for r in cur.fetchall()]
                total_impressions = int(sum(int(r.get("impressions") or 0) for r in items))
                total_clicks = int(sum(int(r.get("clicks") or 0) for r in items))
                total_cost = round(sum(float(r.get("cost") or 0.0) for r in items), 2)
                return {
                    "ready": True,
                    "days": safe_days,
                    "limit": safe_limit,
                    "count": len(items),
                    "items": items,
                    "totals": {
                        "impressions": total_impressions,
                        "clicks": total_clicks,
                        "cost": total_cost,
                    },
                }
        except Exception as exc:  # noqa: BLE001
            if self._is_undefined_table_error(exc):
                return {
                    "ready": False,
                    "days": safe_days,
                    "limit": safe_limit,
                    "count": 0,
                    "items": [],
                    "totals": {"impressions": 0, "clicks": 0, "cost": 0.0},
                    "error": "stg_direct_campaign_daily is missing",
                }
            return {
                "ready": False,
                "days": safe_days,
                "limit": safe_limit,
                "count": 0,
                "items": [],
                "totals": {"impressions": 0, "clicks": 0, "cost": 0.0},
                "error": str(exc),
            }
        finally:
            conn.close()

    def get_ad_templates(
        self,
        *,
        days: int = 90,
        min_audience_size: int = 1,
        include_small: bool = True,
        variants: int = 3,
    ) -> dict[str, Any]:
        safe_days = max(1, min(int(days or 90), 365))
        safe_min = max(1, min(int(min_audience_size or 1), 100000))
        safe_variants = max(1, min(int(variants or 3), 5))

        plan = self.get_activation_plan(
            days=safe_days,
            min_audience_size=safe_min,
            export_limit=5000,
        )
        if not plan.get("ready", False):
            return {
                "ready": False,
                "days": safe_days,
                "count": 0,
                "items": [],
                "error": plan.get("error", "activation plan is not ready"),
            }

        lead_ref_30 = self._fetch_click_to_lead_reference(days=30)
        lead_ref_90 = self._fetch_click_to_lead_reference(days=90)
        selected_ref = lead_ref_30 if safe_days <= 45 else lead_ref_90
        selected_click_to_lead = selected_ref.get("click_to_lead_pct") if selected_ref.get("ready", False) else None

        reaction = self.get_activation_reaction(days=min(safe_days, 90), limit=200)
        reaction_items = reaction.get("items", []) if reaction.get("ready", False) else []
        reaction_by_tag = {str(i.get("direct_tag") or "").strip().lower(): i for i in reaction_items if i.get("direct_tag")}

        mapping = self._read_direct_mapping()
        out: list[dict[str, Any]] = []
        for c in plan.get("cohorts") or []:
            audience_size = int(c.get("audience_size") or 0)
            if audience_size <= 0:
                continue
            if not include_small and not bool(c.get("eligible", False)):
                continue

            segment = str(c.get("segment") or "").strip().lower()
            os_root = str(c.get("os_root") or "all").strip().lower()
            window_days = int(c.get("window_days") or safe_days)
            source_hint = str(c.get("source_hint") or c.get("traffic_source") or "unknown")
            reason_hint = str(c.get("short_reason_hint") or c.get("short_reason") or "")
            cohort_name = str(c.get("cohort_name") or "")

            mapping_row = mapping.get(cohort_name) or {}
            ad_group_id = mapping_row.get("ad_group_id")
            retargeting_list_id = mapping_row.get("retargeting_list_id")
            direct_tag = str(c.get("direct_tag") or "").strip().lower()
            baseline_row = reaction_by_tag.get(direct_tag, {})

            variants_rows = build_creative_variants(
                segment=segment,
                short_reason=reason_hint,
                traffic_source=source_hint,
                max_variants=safe_variants,
            )
            kpi_hypothesis = build_kpi_hypothesis(
                segment=segment,
                short_reason=reason_hint,
                traffic_source=source_hint,
                baseline=baseline_row,
                click_to_lead_actual_pct=selected_click_to_lead,
                reference_window_days=int(selected_ref.get("days") or safe_days),
            )
            out.append(
                {
                    "cohort_name": cohort_name,
                    "segment": segment,
                    "os_root": os_root,
                    "window_days": window_days,
                    "direct_tag": c.get("direct_tag"),
                    "audience_size": audience_size,
                    "eligible": bool(c.get("eligible", False)),
                    "source_hint": source_hint,
                    "short_reason_hint": reason_hint,
                    "ad_group_id": ad_group_id,
                    "retargeting_list_id": retargeting_list_id,
                    "strategy_priority": mapping_row.get("strategy_priority"),
                    "goal_id": mapping_row.get("goal_id"),
                    "performance_baseline": baseline_row,
                    "conversion_reference": {
                        "click_to_lead_pct_30d": lead_ref_30.get("click_to_lead_pct"),
                        "click_to_lead_pct_90d": lead_ref_90.get("click_to_lead_pct"),
                        "selected_window_days": int(selected_ref.get("days") or safe_days),
                        "selected_click_to_lead_pct": selected_click_to_lead,
                        "selected_clicks": selected_ref.get("clicks"),
                        "selected_leads": selected_ref.get("lead_sessions"),
                        "source_mode": selected_ref.get("source_mode"),
                        "primary_goal_ids": selected_ref.get("primary_goal_ids") or [],
                        "assist_goal_ids": selected_ref.get("assist_goal_ids") or [],
                    },
                    "kpi_hypothesis": kpi_hypothesis,
                    "variants": variants_rows,
                }
            )

        return {
            "ready": True,
            "days": safe_days,
            "min_audience_size": safe_min,
            "include_small": bool(include_small),
            "variants": safe_variants,
            "count": len(out),
            "items": out,
        }

    @staticmethod
    def _goal_ids_from_env(name: str, defaults: list[int]) -> list[int]:
        raw = str(os.getenv(name, "")).strip()
        if not raw:
            return defaults
        out: list[int] = []
        for token in raw.replace(";", ",").replace(" ", ",").split(","):
            t = token.strip()
            if t.isdigit():
                out.append(int(t))
        return out or defaults

    def _fetch_click_to_lead_reference(self, days: int) -> dict[str, Any]:
        safe_days = max(1, min(int(days or 30), 365))
        primary_goal_ids = self._goal_ids_from_env("SCORING_LEAD_PRIMARY_GOAL_IDS", [437747318])
        assist_goal_ids = self._goal_ids_from_env(
            "SCORING_LEAD_ASSIST_GOAL_IDS",
            [519273838, 519273814, 327368948, 327368949],
        )
        goal_ids = [*primary_goal_ids, *assist_goal_ids]
        seen: set[int] = set()
        goal_ids = [gid for gid in goal_ids if not (gid in seen or seen.add(gid))]
        if not goal_ids:
            return {
                "ready": False,
                "days": safe_days,
                "error": "goal ids are not configured",
                "click_to_lead_pct": None,
                "clicks": 0,
                "lead_sessions": 0,
                "primary_goal_ids": primary_goal_ids,
                "assist_goal_ids": assist_goal_ids,
            }

        clicks = 0
        conn = get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    select coalesce(sum(clicks), 0)::bigint as clicks
                    from stg_direct_campaign_daily
                    where date >= current_date - (%s::int - 1)
                    """,
                    (safe_days,),
                )
                clicks = int((cur.fetchone() or {}).get("clicks") or 0)
        except Exception as exc:  # noqa: BLE001
            if self._is_undefined_table_error(exc):
                return {
                    "ready": False,
                    "days": safe_days,
                    "error": "stg_direct_campaign_daily is missing",
                    "click_to_lead_pct": None,
                    "clicks": 0,
                    "lead_sessions": 0,
                    "primary_goal_ids": primary_goal_ids,
                    "assist_goal_ids": assist_goal_ids,
                }
            return {
                "ready": False,
                "days": safe_days,
                "error": str(exc),
                "click_to_lead_pct": None,
                "clicks": 0,
                "lead_sessions": 0,
                "primary_goal_ids": primary_goal_ids,
                "assist_goal_ids": assist_goal_ids,
            }
        finally:
            conn.close()

        token = str(Settings.METRICA_TOKEN or "").strip()
        counter_id = str(Settings.METRICA_COUNTER_ID or "").strip()
        if not token or not counter_id:
            return {
                "ready": False,
                "days": safe_days,
                "error": "METRICA_TOKEN or METRICA_COUNTER_ID is missing",
                "click_to_lead_pct": None,
                "clicks": clicks,
                "lead_sessions": 0,
                "primary_goal_ids": primary_goal_ids,
                "assist_goal_ids": assist_goal_ids,
            }

        metrics = ",".join(f"ym:s:goal{gid}reaches" for gid in goal_ids)
        params = {
            "ids": counter_id,
            "date1": f"{safe_days}daysAgo",
            "date2": "yesterday",
            "metrics": metrics,
            "dimensions": "ym:s:lastTrafficSource",
            "accuracy": "full",
            "limit": "100000",
        }
        headers = {"Authorization": f"OAuth {token}"}

        try:
            resp = requests.get("https://api-metrika.yandex.net/stat/v1/data", params=params, headers=headers, timeout=60)
            resp.raise_for_status()
            payload = resp.json()
            rows = payload.get("data", []) or []
        except Exception as exc:  # noqa: BLE001
            return {
                "ready": False,
                "days": safe_days,
                "error": str(exc),
                "click_to_lead_pct": None,
                "clicks": clicks,
                "lead_sessions": 0,
                "primary_goal_ids": primary_goal_ids,
                "assist_goal_ids": assist_goal_ids,
            }

        paid_sources = {
            "ad",
            "paid",
            "cpc",
            "реклама",
            "переходы по рекламе",
            "ads",
        }

        def _row_sum(row: dict[str, Any]) -> dict[int, int]:
            mets = row.get("metrics") or []
            out_local: dict[int, int] = {}
            for idx, gid in enumerate(goal_ids):
                val = mets[idx] if idx < len(mets) else 0
                out_local[gid] = int(float(val)) if val is not None else 0
            return out_local

        totals_paid: dict[int, int] = {gid: 0 for gid in goal_ids}
        totals_all: dict[int, int] = {gid: 0 for gid in goal_ids}
        paid_rows = 0

        for row in rows:
            dims = row.get("dimensions") or []
            source_name = str((dims[0] or {}).get("name") or "").strip().lower() if dims else ""
            row_vals = _row_sum(row)
            for gid in goal_ids:
                totals_all[gid] += int(row_vals.get(gid, 0))
            if source_name in paid_sources:
                paid_rows += 1
                for gid in goal_ids:
                    totals_paid[gid] += int(row_vals.get(gid, 0))

        source_mode = "paid_source"
        selected = totals_paid if paid_rows > 0 else totals_all
        if paid_rows == 0:
            source_mode = "all_sources_fallback"

        primary_leads = int(sum(int(selected.get(gid, 0)) for gid in primary_goal_ids))
        assist_leads = int(sum(int(selected.get(gid, 0)) for gid in assist_goal_ids))
        lead_sessions = primary_leads if primary_leads > 0 else assist_leads
        click_to_lead_pct = round((lead_sessions / clicks) * 100, 4) if clicks > 0 and lead_sessions > 0 else None

        return {
            "ready": True,
            "days": safe_days,
            "source_mode": source_mode,
            "clicks": clicks,
            "primary_leads": primary_leads,
            "assist_leads": assist_leads,
            "lead_sessions": lead_sessions,
            "click_to_lead_pct": click_to_lead_pct,
            "primary_goal_ids": primary_goal_ids,
            "assist_goal_ids": assist_goal_ids,
        }

    def generate_ad_template_banners(
        self,
        *,
        cohort_name: str,
        variant_key: str | None = None,
        days: int = 90,
        min_audience_size: int = 1,
        include_small: bool = True,
        variants: int = 3,
        images_per_variant: int = 1,
        size: str = "1536x1024",
        quality: str = "medium",
        output_format: str = "png",
    ) -> dict[str, Any]:
        name = str(cohort_name or "").strip()
        if not name:
            return {"ok": False, "ready": False, "error": "cohort_name is required"}

        templates = self.get_ad_templates(
            days=days,
            min_audience_size=min_audience_size,
            include_small=include_small,
            variants=variants,
        )
        if not templates.get("ready", False):
            return {
                "ok": False,
                "ready": False,
                "error": templates.get("error", "ad templates are not ready"),
            }

        items = templates.get("items") or []
        item = next((x for x in items if str(x.get("cohort_name") or "").strip() == name), None)
        if not item:
            return {"ok": False, "ready": False, "error": f"cohort not found: {name}"}

        result = generate_template_banners(
            template_item=item,
            variant_key=variant_key,
            images_per_variant=images_per_variant,
            size=size,
            quality=quality,
            output_format=output_format,
        )
        return {
            "ok": bool(result.get("ok", False)),
            "ready": True,
            "cohort_name": name,
            "segment": item.get("segment"),
            "provider_requested": result.get("provider_requested"),
            "provider_used": result.get("provider_used"),
            "generated_count": int(result.get("generated_count") or 0),
            "failed_count": int(result.get("failed_count") or 0),
            "images_per_variant": int(result.get("images_per_variant") or images_per_variant),
            "model": result.get("model"),
            "model_used": result.get("model_used"),
            "size": result.get("size"),
            "quality": result.get("quality"),
            "output_format": result.get("output_format"),
            "cost_usd": result.get("cost_usd"),
            "cost_source": result.get("cost_source"),
            "cost_estimated_usd": result.get("cost_estimated_usd"),
            "cost_reported_usd": result.get("cost_reported_usd"),
            "openrouter_credits_before": result.get("openrouter_credits_before"),
            "openrouter_credits_after": result.get("openrouter_credits_after"),
            "usage": result.get("usage") or {},
            "generated": result.get("generated") or [],
            "failed": result.get("failed") or [],
            "error": result.get("error"),
        }

    def bootstrap_activation_direct(
        self,
        *,
        days: int = 90,
        min_audience_size: int = 100,
        export_limit: int = 5000,
        campaign_id: int | None = None,
        region_ids: list[int] | None = None,
        apply: bool = False,
        env_path: str = "/home/kv145/traffic-analytics/.env",
        include_small: bool = False,
    ) -> dict[str, Any]:
        safe_days = max(1, min(int(days or 90), 365))
        safe_min = max(1, min(int(min_audience_size or 100), 100000))
        safe_limit = max(1, min(int(export_limit or 5000), 50000))
        plan = self.get_activation_plan(
            days=safe_days,
            min_audience_size=safe_min,
            export_limit=safe_limit,
        )
        if not plan.get("ready", False):
            return {
                "ok": False,
                "ready": False,
                "error": plan.get("error", "activation plan is not ready"),
                "plan": plan,
            }

        selected_campaign_id = int(campaign_id) if campaign_id else self._infer_campaign_id_for_bootstrap()
        if not selected_campaign_id:
            return {
                "ok": False,
                "ready": False,
                "error": "campaign_id is missing and cannot be inferred from stg_direct_campaign_daily",
                "plan": {
                    "count": int(plan.get("count") or 0),
                    "eligible_count": int(plan.get("eligible_count") or 0),
                },
            }

        cohorts_raw = plan.get("cohorts") or []
        cohorts = [
            c
            for c in cohorts_raw
            if int(c.get("audience_size") or 0) > 0 and (include_small or bool(c.get("eligible", False)))
        ]
        bootstrap = bootstrap_direct_entities(
            cohorts=cohorts,
            campaign_id=selected_campaign_id,
            region_ids=region_ids or [0],
            apply=bool(apply),
            env_path=env_path,
            enable_auto_sync=True,
        )
        return {
            "ok": bool(bootstrap.get("ok", False)),
            "ready": True,
            "apply": bool(apply),
            "days": safe_days,
            "campaign_id": selected_campaign_id,
            "cohorts_selected": len(cohorts),
            "include_small": bool(include_small),
            "plan_count": int(plan.get("count") or 0),
            "eligible_count": int(plan.get("eligible_count") or 0),
            "bootstrap": bootstrap,
        }

    @staticmethod
    def _fetch_metrica_gender_age(days: int) -> dict[str, Any]:
        token = str(Settings.METRICA_TOKEN or "").strip()
        counter_id = str(Settings.METRICA_COUNTER_ID or "").strip()
        if not token or not counter_id:
            return {"ready": False, "items": [], "error": "METRICA_TOKEN or METRICA_COUNTER_ID is missing"}

        params = {
            "ids": counter_id,
            "date1": f"{days}daysAgo",
            "date2": "yesterday",
            "metrics": "ym:s:visits,ym:s:users",
            "dimensions": "ym:s:gender,ym:s:ageInterval",
            "accuracy": "full",
            "limit": "100000",
            "sort": "-ym:s:visits",
        }
        headers = {"Authorization": f"OAuth {token}"}
        try:
            resp = requests.get("https://api-metrika.yandex.net/stat/v1/data", params=params, headers=headers, timeout=60)
            resp.raise_for_status()
            payload = resp.json()
            rows = payload.get("data", []) or []
            items: list[dict[str, Any]] = []
            for row in rows:
                dims = row.get("dimensions") or []
                mets = row.get("metrics") or []
                gender = str((dims[0] or {}).get("name") or "") if len(dims) > 0 else ""
                age = str((dims[1] or {}).get("name") or "") if len(dims) > 1 else ""
                visits = int(float(mets[0])) if len(mets) > 0 and mets[0] is not None else 0
                users = int(float(mets[1])) if len(mets) > 1 and mets[1] is not None else 0
                items.append(
                    {
                        "gender": gender or "unknown",
                        "age_interval": age or "unknown",
                        "visits": visits,
                        "users": users,
                    }
                )
            return {"ready": True, "items": items}
        except Exception as exc:  # noqa: BLE001
            return {"ready": False, "items": [], "error": str(exc)}

    @staticmethod
    def _fetch_metrica_device_mix(days: int, limit: int = 12) -> list[dict[str, Any]]:
        token = str(Settings.METRICA_TOKEN or "").strip()
        counter_id = str(Settings.METRICA_COUNTER_ID or "").strip()
        if not token or not counter_id:
            return []

        params = {
            "ids": counter_id,
            "date1": f"{days}daysAgo",
            "date2": "yesterday",
            "metrics": "ym:s:visits,ym:s:users",
            "dimensions": "ym:s:deviceCategory",
            "accuracy": "full",
            "limit": str(max(1, min(int(limit), 50))),
            "sort": "-ym:s:visits",
        }
        headers = {"Authorization": f"OAuth {token}"}
        try:
            resp = requests.get("https://api-metrika.yandex.net/stat/v1/data", params=params, headers=headers, timeout=60)
            resp.raise_for_status()
            payload = resp.json()
            rows = payload.get("data", []) or []
            items: list[dict[str, Any]] = []
            for row in rows:
                dims = row.get("dimensions") or []
                mets = row.get("metrics") or []
                raw = str((dims[0] or {}).get("name") or "") if len(dims) > 0 else ""
                visits = int(float(mets[0])) if len(mets) > 0 and mets[0] is not None else 0
                users = int(float(mets[1])) if len(mets) > 1 and mets[1] is not None else 0
                items.append({"device_type": raw or "unknown", "visitors": visits, "users": users})
            return items
        except Exception:
            return []

    @staticmethod
    def _fetch_metrica_mobile_os_mix(days: int, limit: int = 12) -> list[dict[str, Any]]:
        token = str(Settings.METRICA_TOKEN or "").strip()
        counter_id = str(Settings.METRICA_COUNTER_ID or "").strip()
        if not token or not counter_id:
            return []

        params = {
            "ids": counter_id,
            "date1": f"{days}daysAgo",
            "date2": "yesterday",
            "metrics": "ym:s:visits,ym:s:users",
            "dimensions": "ym:s:deviceCategory,ym:s:operatingSystemRoot",
            "accuracy": "full",
            "limit": str(max(1000, min(int(limit) * 20, 100000))),
            "sort": "-ym:s:visits",
        }
        headers = {"Authorization": f"OAuth {token}"}
        try:
            resp = requests.get("https://api-metrika.yandex.net/stat/v1/data", params=params, headers=headers, timeout=60)
            resp.raise_for_status()
            payload = resp.json()
            rows = payload.get("data", []) or []
            agg: dict[str, dict[str, int | str]] = {}
            for row in rows:
                dims = row.get("dimensions") or []
                device_raw = str((dims[0] or {}).get("name") or "").strip().lower() if len(dims) > 0 else ""
                os_raw = str((dims[1] or {}).get("name") or "").strip().lower() if len(dims) > 1 else ""
                visits = int(float((row.get("metrics") or [0, 0])[0] or 0))
                users = int(float((row.get("metrics") or [0, 0])[1] or 0))

                if device_raw not in {"2", "mobile devices", "mobile", "smartphones"}:
                    continue

                if "android" in os_raw:
                    key = "android"
                    label = "Android"
                elif "ios" in os_raw or "apple" in os_raw:
                    key = "ios"
                    label = "Apple iOS"
                elif not os_raw or os_raw == "unknown":
                    key = "unknown"
                    label = "Не определено"
                else:
                    key = "other"
                    label = "Другая мобильная ОС"

                if key not in agg:
                    agg[key] = {"os_root": key, "os_label": label, "visits": 0, "users": 0}
                agg[key]["visits"] = int(agg[key]["visits"]) + visits
                agg[key]["users"] = int(agg[key]["users"]) + users

            out = sorted(agg.values(), key=lambda x: int(x["visits"]), reverse=True)
            return out[: max(1, min(int(limit), 50))]
        except Exception:
            return []

    @staticmethod
    def _fetch_scoring_source_mix(days: int, limit: int = 12) -> list[dict[str, Any]]:
        conn = get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    select
                        coalesce(nullif(traffic_source, ''), 'unknown') as source,
                        count(*)::int as visitors
                    from mart_visitor_scoring
                    where scored_at::date >= current_date - (%s::int - 1)
                    group by 1
                    order by visitors desc
                    limit %s
                    """,
                    (days, max(1, min(int(limit), 50))),
                )
                return [dict(row) for row in cur.fetchall()]
        except Exception:
            return []
        finally:
            conn.close()

    @staticmethod
    def _fetch_scoring_device_mix(days: int, limit: int = 12) -> list[dict[str, Any]]:
        conn = get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    select
                        coalesce(nullif(device_type, ''), 'unknown') as device_type,
                        count(*)::int as visitors
                    from mart_visitor_scoring
                    where scored_at::date >= current_date - (%s::int - 1)
                    group by 1
                    order by visitors desc
                    limit %s
                    """,
                    (days, max(1, min(int(limit), 50))),
                )
                return [dict(row) for row in cur.fetchall()]
        except Exception:
            return []
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
                os_root,
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
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now()
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
                os_root = excluded.os_root,
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
                features.os_root,
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

    @staticmethod
    def _infer_campaign_id_for_bootstrap() -> int | None:
        conn = get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    select campaign_id
                    from stg_direct_campaign_daily
                    where date >= current_date - 30
                    group by campaign_id
                    order by sum(cost) desc nulls last
                    limit 1
                    """
                )
                row = cur.fetchone() or {}
                campaign_id = row.get("campaign_id")
                return int(campaign_id) if campaign_id is not None else None
        except Exception:
            return None
        finally:
            conn.close()

    @staticmethod
    def _read_direct_mapping() -> dict[str, dict[str, Any]]:
        raw = (
            str(os.getenv("SCORING_DIRECT_RETARGET_MAP_JSON", "")).strip()
            or str(os.getenv("SCORING_DIRECT_RETARGET_MAP", "")).strip()
        )
        if not raw:
            return {}
        try:
            data = json.loads(raw)
        except Exception:  # noqa: BLE001
            return {}
        if not isinstance(data, dict):
            return {}
        return {str(k): (v if isinstance(v, dict) else {}) for k, v in data.items()}


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


def get_scoring_timeseries(days: int = 90) -> dict[str, Any]:
    return _service.get_timeseries(days=days)


def get_scoring_audience_report(days: int = 90) -> dict[str, Any]:
    return _service.get_audience_report(days=days)


def get_scoring_attribution_quality(days: int = 90) -> dict[str, Any]:
    return _service.get_attribution_quality(days=days)


def get_scoring_creative_plan(days: int = 90, limit_per_segment: int = 5) -> dict[str, Any]:
    return _service.get_creative_plan(days=days, limit_per_segment=limit_per_segment)


def get_scoring_audiences_cohorts(days: int = 90) -> dict[str, Any]:
    return _service.get_audiences_cohorts(days=days)


def get_scoring_audience_export(
    *,
    days: int = 90,
    segment: str | None = None,
    os_root: str | None = None,
    source: str | None = None,
    min_score: float | None = None,
    limit: int = 5000,
    numeric_only: bool = True,
) -> dict[str, Any]:
    return _service.get_audience_export(
        days=days,
        segment=segment,
        os_root=os_root,
        source=source,
        min_score=min_score,
        limit=limit,
        numeric_only=numeric_only,
    )


def get_scoring_activation_plan(
    *,
    days: int = 90,
    min_audience_size: int = 100,
    export_limit: int = 5000,
) -> dict[str, Any]:
    return _service.get_activation_plan(
        days=days,
        min_audience_size=min_audience_size,
        export_limit=export_limit,
    )


def sync_scoring_activation_to_direct(
    *,
    days: int = 90,
    min_audience_size: int = 100,
    export_limit: int = 5000,
    dry_run: bool = True,
) -> dict[str, Any]:
    return _service.sync_activation_to_direct(
        days=days,
        min_audience_size=min_audience_size,
        export_limit=export_limit,
        dry_run=dry_run,
    )


def get_scoring_activation_reaction(*, days: int = 30, limit: int = 50) -> dict[str, Any]:
    return _service.get_activation_reaction(days=days, limit=limit)


def bootstrap_scoring_activation_direct(
    *,
    days: int = 90,
    min_audience_size: int = 100,
    export_limit: int = 5000,
    campaign_id: int | None = None,
    region_ids: list[int] | None = None,
    apply: bool = False,
    env_path: str = "/home/kv145/traffic-analytics/.env",
    include_small: bool = False,
) -> dict[str, Any]:
    return _service.bootstrap_activation_direct(
        days=days,
        min_audience_size=min_audience_size,
        export_limit=export_limit,
        campaign_id=campaign_id,
        region_ids=region_ids,
        apply=apply,
        env_path=env_path,
        include_small=include_small,
    )


def get_scoring_ad_templates(
    *,
    days: int = 90,
    min_audience_size: int = 1,
    include_small: bool = True,
    variants: int = 3,
) -> dict[str, Any]:
    return _service.get_ad_templates(
        days=days,
        min_audience_size=min_audience_size,
        include_small=include_small,
        variants=variants,
    )


def generate_scoring_ad_template_banners(
    *,
    cohort_name: str,
    variant_key: str | None = None,
    days: int = 90,
    min_audience_size: int = 1,
    include_small: bool = True,
    variants: int = 3,
    images_per_variant: int = 1,
    size: str = "1536x1024",
    quality: str = "medium",
    output_format: str = "png",
) -> dict[str, Any]:
    return _service.generate_ad_template_banners(
        cohort_name=cohort_name,
        variant_key=variant_key,
        days=days,
        min_audience_size=min_audience_size,
        include_small=include_small,
        variants=variants,
        images_per_variant=images_per_variant,
        size=size,
        quality=quality,
        output_format=output_format,
    )
