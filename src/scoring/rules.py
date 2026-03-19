from __future__ import annotations

from src.scoring.feature_builder import VisitorFeatures

SCORING_VERSION = "v1_rules_2026_03"
MIN_RAW_SCORE = -2
MAX_RAW_SCORE = 29


def is_high_intent_source(traffic_source: str, utm_source: str, utm_medium: str) -> bool:
    blob = " ".join([traffic_source or "", utm_source or "", utm_medium or ""]).lower()
    positive_tokens = [
        "direct",
        "brand",
        "search",
        "organic",
        "cpc",
        "ppc",
        "retarget",
        "remarketing",
        "email",
        "yandex",
        "google",
        "поиск",
        "бренд",
        "директ",
    ]
    return any(token in blob for token in positive_tokens)


def calculate_rule_contributions(features: VisitorFeatures) -> dict[str, int]:
    points: dict[str, int] = {}

    if features.visited_price_page:
        points["visited_price_page"] = 3

    if features.visited_program_page:
        points["visited_program_page"] = 2

    if features.visited_booking_page:
        points["visited_booking_page"] = 5

    if features.clicked_booking_button:
        points["clicked_booking_button"] = 7

    if features.sessions_count > 1:
        points["sessions_count_gt_1"] = 2

    if features.sessions_count > 2:
        points["sessions_count_gt_2"] = 1

    if features.total_time_sec > 120:
        points["total_time_gt_120"] = 2

    if features.total_time_sec > 300:
        points["total_time_gt_300"] = 1

    if features.pageviews >= 4:
        points["pageviews_gte_4"] = 1

    if features.pageviews >= 8:
        points["pageviews_gte_8"] = 1

    if features.scroll_70:
        points["scroll_70"] = 1

    if features.return_visitor:
        points["return_visitor"] = 2

    if is_high_intent_source(features.traffic_source, features.utm_source, features.utm_medium):
        points["high_intent_source"] = 1

    if features.is_bounce:
        points["bounce_session"] = -2

    return points


def normalize_raw_score(raw_score: int) -> float:
    if MAX_RAW_SCORE <= MIN_RAW_SCORE:
        return 0.0
    normalized = (raw_score - MIN_RAW_SCORE) / (MAX_RAW_SCORE - MIN_RAW_SCORE)
    return max(0.0, min(1.0, normalized))


def segment_from_score(normalized_score: float) -> str:
    if normalized_score >= 0.70:
        return "hot"
    if normalized_score >= 0.40:
        return "warm"
    return "cold"


def recommendation_for_segment(segment: str) -> str:
    if segment == "hot":
        return "Показать сильный CTA на бронирование / передать в приоритетный follow-up"
    if segment == "warm":
        return "Дожимать контентом: программа, преимущества, кейсы, отзывы"
    return "Прогревать через объяснение концепции лагеря, доверие и пользу лагеря"
