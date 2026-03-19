from __future__ import annotations

from typing import Any

from src.scoring.feature_builder import VisitorFeatures
from src.scoring.rules import recommendation_for_segment


FACTOR_LABELS = {
    "visited_price_page": "изучал страницу цен",
    "visited_program_page": "просматривал страницу программы",
    "visited_booking_page": "заходил на страницу бронирования",
    "clicked_booking_button": "нажимал кнопку бронирования",
    "sessions_count_gt_1": "возвращался на сайт",
    "sessions_count_gt_2": "совершил несколько визитов",
    "total_time_gt_120": "провел на сайте больше 2 минут",
    "total_time_gt_300": "провел на сайте больше 5 минут",
    "pageviews_gte_4": "просмотрел несколько страниц",
    "pageviews_gte_8": "глубоко изучал контент",
    "scroll_70": "доскроллил страницу глубоко",
    "return_visitor": "является повторным посетителем",
    "high_intent_source": "пришел из канала с высоким намерением",
    "bounce_session": "есть признаки короткой сессии",
}


def format_factor_list(explanation: dict[str, int], max_items: int = 3) -> list[str]:
    ranked = sorted(explanation.items(), key=lambda kv: kv[1], reverse=True)
    out: list[str] = []
    for key, _value in ranked:
        if key in FACTOR_LABELS:
            out.append(FACTOR_LABELS[key])
        if len(out) >= max_items:
            break
    return out


def pick_short_reason(
    features: VisitorFeatures,
    explanation: dict[str, int],
    segment: str,
) -> str:
    if features.clicked_booking_button or features.visited_booking_page:
        return "booking_intent"

    if features.visited_price_page:
        return "price_interest"

    if features.return_visitor and (features.sessions_count > 1 or features.total_time_sec > 120 or features.pageviews >= 4):
        return "returning_engaged"

    if features.visited_program_page or features.total_time_sec > 120 or features.pageviews >= 4:
        return "content_engaged"

    if features.is_bounce or explanation.get("bounce_session"):
        return "bounce_like_session"

    if segment == "cold":
        return "exploratory_low_intent"

    return "content_engaged"


def build_human_explanation(
    features: VisitorFeatures,
    explanation: dict[str, int],
    segment: str,
) -> str:
    reasons = format_factor_list(explanation, max_items=4)
    reasons_text = ", ".join(reasons)

    if segment == "hot":
        base = (
            "Высокий score, потому что посетитель проявил явный интерес к покупке: "
            "изучал цену, заходил на страницу бронирования и возвращался на сайт повторно."
        )
        if reasons_text:
            return f"{base} Ключевые сигналы: {reasons_text}."
        return base

    if segment == "warm":
        base = (
            "Средний score, потому что посетитель вовлечён в изучение лагеря: "
            "просматривал программу, провёл заметное время на сайте, но не дошёл до явного шага бронирования."
        )
        if reasons_text:
            return f"{base} Основные сигналы: {reasons_text}."
        return base

    base = (
        "Низкий score, потому что поведение посетителя пока выглядит ознакомительным: "
        "мало просмотров, короткая сессия и нет сильных признаков интереса к бронированию."
    )
    if features.is_bounce or explanation.get("bounce_session"):
        return f"{base} Дополнительно: есть признаки bounce-сессии."
    if reasons_text:
        return f"{base} Наблюдаемые сигналы: {reasons_text}."
    return base


def build_explainable_fields(
    features: VisitorFeatures,
    explanation: dict[str, int],
    segment: str,
) -> dict[str, Any]:
    short_reason = pick_short_reason(features=features, explanation=explanation, segment=segment)
    human_explanation = build_human_explanation(features=features, explanation=explanation, segment=segment)
    recommended_action = recommendation_for_segment(segment)

    return {
        "short_reason": short_reason,
        "human_explanation": human_explanation,
        "recommended_action": recommended_action,
    }
