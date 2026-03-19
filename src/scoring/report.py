from __future__ import annotations

import html
import os
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv


ENV_PATH = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(ENV_PATH)


def _get_telegram_credentials() -> tuple[str, str]:
    token = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TG_TOKEN") or ""
    chat_id = os.getenv("CHAT_ID") or os.getenv("TG_CHAT") or ""
    return token.strip(), chat_id.strip()


def _build_auto_text(summary: dict[str, Any]) -> str:
    hot = int(summary.get("hot_count") or 0)
    warm = int(summary.get("warm_count") or 0)
    cold = int(summary.get("cold_count") or 0)

    if hot >= max(warm, cold):
        return "Усиливаем конверсию: для HOT запускаем срочный оффер и менеджерский follow-up."
    if warm >= max(hot, cold):
        return "Фокус на дожиме: прогреваем WARM сегмент через кейсы и ценностные офферы."
    return "Больше прогрева: COLD сегменту нужны доверие, контент и образовательные касания."


def _build_scoring_message(summary: dict[str, Any], top_visitors: list[dict[str, Any]]) -> str:
    hot = int(summary.get("hot_count") or 0)
    warm = int(summary.get("warm_count") or 0)
    cold = int(summary.get("cold_count") or 0)

    lines: list[str] = [
        "🔥 SCORING UPDATE",
        "",
        "<b>Сегменты:</b>",
        f"🟢 HOT: {hot}",
        f"🟠 WARM: {warm}",
        f"⚪ COLD: {cold}",
        "",
        "<b>TOP HOT:</b>",
    ]

    if top_visitors:
        for visitor in top_visitors[:5]:
            visitor_id = html.escape(str(visitor.get("visitor_id") or "-"))
            score = float(visitor.get("normalized_score") or 0)
            source = html.escape(
                str(visitor.get("traffic_source") or visitor.get("utm_source") or visitor.get("utm_medium") or "-")
            )
            lines.append(f"- {visitor_id} | {score:.2f} | {source}")
    else:
        lines.append("- нет HOT посетителей")

    lines.extend(
        [
            "",
            "<b>Рекомендация:</b>",
            html.escape(_build_auto_text(summary)),
        ]
    )

    return "\n".join(lines)


def send_scoring_report(summary: dict[str, Any], top_visitors: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Отправляет HTML-отчет Scoring v1 в Telegram.

    Fail-safe: возвращает ok=False и ошибку, но не кидает исключение наружу.
    """

    token, chat_id = _get_telegram_credentials()
    if not token or not chat_id:
        return {
            "ok": False,
            "sent": False,
            "error": "telegram credentials are missing (TELEGRAM_BOT_TOKEN/CHAT_ID)",
        }

    text = _build_scoring_message(summary=summary, top_visitors=top_visitors)

    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": text[:4000],
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=30,
        )
        resp.raise_for_status()
        payload = resp.json() if resp.text else {}
        if payload.get("ok") is False:
            return {"ok": False, "sent": False, "error": str(payload)}
        return {"ok": True, "sent": True, "message": "scoring report sent"}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "sent": False, "error": str(exc)}
