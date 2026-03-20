from __future__ import annotations

import json
import os
from typing import Any

import requests

from src.settings import Settings

DIRECT_API_ROOT = "https://api.direct.yandex.com/json/v5"


def _parse_bool_env(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "")).strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _direct_credentials() -> tuple[str, str]:
    token = (
        str(Settings.DIRECT_TOKEN or "").strip()
        or str(os.getenv("YANDEX_DIRECT_TOKEN", "")).strip()
        or str(os.getenv("DIRECT_API_TOKEN", "")).strip()
    )
    login = (
        str(Settings.DIRECT_CLIENT_LOGIN or "").strip()
        or str(os.getenv("YANDEX_DIRECT_LOGIN", "")).strip()
    )
    return token, login


def _direct_request(service: str, method: str, params: dict[str, Any]) -> dict[str, Any]:
    token, login = _direct_credentials()
    if not token or not login:
        raise RuntimeError("DIRECT_TOKEN / DIRECT_CLIENT_LOGIN is missing")

    url = f"{DIRECT_API_ROOT}/{service}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Client-Login": login,
        "Accept-Language": "ru",
        "Content-Type": "application/json; charset=utf-8",
    }
    payload = {"method": method, "params": params}
    response = requests.post(url, headers=headers, json=payload, timeout=90)
    if response.status_code != 200:
        raise RuntimeError(f"Direct API HTTP {response.status_code}: {response.text[:400]}")
    data = response.json()
    if "error" in data:
        raise RuntimeError(str(data.get("error")))
    return data.get("result") or {}


def _cohort_map_from_env() -> dict[str, dict[str, Any]]:
    raw = (
        str(os.getenv("SCORING_DIRECT_RETARGET_MAP_JSON", "")).strip()
        or str(os.getenv("SCORING_DIRECT_RETARGET_MAP", "")).strip()
    )
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            return {str(k): (v if isinstance(v, dict) else {}) for k, v in data.items()}
    except Exception:  # noqa: BLE001
        return {}
    return {}


def sync_audience_targets(*, cohorts: list[dict[str, Any]], dry_run: bool = True) -> dict[str, Any]:
    mapping = _cohort_map_from_env()
    auto_enabled = _parse_bool_env("SCORING_DIRECT_SYNC_ENABLED", default=False)
    execute = (not dry_run) and auto_enabled

    attempted = 0
    applied = 0
    skipped = 0
    errors = 0
    items: list[dict[str, Any]] = []

    for cohort in cohorts:
        name = str(cohort.get("cohort_name") or "").strip()
        visitors = int(cohort.get("audience_size") or cohort.get("visitors") or 0)
        if not name:
            continue
        if visitors <= 0:
            skipped += 1
            items.append(
                {
                    "cohort_name": name,
                    "status": "skipped",
                    "reason": "empty cohort",
                    "visitors": visitors,
                }
            )
            continue

        conf = mapping.get(name) or {}
        ad_group_id = conf.get("ad_group_id")
        retargeting_list_id = conf.get("retargeting_list_id")
        if not ad_group_id or not retargeting_list_id:
            skipped += 1
            items.append(
                {
                    "cohort_name": name,
                    "status": "skipped",
                    "reason": "mapping is missing",
                    "visitors": visitors,
                }
            )
            continue

        attempted += 1
        payload: dict[str, Any] = {
            "AdGroupId": int(ad_group_id),
            "RetargetingListId": int(retargeting_list_id),
        }
        priority = str(conf.get("strategy_priority") or "").strip().upper()
        if priority in {"LOW", "NORMAL", "HIGH"}:
            payload["StrategyPriority"] = priority
        if conf.get("context_bid") is not None:
            payload["ContextBid"] = int(conf["context_bid"])

        if not execute:
            items.append(
                {
                    "cohort_name": name,
                    "status": "dry_run",
                    "visitors": visitors,
                    "payload": payload,
                }
            )
            continue

        try:
            result = _direct_request("audiencetargets", "add", {"AudienceTargets": [payload]})
            applied += 1
            items.append(
                {
                    "cohort_name": name,
                    "status": "applied",
                    "visitors": visitors,
                    "payload": payload,
                    "result": result,
                }
            )
        except Exception as exc:  # noqa: BLE001
            errors += 1
            items.append(
                {
                    "cohort_name": name,
                    "status": "error",
                    "visitors": visitors,
                    "payload": payload,
                    "error": str(exc),
                }
            )

    return {
        "ok": errors == 0,
        "dry_run": not execute,
        "auto_sync_enabled": auto_enabled,
        "mapping_count": len(mapping),
        "attempted": attempted,
        "applied": applied,
        "skipped": skipped,
        "errors": errors,
        "items": items,
    }
