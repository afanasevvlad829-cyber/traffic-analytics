from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import requests

from src.settings import Settings

DIRECT_API_ROOT = "https://api.direct.yandex.com/json/v5"
METRICA_GOALS_URL = "https://api-metrika.yandex.net/management/v1/counter/{counter_id}/goals"


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
        raise RuntimeError("DIRECT token/login is missing")

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
        raise RuntimeError(f"{service}.{method} http={response.status_code}: {response.text[:400]}")
    data = response.json()
    if "error" in data:
        raise RuntimeError(f"{service}.{method} error: {data['error']}")
    return data.get("result") or {}


def _load_goal_map_env() -> dict[str, int]:
    raw = str(os.getenv("SCORING_DIRECT_GOAL_MAP_JSON", "")).strip()
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            return {}
        out: dict[str, int] = {}
        for key, value in payload.items():
            try:
                out[str(key).strip().lower()] = int(value)
            except Exception:  # noqa: BLE001
                continue
        return out
    except Exception:  # noqa: BLE001
        return {}


def _fetch_metrica_goals() -> list[dict[str, Any]]:
    token = str(Settings.METRICA_TOKEN or "").strip()
    counter_id = str(Settings.METRICA_COUNTER_ID or "").strip()
    if not token or not counter_id:
        return []
    url = METRICA_GOALS_URL.format(counter_id=counter_id)
    headers = {"Authorization": f"OAuth {token}"}
    response = requests.get(url, headers=headers, timeout=60)
    if response.status_code != 200:
        return []
    payload = response.json()
    goals = payload.get("goals") or []
    return [g for g in goals if isinstance(g, dict)]


def _pick_goal_id(segment: str, goals: list[dict[str, Any]], goal_map: dict[str, int]) -> int | None:
    seg = str(segment or "").strip().lower()
    if seg in goal_map:
        return int(goal_map[seg])
    if "default" in goal_map:
        return int(goal_map["default"])

    scored: list[tuple[int, int]] = []
    for g in goals:
        try:
            goal_id = int(g.get("id"))
        except Exception:  # noqa: BLE001
            continue
        name = str(g.get("name") or "").strip().lower()
        gtype = str(g.get("type") or "").strip().lower()
        text = f"{name} {gtype}"
        weight = 0
        if seg == "hot":
            if any(k in text for k in ("заяв", "lead", "form", "book", "брон", "запис", "call", "phone")):
                weight += 8
            if any(k in text for k in ("visit", "просмотр")):
                weight += 1
        elif seg == "warm":
            if any(k in text for k in ("price", "цена", "program", "програм", "about", "contact")):
                weight += 7
            if any(k in text for k in ("scroll", "engage")):
                weight += 3
        else:
            if any(k in text for k in ("visit", "посещение", "session", "landing")):
                weight += 6
            if weight == 0:
                weight = 1
        if weight > 0:
            scored.append((weight, goal_id))
    if scored:
        scored.sort(reverse=True)
        return scored[0][1]
    try:
        return int(goals[0].get("id")) if goals else None
    except Exception:  # noqa: BLE001
        return None


def _segment_priority(segment: str) -> str:
    seg = str(segment or "").strip().lower()
    if seg == "hot":
        return "HIGH"
    if seg == "warm":
        return "NORMAL"
    return "LOW"


def _create_adgroup(campaign_id: int, name: str, region_ids: list[int]) -> int:
    result = _direct_request(
        "adgroups",
        "add",
        {
            "AdGroups": [
                {
                    "Name": name[:255],
                    "CampaignId": int(campaign_id),
                    "RegionIds": [int(x) for x in region_ids] or [0],
                }
            ]
        },
    )
    add_results = result.get("AddResults") or []
    if not add_results:
        raise RuntimeError("adgroups.add empty result")
    first = add_results[0] or {}
    if first.get("Errors"):
        raise RuntimeError(str(first.get("Errors")))
    return int(first.get("Id"))


def _adgroup_exists(ad_group_id: int) -> bool:
    try:
        result = _direct_request(
            "adgroups",
            "get",
            {
                "SelectionCriteria": {"Ids": [int(ad_group_id)]},
                "FieldNames": ["Id", "CampaignId"],
            },
        )
        rows = result.get("AdGroups") or []
        return len(rows) > 0
    except Exception:  # noqa: BLE001
        return False


def _create_retargeting_list(name: str, goal_id: int, window_days: int) -> int:
    result = _direct_request(
        "retargetinglists",
        "add",
        {
            "RetargetingLists": [
                {
                    "Type": "RETARGETING",
                    "Name": name[:250],
                    "Description": f"scoring auto bootstrap, goal={goal_id}",
                    "Rules": [
                        {
                            "Operator": "ANY",
                            "Arguments": [
                                {
                                    "ExternalId": int(goal_id),
                                    "MembershipLifeSpan": max(1, min(int(window_days or 30), 540)),
                                }
                            ],
                        }
                    ],
                }
            ]
        },
    )
    add_results = result.get("AddResults") or []
    if not add_results:
        raise RuntimeError("retargetinglists.add empty result")
    first = add_results[0] or {}
    if first.get("Errors"):
        raise RuntimeError(str(first.get("Errors")))
    return int(first.get("Id"))


def _retargeting_list_exists(retargeting_list_id: int) -> bool:
    try:
        result = _direct_request(
            "retargetinglists",
            "get",
            {
                "SelectionCriteria": {"Ids": [int(retargeting_list_id)]},
                "FieldNames": ["Id", "Name", "Type"],
            },
        )
        rows = result.get("RetargetingLists") or []
        return len(rows) > 0
    except Exception:  # noqa: BLE001
        return False


def _find_retargeting_list_by_goal(goal_id: int) -> int | None:
    offset = 0
    limit = 1000
    for _ in range(10):
        try:
            result = _direct_request(
                "retargetinglists",
                "get",
                {
                    "SelectionCriteria": {"Types": ["RETARGETING"]},
                    "FieldNames": ["Id", "Rules"],
                    "Page": {"Limit": limit, "Offset": offset},
                },
            )
        except Exception:  # noqa: BLE001
            return None
        rows = result.get("RetargetingLists") or []
        if not rows:
            return None
        for row in rows:
            for rule in (row.get("Rules") or []):
                for arg in (rule.get("Arguments") or []):
                    try:
                        if int(arg.get("ExternalId")) == int(goal_id):
                            return int(row.get("Id"))
                    except Exception:  # noqa: BLE001
                        continue
        limited_by = result.get("LimitedBy")
        if limited_by is None:
            return None
        try:
            if int(limited_by) < (offset + limit):
                return None
        except Exception:  # noqa: BLE001
            return None
        offset += limit
    return None


def _attach_audience_target(ad_group_id: int, retargeting_list_id: int, priority: str) -> dict[str, Any]:
    payload = {
        "AudienceTargets": [
            {
                "AdGroupId": int(ad_group_id),
                "RetargetingListId": int(retargeting_list_id),
                "StrategyPriority": str(priority).upper(),
            }
        ]
    }
    result = _direct_request("audiencetargets", "add", payload)
    add_results = result.get("AddResults") or []
    first = add_results[0] if add_results else {}
    return {"result": result, "first": first or {}}


def _read_env_mapping() -> dict[str, dict[str, Any]]:
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


def _write_env_values(env_path: str, updates: dict[str, str]) -> dict[str, Any]:
    path = Path(env_path).expanduser()
    if not path.exists():
        path.write_text("", encoding="utf-8")
    lines = path.read_text(encoding="utf-8").splitlines()
    keys = set(updates.keys())
    out: list[str] = []
    replaced: set[str] = set()
    for line in lines:
        if not line or line.lstrip().startswith("#") or "=" not in line:
            out.append(line)
            continue
        key = line.split("=", 1)[0].strip()
        if key in keys:
            out.append(f"{key}={updates[key]}")
            replaced.add(key)
        else:
            out.append(line)
    for key in keys - replaced:
        out.append(f"{key}={updates[key]}")
    path.write_text("\n".join(out) + "\n", encoding="utf-8")
    return {"path": str(path), "updated_keys": sorted(keys)}


def bootstrap_direct_entities(
    *,
    cohorts: list[dict[str, Any]],
    campaign_id: int,
    region_ids: list[int] | None = None,
    apply: bool = False,
    env_path: str = "/home/kv145/traffic-analytics/.env",
    enable_auto_sync: bool = True,
) -> dict[str, Any]:
    mapping = _read_env_mapping()
    goals = _fetch_metrica_goals()
    goal_map = _load_goal_map_env()
    region_ids = region_ids or [0]
    goal_to_list: dict[int, int] = {}
    for mv in mapping.values():
        try:
            gid = int(mv.get("goal_id"))
            rid = int(mv.get("retargeting_list_id"))
        except Exception:  # noqa: BLE001
            continue
        if _retargeting_list_exists(rid):
            goal_to_list[gid] = rid

    created_adgroups = 0
    created_lists = 0
    attached_targets = 0
    skipped = 0
    errors = 0
    items: list[dict[str, Any]] = []

    for c in cohorts:
        name = str(c.get("cohort_name") or "").strip()
        if not name:
            continue
        audience_size = int(c.get("audience_size") or c.get("visitors") or 0)
        if audience_size <= 0:
            skipped += 1
            items.append({"cohort_name": name, "status": "skipped", "reason": "empty cohort", "audience_size": audience_size})
            continue

        segment = str(c.get("segment") or "").strip().lower()
        window_days = int(c.get("window_days") or 30)
        existing = mapping.get(name) or {}
        ad_group_id = existing.get("ad_group_id")
        retargeting_list_id = existing.get("retargeting_list_id")
        priority = str(existing.get("strategy_priority") or _segment_priority(segment)).upper()

        if ad_group_id and not _adgroup_exists(int(ad_group_id)):
            ad_group_id = None
        if retargeting_list_id and not _retargeting_list_exists(int(retargeting_list_id)):
            retargeting_list_id = None

        goal_id = _pick_goal_id(segment=segment, goals=goals, goal_map=goal_map)
        if not goal_id:
            skipped += 1
            items.append({"cohort_name": name, "status": "skipped", "reason": "no metrica goal", "audience_size": audience_size})
            continue

        if not apply:
            items.append(
                {
                    "cohort_name": name,
                    "status": "dry_run",
                    "audience_size": audience_size,
                    "campaign_id": int(campaign_id),
                    "goal_id": int(goal_id),
                    "would_create_ad_group": not bool(ad_group_id),
                    "would_create_retargeting_list": not bool(retargeting_list_id),
                }
            )
            continue

        try:
            if not ad_group_id:
                ad_group_id = _create_adgroup(
                    campaign_id=int(campaign_id),
                    name=f"SCORING {name}".replace("_", " "),
                    region_ids=region_ids,
                )
                created_adgroups += 1

            if not retargeting_list_id:
                if int(goal_id) in goal_to_list and _retargeting_list_exists(int(goal_to_list[int(goal_id)])):
                    retargeting_list_id = int(goal_to_list[int(goal_id)])
                else:
                    try:
                        retargeting_list_id = _create_retargeting_list(
                            name=f"SCORING {name}",
                            goal_id=int(goal_id),
                            window_days=window_days,
                        )
                        created_lists += 1
                    except Exception as create_exc:  # noqa: BLE001
                        msg = str(create_exc).lower()
                        if "уже существует" in msg or "already exists" in msg or "неконсистентное состояние объекта" in msg:
                            found = _find_retargeting_list_by_goal(int(goal_id))
                            if not found:
                                raise
                            retargeting_list_id = int(found)
                        else:
                            raise
                goal_to_list[int(goal_id)] = int(retargeting_list_id)

            attach = _attach_audience_target(
                ad_group_id=int(ad_group_id),
                retargeting_list_id=int(retargeting_list_id),
                priority=priority,
            )
            first = attach.get("first") or {}
            if first.get("Errors"):
                msg = str(first.get("Errors"))
                if "already" in msg.lower() or "уже" in msg.lower():
                    pass
                else:
                    raise RuntimeError(msg)
            else:
                attached_targets += 1

            mapping[name] = {
                "ad_group_id": int(ad_group_id),
                "retargeting_list_id": int(retargeting_list_id),
                "strategy_priority": priority,
                "goal_id": int(goal_id),
            }
            items.append(
                {
                    "cohort_name": name,
                    "status": "applied",
                    "audience_size": audience_size,
                    "goal_id": int(goal_id),
                    "ad_group_id": int(ad_group_id),
                    "retargeting_list_id": int(retargeting_list_id),
                    "strategy_priority": priority,
                }
            )
        except Exception as exc:  # noqa: BLE001
            errors += 1
            items.append(
                {
                    "cohort_name": name,
                    "status": "error",
                    "audience_size": audience_size,
                    "goal_id": int(goal_id),
                    "error": str(exc),
                }
            )

    env_write = None
    if apply:
        updates = {
            "SCORING_DIRECT_RETARGET_MAP_JSON": "'" + json.dumps(mapping, ensure_ascii=False, separators=(",", ":")) + "'",
        }
        if enable_auto_sync:
            updates["SCORING_DIRECT_SYNC_ENABLED"] = "1"
        env_write = _write_env_values(env_path=env_path, updates=updates)

    return {
        "ok": errors == 0,
        "apply": bool(apply),
        "campaign_id": int(campaign_id),
        "goals_count": len(goals),
        "mapping_count": len(mapping),
        "created_adgroups": created_adgroups,
        "created_retargeting_lists": created_lists,
        "attached_audience_targets": attached_targets,
        "skipped": skipped,
        "errors": errors,
        "env_write": env_write,
        "items": items,
    }
