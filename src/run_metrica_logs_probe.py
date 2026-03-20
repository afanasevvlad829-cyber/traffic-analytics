from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import time
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import requests

from src.settings import Settings

MGMT_BASE = "https://api-metrika.yandex.net/management/v1/counter/{counter_id}/logrequests"
REQUEST_BASE = "https://api-metrika.yandex.net/management/v1/counter/{counter_id}/logrequest"


@dataclass
class ProbeResult:
    source: str
    request_ok: bool
    rows_fetched: int
    columns_present: list[str]
    has_client_id: bool = False
    has_visit_id: bool = False
    has_referer: bool = False
    has_source_fields: bool = False
    has_utm_fields: bool = False
    has_url: bool = False
    has_traffic_source_id: bool = False
    has_adv_engine_id: bool = False
    samples: list[dict[str, Any]] | None = None
    error: str | None = None


def _headers(token: str) -> dict[str, str]:
    return {"Authorization": f"OAuth {token}"}


def _safe_json(resp: requests.Response) -> dict[str, Any]:
    try:
        return resp.json()
    except Exception:  # noqa: BLE001
        return {}


def _candidate_field_sets(source: str) -> list[list[str]]:
    if source == "visits":
        return [
            [
                "ym:s:clientID",
                "ym:s:visitID",
                "ym:s:lastTrafficSource",
                "ym:s:lastAdvEngine",
                "ym:s:lastUTMSource",
                "ym:s:lastUTMMedium",
                "ym:s:startURL",
                "ym:s:referer",
            ],
            ["ym:s:clientID", "ym:s:visitID", "ym:s:startURL", "ym:s:referer"],
            ["ym:s:clientID", "ym:s:visitID", "ym:s:startURL"],
        ]
    return [
        ["ym:pv:URL", "ym:pv:visitID", "ym:pv:referer"],
        ["ym:pv:URL", "ym:pv:visitID"],
    ]


def _create_logrequest(token: str, counter_id: str, source: str, fields: list[str], date1: str, date2: str) -> int:
    url = MGMT_BASE.format(counter_id=counter_id)
    payload = {"source": source, "fields": ",".join(fields), "date1": date1, "date2": date2}
    resp = requests.post(url, params=payload, headers=_headers(token), timeout=60)
    resp.raise_for_status()
    data = _safe_json(resp)
    return int((data.get("log_request") or {}).get("request_id") or 0)


def _create_logrequest_with_fallback(
    token: str, counter_id: str, source: str, date1: str, date2: str
) -> tuple[int, list[str]]:
    errors: list[str] = []
    for fields in _candidate_field_sets(source):
        url = MGMT_BASE.format(counter_id=counter_id)
        payload = {"source": source, "fields": ",".join(fields), "date1": date1, "date2": date2}
        resp = requests.post(url, params=payload, headers=_headers(token), timeout=60)
        if resp.status_code < 300:
            data = _safe_json(resp)
            request_id = int((data.get("log_request") or {}).get("request_id") or 0)
            if request_id:
                return request_id, fields
        errors.append(f"{resp.status_code}:{resp.text[:180].replace(chr(10), ' ')}")
    raise RuntimeError(f"logrequest create failed for source={source}; errors={errors}")


def _poll_logrequest(token: str, counter_id: str, request_id: int, timeout_sec: int = 180) -> dict[str, Any]:
    url = REQUEST_BASE.format(counter_id=counter_id) + f"/{request_id}"
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        resp = requests.get(url, headers=_headers(token), timeout=60)
        resp.raise_for_status()
        data = _safe_json(resp)
        status = str((data.get("log_request") or {}).get("status") or "").lower()
        if status in {"processed", "created", "cleaned_by_user", "cleaned_automatically"}:
            return data
        if status in {"failed", "canceled"}:
            return data
        time.sleep(3)
    return {"log_request": {"status": "timeout"}}


def _download_first_part(token: str, counter_id: str, request_id: int, part_number: int) -> str:
    url = REQUEST_BASE.format(counter_id=counter_id) + f"/{request_id}/part/{part_number}/download"
    resp = requests.get(url, headers=_headers(token), timeout=120)
    resp.raise_for_status()
    raw = resp.content
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return raw.decode("utf-8", errors="replace")


def _delete_request(token: str, counter_id: str, request_id: int) -> None:
    url = REQUEST_BASE.format(counter_id=counter_id) + f"/{request_id}/clean"
    try:
        requests.post(url, headers=_headers(token), timeout=30)
    except Exception:  # noqa: BLE001
        pass


def _parse_tsv_sample(tsv_text: str, sample_limit: int) -> tuple[list[str], list[dict[str, str]], int]:
    if not tsv_text.strip():
        return [], [], 0
    reader = csv.DictReader(io.StringIO(tsv_text), delimiter="\t")
    columns = list(reader.fieldnames or [])
    samples: list[dict[str, str]] = []
    total = 0
    for row in reader:
        total += 1
        if len(samples) < sample_limit:
            samples.append({k: str(v or "") for k, v in row.items()})
    return columns, samples, total


def _has_any(columns: list[str], keys: list[str]) -> bool:
    cols = {c.lower() for c in columns}
    for key in keys:
        if key.lower() in cols:
            return True
    return False


def _probe_source(token: str, counter_id: str, source: str, date1: str, date2: str, sample_limit: int) -> ProbeResult:
    request_id = 0
    try:
        request_id, _ = _create_logrequest_with_fallback(
            token=token, counter_id=counter_id, source=source, date1=date1, date2=date2
        )
        meta = _poll_logrequest(token=token, counter_id=counter_id, request_id=request_id)
        log_request = meta.get("log_request") or {}
        status = str(log_request.get("status") or "").lower()
        if status != "processed":
            return ProbeResult(source=source, request_ok=False, rows_fetched=0, columns_present=[], error=f"status={status}")

        parts = log_request.get("parts") or []
        if not parts:
            return ProbeResult(source=source, request_ok=True, rows_fetched=0, columns_present=[], samples=[])
        part_number = int((parts[0] or {}).get("part_number") or 0)
        tsv_text = _download_first_part(token=token, counter_id=counter_id, request_id=request_id, part_number=part_number)
        columns, samples, rows_fetched = _parse_tsv_sample(tsv_text=tsv_text, sample_limit=sample_limit)

        if source == "visits":
            return ProbeResult(
                source=source,
                request_ok=True,
                rows_fetched=rows_fetched,
                columns_present=columns,
                has_client_id=_has_any(columns, ["ClientID", "ym:s:ClientID"]),
                has_visit_id=_has_any(columns, ["VisitID", "ym:s:VisitID"]),
                has_referer=_has_any(columns, ["Referer", "ym:s:Referer"]),
                has_source_fields=_has_any(columns, ["TrafficSourceID", "AdvEngineID", "LastTrafficSource", "lastTrafficSource"]),
                has_utm_fields=_has_any(columns, ["UTMSource", "UTMMedium", "UTMCampaign"]),
                samples=samples,
            )
        return ProbeResult(
            source=source,
            request_ok=True,
            rows_fetched=rows_fetched,
            columns_present=columns,
            has_url=_has_any(columns, ["URL", "ym:pv:URL"]),
            has_referer=_has_any(columns, ["Referer", "ym:pv:Referer"]),
            has_traffic_source_id=_has_any(columns, ["TrafficSourceID", "ym:pv:TrafficSourceID"]),
            has_adv_engine_id=_has_any(columns, ["AdvEngineID", "ym:pv:AdvEngineID"]),
            has_utm_fields=_has_any(columns, ["UTMSource", "UTMMedium", "UTMCampaign"]),
            samples=samples,
        )
    except Exception as exc:  # noqa: BLE001
        return ProbeResult(source=source, request_ok=False, rows_fetched=0, columns_present=[], error=str(exc))
    finally:
        if request_id:
            _delete_request(token=token, counter_id=counter_id, request_id=request_id)


def _verdict(sessions: ProbeResult, hits: ProbeResult) -> dict[str, str]:
    sessions_ok = sessions.request_ok and sessions.rows_fetched > 0 and sessions.has_client_id and sessions.has_visit_id
    hits_ok = hits.request_ok and hits.rows_fetched > 0 and (hits.has_traffic_source_id or hits.has_adv_engine_id)
    if sessions_ok and hits_ok:
        return {
            "logs_api_suitable": "yes",
            "best_dataset": "both",
            "reason": "sessions дают visitor/session ключи, hits дают URL/referrer/traffic IDs",
        }
    if sessions_ok:
        return {
            "logs_api_suitable": "yes",
            "best_dataset": "sessions",
            "reason": "достаточно для visitor attribution, hits можно подключить позже",
        }
    if hits_ok:
        return {
            "logs_api_suitable": "yes",
            "best_dataset": "hits",
            "reason": "hits содержат URL/referrer/source IDs, sessions недоступны/пусты",
        }
    return {
        "logs_api_suitable": "no",
        "best_dataset": "none",
        "reason": "нет достаточного набора полей/строк в probe",
    }


def run_probe(days: int, sample_limit: int) -> dict[str, Any]:
    token = (Settings.METRICA_TOKEN or "").strip()
    counter_id = (Settings.METRICA_COUNTER_ID or "").strip()
    if not token or not counter_id:
        return {
            "ok": False,
            "error": "METRICA_TOKEN or METRICA_COUNTER_ID is missing",
            "sessions": {},
            "hits": {},
            "samples": {"sessions": [], "hits": []},
            "verdict": {"logs_api_suitable": "no", "best_dataset": "none", "reason": "missing credentials"},
        }

    safe_days = max(1, min(int(days or 2), 3))
    safe_sample = max(1, min(int(sample_limit or 10), 10))
    date2 = (date.today() - timedelta(days=1)).isoformat()
    date1 = (date.today() - timedelta(days=safe_days)).isoformat()

    sessions = _probe_source(token=token, counter_id=counter_id, source="visits", date1=date1, date2=date2, sample_limit=safe_sample)
    hits = _probe_source(token=token, counter_id=counter_id, source="hits", date1=date1, date2=date2, sample_limit=safe_sample)
    verdict = _verdict(sessions=sessions, hits=hits)

    return {
        "ok": True,
        "date1": date1,
        "date2": date2,
        "counter_id": counter_id,
        "sessions": {
            "request_ok": sessions.request_ok,
            "rows_fetched": sessions.rows_fetched,
            "columns_present": sessions.columns_present,
            "has_client_id": sessions.has_client_id,
            "has_visit_id": sessions.has_visit_id,
            "has_referer": sessions.has_referer,
            "has_source_fields": sessions.has_source_fields,
            "has_utm_fields": sessions.has_utm_fields,
            "error": sessions.error,
        },
        "hits": {
            "request_ok": hits.request_ok,
            "rows_fetched": hits.rows_fetched,
            "columns_present": hits.columns_present,
            "has_url": hits.has_url,
            "has_referer": hits.has_referer,
            "has_traffic_source_id": hits.has_traffic_source_id,
            "has_adv_engine_id": hits.has_adv_engine_id,
            "has_utm_fields": hits.has_utm_fields,
            "error": hits.error,
        },
        "samples": {
            "sessions": (sessions.samples or [])[:safe_sample],
            "hits": (hits.samples or [])[:safe_sample],
        },
        "verdict": verdict,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Yandex Metrica Logs API smoke probe (read-only)")
    parser.add_argument("--days", type=int, default=2, help="lookback days (1..3)")
    parser.add_argument("--sample-limit", type=int, default=10, help="max sample rows per source (1..10)")
    args = parser.parse_args()
    report = run_probe(days=args.days, sample_limit=args.sample_limit)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
