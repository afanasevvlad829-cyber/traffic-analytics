from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import time
from datetime import date, timedelta
from typing import Any

import requests

from src.settings import Settings

LOGREQUESTS_BASE = "https://api-metrika.yandex.net/management/v1/counter/{counter_id}/logrequests"
LOGREQUEST_BASE = "https://api-metrika.yandex.net/management/v1/counter/{counter_id}/logrequest"
STAT_BASE = "https://api-metrika.yandex.net/stat/v1/data"


def _headers(token: str) -> dict[str, str]:
    return {"Authorization": f"OAuth {token}"}


def _safe_json(resp: requests.Response) -> dict[str, Any]:
    try:
        return resp.json()
    except Exception:  # noqa: BLE001
        return {}


def _create_logs_request(token: str, counter_id: str, date1: str, date2: str) -> int:
    url = LOGREQUESTS_BASE.format(counter_id=counter_id)
    params = {
        "source": "visits",
        "fields": "ym:s:clientID,ym:s:visitID",
        "date1": date1,
        "date2": date2,
    }
    resp = requests.post(url, params=params, headers=_headers(token), timeout=60)
    resp.raise_for_status()
    payload = _safe_json(resp)
    return int((payload.get("log_request") or {}).get("request_id") or 0)


def _poll_logs_processed(token: str, counter_id: str, request_id: int, timeout_sec: int = 420) -> dict[str, Any]:
    url = LOGREQUEST_BASE.format(counter_id=counter_id) + f"/{request_id}"
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        resp = requests.get(url, headers=_headers(token), timeout=60)
        resp.raise_for_status()
        payload = _safe_json(resp)
        status = str((payload.get("log_request") or {}).get("status") or "").lower()
        if status == "processed":
            return payload
        if status in {"failed", "canceled", "cleaned_by_user", "cleaned_automatically"}:
            return payload
        time.sleep(3)
    return {"log_request": {"status": "timeout"}}


def _download_logs_part(token: str, counter_id: str, request_id: int, part_number: int) -> str:
    url = LOGREQUEST_BASE.format(counter_id=counter_id) + f"/{request_id}/part/{part_number}/download"
    resp = requests.get(url, headers=_headers(token), timeout=120)
    resp.raise_for_status()
    raw = resp.content
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return raw.decode("utf-8", errors="replace")


def _clean_logs_request(token: str, counter_id: str, request_id: int) -> None:
    url = LOGREQUEST_BASE.format(counter_id=counter_id) + f"/{request_id}/clean"
    try:
        requests.post(url, headers=_headers(token), timeout=30)
    except Exception:  # noqa: BLE001
        pass


def _collect_logs_client_ids(
    token: str,
    counter_id: str,
    date1: str,
    date2: str,
    client_limit: int,
) -> tuple[set[str], int, str]:
    request_id = 0
    try:
        request_id = _create_logs_request(token=token, counter_id=counter_id, date1=date1, date2=date2)
        payload = _poll_logs_processed(token=token, counter_id=counter_id, request_id=request_id)
        log_request = payload.get("log_request") or {}
        status = str(log_request.get("status") or "").lower()
        if status != "processed":
            return set(), 0, status

        parts = log_request.get("parts") or []
        client_ids: set[str] = set()
        rows_total = 0
        for part in parts:
            part_number = int((part or {}).get("part_number") or 0)
            if part_number <= 0:
                continue
            tsv = _download_logs_part(token=token, counter_id=counter_id, request_id=request_id, part_number=part_number)
            reader = csv.DictReader(io.StringIO(tsv), delimiter="\t")
            for row in reader:
                rows_total += 1
                client_id = str(row.get("ym:s:clientID") or row.get("ClientID") or "").strip()
                if client_id:
                    client_ids.add(client_id)
                if len(client_ids) >= client_limit:
                    return client_ids, rows_total, "processed"
        return client_ids, rows_total, "processed"
    finally:
        if request_id:
            _clean_logs_request(token=token, counter_id=counter_id, request_id=request_id)


def _fetch_reports_client_demography(
    token: str,
    counter_id: str,
    date1: str,
    date2: str,
    limit: int,
) -> tuple[list[dict[str, Any]], str | None]:
    params = {
        "ids": counter_id,
        "date1": date1,
        "date2": date2,
        "metrics": "ym:s:visits",
        "dimensions": "ym:s:clientID,ym:s:gender,ym:s:ageInterval",
        "accuracy": "full",
        "limit": str(limit),
    }
    resp = requests.get(STAT_BASE, params=params, headers=_headers(token), timeout=90)
    if resp.status_code >= 400:
        return [], f"{resp.status_code}: {resp.text[:200].replace(chr(10), ' ')}"
    payload = _safe_json(resp)
    return payload.get("data", []) or [], None


def _dim_name(item: dict[str, Any], idx: int) -> str:
    dimensions = item.get("dimensions") or []
    if idx >= len(dimensions):
        return ""
    return str((dimensions[idx] or {}).get("name") or "").strip()


def run_probe(days: int = 2, client_limit: int = 1500, sample_limit: int = 20) -> dict[str, Any]:
    token = (Settings.METRICA_TOKEN or "").strip()
    counter_id = str(Settings.METRICA_COUNTER_ID or "").strip()
    if not token or not counter_id:
        return {"ok": False, "error": "METRICA_TOKEN or METRICA_COUNTER_ID is missing"}

    safe_days = max(1, min(int(days or 2), 7))
    safe_client_limit = max(100, min(int(client_limit or 1500), 10000))
    safe_sample_limit = max(1, min(int(sample_limit or 20), 20))
    date2 = (date.today() - timedelta(days=1)).isoformat()
    date1 = (date.today() - timedelta(days=safe_days)).isoformat()

    logs_clients, logs_rows_total, logs_status = _collect_logs_client_ids(
        token=token,
        counter_id=counter_id,
        date1=date1,
        date2=date2,
        client_limit=safe_client_limit,
    )
    if logs_status != "processed":
        return {
            "ok": False,
            "date1": date1,
            "date2": date2,
            "error": f"logs status={logs_status}",
        }

    report_rows, report_err = _fetch_reports_client_demography(
        token=token,
        counter_id=counter_id,
        date1=date1,
        date2=date2,
        limit=100000,
    )
    if report_err:
        return {
            "ok": False,
            "date1": date1,
            "date2": date2,
            "logs_clients_count": len(logs_clients),
            "error": f"reports error: {report_err}",
        }

    # client_id -> set[(gender, age)]
    report_map: dict[str, set[tuple[str, str]]] = {}
    for item in report_rows:
        client_id = _dim_name(item, 0)
        if not client_id:
            continue
        gender = _dim_name(item, 1)
        age = _dim_name(item, 2)
        report_map.setdefault(client_id, set()).add((gender, age))

    matched = logs_clients.intersection(report_map.keys())
    with_gender = 0
    with_age = 0
    with_both = 0
    stable_demography = 0
    unstable_demography = 0
    sample_rows: list[dict[str, str]] = []

    for cid in sorted(matched):
        pairs = report_map.get(cid) or set()
        non_empty_gender = {g for g, _ in pairs if g}
        non_empty_age = {a for _, a in pairs if a}
        if non_empty_gender:
            with_gender += 1
        if non_empty_age:
            with_age += 1
        if non_empty_gender and non_empty_age:
            with_both += 1
        if len(non_empty_gender) <= 1 and len(non_empty_age) <= 1 and (non_empty_gender or non_empty_age):
            stable_demography += 1
        if len(non_empty_gender) > 1 or len(non_empty_age) > 1:
            unstable_demography += 1
        if len(sample_rows) < safe_sample_limit:
            sample_rows.append(
                {
                    "client_id": cid,
                    "pairs": ", ".join(sorted([f"{g or '-'}|{a or '-'}" for g, a in pairs]))[:180],
                    "gender_values": ", ".join(sorted(non_empty_gender))[:80],
                    "age_values": ", ".join(sorted(non_empty_age))[:80],
                }
            )

    logs_count = len(logs_clients)
    matched_count = len(matched)
    coverage_pct = round((matched_count / logs_count) * 100, 2) if logs_count else 0.0
    both_pct_of_matched = round((with_both / matched_count) * 100, 2) if matched_count else 0.0
    stable_pct_of_matched = round((stable_demography / matched_count) * 100, 2) if matched_count else 0.0

    return {
        "ok": True,
        "date1": date1,
        "date2": date2,
        "logs_rows_scanned": logs_rows_total,
        "logs_clients_count": logs_count,
        "reports_rows_count": len(report_rows),
        "matched_clients_count": matched_count,
        "match_coverage_pct": coverage_pct,
        "matched_with_gender_count": with_gender,
        "matched_with_age_count": with_age,
        "matched_with_both_count": with_both,
        "matched_with_both_pct": both_pct_of_matched,
        "stable_demography_count": stable_demography,
        "stable_demography_pct": stable_pct_of_matched,
        "unstable_demography_count": unstable_demography,
        "samples": sample_rows,
        "verdict": (
            "usable_for_soft_signal"
            if matched_count > 0 and both_pct_of_matched >= 30
            else "weak_for_visitor_level"
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Probe clientID matching between Metrica Logs and Reports for gender/age")
    parser.add_argument("--days", type=int, default=2, help="lookback days (1..7)")
    parser.add_argument("--client-limit", type=int, default=1500, help="max unique logs clientIDs to scan (100..10000)")
    parser.add_argument("--sample-limit", type=int, default=20, help="sample rows in output (1..20)")
    args = parser.parse_args()
    report = run_probe(days=args.days, client_limit=args.client_limit, sample_limit=args.sample_limit)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
