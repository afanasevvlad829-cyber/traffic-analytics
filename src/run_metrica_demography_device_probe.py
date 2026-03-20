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


def _headers(token: str) -> dict[str, str]:
    return {"Authorization": f"OAuth {token}"}


def _safe_json(resp: requests.Response) -> dict[str, Any]:
    try:
        return resp.json()
    except Exception:  # noqa: BLE001
        return {}


def _create_request(token: str, counter_id: str, source: str, fields: list[str], date1: str, date2: str) -> tuple[int, str | None]:
    url = LOGREQUESTS_BASE.format(counter_id=counter_id)
    params = {"source": source, "fields": ",".join(fields), "date1": date1, "date2": date2}
    resp = requests.post(url, params=params, headers=_headers(token), timeout=60)
    if resp.status_code >= 400:
        return 0, f"{resp.status_code}: {resp.text[:180].replace(chr(10), ' ')}"
    payload = _safe_json(resp)
    request_id = int((payload.get("log_request") or {}).get("request_id") or 0)
    return request_id, None


def _poll_processed(token: str, counter_id: str, request_id: int, timeout_sec: int = 240) -> tuple[dict[str, Any], str]:
    url = LOGREQUEST_BASE.format(counter_id=counter_id) + f"/{request_id}"
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        resp = requests.get(url, headers=_headers(token), timeout=60)
        resp.raise_for_status()
        payload = _safe_json(resp)
        status = str((payload.get("log_request") or {}).get("status") or "").lower()
        if status == "processed":
            return payload, status
        if status in {"failed", "canceled", "cleaned_by_user", "cleaned_automatically"}:
            return payload, status
        time.sleep(3)
    return {"log_request": {"status": "timeout"}}, "timeout"


def _download_first_part(token: str, counter_id: str, request_id: int, part_number: int) -> str:
    url = LOGREQUEST_BASE.format(counter_id=counter_id) + f"/{request_id}/part/{part_number}/download"
    resp = requests.get(url, headers=_headers(token), timeout=120)
    resp.raise_for_status()
    raw = resp.content
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return raw.decode("utf-8", errors="replace")


def _clean_request(token: str, counter_id: str, request_id: int) -> None:
    url = LOGREQUEST_BASE.format(counter_id=counter_id) + f"/{request_id}/clean"
    try:
        requests.post(url, headers=_headers(token), timeout=30)
    except Exception:  # noqa: BLE001
        pass


def _parse_rows(tsv_text: str, sample_limit: int) -> tuple[list[str], list[dict[str, str]], int]:
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


def _sample_values(samples: list[dict[str, str]], field: str, max_values: int = 3) -> list[str]:
    values: list[str] = []
    for row in samples:
        value = str(row.get(field) or "").strip()
        if not value:
            continue
        if value not in values:
            values.append(value)
        if len(values) >= max_values:
            break
    return values


def run_probe(days: int = 2, sample_limit: int = 10) -> dict[str, Any]:
    token = (Settings.METRICA_TOKEN or "").strip()
    counter_id = (Settings.METRICA_COUNTER_ID or "").strip()
    if not token or not counter_id:
        return {"ok": False, "error": "METRICA_TOKEN or METRICA_COUNTER_ID is missing"}

    safe_days = max(1, min(int(days or 2), 3))
    safe_sample = max(1, min(int(sample_limit or 10), 10))
    date2 = (date.today() - timedelta(days=1)).isoformat()
    date1 = (date.today() - timedelta(days=safe_days)).isoformat()

    source = "visits"
    base_fields = ["ym:s:clientID", "ym:s:visitID"]
    candidates = [
        "ym:s:gender",
        "ym:s:ageInterval",
        "ym:s:deviceCategory",
        "ym:s:mobilePhone",
        "ym:s:mobilePhoneModel",
        "ym:s:operatingSystemRoot",
        "ym:s:browser",
        "ym:s:regionCountry",
        "ym:s:regionCity",
    ]

    results: list[dict[str, Any]] = []
    available: list[str] = []

    for field in candidates:
        request_id = 0
        try:
            request_id, err = _create_request(
                token=token,
                counter_id=counter_id,
                source=source,
                fields=base_fields + [field],
                date1=date1,
                date2=date2,
            )
            ok = request_id > 0 and not err
            results.append({"field": field, "available": ok, "error": err})
            if ok:
                available.append(field)
        finally:
            if request_id:
                _clean_request(token=token, counter_id=counter_id, request_id=request_id)

    combined_fields = base_fields + available
    combined_request_id = 0
    samples: list[dict[str, str]] = []
    rows_fetched = 0
    combined_status = "not_requested"
    combined_error = ""

    if combined_fields:
        try:
            combined_request_id, err = _create_request(
                token=token,
                counter_id=counter_id,
                source=source,
                fields=combined_fields,
                date1=date1,
                date2=date2,
            )
            if err or not combined_request_id:
                combined_status = "create_failed"
                combined_error = err or "unknown create error"
            else:
                meta, status = _poll_processed(token=token, counter_id=counter_id, request_id=combined_request_id)
                combined_status = status
                if status == "processed":
                    parts = (meta.get("log_request") or {}).get("parts") or []
                    if parts:
                        part_number = int((parts[0] or {}).get("part_number") or 0)
                        raw_tsv = _download_first_part(
                            token=token, counter_id=counter_id, request_id=combined_request_id, part_number=part_number
                        )
                        _, samples, rows_fetched = _parse_rows(raw_tsv, sample_limit=safe_sample)
                else:
                    combined_error = f"status={status}"
        finally:
            if combined_request_id:
                _clean_request(token=token, counter_id=counter_id, request_id=combined_request_id)

    sample_by_field = {field: _sample_values(samples, field) for field in available}

    return {
        "ok": True,
        "date1": date1,
        "date2": date2,
        "source": source,
        "base_fields": base_fields,
        "candidate_results": results,
        "available_fields": available,
        "combined_request_status": combined_status,
        "combined_request_error": combined_error,
        "rows_fetched": rows_fetched,
        "sample_values_by_field": sample_by_field,
        "samples": samples[:safe_sample],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Metrica Logs mini-probe for demography/device fields")
    parser.add_argument("--days", type=int, default=2, help="lookback days (1..3)")
    parser.add_argument("--sample-limit", type=int, default=10, help="sample rows limit (1..10)")
    args = parser.parse_args()
    report = run_probe(days=args.days, sample_limit=args.sample_limit)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
