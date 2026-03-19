from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

SEED_PREFIX = "scoring_v1_seed_"
EXPECTED_SEGMENTS = {"hot", "warm", "cold"}


class SmokeFailure(Exception):
    pass


def get_connection():
    from src.db import get_connection as _get_connection

    return _get_connection()


def api_get(base_url: str, path: str, params: dict | None = None) -> dict:
    url = base_url.rstrip("/") + path
    if params:
        url += "?" + urlencode(params)

    req = Request(url=url, method="GET", headers={"Content-Type": "application/json"})
    try:
        with urlopen(req, timeout=30) as resp:
            payload = resp.read().decode("utf-8")
            return json.loads(payload) if payload else {}
    except HTTPError as exc:  # noqa: PERF203
        body = exc.read().decode("utf-8", errors="ignore")
        raise SmokeFailure(f"GET {url} failed: HTTP {exc.code} {body}") from exc
    except URLError as exc:
        raise SmokeFailure(f"GET {url} failed: {exc}") from exc


def api_post(base_url: str, path: str, payload: dict) -> dict:
    url = base_url.rstrip("/") + path
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = Request(
        url=url,
        data=data,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urlopen(req, timeout=60) as resp:
            text = resp.read().decode("utf-8")
            return json.loads(text) if text else {}
    except HTTPError as exc:  # noqa: PERF203
        body = exc.read().decode("utf-8", errors="ignore")
        raise SmokeFailure(f"POST {url} failed: HTTP {exc.code} {body}") from exc
    except URLError as exc:
        raise SmokeFailure(f"POST {url} failed: {exc}") from exc


def apply_seed_sql(seed_sql_path: Path) -> list[str]:
    sql_text = seed_sql_path.read_text(encoding="utf-8")

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql_text)
        conn.commit()
    finally:
        conn.close()

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                select visitor_id
                from stg_metrica_visitors_features
                where visitor_id like %s
                order by visitor_id
                """,
                (f"{SEED_PREFIX}%",),
            )
            rows = cur.fetchall()
            return [str(row[0]) for row in rows]
    finally:
        conn.close()


def cleanup_seed_rows() -> None:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "delete from mart_visitor_scoring where visitor_id like %s",
                (f"{SEED_PREFIX}%",),
            )
            cur.execute(
                "delete from stg_metrica_visitors_features where visitor_id like %s",
                (f"{SEED_PREFIX}%",),
            )
        conn.commit()
    finally:
        conn.close()


def normalize_counts(items: list[dict]) -> dict[str, int]:
    counts = {"hot": 0, "warm": 0, "cold": 0}
    for row in items:
        segment = str(row.get("segment") or "").lower()
        if segment in counts:
            counts[segment] += 1
    return counts


def run_smoke(base_url: str, seed_sql_path: Path, skip_seed: bool) -> dict:
    inserted_visitors: list[str] = []
    if not skip_seed:
        inserted_visitors = apply_seed_sql(seed_sql_path)

    rebuild = api_post(base_url, "/api/scoring/rebuild", {"use_fallback": False})
    if not rebuild.get("ok", False):
        raise SmokeFailure(f"scoring rebuild failed: {rebuild}")

    summary = api_get(base_url, "/api/scoring/summary")
    if summary.get("ready") is False:
        raise SmokeFailure(f"summary is not ready: {summary}")

    visitors = api_get(base_url, "/api/scoring/visitors", {"limit": 200, "source": "seed_"})
    items = visitors.get("items") or []
    if not items:
        raise SmokeFailure("visitors API returned no seeded rows")

    inserted_set = set(inserted_visitors)
    seeded_items = [row for row in items if str(row.get("visitor_id")) in inserted_set] if inserted_set else items

    if len(seeded_items) < 3:
        raise SmokeFailure(f"expected seeded visitors in visitors API, got {len(seeded_items)}")

    segment_counts = normalize_counts(seeded_items)
    got_segments = {segment for segment, cnt in segment_counts.items() if cnt > 0}
    if not EXPECTED_SEGMENTS.issubset(got_segments):
        raise SmokeFailure(
            f"expected seeded segments {sorted(EXPECTED_SEGMENTS)}, got {sorted(got_segments)}"
        )

    sample_id = str(seeded_items[0].get("visitor_id") or "")
    if not sample_id:
        raise SmokeFailure("cannot pick sample visitor_id for detail check")

    detail = api_get(base_url, f"/api/scoring/visitor/{sample_id}")
    if str(detail.get("visitor_id") or "") != sample_id:
        raise SmokeFailure(f"detail API returned wrong visitor: expected={sample_id}, got={detail}")

    explanation = detail.get("explanation_json")
    if not isinstance(explanation, dict) or not explanation:
        raise SmokeFailure(f"detail explanation_json is empty or invalid: {detail}")

    return {
        "inserted_test_visitors": inserted_visitors,
        "rebuild": rebuild,
        "summary": summary,
        "seed_segment_counts": segment_counts,
        "sample_visitor_detail": {
            "visitor_id": detail.get("visitor_id"),
            "segment": detail.get("segment"),
            "normalized_score": detail.get("normalized_score"),
            "raw_score": detail.get("raw_score"),
            "recommended_action": detail.get("recommended_action"),
            "explanation_json": detail.get("explanation_json"),
        },
    }


def print_report(ok: bool, payload: dict | None = None, error: str | None = None) -> None:
    payload = payload or {}
    print("inserted test visitors:")
    print(payload.get("inserted_test_visitors") or [])

    print("rebuild done:")
    print(json.dumps(payload.get("rebuild") or {}, ensure_ascii=False, default=str, indent=2))

    print("hot/warm/cold counts:")
    print(json.dumps(payload.get("seed_segment_counts") or {}, ensure_ascii=False, indent=2))

    print("sample visitor detail:")
    print(json.dumps(payload.get("sample_visitor_detail") or {}, ensure_ascii=False, default=str, indent=2))

    if ok:
        print("PASS")
    else:
        print("FAIL")
        if error:
            print(error)


def main() -> None:
    parser = argparse.ArgumentParser(description="Scoring v1 smoke test")
    parser.add_argument("--base-url", default="http://127.0.0.1:8088", help="webapp base URL")
    parser.add_argument(
        "--seed-sql",
        default=str(Path(__file__).resolve().parents[1] / "sql" / "041_scoring_v1_seed.sql"),
        help="path to SQL seed file",
    )
    parser.add_argument(
        "--skip-seed",
        action="store_true",
        help="do not apply seed SQL before smoke checks",
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="delete seed rows from staging/scoring tables after check",
    )
    args = parser.parse_args()

    seed_sql_path = Path(args.seed_sql)
    if not args.skip_seed and not seed_sql_path.exists():
        print_report(False, error=f"seed SQL file not found: {seed_sql_path}")
        sys.exit(1)

    payload: dict | None = None
    err: str | None = None
    ok = False

    try:
        payload = run_smoke(args.base_url, seed_sql_path, skip_seed=args.skip_seed)
        ok = True
    except Exception as exc:  # noqa: BLE001
        err = str(exc)
    finally:
        if args.cleanup:
            try:
                cleanup_seed_rows()
            except Exception as cleanup_exc:  # noqa: BLE001
                ok = False
                err = f"{err or ''}\ncleanup failed: {cleanup_exc}".strip()

    print_report(ok=ok, payload=payload, error=err)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
