import argparse
import json
import sys

from src.scoring.report import send_scoring_report
from src.scoring.service import (
    get_scoring_summary,
    get_scoring_visitors,
    rebuild_scoring_v1,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Scoring v1 rebuild")
    parser.add_argument("--limit", type=int, default=None, help="limit visitors to process")
    parser.add_argument(
        "--no-fallback",
        action="store_true",
        help="disable fallback source built from stg_metrica_source_daily",
    )
    parser.add_argument(
        "--report",
        action="store_true",
        help="send scoring summary report to Telegram after rebuild",
    )
    args = parser.parse_args()

    result = rebuild_scoring_v1(limit=args.limit, use_fallback=not args.no_fallback)

    if args.report:
        try:
            summary = get_scoring_summary()
            hot_visitors = get_scoring_visitors(limit=5, segment="hot").get("items", [])
            report_status = send_scoring_report(summary=summary, top_visitors=hot_visitors)
            result["report"] = report_status
            if not report_status.get("ok", False):
                print(f"[scoring-report] {report_status.get('error')}", file=sys.stderr)
        except Exception as exc:  # noqa: BLE001
            result["report"] = {"ok": False, "sent": False, "error": str(exc)}
            print(f"[scoring-report] {exc}", file=sys.stderr)

    print(json.dumps(result, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
