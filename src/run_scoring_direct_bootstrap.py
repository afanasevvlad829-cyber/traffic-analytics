import argparse
import json

from src.scoring.service import bootstrap_scoring_activation_direct


def _parse_regions(raw: str) -> list[int]:
    out: list[int] = []
    for part in (raw or "").split(","):
        p = part.strip()
        if not p:
            continue
        try:
            out.append(int(p))
        except ValueError:
            continue
    return out or [0]


def main() -> None:
    parser = argparse.ArgumentParser(description="Bootstrap Direct entities for scoring cohorts")
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--min-audience-size", type=int, default=100)
    parser.add_argument("--export-limit", type=int, default=5000)
    parser.add_argument("--campaign-id", type=int, default=None, help="Direct campaign id; if empty inferred from stg_direct_campaign_daily")
    parser.add_argument("--region-ids", type=str, default="0", help="comma-separated region ids, default 0 (all regions)")
    parser.add_argument("--env-path", type=str, default="/home/kv145/traffic-analytics/.env")
    parser.add_argument("--include-small", action="store_true", help="include non-eligible (small) cohorts if audience_size > 0")
    parser.add_argument("--apply", action="store_true", help="create entities and write IDs to env; default dry-run")
    args = parser.parse_args()

    result = bootstrap_scoring_activation_direct(
        days=args.days,
        min_audience_size=args.min_audience_size,
        export_limit=args.export_limit,
        campaign_id=args.campaign_id,
        region_ids=_parse_regions(args.region_ids),
        apply=bool(args.apply),
        env_path=args.env_path,
        include_small=bool(args.include_small),
    )
    print(json.dumps(result, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
