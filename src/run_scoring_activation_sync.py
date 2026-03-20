import argparse
import json

from src.scoring.service import (
    get_scoring_activation_plan,
    get_scoring_activation_reaction,
    sync_scoring_activation_to_direct,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Scoring -> Direct activation runner")
    parser.add_argument("--days", type=int, default=90, help="lookback window for cohorts")
    parser.add_argument(
        "--min-audience-size",
        type=int,
        default=100,
        help="minimum cohort size required for activation",
    )
    parser.add_argument(
        "--export-limit",
        type=int,
        default=5000,
        help="max visitor ids exported per cohort",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="apply changes to Direct (default is dry-run)",
    )
    parser.add_argument(
        "--with-reaction",
        action="store_true",
        help="append Direct reaction summary from stg_direct_campaign_daily",
    )
    args = parser.parse_args()

    if args.execute:
        result = sync_scoring_activation_to_direct(
            days=args.days,
            min_audience_size=args.min_audience_size,
            export_limit=args.export_limit,
            dry_run=False,
        )
    else:
        result = get_scoring_activation_plan(
            days=args.days,
            min_audience_size=args.min_audience_size,
            export_limit=args.export_limit,
        )

    if args.with_reaction:
        result["reaction"] = get_scoring_activation_reaction(days=min(args.days, 90), limit=50)

    print(json.dumps(result, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()

