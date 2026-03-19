import argparse
import json

from src.scoring.feature_sync import build_scoring_features


def main() -> None:
    parser = argparse.ArgumentParser(description="Build visitor-level scoring features from Yandex Metrica")
    parser.add_argument("--days", type=int, default=30, help="lookback window in days")
    parser.add_argument("--max-rows", type=int, default=50000, help="max rows to fetch from Metrica API")
    parser.add_argument("--page-limit", type=int, default=10000, help="page size for Metrica API pagination")
    args = parser.parse_args()

    result = build_scoring_features(
        days=args.days,
        max_rows=args.max_rows,
        page_limit=args.page_limit,
    )
    print(json.dumps(result, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
