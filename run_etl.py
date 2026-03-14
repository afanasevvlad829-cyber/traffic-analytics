import os
import subprocess
from datetime import datetime, date, timedelta

from src.extract_metrica import run as run_metrica
from src.extract_webmaster import run as run_webmaster
from src.extract_direct import run as run_direct


def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


def main():

    report_date = (date.today() - timedelta(days=1)).isoformat()
    webmaster_from = (date.today() - timedelta(days=14)).isoformat()
    webmaster_to = report_date

    log("=== ETL START ===")
    log(f"Processing date: {report_date}")

    try:
        log("Running METRICA extractor")
        result = run_metrica(report_date)
        log(f"METRICA result: {result}")
    except Exception as e:
        log(f"ERROR in METRICA: {e}")

    try:
        log("Running WEBMASTER extractor")
        result = run_webmaster(webmaster_from, webmaster_to)
        log(f"WEBMASTER result: {result}")
    except Exception as e:
        log(f"ERROR in WEBMASTER: {e}")

    try:
        log("Running DIRECT extractor")
        result = run_direct(report_date)
        log(f"DIRECT result: {result}")
    except Exception as e:
        log(f"ERROR in DIRECT: {e}")

    try:
        log("Rebuilding mart_channel_daily")

        env = os.environ.copy()
        env["PGPASSWORD"] = env.get("PG_PASSWORD", "StrongPassword123")

        subprocess.run(
            [
                "psql",
                "-h", "localhost",
                "-U", "traffic_admin",
                "-d", "traffic_analytics",
                "-f", "/home/kv145/traffic-analytics/sql/006_rebuild_mart_channel_with_cost.sql",
            ],
            check=True,
            env=env
        )

        log("mart_channel_daily rebuilt")

    except Exception as e:
        log(f"ERROR rebuilding mart_channel_daily: {e}")

    log("=== ETL FINISHED ===")


if __name__ == "__main__":
    main()
