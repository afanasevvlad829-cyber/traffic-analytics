import os
import subprocess
from datetime import datetime, date, timedelta

from src.extract_metrica import run as run_metrica
from src.extract_webmaster import run as run_webmaster
from src.extract_direct import run as run_direct
from src.scoring.feature_sync import build_scoring_features


def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


def run_sql(path: str, env: dict):
    subprocess.run(
        [
            "psql",
            "-h", "localhost",
            "-U", "traffic_admin",
            "-d", "traffic_analytics",
            "-f", path,
        ],
        check=True,
        env=env,
    )


def main():
    report_date = (date.today() - timedelta(days=1)).isoformat()
    webmaster_from = (date.today() - timedelta(days=14)).isoformat()
    webmaster_to = report_date

    log("=== ETL START ===")
    log(f"Processing date: {report_date}")

    try:
        log("Running METRICA extractor")
        log(f"METRICA result: {run_metrica(report_date)}")
    except Exception as e:
        log(f"ERROR in METRICA: {e}")

    try:
        log("Building scoring visitor-level features from METRICA")
        log(f"SCORING FEATURES result: {build_scoring_features(days=30, max_rows=50000)}")
    except Exception as e:
        log(f"ERROR in scoring feature build: {e}")

    try:
        log("Running WEBMASTER extractor")
        log(f"WEBMASTER result: {run_webmaster(webmaster_from, webmaster_to)}")
    except Exception as e:
        log(f"ERROR in WEBMASTER: {e}")

    try:
        log("Running DIRECT extractor")
        log(f"DIRECT result: {run_direct(report_date)}")
    except Exception as e:
        log(f"ERROR in DIRECT: {e}")

    alfacrm_xlsx = os.getenv("ALFACRM_XLSX_FILE", "").strip()
    if alfacrm_xlsx:
        try:
            log(f"Running ALFACRM CRM loader from file: {alfacrm_xlsx}")
            from src.load_alfacrm_crm_xlsx import run as run_alfacrm_crm_load

            log(
                f"ALFACRM CRM result: {run_alfacrm_crm_load(xlsx_path=alfacrm_xlsx, report_date=report_date)}"
            )
        except Exception as e:
            log(f"ERROR in ALFACRM CRM loader: {e}")

    env = os.environ.copy()
    env["PGPASSWORD"] = env.get("PG_PASSWORD", "StrongPassword123")

    try:
        log("Rebuilding mart_channel_daily")
        run_sql("/home/kv145/traffic-analytics/sql/006_rebuild_mart_channel_with_cost.sql", env)
        log("mart_channel_daily rebuilt")
    except Exception as e:
        log(f"ERROR rebuilding mart_channel_daily: {e}")

    try:
        log("Rebuilding mart_unit_economics")
        run_sql("/home/kv145/traffic-analytics/sql/009_build_unit_economics.sql", env)
        log("mart_unit_economics rebuilt")
    except Exception as e:
        log(f"ERROR rebuilding mart_unit_economics: {e}")

    try:
        log("Rebuilding mart_growth_opportunities")
        run_sql("/home/kv145/traffic-analytics/sql/010_build_growth_opportunities.sql", env)
        log("mart_growth_opportunities rebuilt")
    except Exception as e:
        log(f"ERROR rebuilding mart_growth_opportunities: {e}")

    try:
        log("Rebuilding mart_direct_ai tables")
        run_sql("/home/kv145/traffic-analytics/sql/013_create_direct_ai_tables.sql", env)
        log("Running direct AI diagnostics")
        from src.run_direct_ai import run as run_direct_ai
        log(f"DIRECT AI result: {run_direct_ai(report_date)}")
    except Exception as e:
        log(f"ERROR in DIRECT AI: {e}")

    log("=== ETL FINISHED ===")


if __name__ == "__main__":
    main()
