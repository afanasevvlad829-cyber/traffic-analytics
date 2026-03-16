import csv
import sys
from src.db import db_cursor

def run(csv_path):
    inserted = 0
    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        with db_cursor() as (_, cur):
            for row in reader:
                cur.execute("""
                    insert into stg_direct_search_detail
                    (date, campaign_name, ad_group_name, keyword, search_query, impressions, clicks, cost, avg_cpc, ctr, conversions, loaded_at)
                    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())
                """, (
                    row.get('date') or row.get('Date'),
                    row.get('campaign_name') or row.get('CampaignName'),
                    row.get('ad_group_name') or row.get('AdGroupName'),
                    row.get('keyword') or row.get('Keyword'),
                    row.get('search_query') or row.get('SearchQuery'),
                    int(row.get('impressions') or row.get('Impressions') or 0),
                    int(row.get('clicks') or row.get('Clicks') or 0),
                    float(row.get('cost') or row.get('Cost') or 0),
                    float(row.get('avg_cpc') or row.get('AvgCpc') or 0),
                    float(row.get('ctr') or row.get('Ctr') or 0),
                    int(row.get('conversions') or row.get('Conversions') or 0),
                ))
                inserted += 1
    return {"status": "ok", "rows_loaded": inserted}

if __name__ == "__main__":
    print(run(sys.argv[1]))
