import os
import json
import requests
import psycopg2

API_URL = "https://api.direct.yandex.com/json/v5/ads"
ENV_PATH = "/home/kv145/traffic-analytics/.env"

def load_env(path: str):
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            if line.startswith("export "):
                line = line[len("export "):].strip()
            k, v = line.split("=", 1)
            os.environ[k.strip()] = v.strip().strip('"').strip("'")

def env_first(*names: str, default: str = "") -> str:
    for n in names:
        v = os.getenv(n)
        if v:
            return v
    return default

def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

def main():
    load_env(ENV_PATH)

    direct_token = env_first(
        "DIRECT_TOKEN",
        "DIRECT_API_TOKEN",
        "YANDEX_DIRECT_TOKEN",
        "YDIRECT_TOKEN"
    )
    direct_login = env_first(
        "DIRECT_CLIENT_LOGIN",
        "YANDEX_DIRECT_LOGIN",
        "YDIRECT_LOGIN"
    )

    print("DIRECT_API_TOKEN present:", bool(direct_token))
    print("DIRECT_CLIENT_LOGIN present:", bool(direct_login))
    if direct_login:
        print("DIRECT_CLIENT_LOGIN:", direct_login)

    if not direct_token:
        raise RuntimeError("DIRECT API token not found in unified .env")
    if not direct_login:
        raise RuntimeError("DIRECT client login not found in unified .env")

    conn = psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        dbname=os.getenv("PGDATABASE", "traffic_analytics"),
        user=os.getenv("PGUSER", "traffic_admin"),
        password=os.getenv("PGPASSWORD"),
    )
    cur = conn.cursor()

    cur.execute("""
        select distinct campaign_id
        from stg_direct_search_queries
        where campaign_id is not null
        order by campaign_id
    """)
    campaign_ids = [r[0] for r in cur.fetchall()]
    print("campaign_ids:", len(campaign_ids))

    if not campaign_ids:
        print("No campaign IDs found in stg_direct_search_queries")
        cur.close()
        conn.close()
        return

    cur.execute("truncate table stg_direct_ads_meta")
    conn.commit()

    total_loaded = 0

    headers = {
        "Authorization": f"Bearer {direct_token}",
        "Client-Login": direct_login,
        "Accept-Language": "ru",
        "Content-Type": "application/json; charset=utf-8",
    }

    for batch in chunked(campaign_ids, 10):
        body = {
            "method": "get",
            "params": {
                "SelectionCriteria": {
                    "CampaignIds": batch
                },
                "FieldNames": ["Id", "CampaignId", "AdGroupId", "Type", "Subtype"],
                "TextAdFieldNames": ["Title", "Title2", "Text", "Href", "AdImageHash"],
                "TextImageAdFieldNames": ["AdImageHash"],
                "TextAdBuilderAdFieldNames": ["Creative", "Href"],
                "Page": {"Limit": 1000, "Offset": 0}
            }
        }

        resp = requests.post(API_URL, headers=headers, json=body, timeout=120)
        print("status:", resp.status_code, "campaign batch:", batch)

        try:
            data = resp.json()
        except Exception:
            print("Non-JSON response:", resp.text[:1000])
            resp.raise_for_status()
            continue

        if "error" in data:
            print("Direct API error:", json.dumps(data["error"], ensure_ascii=False))
            continue

        ads = data.get("result", {}).get("Ads", []) or []
        print("ads returned:", len(ads))

        for ad in ads:
            ad_id = ad.get("Id")
            campaign_id = ad.get("CampaignId")
            ad_group_id = ad.get("AdGroupId")
            ad_type = ad.get("Type")
            ad_subtype = ad.get("Subtype")

            text_ad = ad.get("TextAd") or {}
            text_builder = ad.get("TextAdBuilderAd") or {}

            title = text_ad.get("Title")
            title2 = text_ad.get("Title2")
            body_text = text_ad.get("Text")
            href = text_ad.get("Href") or text_builder.get("Href")

            ad_image_hash = text_ad.get("AdImageHash")

            creative = text_builder.get("Creative") or {}
            creative_id = creative.get("CreativeId")
            thumbnail_url = creative.get("ThumbnailUrl")
            preview_url = creative.get("PreviewUrl")

            cur.execute("""
                insert into stg_direct_ads_meta (
                    ad_id,
                    ad_group_id,
                    campaign_id,
                    title,
                    title2,
                    body_text,
                    href,
                    ad_type,
                    ad_subtype,
                    ad_image_hash,
                    creative_id,
                    thumbnail_url,
                    preview_url,
                    loaded_at
                )
                values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())
            """, (
                ad_id,
                ad_group_id,
                campaign_id,
                title,
                title2,
                body_text,
                href,
                ad_type,
                ad_subtype,
                ad_image_hash,
                creative_id,
                thumbnail_url,
                preview_url
            ))
            total_loaded += 1

        conn.commit()

    cur.close()
    conn.close()
    print(f"ads_meta loaded: {total_loaded}")

if __name__ == "__main__":
    main()
