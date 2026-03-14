drop table if exists mart_seo_query_daily;

create table mart_seo_query_daily as
select
    date,
    query_id,
    query_text,
    impressions,
    clicks,
    ctr,
    avg_position,
    case
        when avg_position <= 3 then 'top_3'
        when avg_position <= 10 then 'top_10'
        else 'below_10'
    end as position_bucket,
    case
        when ctr >= 0.3 then 'high_ctr'
        when ctr >= 0.1 then 'mid_ctr'
        else 'low_ctr'
    end as ctr_bucket
from stg_webmaster_query_daily;
