drop table if exists mart_seo_summary_daily;

create table mart_seo_summary_daily as
select
    date,
    count(*) as queries_total,
    sum(impressions) as impressions_total,
    sum(clicks) as clicks_total,
    case
        when sum(impressions) > 0
            then round(sum(clicks)::numeric / sum(impressions), 4)
        else 0
    end as ctr_total,
    round(avg(avg_position)::numeric, 2) as avg_position_mean,

    sum(case when avg_position <= 3 then 1 else 0 end) as queries_top_3,
    sum(case when avg_position <= 10 then 1 else 0 end) as queries_top_10,
    sum(case when clicks > 0 then 1 else 0 end) as queries_with_clicks,

    sum(case when impressions >= 30 and ctr < 0.05 then 1 else 0 end) as low_ctr_high_impression_queries,
    sum(case when avg_position between 4 and 10 then 1 else 0 end) as growth_zone_queries

from stg_webmaster_query_daily
group by date
order by date;
