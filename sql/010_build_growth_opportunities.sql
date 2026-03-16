drop table if exists mart_growth_opportunities;

create table mart_growth_opportunities as

select
    'seo_low_ctr' as opportunity_type,
    date,
    query_text as object_name,
    impressions,
    clicks,
    ctr,
    avg_position,
    null::numeric as cost,
    null::numeric as cpl,
    'Высокие показы, низкий CTR — проверить title, description и релевантность страницы' as recommendation
from stg_webmaster_query_daily
where impressions >= 30 and ctr < 0.05

union all

select
    'seo_growth_zone' as opportunity_type,
    date,
    query_text as object_name,
    impressions,
    clicks,
    ctr,
    avg_position,
    null::numeric as cost,
    null::numeric as cpl,
    'Запрос близко к топу — усилить страницу, внутренние ссылки и контент' as recommendation
from stg_webmaster_query_daily
where avg_position between 4 and 10

union all

select
    'channel_expensive_lead' as opportunity_type,
    date,
    channel_group as object_name,
    null::bigint as impressions,
    null::bigint as clicks,
    null::numeric as ctr,
    null::numeric as avg_position,
    cost,
    cpl,
    'Дорогой лид — проверить оффер, креативы, сегментацию и посадочную' as recommendation
from mart_unit_economics
where cpl is not null and cpl > 500

union all

select
    'channel_best_romi' as opportunity_type,
    date,
    channel_group as object_name,
    null::bigint as impressions,
    null::bigint as clicks,
    null::numeric as ctr,
    null::numeric as avg_position,
    cost,
    cpl,
    'Лучший ROMI — можно масштабировать бюджет или усилия' as recommendation
from mart_unit_economics
where romi is not null and romi > 3;
