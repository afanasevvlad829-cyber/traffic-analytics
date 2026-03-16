drop table if exists mart_growth_actions;

create table mart_growth_actions as

select
    date,
    'SEO' as source_type,
    query_text as object_name,
    impressions,
    clicks,
    ctr,
    avg_position,
    'rewrite_snippet' as action_type,
    'high' as priority,
    'Высокие показы и низкий CTR: переписать title/description, проверить интент и релевантность посадочной' as action_note
from stg_webmaster_query_daily
where impressions >= 30
  and ctr < 0.05

union all

select
    date,
    'SEO' as source_type,
    query_text as object_name,
    impressions,
    clicks,
    ctr,
    avg_position,
    'boost_page' as action_type,
    'high' as priority,
    'Запрос в зоне роста 4-10: усилить страницу, контент, внутренние ссылки и анкор-линковку' as action_note
from stg_webmaster_query_daily
where avg_position between 4 and 10
  and impressions >= 20

union all

select
    date,
    'SEO' as source_type,
    query_text as object_name,
    impressions,
    clicks,
    ctr,
    avg_position,
    'create_landing' as action_type,
    'high' as priority,
    'Есть спрос, но слабый CTR/позиция: вынести в отдельную посадочную или кластерную страницу' as action_note
from stg_webmaster_query_daily
where impressions >= 50
  and clicks = 0

union all

select
    date,
    'DIRECT' as source_type,
    channel_group as object_name,
    null::bigint as impressions,
    null::bigint as clicks,
    null::numeric as ctr,
    null::numeric as avg_position,
    'reduce_cpl' as action_type,
    'high' as priority,
    'Дорогой лид: проверить сегменты, оффер, креативы, поисковые фразы и посадочную' as action_note
from mart_unit_economics
where cpl is not null
  and cpl > 500

union all

select
    date,
    'DIRECT' as source_type,
    channel_group as object_name,
    null::bigint as impressions,
    null::bigint as clicks,
    null::numeric as ctr,
    null::numeric as avg_position,
    'scale_budget' as action_type,
    'medium' as priority,
    'Канал показывает высокий ROMI: можно осторожно масштабировать бюджет' as action_note
from mart_unit_economics
where romi is not null
  and romi > 3;
