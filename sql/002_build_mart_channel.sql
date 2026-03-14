truncate table mart_channel_daily;

insert into mart_channel_daily
(date, channel_group, sessions, users, leads, cost, cpl, conversion_rate)
select
    date,
    case
        when lower(traffic_source) like '%search engine%' then 'SEO'
        when lower(traffic_source) like '%direct traffic%' then 'Direct'
        when lower(traffic_source) like '%link traffic%' then 'Referral'
        when lower(traffic_source) like '%internal traffic%' then 'Internal'
        else 'Other'
    end as channel_group,
    sum(sessions) as sessions,
    sum(users) as users,
    0 as leads,
    0 as cost,
    null as cpl,
    null as conversion_rate
from stg_metrica_source_daily
group by 1, 2
order by 1, 2;
