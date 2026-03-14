truncate table mart_channel_daily;

insert into mart_channel_daily
(date, channel_group, sessions, users, cost, leads, cpl, conversion_rate)
select
    m.date,
    case
        when lower(m.traffic_source) like '%search engine%' then 'SEO'
        when lower(m.traffic_source) like '%direct traffic%' then 'Direct'
        when lower(m.traffic_source) like '%link traffic%' then 'Referral'
        when lower(m.traffic_source) like '%internal traffic%' then 'Internal'
        else 'Other'
    end as channel_group,
    sum(m.sessions) as sessions,
    sum(m.users) as users,
    case
        when lower(m.traffic_source) like '%direct traffic%' then coalesce(d.direct_cost, 0)
        else 0
    end as cost,
    0 as leads,
    null as cpl,
    null as conversion_rate
from stg_metrica_source_daily m
left join (
    select
        date,
        sum(cost) as direct_cost
    from stg_direct_campaign_daily
    group by date
) d
    on m.date = d.date
group by
    m.date,
    case
        when lower(m.traffic_source) like '%search engine%' then 'SEO'
        when lower(m.traffic_source) like '%direct traffic%' then 'Direct'
        when lower(m.traffic_source) like '%link traffic%' then 'Referral'
        when lower(m.traffic_source) like '%internal traffic%' then 'Internal'
        else 'Other'
    end,
    case
        when lower(m.traffic_source) like '%direct traffic%' then coalesce(d.direct_cost, 0)
        else 0
    end
order by 1, 2;
