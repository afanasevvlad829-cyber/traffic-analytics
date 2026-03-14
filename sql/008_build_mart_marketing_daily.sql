drop table if exists mart_marketing_daily;

create table mart_marketing_daily as
select
    c.date,
    c.channel_group,
    c.sessions,
    c.users,
    c.cost,
    coalesce(l.leads, 0) as leads,
    l.revenue,
    case
        when c.users > 0 then round(c.sessions::numeric / c.users, 4)
        else null
    end as sessions_per_user,
    case
        when coalesce(l.leads, 0) > 0 then round(c.cost / l.leads, 2)
        else null
    end as cpl,
    case
        when c.sessions > 0 then round(coalesce(l.leads, 0)::numeric / c.sessions, 4)
        else null
    end as lead_conversion_rate
from mart_channel_daily c
left join mart_leads_daily l
    on c.date = l.date
   and c.channel_group = l.channel_group
order by c.date, c.channel_group;
