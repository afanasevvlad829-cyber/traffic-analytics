drop table if exists mart_unit_economics;

create table mart_unit_economics as
select
    m.date,
    m.channel_group,
    m.sessions,
    m.users,
    m.cost,

    m.leads as leads_from_marketing,
    coalesce(l.leads,0) as leads,
    coalesce(l.sales,0) as sales,
    coalesce(l.revenue,0) as revenue,

    case
        when coalesce(l.leads,0) > 0
        then round(m.cost / l.leads,2)
    end as cpl,

    case
        when coalesce(l.sales,0) > 0
        then round(m.cost / l.sales,2)
    end as cac,

    case
        when m.cost > 0
        then round((coalesce(l.revenue,0) - m.cost) / m.cost,2)
    end as romi,

    case
        when m.sessions > 0
        then round(coalesce(l.leads,0)::numeric / m.sessions,4)
    end as lead_conversion_rate,

    case
        when coalesce(l.leads,0) > 0
        then round(coalesce(l.sales,0)::numeric / l.leads,4)
    end as sale_conversion_rate,

    case
        when coalesce(l.sales,0) > 0
        then round(coalesce(l.revenue,0)::numeric / l.sales,2)
    end as average_check

from mart_marketing_daily m
left join mart_leads_daily l
    on m.date = l.date
   and m.channel_group = l.channel_group
order by m.date, m.channel_group;
