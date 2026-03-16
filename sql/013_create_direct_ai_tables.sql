create table if not exists mart_direct_ai_recommendations (
    created_at timestamp default now(),
    date date,
    campaign_name text,
    problem_type text,
    metric_value numeric,
    recommendation text,
    priority text
);

drop table if exists mart_direct_campaign_diagnostics;

create table mart_direct_campaign_diagnostics as
select
    d.date,
    d.campaign_name,
    d.impressions,
    d.clicks,
    d.cost,
    case
        when d.impressions > 0 then round(d.clicks::numeric / d.impressions, 4)
        else 0
    end as ctr,
    case
        when d.clicks > 0 then round(d.cost / d.clicks, 2)
        else 0
    end as avg_cpc,

    coalesce(u.sessions, 0) as sessions,
    coalesce(u.users, 0) as users,
    coalesce(u.leads, 0) as leads,
    coalesce(u.sales, 0) as sales,
    coalesce(u.revenue, 0) as revenue,
    u.cpl,
    u.cac,
    u.romi

from stg_direct_campaign_daily d
left join mart_unit_economics u
    on d.date = u.date
   and u.channel_group = 'Direct';

drop view if exists vw_dashboard_direct_ai;

create view vw_dashboard_direct_ai as
select
    created_at,
    date,
    campaign_name,
    problem_type,
    metric_value,
    recommendation,
    priority
from mart_direct_ai_recommendations;
