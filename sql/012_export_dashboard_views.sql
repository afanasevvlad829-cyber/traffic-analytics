drop view if exists vw_dashboard_channels;
drop view if exists vw_dashboard_seo;
drop view if exists vw_dashboard_actions;

create view vw_dashboard_channels as
select
    date,
    channel_group,
    sessions,
    users,
    cost,
    leads,
    sales,
    revenue,
    cpl,
    cac,
    romi,
    lead_conversion_rate,
    sale_conversion_rate,
    average_check
from mart_unit_economics;

create view vw_dashboard_seo as
select
    date,
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
        when impressions >= 50 and clicks = 0 then 'high_demand_zero_clicks'
        when impressions >= 30 and ctr < 0.05 then 'low_ctr'
        when avg_position between 4 and 10 then 'growth_zone'
        else 'other'
    end as seo_segment
from stg_webmaster_query_daily;

create view vw_dashboard_actions as
select
    date,
    source_type,
    object_name,
    action_type,
    priority,
    impressions,
    clicks,
    ctr,
    avg_position,
    action_note
from mart_growth_actions;
