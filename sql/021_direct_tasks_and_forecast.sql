drop view if exists vw_direct_task_queue;
drop table if exists mart_direct_task_queue;

create table mart_direct_task_queue as
select
    row_number() over (order by date desc, priority_score desc, cost desc) as task_id,
    date,
    campaign_name,
    ad_group_id,
    search_query,
    decision,
    exact_action,
    reason,
    impressions,
    clicks,
    cost,
    ctr,
    avg_cpc,
    campaign_age_days,
    account_avg_ctr,
    account_median_ctr,
    account_p75_cpc,
    priority_score,

    case
        when decision = 'REWRITE_AD'
            then round(least(account_avg_ctr, ctr * 1.5) * 100, 2)
        when decision = 'SCALE'
            then round(greatest(ctr, account_avg_ctr) * 100, 2)
        else null
    end as forecast_target_ctr_pct,

    case
        when decision in ('REWRITE_AD', 'LOWER_BID_OR_SPLIT')
            then round(avg_cpc * 0.9, 2)
        else null
    end as forecast_target_cpc,

    case
        when decision = 'EXCLUDE'
            then round(cost, 2)
        when decision in ('REWRITE_AD', 'LOWER_BID_OR_SPLIT')
            then round((avg_cpc - (avg_cpc * 0.9)) * greatest(clicks,1), 2)
        else null
    end as forecast_cost_effect,

    'OPEN'::text as status,
    now() as created_at,
    null::timestamp as completed_at,
    null::text as completed_by
from mart_direct_action_queue_v3
where decision in ('EXCLUDE','REWRITE_AD','LOWER_BID_OR_SPLIT','LANDING_CHECK','SCALE');

create view vw_direct_task_queue as
select
    task_id,
    date,
    campaign_name,
    search_query,
    decision,
    exact_action,
    reason,
    impressions,
    clicks,
    cost,
    round(ctr * 100, 2) as ctr_pct,
    avg_cpc,
    round(account_avg_ctr * 100, 2) as account_avg_ctr_pct,
    forecast_target_ctr_pct,
    forecast_target_cpc,
    forecast_cost_effect,
    status,
    created_at,
    completed_at,
    completed_by
from mart_direct_task_queue;
