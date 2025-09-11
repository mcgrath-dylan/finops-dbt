

-- Pro Pack executive summary (informational). 
-- Outputs identify candidates and estimated dollars to review; not realized savings.

with warehouse_opportunities as (
    select
        'Warehouse Auto-Suspend Optimization' as opportunity_type,
        count(*) as opportunity_count,
        -- from Pro model column
        sum(estimated_monthly_idle_cost_usd) as estimated_monthly_cost_to_review_usd,
        'Adjust auto-suspend settings to reduce idle time' as primary_action,
        'Low' as implementation_difficulty,
        'Immediate' as time_to_value
    from DM_AE_FINOPS_DB.STG.int_warehouse_optimization
    where recommended_auto_suspend_seconds < 600
),

expensive_queries as (
    select
        'Expensive Query Optimization' as opportunity_type,
        count(distinct query_id) as opportunity_count,
        -- simple month proxy for visibility; still an estimate
        sum(estimated_query_cost_usd) * 30 as estimated_monthly_cost_to_review_usd,
        'Optimize query patterns and add filters' as primary_action,
        'Medium' as implementation_difficulty,
        '1-2 weeks' as time_to_value
    from DM_AE_FINOPS_DB.STG.int_query_cost_attribution
    where estimated_query_cost_usd > 1
)

select * from warehouse_opportunities
union all
select * from expensive_queries