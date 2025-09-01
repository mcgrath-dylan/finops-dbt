{{
    config(
        materialized='table',
        tags=['executive_dashboard'],
        enabled=var('enable_executive_summary', false)
    )
}}

-- Executive summary combining all optimization opportunities

with warehouse_opportunities as (
    select
        'Warehouse Auto-Suspend Optimization' as opportunity_type,
        count(*) as opportunity_count,
        sum(potential_monthly_savings) as monthly_savings_usd,
        'Adjust auto-suspend settings to reduce idle time' as primary_action,
        'Low' as implementation_difficulty,
        'Immediate' as time_to_value
    from {{ ref('int_warehouse_optimization') }}
    where recommended_auto_suspend_seconds < 600  -- Recommending faster suspend
),

expensive_queries as (
    select
        'Expensive Query Optimization' as opportunity_type,
        count(distinct query_id) as opportunity_count,
        sum(estimated_query_cost_usd) * 30 as monthly_savings_usd,  
        'Optimize query patterns and add filters' as primary_action,
        'Medium' as implementation_difficulty,
        '1-2 weeks' as time_to_value
    from {{ ref('int_query_cost_attribution') }}
    where estimated_query_cost_usd > {{ var('expensive_query_threshold_usd') }}
)

select * from warehouse_opportunities
union all
select * from expensive_queries
