{{
    config(
        materialized='table'
    )
}}

with hourly_patterns as (
    select
        warehouse_name,
        usage_date,
        hour_start,
        total_cost_usd,
        queries_executed,
        unique_users,
        is_potentially_idle,
        idle_cost_usd
    from {{ ref('int_hourly_compute_costs') }}
    where usage_date >= dateadd('day', -14, current_date())  -- Last 2 weeks
),

daily_summary as (
    select
        warehouse_name,
        usage_date,
        sum(total_cost_usd) as daily_cost,
        sum(queries_executed) as daily_queries,
        sum(case when is_potentially_idle then 1 else 0 end) as idle_hours,
        sum(idle_cost_usd) as daily_idle_cost,
        max(unique_users) as max_concurrent_users
    from hourly_patterns
    group by 1, 2
),

warehouse_analysis as (
    select
        warehouse_name,
        
        -- Cost metrics
        avg(daily_cost) as avg_daily_cost,
        sum(daily_cost) as total_period_cost,
        avg(daily_idle_cost) as avg_daily_idle_cost,
        sum(daily_idle_cost) as total_idle_cost,
        
        -- Usage patterns
        avg(daily_queries) as avg_daily_queries,
        avg(idle_hours) as avg_idle_hours_per_day,
        max(max_concurrent_users) as peak_concurrent_users,
        
        -- Calculate idle percentage
        case 
            when sum(daily_cost) > 0 
            then 100.0 * sum(daily_idle_cost) / sum(daily_cost)
            else 0
        end as idle_cost_percentage,
        
        -- Days with any activity
        count(distinct case when daily_queries > 0 then usage_date end) as active_days,
        count(distinct usage_date) as total_days,
        
        -- Weekend vs weekday patterns
        avg(case when dayofweek(usage_date) in (0, 6) then daily_cost else null end) as avg_weekend_cost,
        avg(case when dayofweek(usage_date) not in (0, 6) then daily_cost else null end) as avg_weekday_cost
        
    from daily_summary
    group by 1
),

optimization_recommendations as (
    select
        warehouse_name,
        avg_daily_cost,
        avg_daily_idle_cost,
        idle_cost_percentage,
        avg_daily_queries,
        avg_idle_hours_per_day,
        peak_concurrent_users,
        active_days,
        total_days,
        
        -- Auto-suspend recommendation
        case
            when avg_idle_hours_per_day > 12 then 60  -- 1 minute if idle >50% of day
            when avg_idle_hours_per_day > 6 then 300  -- 5 minutes if idle >25% of day
            else 600  -- 10 minutes default
        end as recommended_auto_suspend_seconds,
        
        -- Schedule recommendation
        case
            when avg_weekend_cost < avg_weekday_cost * 0.1 then 'Consider suspending on weekends'
            when active_days < total_days * 0.5 then 'Consider scheduled suspension'
            else 'Current schedule appears appropriate'
        end as schedule_recommendation,
        
        -- Sizing recommendation (conservative - based on concurrent users)
        case
            when peak_concurrent_users <= 2 and avg_daily_queries < 100 then 'Consider downsizing'
            when peak_concurrent_users > 10 and avg_idle_hours_per_day < 2 then 'Consider upsizing'
            else 'Current size appears appropriate'
        end as sizing_recommendation,
        
        -- Potential monthly savings from auto-suspend optimization
        avg_daily_idle_cost * 30 as potential_monthly_savings,
        
        -- Priority score (higher = more important to optimize)
        case
            when avg_daily_idle_cost > 50 then 100
            when avg_daily_idle_cost > 20 then 80
            when idle_cost_percentage > 30 then 60
            when idle_cost_percentage > 20 then 40
            else 20
        end as optimization_priority
        
    from warehouse_analysis
    where avg_daily_cost > 1  -- Only include warehouses with meaningful costs
)

select * from optimization_recommendations
order by optimization_priority desc, potential_monthly_savings desc