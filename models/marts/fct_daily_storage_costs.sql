{{
    config(
        materialized='table',
        contract={ 'enforced': true }
    )
}}

{#
  fct_daily_storage_costs
  Authoritative daily storage cost by database; counterpart to fct_daily_costs (compute).
  Grain: database x day.
  Adds MTD running totals and 30-day rolling averages for trend analysis.
#}

with storage as (

    select * from {{ ref('stg_storage_usage') }}

),

with_trends as (

    select
        {{ dbt_utils.generate_surrogate_key(['database_name', 'usage_date']) }}
                                                as daily_storage_key,

        usage_date,
        database_id,
        database_name,

        -- Storage volumes (TB)
        round(avg_database_tb, 6)               as active_storage_tb,
        round(avg_failsafe_tb, 6)               as failsafe_storage_tb,
        round(avg_stage_tb, 6)                  as stage_storage_tb,
        round(total_storage_tb, 6)              as total_storage_tb,

        -- Daily cost components
        round(estimated_active_cost_usd, 9)     as estimated_active_cost_usd,
        round(estimated_failsafe_cost_usd, 9)   as estimated_failsafe_cost_usd,
        round(estimated_stage_cost_usd, 9)      as estimated_stage_cost_usd,
        round(estimated_storage_cost_usd, 9)    as estimated_storage_cost_usd,

        -- Month-to-date (running sum within each database x calendar month)
        round(sum(estimated_storage_cost_usd) over (
            partition by database_name, date_trunc('month', usage_date)
            order by usage_date
            rows between unbounded preceding and current row
        ), 9)                                   as month_to_date_storage_cost,

        -- 30-day rolling average
        round(avg(estimated_storage_cost_usd) over (
            partition by database_name
            order by usage_date
            rows between 29 preceding and current row
        ), 9)                                   as storage_cost_30day_avg

    from storage

)

select * from with_trends
