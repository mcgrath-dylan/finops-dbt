{{
    config(
        materialized='table'
    )
}}

{#
  fct_top_spenders
  Ranked leaderboard of users by daily cost / query volume.
  Grain: user_name x usage_date.
  Rolls up int_top_spenders (user x role x db x warehouse x day) to user x day.
#}

with primary_warehouse as (

    -- For each user x day, find the warehouse with the highest query count
    select
        usage_date,
        user_name,
        warehouse_name                          as primary_warehouse_name
    from {{ ref('int_top_spenders') }}
    qualify row_number() over (
        partition by user_name, usage_date
        order by query_count desc
    ) = 1

),

spenders as (

    select
        t.usage_date,
        t.user_name,
        sum(t.query_count)                      as query_count,
        sum(t.total_runtime_seconds)            as total_runtime_seconds,
        sum(t.long_running_query_count)         as long_running_query_count,
        sum(t.gb_scanned)                       as gb_scanned,
        sum(t.estimated_cost_usd)               as estimated_cost_usd,
        max(t.has_cost_estimate::int)::boolean  as has_cost_estimate
    from {{ ref('int_top_spenders') }} t
    group by t.usage_date, t.user_name

),

with_rankings as (

    select
        {{ dbt_utils.generate_surrogate_key(['s.user_name', 's.usage_date']) }}
                                                as top_spender_id,

        s.usage_date,
        s.user_name,
        pw.primary_warehouse_name,
        s.query_count,
        s.total_runtime_seconds,
        s.long_running_query_count,
        s.gb_scanned,
        s.estimated_cost_usd,
        s.has_cost_estimate,

        -- Rankings within each day
        rank() over (partition by s.usage_date order by s.query_count desc)
                                                as rank_by_query_count,
        rank() over (partition by s.usage_date order by s.total_runtime_seconds desc)
                                                as rank_by_runtime,
        rank() over (
            partition by s.usage_date
            order by s.estimated_cost_usd desc nulls last
        )                                       as rank_by_cost,

        -- Share of daily totals
        round(
            100.0 * s.query_count
            / nullif(sum(s.query_count) over (partition by s.usage_date), 0),
        2)                                      as pct_of_daily_query_total,

        round(
            100.0 * s.estimated_cost_usd
            / nullif(sum(s.estimated_cost_usd) over (partition by s.usage_date), 0),
        2)                                      as pct_of_daily_cost,

        -- 7-day rolling cost per user
        sum(s.estimated_cost_usd) over (
            partition by s.user_name
            order by s.usage_date
            rows between 6 preceding and current row
        )                                       as rolling_7d_cost_usd

    from spenders s
    left join primary_warehouse pw
        on s.user_name = pw.user_name
        and s.usage_date = pw.usage_date

)

select * from with_rankings
