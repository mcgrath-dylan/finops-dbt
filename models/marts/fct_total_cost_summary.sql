{{
    config(
        materialized='table'
    )
}}

{#
  fct_total_cost_summary
  Single source of truth for total Snowflake spend, broken out by cost category per day.
  Unions: COMPUTE + STORAGE + AUTO_CLUSTERING (Pro) + SERVERLESS (future).
  Grain: cost_category x usage_date.
#}

with compute_costs as (

    select
        usage_date,
        'COMPUTE'                       as cost_category,
        sum(compute_credits)            as credits_used,
        sum(total_cost)                 as cost_usd
    from {{ ref('fct_daily_costs') }}
    group by usage_date

),

storage_costs as (

    select
        usage_date,
        'STORAGE'                       as cost_category,
        null::number(38,4)              as credits_used,
        sum(estimated_storage_cost_usd) as cost_usd
    from {{ ref('fct_daily_storage_costs') }}
    group by usage_date

),

{% if var('enable_pro_pack', false) %}
auto_clustering_costs as (

    select
        cast(usage_hour as date)        as usage_date,
        'AUTO_CLUSTERING'               as cost_category,
        sum(credits_used)               as credits_used,
        sum(credits_used) * {{ var('cost_per_credit', 3.0) }}
                                        as cost_usd
    from {{ ref('stg_automatic_clustering_history') }}
    group by cast(usage_hour as date)

),
{% endif %}

all_costs as (

    select * from compute_costs
    union all
    select * from storage_costs
    {% if var('enable_pro_pack', false) %}
    union all
    select * from auto_clustering_costs
    {% endif %}

),

with_totals as (

    select
        {{ dbt_utils.generate_surrogate_key(['cost_category', 'usage_date']) }}
                                        as total_cost_summary_id,

        usage_date,
        cost_category,
        credits_used,
        cost_usd,

        round(
            100.0 * cost_usd / nullif(sum(cost_usd) over (partition by usage_date), 0),
            2
        )                               as pct_of_daily_total,

        sum(cost_usd) over (
            partition by cost_category, date_trunc('month', usage_date)
            order by usage_date
            rows between unbounded preceding and current row
        )                               as mtd_cost_usd

    from all_costs
    where cost_usd is not null
      and cost_usd > 0

)

select * from with_totals
