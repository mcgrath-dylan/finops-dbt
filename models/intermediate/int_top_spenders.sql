{{
    config(
        materialized='table'
    )
}}

{#
  int_top_spenders
  Aggregates query activity to user x role x database x warehouse x day.
  In Pro mode, joins int_query_cost_attribution for estimated per-user cost.
  In Starter mode, cost columns are null; volume metrics are still available.
#}

with query_base as (

    select
        cast(usage_hour_ntz as date)            as usage_date,
        user_name,
        role_name,
        database_name,
        warehouse_name,
        query_id,
        total_elapsed_seconds,
        gb_scanned,
        rows_produced,
        runtime_category
    from {{ ref('stg_query_history') }}

),

{% if var('enable_pro_pack', false) %}
cost_attribution as (

    select
        query_id,
        estimated_query_cost_usd
    from {{ ref('int_query_cost_attribution') }}

),
{% endif %}

aggregated as (

    select
        q.usage_date,
        q.user_name,
        q.role_name,
        q.database_name,
        q.warehouse_name,

        count(q.query_id)                               as query_count,
        sum(q.total_elapsed_seconds)                    as total_runtime_seconds,
        avg(q.total_elapsed_seconds)                    as avg_runtime_seconds,
        sum(case when q.runtime_category = 'long_running' then 1 else 0 end)
                                                        as long_running_query_count,
        sum(q.gb_scanned)                               as gb_scanned,
        sum(q.rows_produced)                            as rows_produced,

        {% if var('enable_pro_pack', false) %}
        sum(ca.estimated_query_cost_usd)                as estimated_cost_usd,
        true                                            as has_cost_estimate
        {% else %}
        null::number(38,4)                              as estimated_cost_usd,
        false                                           as has_cost_estimate
        {% endif %}

    from query_base q
    {% if var('enable_pro_pack', false) %}
    left join cost_attribution ca on q.query_id = ca.query_id
    {% endif %}
    group by
        q.usage_date,
        q.user_name,
        q.role_name,
        q.database_name,
        q.warehouse_name

)

select * from aggregated
