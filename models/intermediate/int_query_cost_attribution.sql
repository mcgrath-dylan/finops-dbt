{{
    config(
        materialized='incremental',
        unique_key='query_id',
        on_schema_change='fail',
        enabled=var('enable_pro_pack', false)
    )
}}

-- IMPORTANT: Estimated attribution only (for analysis). Billing truth is metering history.

with queries as (
    select
        query_id,
        warehouse_name,
        user_name,
        role_name,
        database_name,
        query_type,
        START_TIME as query_start_time,
        END_TIME   as query_end_time,
        date_trunc('hour', START_TIME) as usage_hour,
        total_elapsed_seconds,
        gb_scanned,
        rows_produced,
        runtime_category
    from {{ ref('stg_query_history') }}
    {% if is_incremental() %}
      where date(END_TIME) >= (
        select coalesce(max(date(t.query_end_time)), '1900-01-01'::date)
        from {{ this }} as t
      )
    {% endif %}
),

hourly_costs as (
    select * from {{ ref('int_hourly_compute_costs') }}
),

query_share as (
    select
        q.*,
        sum(q.total_elapsed_seconds) over (
            partition by q.warehouse_name, q.usage_hour
        ) as hour_total_runtime,
        case 
            when sum(q.total_elapsed_seconds) over (partition by q.warehouse_name, q.usage_hour) > 0
            then q.total_elapsed_seconds 
                 / sum(q.total_elapsed_seconds) over (partition by q.warehouse_name, q.usage_hour)
            else 0
        end as runtime_share_of_hour
    from queries q
),

attributed_costs as (
    select
        qs.*,
        hc.total_cost_usd     as warehouse_hour_cost,
        hc.total_credits_used as warehouse_hour_credits,

        -- Estimated attribution based on runtime share
        round(hc.total_cost_usd     * qs.runtime_share_of_hour, 4) as estimated_query_cost_usd,
        round(hc.total_credits_used * qs.runtime_share_of_hour, 6) as estimated_query_credits,

        case when qs.gb_scanned    > 0 then round((hc.total_cost_usd * qs.runtime_share_of_hour) / qs.gb_scanned, 4) end as estimated_cost_per_gb,
        case when qs.rows_produced > 0 then round((hc.total_cost_usd * qs.runtime_share_of_hour) / qs.rows_produced * 1000000, 6) end as estimated_cost_per_million_rows,

        current_timestamp() as _loaded_at
    from query_share qs
    left join hourly_costs hc
      on qs.warehouse_name = hc.warehouse_name
     and qs.usage_hour     = hc.hour_start
)

select * 
from attributed_costs
where estimated_query_cost_usd > 0
