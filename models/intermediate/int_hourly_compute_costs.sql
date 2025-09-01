{{
    config(
        materialized='incremental',
        unique_key='cost_hour_key',
        on_schema_change='fail'
    )
}}

with metering as (
    select * from {{ ref('stg_warehouse_metering') }}
    {% if is_incremental() %}
        where hour_start > (select max(hour_start) from {{ this }})
    {% endif %}
),

queries as (
    select 
        warehouse_name,
        date_trunc('hour', START_TIME) as usage_hour,
        count(*) as query_count,
        sum(total_elapsed_seconds) as total_runtime_seconds,
        sum(gb_scanned) as total_gb_scanned,
        count(distinct user_name) as unique_users
    from {{ ref('stg_query_history') }}
    {% if is_incremental() %}
        where date_trunc('hour', END_TIME) >= (
            select coalesce(max(t.hour_start), '1900-01-01'::timestamp)
            from {{ this }} as t
        )
    {% endif %}
    group by 1, 2
),

hourly_costs as (
    select
        m.hour_start,
        m.hour_end,
        m.usage_date,
        m.warehouse_name,
        
        -- Actual costs from metering (authoritative)
        m.total_credits_used,
        m.total_cost_usd,
        m.compute_cost_usd,
        m.cloud_services_cost_usd,
        
        -- Query activity in this hour
        coalesce(q.query_count, 0) as queries_executed,
        coalesce(q.total_runtime_seconds, 0) as total_runtime_seconds,
        coalesce(q.total_gb_scanned, 0) as gb_scanned,
        coalesce(q.unique_users, 0) as unique_users,
        
        -- Efficiency metrics
        case 
            when q.query_count > 0 then m.total_cost_usd / q.query_count
            else null
        end as avg_cost_per_query,
        
        case
            when q.total_runtime_seconds > 0 then m.total_cost_usd / (q.total_runtime_seconds / 3600.0)
            else null
        end as cost_per_runtime_hour,
        
        -- Idle detection (no queries but credits consumed)
        case 
            when coalesce(q.query_count, 0) = 0 and m.total_credits_used > 0 then true
            else false
        end as is_potentially_idle,
        
        case 
            when coalesce(q.query_count, 0) = 0 and m.total_credits_used > 0 
            then m.total_cost_usd
            else 0
        end as idle_cost_usd,
        
        -- Composite key
        {{ dbt_utils.generate_surrogate_key(['m.warehouse_name', 'm.hour_start']) }} as cost_hour_key,
        
        current_timestamp() as _loaded_at
        
    from metering m
    left join queries q
        on m.warehouse_name = q.warehouse_name
       and m.hour_start    = q.usage_hour
)

select * from hourly_costs
