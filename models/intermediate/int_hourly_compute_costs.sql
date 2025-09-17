{% set hourly_cost_pre_hooks = [] %}
{% if execute %}
  {% set existing_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.identifier) %}
  {% if existing_relation %}
    {% set existing_relation_str = existing_relation.render() %}
    {% set existing_columns = adapter.get_columns_in_relation(existing_relation) %}
    {% set existing_column_names = [] %}
    {% for col in existing_columns %}
      {% do existing_column_names.append(col.name | lower) %}
    {% endfor %}

    {% if 'usage_hour_ntz' not in existing_column_names %}
      {% do hourly_cost_pre_hooks.append("alter table " ~ existing_relation_str ~ " add column usage_hour_ntz timestamp_ntz") %}
    {% endif %}
    {% if 'warehouse_id' not in existing_column_names %}
      {% do hourly_cost_pre_hooks.append("alter table " ~ existing_relation_str ~ " add column warehouse_id number(38,0)") %}
    {% endif %}

    {% do hourly_cost_pre_hooks.append(
        "update " ~ existing_relation_str ~ " as target set usage_hour_ntz = date_trunc('hour', target.hour_end::timestamp_ntz) "
        ~ "where target.usage_hour_ntz is null or target.usage_hour_ntz is distinct from date_trunc('hour', target.hour_end::timestamp_ntz)"
    ) %}

    {% do hourly_cost_pre_hooks.append(
        "update " ~ existing_relation_str ~ " as target set usage_date = cast(date_trunc('hour', target.hour_end::timestamp_ntz) as date) "
        ~ "where target.usage_date is distinct from cast(date_trunc('hour', target.hour_end::timestamp_ntz) as date)"
    ) %}

    {% set metering_relation = ref('stg_warehouse_metering') %}
    {% set metering_relation_str = metering_relation.render() %}

    {% do hourly_cost_pre_hooks.append(
        "update " ~ existing_relation_str ~ " as target set warehouse_id = source.warehouse_id "
        ~ "from " ~ metering_relation_str ~ " as source "
        ~ "where (target.warehouse_id is null or target.warehouse_id <> source.warehouse_id) "
        ~ "and source.warehouse_name = target.warehouse_name "
        ~ "and date_trunc('hour', source.hour_end::timestamp_ntz) = date_trunc('hour', target.hour_end::timestamp_ntz)"
    ) %}

    {% set new_cost_hour_key = dbt_utils.generate_surrogate_key(['target.warehouse_id', 'target.usage_hour_ntz']) %}
    {% do hourly_cost_pre_hooks.append(
        "update " ~ existing_relation_str ~ " as target set cost_hour_key = " ~ new_cost_hour_key ~ " "
        ~ "where target.usage_hour_ntz is not null "
        ~ "and (target.cost_hour_key is null or target.cost_hour_key <> " ~ new_cost_hour_key ~ ")"
    ) %}
  {% endif %}
{% endif %}

{{
    config(
        materialized='incremental',
        unique_key='cost_hour_key',
        on_schema_change='sync_all_columns',
        pre_hook=hourly_cost_pre_hooks
    )
}}

{% set existing_cols = [] %}
{% if execute and is_incremental() %}
  {% set existing_cols = adapter.get_columns_in_relation(this) %}
{% endif %}
{% set existing_col_names = [] %}
{% for col in existing_cols %}
  {% do existing_col_names.append(col.name | lower) %}
{% endfor %}
{% set has_usage_hour_ntz = 'usage_hour_ntz' in existing_col_names %}

with metering as (
    select * from {{ ref('stg_warehouse_metering') }}
    {% if is_incremental() %}
        where usage_hour_ntz >= (
            select coalesce(
                dateadd(
                    'hour',
                    -1,
                    max(
                        {% if has_usage_hour_ntz %}
                            t.usage_hour_ntz
                        {% else %}
                            t.hour_start::timestamp_ntz
                        {% endif %}
                    )
                ),
                '1970-01-01'::timestamp_ntz
            )
            from {{ this }} as t
        )
    {% endif %}
),

queries as (
    select
        warehouse_name,
        usage_hour_ntz,
        count(*) as query_count,
        sum(total_elapsed_seconds) as total_runtime_seconds,
        sum(gb_scanned) as total_gb_scanned,
        count(distinct user_name) as unique_users
    from {{ ref('stg_query_history') }}
    {% if is_incremental() %}
        where usage_hour_ntz >= (
            select coalesce(
                dateadd(
                    'hour',
                    -1,
                    max(
                        {% if has_usage_hour_ntz %}
                            t.usage_hour_ntz
                        {% else %}
                            t.hour_start::timestamp_ntz
                        {% endif %}
                    )
                ),
                '1970-01-01'::timestamp_ntz
            )
            from {{ this }} as t
        )
    {% endif %}
    group by 1, 2
),

hourly_costs as (
    select
        m.usage_hour_ntz,
        m.hour_start,
        m.hour_end,
        cast(m.usage_hour_ntz as date) as usage_date,
        m.warehouse_id,
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
        {{ dbt_utils.generate_surrogate_key(['m.warehouse_id', 'm.usage_hour_ntz']) }} as cost_hour_key,

        current_timestamp() as _loaded_at

    from metering m
    left join queries q
        on m.warehouse_name = q.warehouse_name
       and m.usage_hour_ntz = q.usage_hour_ntz
)

select * from hourly_costs
