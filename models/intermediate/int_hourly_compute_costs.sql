{{
    config(
        materialized='incremental',
        unique_key='cost_hour_key',
        on_schema_change='sync_all_columns'
    )
}}

{% set hourly_costs_watermark_column = 'usage_hour_ntz' %}
{% set existing_column_names = [] %}
{% if is_incremental() and execute %}
  {% set existing_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.identifier) %}
  {% if existing_relation %}
    {% set existing_columns = adapter.get_columns_in_relation(existing_relation) %}
    {% for col in existing_columns %}
      {% do existing_column_names.append(col.name | lower) %}
    {% endfor %}

    {% if 'usage_hour_ntz' not in existing_column_names %}
      {% do run_query('alter table ' ~ existing_relation ~ ' add column usage_hour_ntz timestamp_ntz') %}
      {% do existing_column_names.append('usage_hour_ntz') %}
    {% endif %}
    {% if 'warehouse_id' not in existing_column_names %}
      {% do run_query('alter table ' ~ existing_relation ~ ' add column warehouse_id number') %}
      {% do existing_column_names.append('warehouse_id') %}
    {% endif %}

    {% if 'hour_start' in existing_column_names %}
      {% set backfill_usage_sql %}
        update {{ existing_relation }}
        set usage_hour_ntz = coalesce(usage_hour_ntz, {{ ntz_hour('hour_start') }})
        where usage_hour_ntz is null
      {% endset %}
      {% do run_query(backfill_usage_sql) %}
    {% endif %}

    {% if 'warehouse_id' in existing_column_names %}
      {% set backfill_wh_sql %}
        update {{ existing_relation }} as target
        set warehouse_id = src.warehouse_id
        from {{ ref('stg_warehouse_metering') }} as src
        where target.warehouse_id is null
          and target.warehouse_name = src.warehouse_name
          and coalesce(target.usage_hour_ntz, {{ ntz_hour('target.hour_start') }}) = coalesce(src.usage_hour_ntz, {{ ntz_hour('src.hour_end') }})
      {% endset %}
      {% do run_query(backfill_wh_sql) %}
    {% endif %}

    {% if 'usage_hour_ntz' in existing_column_names and 'warehouse_id' in existing_column_names %}
      {% set sync_key_sql %}
        update {{ existing_relation }} as target
        set cost_hour_key = {{ dbt_utils.generate_surrogate_key(['target.warehouse_id', 'target.usage_hour_ntz']) }}
        where target.usage_hour_ntz is not null
          and target.warehouse_id is not null
      {% endset %}
      {% do run_query(sync_key_sql) %}
    {% endif %}

    {% if 'usage_hour_ntz' in existing_column_names %}
      {% set hourly_costs_watermark_column = 'usage_hour_ntz' %}
    {% elif 'hour_start' in existing_column_names %}
      {% set hourly_costs_watermark_column = 'hour_start' %}
    {% else %}
      {% set hourly_costs_watermark_column = none %}
    {% endif %}
  {% else %}
    {% set hourly_costs_watermark_column = none %}
  {% endif %}
{% endif %}

with metering as (
    select *
    from {{ ref('stg_warehouse_metering') }}
    {% if is_incremental() %}
        {% if hourly_costs_watermark_column == 'usage_hour_ntz' %}
            where usage_hour_ntz >= (
                select coalesce(
                    dateadd('hour', -1, max(usage_hour_ntz)),
                    '1970-01-01'::timestamp_ntz
                )
                from {{ this }}
            )
        {% elif hourly_costs_watermark_column == 'hour_start' %}
            where usage_hour_ntz >= (
                select coalesce(
                    dateadd('hour', -1, max(hour_start::timestamp_ntz)),
                    '1970-01-01'::timestamp_ntz
                )
                from {{ this }}
            )
        {% else %}
            where usage_hour_ntz >= '1970-01-01'::timestamp_ntz
        {% endif %}
    {% endif %}
),

queries as (
    select
        warehouse_name,
        usage_hour_ntz,
        count(*)                         as query_count,
        sum(total_elapsed_seconds)       as total_runtime_seconds,
        sum(gb_scanned)                  as total_gb_scanned,
        count(distinct user_name)        as unique_users
    from {{ ref('stg_query_history') }}
    {% if is_incremental() %}
        {% if hourly_costs_watermark_column == 'usage_hour_ntz' %}
            where usage_hour_ntz >= (
                select coalesce(
                    dateadd('hour', -1, max(usage_hour_ntz)),
                    '1970-01-01'::timestamp_ntz
                )
                from {{ this }}
            )
        {% elif hourly_costs_watermark_column == 'hour_start' %}
            where usage_hour_ntz >= (
                select coalesce(
                    dateadd('hour', -1, max(hour_start::timestamp_ntz)),
                    '1970-01-01'::timestamp_ntz
                )
                from {{ this }}
            )
        {% else %}
            where usage_hour_ntz >= '1970-01-01'::timestamp_ntz
        {% endif %}
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
        coalesce(q.query_count, 0)           as queries_executed,
        coalesce(q.total_runtime_seconds, 0) as total_runtime_seconds,
        coalesce(q.total_gb_scanned, 0)      as gb_scanned,
        coalesce(q.unique_users, 0)          as unique_users,

        -- Efficiency metrics
        case
            when coalesce(q.query_count, 0) > 0 then m.total_cost_usd / q.query_count
            else null
        end as avg_cost_per_query,

        case
            when coalesce(q.total_runtime_seconds, 0) > 0 then m.total_cost_usd / (q.total_runtime_seconds / 3600.0)
            else null
        end as cost_per_runtime_hour,

        -- Idle detection (no queries but credits consumed)
        case
            when coalesce(q.query_count, 0) = 0 and m.total_credits_used > 0 then true
            else false
        end as is_potentially_idle,

        case
            when coalesce(q.query_count, 0) = 0 and m.total_credits_used > 0 then m.total_cost_usd
            else 0
        end as idle_cost_usd,

        -- Composite key
        {{ dbt_utils.generate_surrogate_key(['m.warehouse_id', 'm.usage_hour_ntz']) }} as cost_hour_key,

        cast(current_timestamp() as timestamp_ntz) as _loaded_at
    from metering m
    left join queries q
        on m.warehouse_name = q.warehouse_name
       and m.usage_hour_ntz = q.usage_hour_ntz
)

select * from hourly_costs
