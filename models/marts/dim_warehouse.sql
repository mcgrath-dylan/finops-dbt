{{
    config(
        materialized='view'
    )
}}

with metering as (

    select
        warehouse_id,
        warehouse_name,
        max(usage_hour_ntz) as last_metered_at
    from {{ ref('stg_warehouse_metering') }}
    group by warehouse_id, warehouse_name

),

query_sizes as (

    select
        warehouse_name,
        upper(replace(warehouse_size, ' ', '-')) as warehouse_size
    from {{ ref('stg_query_history') }}
    where warehouse_size is not null
    qualify row_number() over (
        partition by warehouse_name
        order by usage_hour_ntz desc
    ) = 1

),

latest_config as (

    select
        m.warehouse_id,
        m.warehouse_name,
        'STANDARD'                              as warehouse_type,
        q.warehouse_size,

        case q.warehouse_size
            when 'X-SMALL'   then 1
            when 'SMALL'     then 2
            when 'MEDIUM'    then 4
            when 'LARGE'     then 8
            when 'X-LARGE'   then 16
            when '2X-LARGE'  then 32
            when '3X-LARGE'  then 64
            when '4X-LARGE'  then 128
            else null
        end                                     as warehouse_size_credits_per_hour,

        null::number                            as auto_suspend_seconds,
        null::boolean                           as auto_resume,
        null::number                            as min_cluster_count,
        null::number                            as max_cluster_count,
        null::boolean                           as is_multi_cluster,
        null::varchar                           as scaling_policy,
        null::varchar                           as owner_role,
        null::timestamp_ntz                     as created_on,
        null::timestamp_ntz                     as dropped_on,
        true                                    as is_active,
        m.last_metered_at

    from metering m
    left join query_sizes q
        on m.warehouse_name = q.warehouse_name

)

select * from latest_config
