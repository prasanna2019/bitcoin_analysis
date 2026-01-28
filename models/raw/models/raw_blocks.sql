{{config(
    materialized= 'incremental',
    incremental_strategy= 'insert_overwrite',
    partition_by={
        'field':'timestamp_month',
        'data_type': 'date',
          'granularity': 'day'

    },
    unique_key= 'hash'
)
}}

select * from {{source('google_bitcoin', 'blocks')}} where timestamp_month > date('2023-01-01') and timestamp_month <= current_date()

{% if is_incremental() %}
    AND timestamp_month > (SELECT MAX(timestamp_month) FROM {{ this }} where timestamp_month >= date_sub(current_date(), interval 5 DAY))
{% endif %}
