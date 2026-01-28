{{
    config(
        materialized= 'incremental',
        unique_key= 'hash',
        partition_by={
          "field": "block_timestamp",
          "data_type": "timestamp",
          "granularity": "day"
        },
        incremental_strategy='insert_overwrite'
    )
}}
select * from {{source('google_bitcoin', 'transactions')}} where block_timestamp > timestamp('2023-01-01') and block_timestamp <= current_timestamp()

{% if is_incremental() %}
    AND block_timestamp > (SELECT MAX(block_timestamp) FROM {{ this }} where block_timestamp >= timestamp_sub(current_timestamp(), interval 5 DAY))
{% endif %}