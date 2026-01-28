{{ config(materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={
      "field": "block_timestamp",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=["tx_hash"]
) }}


    select
        `hash` as tx_hash,
        block_hash,
        block_number,
        block_timestamp,
        input_count,
        output_count,
        input_value / 1e8 as input_value_btc,
        output_value / 1e8 as output_value_btc,
        fee / 1e8 as fee_btc,
        inputs,
        outputs,
        is_coinbase
    from {{ref('raw_transactions')}}

{% if is_incremental() %}
      -- Only process data from the last 2 days to handle late-arriving blocks
      where block_timestamp >= timestamp_sub(_dbt_max_partition, interval 2 day)
    {% endif %}