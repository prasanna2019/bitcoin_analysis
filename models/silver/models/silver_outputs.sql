{{ config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',

    partition_by = {
      "field": "created_at_date",
      "data_type": "date",
      "granularity": "day"
    },

    cluster_by = ["created_at_date"],

    require_partition_filter = true,

    on_schema_change = 'fail'
) }}

WITH outputs AS (
    SELECT
        tx.tx_hash AS tx_hash,
        tx.block_number,
        tx.block_timestamp,
        DATE(tx.block_timestamp) AS created_at_date,
        o.index AS output_index,
        o.value / 1e8 AS value_btc,
        CASE
            WHEN ARRAY_LENGTH(o.addresses) > 0
                THEN o.addresses[OFFSET(0)]
            ELSE NULL
        END AS address
    FROM {{ ref('silver_transactions') }} AS tx
    CROSS JOIN UNNEST(tx.outputs) AS o

    {% if is_incremental() %}
      WHERE DATE(tx.block_timestamp) >= DATE_SUB(_dbt_max_partition, INTERVAL 3 DAY)
    {% endif %}
)

SELECT
    tx_hash,
    block_number,
    block_timestamp,
    created_at_date,
    output_index,
    value_btc,
    address,
    CURRENT_TIMESTAMP() AS ingested_at
FROM outputs
