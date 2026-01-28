{{ config(
    materialized = 'table',

    partition_by = {
      "field": "timestamp",
      "data_type": "timestamp",
      "granularity": "day"
    },

    cluster_by = ["block_date"],

    require_partition_filter = true,

    on_schema_change = 'fail',

    persist_docs = {
      "relation": true,
      "columns": true
    }
) }}

SELECT
    `hash` AS block_hash,
    timestamp,
    DATE(timestamp) AS block_date,
    size AS size_bytes,
    weight AS weight_units,
    transaction_count,
    bits,
    nonce,
    version,
    merkle_root,
    SAFE_DIVIDE(size, transaction_count) AS avg_tx_size_bytes,
    CURRENT_TIMESTAMP() AS ingested_at
FROM {{ ref('raw_blocks') }}
