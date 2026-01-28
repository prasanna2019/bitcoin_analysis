{{ config(
    materialized = 'table',
    partition_by = {
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by = ["date"],
    require_partition_filter = true,
    on_schema_change = 'fail',
    tags = ["gold", "bitcoin", "economic", "kpi"],
    persist_docs = {"relation": true, "columns": true}
) }}

WITH issuance AS (
    SELECT
        DATE(block_timestamp) AS date,
        SUM(output_value_btc) AS issued_btc,
        COUNT(DISTINCT block_number) AS blocks_mined
    FROM {{ ref('silver_transactions') }}
    WHERE is_coinbase = true
    GROUP BY 1
),

fees AS (
    SELECT
        DATE(block_timestamp) AS date,
        SUM(fee_btc) AS fees_btc,
        COUNT(*) AS tx_count
    FROM {{ ref('silver_transactions') }}
    WHERE is_coinbase = false 
    GROUP BY 1
),

value_transfer AS (
    SELECT
        created_at_date AS date,
        SUM(value_btc) AS btc_transferred
    FROM {{ ref('silver_int_output_lifecycle') }}
    GROUP BY 1
)

SELECT
    i.date,
    i.issued_btc,
    COALESCE(f.fees_btc, 0) AS fees_btc,
    -- Total Miner Revenue = New Subsidy + Market Fees
    (i.issued_btc + COALESCE(f.fees_btc, 0)) AS miner_revenue_btc,

    SAFE_DIVIDE(
        COALESCE(f.fees_btc, 0),
        NULLIF(i.issued_btc + COALESCE(f.fees_btc, 0), 0)
    ) AS fee_share_of_revenue,

    COALESCE(f.tx_count, 0) AS tx_count,
    i.blocks_mined,
    COALESCE(v.btc_transferred, 0) AS btc_transferred,

    SAFE_DIVIDE(COALESCE(f.fees_btc, 0), NULLIF(f.tx_count, 0)) AS avg_fee_per_tx_btc,
    SAFE_DIVIDE(COALESCE(v.btc_transferred, 0), NULLIF(f.tx_count, 0)) AS avg_btc_transferred_per_tx

FROM issuance i
LEFT JOIN fees f USING (date)
LEFT JOIN value_transfer v USING (date)