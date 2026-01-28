# Bitcoin On-Chain Analytics (BigQuery + dbt)
## Overview

This project builds a Bitcoin on-chain analytics warehouse using Google BigQuery and dbt.
It models raw blockchain data from the public crypto_bitcoin BigQuery dataset into analytics-ready metrics using a medallion architecture.

The focus is on correct Bitcoin data modeling, UTXO-aware transformations, and warehouse-first analytics.

## Tech Stack

Data Warehouse: Google BigQuery

## Source Data: bigquery-public-data.crypto_bitcoin

Transformation: dbt (SQL)

Architecture: Medallion (Bronze / Silver / Gold)

Data Source

Public Bitcoin blockchain data hosted in BigQuery

Includes blocks, transactions, inputs, and outputs

No assumed fields (e.g. block height, subsidy, difficulty)

All economic metrics are derived directly from transaction and output data.

Architecture
🥉 Bronze

Raw views over BigQuery public tables

No transformations

🥈 Silver

Canonical blockchain entities

UTXO-aware models

Re-org-safe incremental processing

Examples:

silver_blocks

silver_transactions

silver_outputs

silver_inputs

🥇 Gold

Aggregated, analytics-ready tables

Daily Bitcoin network and economic metrics

Stable schemas for BI tools

Metrics Covered

Daily transaction count

Blocks per day

On-chain BTC transferred

Miner revenue (coinbase-based)

Transaction fee trends

Average BTC per transaction

All metrics follow UTXO-correct accounting.

dbt Usage

SQL-only transformations

Partitioned and clustered BigQuery tables

Incremental models using insert_overwrite

Use Cases

Bitcoin on-chain analysis

Analytics engineering portfolio project

Foundation for Looker dashboards

Crypto / fintech data engineering workflows
