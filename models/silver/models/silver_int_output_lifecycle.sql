{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={
      "field": "report_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=["report_date"],
    on_schema_change='sync_all_columns'
) }}

with all_outputs as (
    select
        tx_hash,
        block_number,
        block_timestamp,
        date(block_timestamp) as created_at_date,
        output.index as output_index,
        output.value / 1e8 as value_btc,
        -- Handling cases where addresses might be an empty array
        case 
            when array_length(output.addresses) > 0 then output.addresses[offset(0)] 
            else null 
        end as address
    from {{ ref('silver_transactions') }},
    unnest(outputs) as output

    {% if is_incremental() %}
      -- Reprocess recent partitions to account for potential chain re-orgs
      where date(block_timestamp) >= date_sub(_dbt_max_partition, interval 3 day)
    {% endif %}
),

all_inputs as (
    select
        input.spent_transaction_hash,
        input.spent_output_index,
        date(block_timestamp) as spent_at_date
    from {{ ref('silver_transactions') }},
    unnest(inputs) as input
    where input.spent_transaction_hash is not null

    {% if is_incremental() %}
      -- We look at inputs from the same window to find what was spent recently
      where date(block_timestamp) >= date_sub(_dbt_max_partition, interval 3 day)
    {% endif %}
)

select
    out.*,
    inp.spent_at_date,
    case when inp.spent_at_date is null then true else false end as is_unspent
from all_outputs out
left join all_inputs inp
    on out.tx_hash = inp.spent_transaction_hash
    and out.output_index = inp.spent_output_index