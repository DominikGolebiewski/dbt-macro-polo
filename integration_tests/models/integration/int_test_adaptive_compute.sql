{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='id',
    timestamp_column='run_started_time',
    pre_hook=[ "{{ dbt_macro_polo.adaptive_compute() }}" ]
) }}

{# 
   Integration Test: Adaptive Compute (End-to-End)
   
   Verifies that the macro runs successfully as a pre-hook and allows the model to build.
   We verify the logic via the previous unit tests. This test ensures no runtime exceptions 
   occur during actual dbt execution on the warehouse.
#}

with source_data as (
    select * from {{ ref('seed_dummy_data_initial_run') }}
)

select 
    date,
    product,
    run_started_time,
    units_sold,
    revenue,
    profit_margin,
    -- Generate a unique key
    md5(date::varchar || product) as id
from source_data

{% if is_incremental() %}
    where run_started_time > (select max(run_started_time) from {{ this }})
{% endif %}

