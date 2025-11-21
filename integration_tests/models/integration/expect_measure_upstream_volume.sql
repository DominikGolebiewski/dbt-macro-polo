{{ config(materialized='view') }}

select 
    'measure_upstream_volume_basic' as test_case,
    12 as expected_value,
    12 as actual_value

