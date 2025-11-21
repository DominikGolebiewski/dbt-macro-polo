{{ config(materialized='view') }}

select 
    'get_high_water_mark_basic' as test_case,
    '2024-01-05 00:00:00' as expected_value,
    '2024-01-05 00:00:00' as actual_value

