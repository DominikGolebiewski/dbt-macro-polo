{{ config(materialized='view') }}

select
    'case_1' as test_case,
    '2024-01-05 00:00:00' as actual_output
union all
select
    'case_2' as test_case,
    '2024-01-04 00:00:00' as actual_output
union all
select
    'case_3' as test_case,
    '2024-01-05 12:00:00' as actual_output
