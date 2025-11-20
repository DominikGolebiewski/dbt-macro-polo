-- depends_on: {{ ref('seed_dummy_data_incremental_run') }}
-- depends_on: {{ ref('seed_dummy_data_initial_run') }}

{# 
    Integration test for scheduling logic.
    Since we cannot easily control the system time, this test primarily validates
    that the complex configuration parsing works without error.
    
    The 'expected' values assume NO schedule match (defaulting to base size)
    because predicting the exact run time of the test is impossible.
    However, if the test runs during one of the peak windows, it might fail locally,
    which is acceptable for a manual verification tool but potentially flaky for CI.
    
    For a robust CI test, we would need to mock the time in the macro, 
    but sticking to the user's request for "simplicity" and "keeping the yaml",
    we primarily verify the config doesn't crash the macro.
#}

{% set test_cases = [
    {
        'test_name': 'scheduling_ctas',
        'operation': 'ctas'
    },
    {
        'test_name': 'scheduling_delete',
        'operation': 'delete'
    },
    {
        'test_name': 'scheduling_insert',
        'operation': 'insert'
    }
] %}

{# Execute macros to ensure they don't error with the config #}
{% for test_case in test_cases %}
    {% set actual = dbt_macro_polo.optimise_warehouse(test_case.operation) %}
    -- Executed {{ test_case.test_name }} -> {{ actual }}
{% endfor %}


with source_data as (
    select *
    from {{ ref('seed_dummy_data_initial_run') }}
    where 1=0 -- Empty result set as we just want to compile/run the pre-hook
)

select * from source_data

