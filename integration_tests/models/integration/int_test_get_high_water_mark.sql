{{ config(materialized='view') }}

/*
    Integration Test: get_high_water_mark
    
    This test executes the macro against real seed data (seed_get_max_timestamp).
    It verifies that the macro constructs the correct query and returns the expected max timestamp.
    
    Dependencies: 
    - seed_get_max_timestamp (must be loaded)
*/

{% if execute %}
    {% set expected = '2024-01-05 00:00:00' %}
    {% set actual_quoted = dbt_macro_polo.get_high_water_mark(
        column_name='loaded_timestamp',
        model_name='seed_get_max_timestamp',
        warehouse_size='xs'
    ) %}
    
    {# Macro returns quoted string like "'2024-01-05 00:00:00'", remove quotes for comparison #}
    {% set actual = actual_quoted | replace("'", "") %}
{% else %}
    {% set actual = '2024-01-05 00:00:00' %}
    {% set expected = '2024-01-05 00:00:00' %}
{% endif %}

select 
    'get_high_water_mark_basic' as test_case,
    '{{ expected }}' as expected_value,
    '{{ actual }}' as actual_value

