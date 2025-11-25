{{ config(materialized='view') }}

{% if execute %}
    {% set seed_model_name = 'seed_get_max_timestamp' %}

    -- Case 1: Simple Max
    {% set actual_1 = dbt_macro_polo.get_high_water_mark('loaded_timestamp', model_name=seed_model_name) %}

    -- Case 2: With Predicate
    {% set actual_2 = dbt_macro_polo.get_high_water_mark('loaded_timestamp', predicate="status = 'active'", model_name=seed_model_name) %}

    -- Case 3: Custom Timestamp
    {% set actual_3 = dbt_macro_polo.get_high_water_mark('custom_timestamp', model_name=seed_model_name) %}
{% else %}
    -- Dummy values for compilation to prevent errors when execute is false
    {% set actual_1 = "'2024-01-05 00:00:00'" %}
    {% set actual_2 = "'2024-01-04 00:00:00'" %}
    {% set actual_3 = "'2024-01-05 12:00:00'" %}
{% endif %}

select
    'case_1' as test_case,
    cast({{ actual_1 }} as varchar) as actual_output
union all
select
    'case_2' as test_case,
    cast({{ actual_2 }} as varchar) as actual_output
union all
select
    'case_3' as test_case,
    cast({{ actual_3 }} as varchar) as actual_output
