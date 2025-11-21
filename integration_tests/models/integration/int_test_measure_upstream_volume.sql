-- depends_on: {{ ref('seed_dummy_data_initial_run') }}
{{ config(materialized='view') }}

/*
    Integration Test: measure_upstream_volume

    This test verifies that the macro correctly counts rows in an upstream source/model.

    Dependencies:
    - seed_dummy_data_initial_run (12 rows)
*/

{% if execute %}
    {% set expected_volume = 12 %}
    {% set actual_volume = dbt_macro_polo.measure_upstream_volume(
        model_id='test_volume',
        upstream_dependency='seed_dummy_data_initial_run',
        timestamp_column='run_started_time'
    ) %}
{% else %}
    {% set expected_volume = 12 %}
    {% set actual_volume = 12 %}
{% endif %}

select
    'measure_upstream_volume_basic' as test_case,
    {{ expected_volume }} as expected_value,
    {{ actual_volume }} as actual_value
