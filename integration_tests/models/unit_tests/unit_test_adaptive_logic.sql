{{ config(materialized='view') }}

{#
   Test the logic of 'determine_optimal_size' independently of actual table volumes.
   We mock the configuration and pass explicit volume integers.
#}

{% set threshold_config = {
    'warehouse_size': 'xs',
    'volume_based_scaling': {
        'enabled': true,
        'thresholds': [
            {'rows': 100, 'warehouse_size': 's'},
            {'rows': 1000, 'warehouse_size': 'm'}
        ]
    }
} %}

{% set test_cases = [
    {'volume': 10,   'expected': 'xs'},
    {'volume': 150,  'expected': 's'},
    {'volume': 2000, 'expected': 'm'}
] %}

{% for case in test_cases %}
    select
        {{ case.volume }} as input_volume,
        '{{ dbt_macro_polo.determine_optimal_size(threshold_config, case.volume, "unit_test_model") }}' as actual_size
    {% if not loop.last %}union all{% endif %}
{% endfor %}
