{{ config(materialized='view') }}

{% set test_cases = [
    ('xs', 'developer_xs'),
    ('m', 'developer_m'),
    ('xl', 'developer_xl')
] %}

{% for size, expected in test_cases %}
    select 
        '{{ size }}' as input_size,
        '{{ dbt_macro_polo.provision_compute(size) }}' as actual_output
    {% if not loop.last %}union all{% endif %}
{% endfor %}
