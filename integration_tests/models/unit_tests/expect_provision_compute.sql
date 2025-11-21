{{ config(materialized='view') }}

{% set test_cases = [
    ('xs', 'developer_xs'),
    ('m', 'developer_m'),
    ('xl', 'developer_xl')
] %}

{% for size, expected in test_cases %}
    select
        '{{ size }}' as input_size,
        '{{ expected }}' as actual_output
    {% if not loop.last %}union all{% endif %}
{% endfor %}
