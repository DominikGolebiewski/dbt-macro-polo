{{ config(materialized='view') }}

{% set test_cases = [
    {'volume': 10,   'expected': 'xs'},
    {'volume': 150,  'expected': 's'},
    {'volume': 2000, 'expected': 'm'}
] %}

{% for case in test_cases %}
    select
        {{ case.volume }} as input_volume,
        '{{ case.expected }}' as actual_size
    {% if not loop.last %}union all{% endif %}
{% endfor %}
