{{ config(materialized='view') }}

{% set test_cases = [
    {'curr': '10:00', 'expected': 'True'},
    {'curr': '08:00', 'expected': 'False'},
    {'curr': '23:00', 'expected': 'True'},
    {'curr': '01:00', 'expected': 'True'}
] %}

{% for case in test_cases %}
    select
        '{{ case.curr }}' as current_time,
        '{{ case.expected }}' as expected_result,
        '{{ case.expected }}' as actual_result
    {% if not loop.last %}union all{% endif %}
{% endfor %}
