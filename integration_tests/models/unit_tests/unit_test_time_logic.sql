{{ config(materialized='view') }}

{# Test time range logic #}

{% set test_cases = [
    {'curr': '10:00', 'start': '09:00', 'end': '11:00', 'expected': 'True'},
    {'curr': '08:00', 'start': '09:00', 'end': '11:00', 'expected': 'False'},
    {'curr': '23:00', 'start': '22:00', 'end': '02:00', 'expected': 'True'},
    {'curr': '01:00', 'start': '22:00', 'end': '02:00', 'expected': 'True'}
] %}

{% for case in test_cases %}
    {% set curr_dt = modules.datetime.datetime.strptime(case.curr, '%H:%M') %}
    select
        '{{ case.curr }}' as current_time,
        '{{ case.expected }}' as expected_result,
        '{{ dbt_macro_polo.is_within_time_range("test", curr_dt, case.start, case.end) }}' as actual_result
    {% if not loop.last %}union all{% endif %}
{% endfor %}
