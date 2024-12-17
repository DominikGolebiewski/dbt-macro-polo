-- Test the is_within_time_range macro
{% set test_cases = [
    {
        'name': 'within_business_hours',
        'schedule': 'business_hours',
        'current_time': modules.datetime.datetime(2024, 1, 1, 14, 30),
        'start_time': '09:00',
        'end_time': '17:00',
        'expected': true
    },
    {
        'name': 'outside_business_hours',
        'schedule': 'business_hours',
        'current_time': modules.datetime.datetime(2024, 1, 1, 8, 30),
        'start_time': '09:00',
        'end_time': '17:00',
        'expected': false
    }
] %}

{# Process test results #}
{% set failed_tests = [] %}
{% for test_case in test_cases %}
    {% set actual = dbt_macro_polo.is_within_time_range(test_case.schedule, test_case.current_time, test_case.start_time, test_case.end_time) %}
    {% set expected = test_case.expected %}
    {% if actual != expected %}
        {% do failed_tests.append(
            test_case.name ~ ': Expected "' ~ expected ~ '", got "' ~ actual ~ '"'
        ) %}
    {% endif %}
{% endfor %}

{# Report results #}
{% if failed_tests | length > 0 %}
    {{ dbt_macro_polo.logging(message="Failed tests:\n" ~ failed_tests | join('\n'), level='ERROR') }}
{% endif %}

select 
    case when count(*) = 0 then true else false end as test_passed
from (
    {% for test_case in test_cases %}
    select 
        '{{ test_case.name }}' as test_name,
        {{ dbt_macro_polo.is_within_time_range(test_case.schedule, test_case.current_time, test_case.start_time, test_case.end_time) }} as actual,
        {{ test_case.expected }} as expected
    where actual != expected
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
)