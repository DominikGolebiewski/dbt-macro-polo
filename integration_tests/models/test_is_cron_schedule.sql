-- Test the is_cron_schedule macro
{% set test_cases = [
    {
        'name': 'cron_match',
        'schedule': 'midday',
        'cron_expr': '0 12 * * *',
        'current_time': modules.datetime.datetime(2025, 1, 1, 12, 0),
        'expected': true
    },
    {
        'name': 'cron_no_match',
        'schedule': 'morning',
        'cron_expr': '0 9 * * *',
        'current_time': modules.datetime.datetime(2025, 1, 1, 12, 0),
        'expected': false
    }
] %}

{% set failed_tests = [] %}
{% for test_case in test_cases %}
    {% set actual = dbt_macro_polo.is_cron_schedule(test_case.schedule, test_case.cron_expr, test_case.current_time) %}
    {% set expected = test_case.expected %}
    {% if actual != expected %}
        {% do failed_tests.append(test_case.name ~ ': Expected "' ~ expected ~ '", got "' ~ actual ~ '"') %}
    {% endif %}
{% endfor %}

{% if failed_tests | length > 0 %}
    {{ dbt_macro_polo.logging(message="Failed tests:\n" ~ failed_tests | join('\n'), level='ERROR') }}
{% endif %}

select
    case when count(*) = 0 then true else false end as test_passed
from (
    {% for test_case in test_cases %}
    select
        '{{ test_case.name }}' as test_name,
        {{ dbt_macro_polo.is_cron_schedule(test_case.schedule, test_case.cron_expr, test_case.current_time) }} as actual,
        {{ test_case.expected }} as expected
    where actual != expected
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
)
