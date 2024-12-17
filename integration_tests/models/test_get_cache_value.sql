{% set test_cases = [
    {
        'name': 'Basic cache retrieval',
        'key': 'test_key_1',
        'expected': 'test_value_1'
    },
    {
        'name': 'Non-existent key',
        'key': 'non_existent_key',
        'expected': '{}'
    },
    {
        'name': 'Cache update',
        'key': 'new_key',
        'expected': 'new_value'
    }
] %}

{# Setup test cache data #}
{% do var('macro_polo', {}).get('cache', {}).update({'test_key_1': 'test_value_1'}) %}
{% do var('macro_polo', {}).get('cache', {}).update({'new_key': 'new_value'}) %}

{# Process test results #}
{% set failed_tests = [] %}
{% for test_case in test_cases %}
    {% set actual = dbt_macro_polo.get_cache_value(test_case.key) | string %}
    {% if actual != test_case.expected %}
        {% do failed_tests.append(
            test_case.name ~ ': Expected "' ~ test_case.expected ~ '", got "' ~ actual ~ '"'
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
        '{{ dbt_macro_polo.get_cache_value(test_case.key) }}' as actual,
        '{{ test_case.expected }}' as expected
    where actual != expected
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
)