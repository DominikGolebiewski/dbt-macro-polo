{% set test_cases = [
    {
        'name': 'Get existing state key',
        'key': 'test_key_1',
        'expected': 'test_value_1'
    },
    {
        'name': 'Get non-existent state key',
        'key': 'non_existent_key',
        'expected': 'None'
    },
    {
        'name': 'Get newly added state key',
        'key': 'new_key',
        'expected': 'new_value'
    }
] %}

{# Setup test data #}
{% do var('macro_polo').get('runtime_state', {}).update({'test_key_1': 'test_value_1'}) %}

{# Process test results #}
{% set failed_tests = [] %}
{% for test_case in test_cases %}
    {% if test_case.key == 'new_key' %}
        {% do var('macro_polo').get('runtime_state', {}).update({'new_key': 'new_value'}) %}
    {% endif %}

    {% set actual = dbt_macro_polo.get_runtime_state(test_case.key) | string %}
    
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
        '{{ dbt_macro_polo.get_runtime_state(test_case.key) }}' as actual,
        '{{ test_case.expected }}' as expected
    where actual != expected
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
)
