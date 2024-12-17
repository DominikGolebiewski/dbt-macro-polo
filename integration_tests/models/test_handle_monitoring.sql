-- Test the handle_monitoring macro
{% set test_cases = [
    {
        'name': 'large_threshold_match',
        'row_count': 1500000,
        'thresholds': [
            {'rows': 1000000, 'warehouse_size': 'l'},
            {'rows': 100000, 'warehouse_size': 'm'},
            {'rows': 10000, 'warehouse_size': 's'}
        ],
        'expected': 'l'
    },
    {
        'name': 'medium_threshold_match',
        'row_count': 500000,
        'thresholds': [
            {'rows': 1000000, 'warehouse_size': 'l'},
            {'rows': 100000, 'warehouse_size': 'm'},
            {'rows': 10000, 'warehouse_size': 's'}
        ],
        'expected': 'm'
    },
    {
        'name': 'small_threshold_match',
        'row_count': 50000,
        'thresholds': [
            {'rows': 1000000, 'warehouse_size': 'l'},
            {'rows': 100000, 'warehouse_size': 'm'},
            {'rows': 10000, 'warehouse_size': 's'}
        ],
        'expected': 's'
    },
    {
        'name': 'no_threshold_match',
        'row_count': 5000,
        'thresholds': [
            {'rows': 1000000, 'warehouse_size': 'l'},
            {'rows': 100000, 'warehouse_size': 'm'},
            {'rows': 10000, 'warehouse_size': 's'}
        ],
        'expected': 'xs'
    }
] %}

{# Process test results #}
{% set failed_tests = [] %}
{% for test_case in test_cases %}
    {% set actual = dbt_macro_polo.handle_monitoring({}, test_case.row_count, test_case.thresholds, 'xs') %}
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
        '{{ dbt_macro_polo.handle_monitoring({}, test_case.row_count, test_case.thresholds, "xs") }}' as actual,
        '{{ test_case.expected }}' as expected
    where actual != expected
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
)