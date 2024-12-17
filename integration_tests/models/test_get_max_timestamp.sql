{% set test_cases = [
    {
        'name': 'Basic timestamp retrieval',
        'timestamp_column': 'loaded_timestamp',
        'model_name': 'seed_get_max_timestamp',
        'predicate': none,
        'warehouse_size': 'xs',
        'expected': "'2024-01-05 00:00:00'::timestamp"
    },
    {
        'name': 'Custom column timestamp retrieval',
        'timestamp_column': 'custom_timestamp',
        'model_name': 'seed_get_max_timestamp',
        'predicate': none,
        'warehouse_size': 'xs',
        'expected': "'2024-01-05 12:00:00'::timestamp"
    },
    {
        'name': 'Timestamp retrieval with predicate',
        'timestamp_column': 'loaded_timestamp',
        'model_name': 'seed_get_max_timestamp',
        'predicate': "status = 'active'",
        'warehouse_size': 'xs',
        'expected': "'2024-01-04 00:00:00'::timestamp"
    },
    {
        'name': 'Timestamp retrieval with custom warehouse',
        'timestamp_column': 'loaded_timestamp',
        'model_name': 'seed_get_max_timestamp',
        'predicate': none,
        'warehouse_size': 's',
        'expected': "'2024-01-05 00:00:00'::timestamp"
    },
    {
        'name': 'Timestamp retrieval with all parameters',
        'timestamp_column': 'custom_timestamp',
        'model_name': 'seed_get_max_timestamp',
        'predicate': "status = 'active'",
        'warehouse_size': 's',
        'expected': "'2024-01-04 12:00:00'::timestamp"
    }
] %}

{# Process test results #}
{% set failed_tests = [] %}
{% for test_case in test_cases %}
    {% set actual = dbt_macro_polo.get_max_timestamp(
        timestamp_column=test_case.timestamp_column,
        model_name=test_case.model_name,
        predicate=test_case.predicate,
        warehouse_size=test_case.warehouse_size
    ) %}
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
        {{ dbt_macro_polo.get_max_timestamp(
            timestamp_column=test_case.timestamp_column,
            model_name=test_case.model_name,
            predicate=test_case.predicate,
            warehouse_size=test_case.warehouse_size
        ) }} as actual,
        {{ test_case.expected }} as expected
    where actual != expected
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
)