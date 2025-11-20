{% set test_cases = [
    {
        'name': 'Basic max timestamp retrieval',
        'timestamp_column': 'loaded_timestamp',
        'predicate': none,
        'warehouse_size': 'xs',
        'model_name': 'seed_get_max_timestamp',
        'expected': "'2024-01-05 00:00:00'::timestamp"
    },
    {
        'name': 'Max timestamp with custom column',
        'timestamp_column': 'custom_timestamp',
        'predicate': none,
        'warehouse_size': 'xs',
        'model_name': 'seed_get_max_timestamp',
        'expected': "'2024-01-05 12:00:00'::timestamp"
    },
    {
        'name': 'Max timestamp with predicate',
        'timestamp_column': 'loaded_timestamp',
        'predicate': "status = 'active'",
        'warehouse_size': 'xs',
        'model_name': 'seed_get_max_timestamp',
        'expected': "'2024-01-04 00:00:00'::timestamp"
    },
    {
        'name': 'Max timestamp from cache',
        'timestamp_column': 'loaded_timestamp',
        'predicate': none,
        'warehouse_size': 'xs',
        'model_name': 'seed_get_max_timestamp',
        'expected': "'2024-01-05 00:00:00'::timestamp"
    },
    {
        'name': 'Max timestamp from cache with custom column',
        'timestamp_column': 'custom_timestamp',
        'predicate': none,
        'warehouse_size': 's',
        'model_name': 'seed_get_max_timestamp',
        'expected': "'2024-01-05 12:00:00'::timestamp"
    },
    {
        'name': 'Max timestamp from cache with predicate',
        'timestamp_column': 'loaded_timestamp',
        'predicate': "status = 'active'",
        'warehouse_size': 'xs',
        'model_name': 'seed_get_max_timestamp',
        'expected': "'2024-01-04 00:00:00'::timestamp"
    }
] %}


{# Process test results #}
{% set failed_tests = [] %}
{% for test_case in test_cases %}
    {% set actual = dbt_macro_polo.get_high_water_mark(
        timestamp_column=test_case.timestamp_column,
        predicate=test_case.predicate,
        warehouse_size=test_case.warehouse_size,
        model_name=test_case.model_name
    ) | string %}
    
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
        '{{ dbt_macro_polo.get_high_water_mark(
            timestamp_column=test_case.timestamp_column,
            predicate=test_case.predicate,
            warehouse_size=test_case.warehouse_size,
            model_name=test_case.model_name
        ) }}' as actual,
        '{{ test_case.expected }}' as expected
    where actual != expected
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
)
