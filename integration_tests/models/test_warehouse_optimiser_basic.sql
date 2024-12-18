-- depends_on: {{ ref('seed_dummy_data_incremental_run') }}
-- depends_on: {{ ref('seed_dummy_data_initial_run') }}



{# Process test cases #}
{% set failed_tests = [] %}
{% set test_cases = [
    {
        'test_name': 'basic_ctas_operation',
        'operation': 'ctas',
        'expected': 'use warehouse developer_' ~ ('s' if not dbt_macro_polo.should_full_refresh() else 'm')
    },
    {
        'test_name': 'basic_delete_operation',
        'operation': 'delete',
        'expected': 'use warehouse developer_' ~ ('s' if not dbt_macro_polo.should_full_refresh() else 'm')
    },
    {
        'test_name': 'basic_insert_operation',
        'operation': 'insert',
        'expected': 'use warehouse developer_' ~ ('xs' if not dbt_macro_polo.should_full_refresh() else 'm')
    }
] %}

{# Execute test cases and collect failures #}
{% for test_case in test_cases %}
    {% set actual = dbt_macro_polo.warehouse_optimiser(test_case.operation) %}
        {% if actual != test_case.expected %}
            {% do failed_tests.append(test_case.test_name ~ ': Expected "' ~ test_case.expected ~ '", got "' ~ actual ~ '"') %}
        {% endif %}
{% endfor %}

{# Report test failures #}
{% if failed_tests | length > 0 %}
    {{ dbt_macro_polo.logging(message="Failed tests:\n" ~ failed_tests | join('\n'), level='ERROR', model_id=model_id) }}
{% endif %}

{# Final result assembly with base data #}
with source_data as (
    select *
    from {{ ref('seed_dummy_data_initial_run') }}
    {% if is_incremental() %}
        where run_started_time > (
            select coalesce(max(run_started_time), '1900-01-01'::timestamp)
            from {{ this }}
        )
    {% endif %}
)

select 
    md5(concat(
        coalesce(test_name, 'base'),
        coalesce(date::varchar, ''),
        coalesce(product, '')
    )) as id,
    run_started_time,
    date,
    product,
    units_sold,
    revenue,
    profit_margin,
    test_name,
    expected,
    actual,
    case when expected = actual then 'PASS' else 'FAIL' end as status
from source_data
cross join (
    {% for test_case in test_cases %}
    select 
        '{{ test_case.test_name }}' as test_name,
        '{{ test_case.expected }}' as expected,
        '{{ dbt_macro_polo.warehouse_optimiser(test_case.operation) }}' as actual
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
) tests

