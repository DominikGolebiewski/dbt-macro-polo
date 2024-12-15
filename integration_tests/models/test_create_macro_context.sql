{% set test_results = [] %}

{# Test Case 1: Basic macro context creation #}
{% set context = dbt_macro_polo.create_macro_context('test_macro') %}
{% do test_results.append({
    'test_name': 'Basic macro context creation',
    'model_name': this.name,
    'actual': tojson({
        'macro_name': context.macro_name,
        'model_id': context.model_id,
        'materialisation': context.materialisation,
        'logging_level': context.logging_level,
        'is_warehouse_optimiser_enabled': context.is_warehouse_optimiser_enabled
    }),
    'expected': tojson({
        'macro_name': 'TEST_MACRO',
        'model_id': this.schema | lower ~ '.' ~ this.name | lower,
        'materialisation': 'view',
        'logging_level': 'info',
        'is_warehouse_optimiser_enabled': false
    })
}) %}

{# Process test results #}
{% set failed_tests = [] %}
{% for test in test_results %}
    {% if test.actual != test.expected %}
        {% do failed_tests.append(
            test.test_name ~ ': Expected "' ~ test.expected ~ '", got "' ~ test.actual ~ '"' ~
            ' in model ' ~ test.model_name
        ) %}
    {% endif %}
{% endfor %}

{# Report results #}
{% if failed_tests | length > 0 %}
    {{ dbt_macro_polo.logging(message="Failed tests:\n" ~ failed_tests | join('\n'), level='ERROR') }}
{% endif %}

with test_results as (
    {% for test in test_results %}
    select
        '{{ test.test_name }}' as test_name,
        '{{ test.model_name }}' as model_name,
        '{{ test.expected }}' as expected,
        '{{ test.actual }}' as actual,
        case when '{{ test.actual }}' = '{{ test.expected }}' then '✅ PASS' else '❌ FAIL' end as status
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
)
select * from test_results