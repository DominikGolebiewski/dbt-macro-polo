{% set test_cases = [
    {
        'name': 'Basic macro context creation',
        'macro_name': 'test_macro',
        'expected': {
            'macro_name': 'TEST_MACRO',
            'model_id': this.schema | lower ~ '.' ~ this.name | lower,
            'materialisation': 'view',
            'logging_level': 'debug',
            'is_warehouse_optimiser_enabled': false
        }
    }
] %}

{# Process test results #}
{% set failed_tests = [] %}
{% for test_case in test_cases %}
    {% set context = dbt_macro_polo.create_macro_context(test_case.macro_name) %}
    {% set actual = {
        'macro_name': context.macro_name,
        'model_id': context.model_id,
        'materialisation': context.materialisation,
        'logging_level': context.logging_level,
        'is_warehouse_optimiser_enabled': context.is_warehouse_optimiser_enabled
    } %}
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
    {% set context = dbt_macro_polo.create_macro_context(test_case.macro_name) %}
    select 
        '{{ test_case.name }}' as test_name,
        '{{ tojson({
            'macro_name': context.macro_name,
            'model_id': context.model_id,
            'materialisation': context.materialisation,
            'logging_level': context.logging_level,
            'is_warehouse_optimiser_enabled': context.is_warehouse_optimiser_enabled
        }) }}' as actual,
        '{{ tojson(test_case.expected) }}' as expected
    where actual != expected
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
)