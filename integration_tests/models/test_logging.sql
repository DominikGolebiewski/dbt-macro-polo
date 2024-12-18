{% set test_cases = [
    {
        'name': 'Basic INFO logging',
        'message': 'Test info message',
        'level': 'INFO',
        'macro_name': none,
        'model_id': none,
        'status': none,
        'expected': ''
    },
    {
        'name': 'DEBUG logging with macro name',
        'message': 'Test debug message',
        'level': 'DEBUG',
        'macro_name': 'test_macro',
        'model_id': none,
        'status': none,
        'expected': ''
    },
    {
        'name': 'WARN logging with model_id',
        'message': 'Test warning message',
        'level': 'WARN',
        'macro_name': none,
        'model_id': 'test_schema.test_model',
        'status': none,
        'expected': ''
    },
    {
        'name': 'Status indicators and colors',
        'message': 'Test message with status',
        'level': 'INFO',
        'macro_name': none,
        'model_id': none,
        'status': 'TEST',
        'expected': ''
    },
    {
        'name': 'Long message with dots alignment',
        'message': 'This is a very long message that should be aligned with dots',
        'level': 'INFO',
        'macro_name': none,
        'model_id': none,
        'status': none,
        'expected': ''
    }
] %}

{# Setup test environment #}
{% do var('macro_polo', {}).update({'logging_level': 'DEBUG'}) %}

{# Process test results #}
{% set failed_tests = [] %}
{% for test_case in test_cases %}
    {% set actual = dbt_macro_polo.logging(
        message=test_case.message,
        level=test_case.level,
        macro_name=test_case.macro_name,
        model_id=test_case.model_id,
        status=test_case.status
    ) | trim %}
    
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
        '{{ dbt_macro_polo.logging(
            message=test_case.message,
            level=test_case.level,
            macro_name=test_case.macro_name,
            model_id=test_case.model_id,
            status=test_case.status
        ) | trim }}' as actual,
        '{{ test_case.expected }}' as expected
    where actual != expected
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
)