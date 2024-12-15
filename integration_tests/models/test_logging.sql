{% set test_results = [] %}

{# Test Case 1: Basic INFO logging #}
{% set test_message = 'Test info message' %}
{% do test_results.append({
    'test_name': 'Basic INFO logging',
    'model_name': this.name,
    'actual': dbt_macro_polo.logging(message=test_message) | trim,
    'expected': '',
    'level': 'INFO'
}) %}

{# Test Case 2: DEBUG logging with macro name #}
{% do test_results.append({
    'test_name': 'DEBUG logging with macro name',
    'model_name': this.name,
    'actual': dbt_macro_polo.logging(
        macro_name='test_macro',
        message='Test debug message',
        level='DEBUG'
    ) | trim,
    'expected': '',
    'level': 'DEBUG'
}) %}

{# Test Case 3: WARN logging with model_id #}
{% do test_results.append({
    'test_name': 'WARN logging with model_id',
    'model_name': this.name,
    'actual': dbt_macro_polo.logging(
        message='Test warning message',
        level='WARN',
        model_id='test_schema.test_model'
    ) | trim,
    'expected': '',
    'level': 'WARN'
}) %}

{# Test Case 5: Logging with different global logging levels #}
{% do var('macro_polo', {}).update({'logging_level': 'WARN'}) %}
{% do test_results.append({
    'test_name': 'Logging with global WARN level',
    'model_name': this.name,
    'actual': dbt_macro_polo.logging(
        message='This INFO message should not be logged',
        level='INFO'
    ) | trim,
    'expected': '',
    'level': 'INFO'
}) %}

{# Test Case 6: Status indicators and colors #}
{% do var('macro_polo', {}).update({'logging_level': 'info'}) %}

{% do test_results.append({
    'test_name': 'Status indicators and colors',
    'model_name': this.name,
    'actual': dbt_macro_polo.logging(
        message='Test message with status',
        level='INFO',
        status='TEST'
    ) | trim,
    'expected': '',
    'level': 'INFO'
}) %}

{# Test Case 7: Long message with dots alignment #}
{% set long_message = 'This is a very long message that should be aligned with dots.................................................' %}
{% do test_results.append({
    'test_name': 'Long message with dots alignment',
    'model_name': this.name,
    'actual': dbt_macro_polo.logging(
        message=long_message,
        level='INFO'
    ) | trim,
    'expected': '',
    'level': 'INFO'
}) %}

{# Process test results #}
{% set failed_tests = [] %}
{% for test in test_results %}
    {% if test.actual != test.expected %}
        {% do failed_tests.append(
            test.test_name ~ ': Expected "' ~ test.expected ~ '", got "' ~ test.actual ~ '"' ~
            ' in model ' ~ test.model_name ~ ' (Level: ' ~ test.level ~ ')'
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
        '{{ test.level }}' as log_level,
        '{{ test.expected }}' as expected,
        '{{ test.actual }}' as actual,
        case when '{{ test.actual }}' = '{{ test.expected }}' then '✅ PASS' else '❌ FAIL' end as status
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
)
select * from test_results