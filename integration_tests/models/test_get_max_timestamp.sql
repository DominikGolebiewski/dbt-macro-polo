{% set test_results = [] %}
{% set test_model_name = 'seed_get_max_timestamp' %}
{% set test_model = ref(test_model_name) %}

{# Test Case 1: Basic timestamp retrieval #}
{% do test_results.append({
    'test_name': 'Basic timestamp retrieval',
    'model_name': this.name,
    'actual': dbt_macro_polo.get_max_timestamp(
        timestamp_column='loaded_timestamp',
        model_name=test_model_name
    ),
    'expected': "'2024-01-05 00:00:00'::timestamp"
}) %}

{# Test Case 2: Custom column name #}
{% do test_results.append({
    'test_name': 'Custom column timestamp retrieval',
    'model_name': this.name,
    'actual': dbt_macro_polo.get_max_timestamp(
        timestamp_column='custom_timestamp',
        model_name=test_model_name
    ),
    'expected': "'2024-01-05 12:00:00'::timestamp"
}) %}

{# Test Case 3: With predicate #}
{% do test_results.append({
    'test_name': 'Timestamp retrieval with predicate',
    'model_name': this.name,
    'actual': dbt_macro_polo.get_max_timestamp(
        timestamp_column='loaded_timestamp',
        predicate="status = 'active'",
        model_name=test_model_name
    ),
    'expected': "'2024-01-04 00:00:00'::timestamp"
}) %}

{# Test Case 4: With custom warehouse size #}
{% do test_results.append({
    'test_name': 'Timestamp retrieval with custom warehouse',
    'model_name': this.name,
    'actual': dbt_macro_polo.get_max_timestamp(
        timestamp_column='loaded_timestamp',
        warehouse_size='s',
        model_name=test_model_name
    ),
    'expected': "'2024-01-05 00:00:00'::timestamp"
}) %}

{# Test Case 5: Custom all parameters #}
{% do test_results.append({
    'test_name': 'Timestamp retrieval with custom warehouse',
    'model_name': this.name,
    'actual': dbt_macro_polo.get_max_timestamp(
        timestamp_column='custom_timestamp',
        warehouse_size='s',
        predicate="status = 'active'",
        model_name=test_model_name
    ),
    'expected': "'2024-01-04 12:00:00'::timestamp"
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
        {{ test.expected }} as expected,
        {{ test.actual }} as actual,
        '✅ PASS' as status
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
)
select * from test_results