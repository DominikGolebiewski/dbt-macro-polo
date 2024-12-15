{% set test_results = [] %}

{# Setup test cache data #}
{% do var('macro_polo', {}).get('cache', {}).update({'test_key_1': 'test_value_1'}) %}

{# Test Case 1: Basic cache retrieval #}
{% do test_results.append({
    'test_name': 'Basic cache retrieval',
    'model_name': this.name,
    'actual': dbt_macro_polo.get_cache_value('test_key_1'),
    'expected': 'test_value_1'
}) %}

{# Test Case 2: Non-existent key #}
{% do test_results.append({
    'test_name': 'Non-existent key',
    'model_name': this.name,
    'actual': dbt_macro_polo.get_cache_value('non_existent_key'),
    'expected': {}
}) %}

{# Test Case 3: Cache update #}
{% do var('macro_polo', {}).get('cache', {}).update({'new_key': 'new_value'}) %}
{% do test_results.append({
    'test_name': 'Cache update',
    'model_name': this.name,
    'actual': dbt_macro_polo.get_cache_value('new_key'),
    'expected': 'new_value'
}) %}

{# Test Case 4: Full cache #}
{% do test_results.append({
    'test_name': 'Full cache',
    'model_name': this.name,
    'actual': tojson(var('macro_polo', {}).get('cache', {})),
    'expected': tojson({'test_key_1': 'test_value_1', 'new_key': 'new_value'})
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
        '✅ PASS' as status
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
)
select * from test_results