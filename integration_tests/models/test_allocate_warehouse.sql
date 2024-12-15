{% set test_results = [] %}
{% set is_relation_exist = load_relation(this) is not none %}
{% set materialisation = config.get('materialized', 'undefined') | lower %}
{% set is_full_refresh = flags.FULL_REFRESH or not is_relation_exist or materialisation == 'table' %}
{% set warehouse_prefix = var('macro_polo', {}).get('warehouse_config', {}).get('environment', {}).get(target.name, {}).get('warehouse_name_prefix') %}

{# Test Case 1 #}
{% do test_results.append({
    'test_name': 'Full refresh and incremental allocation',
    'model_name': this.name,
    'actual': dbt_macro_polo.allocate_warehouse('xs', 's'),
    'expected': warehouse_prefix ~ ('_s' if is_full_refresh else '_xs')
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