-- Test the extract_compile_features macro
{% set test_cases = [
    {
        'name': 'joins_and_ctes',
        'raw_code': 'with stg as ( select * from a ), dim as ( select * from b ) select * from stg join dim on stg.id = dim.id left join c on c.id = stg.id',
        'upstream_count': 3,
        'expected': {'join_count': 2, 'cte_count': 2, 'union_count': 0, 'window_fn_count': 0, 'upstream_count': 3}
    },
    {
        'name': 'unions_and_windows',
        'raw_code': 'select id, row_number() over (partition by id order by ts) as rn from t union all select id, 1 from u',
        'upstream_count': 2,
        'expected': {'join_count': 0, 'cte_count': 0, 'union_count': 1, 'window_fn_count': 1, 'upstream_count': 2}
    },
    {
        'name': 'simple_select',
        'raw_code': 'select 1 as id',
        'upstream_count': 0,
        'expected': {'join_count': 0, 'cte_count': 0, 'union_count': 0, 'window_fn_count': 0, 'upstream_count': 0}
    },
    {
        'name': 'empty_code',
        'raw_code': none,
        'upstream_count': 0,
        'expected': {'join_count': 0, 'cte_count': 0, 'union_count': 0, 'window_fn_count': 0, 'upstream_count': 0}
    }
] %}

{# Process test results #}
{% set failed_tests = [] %}
{% for test_case in test_cases %}
    {% set actual = dbt_macro_polo.extract_compile_features(test_case.raw_code, test_case.upstream_count) %}
    {% for key, expected_value in test_case.expected.items() %}
        {% if actual.get(key) != expected_value %}
            {% do failed_tests.append(
                test_case.name ~ '.' ~ key ~ ': Expected "' ~ expected_value ~ '", got "' ~ actual.get(key) ~ '"'
            ) %}
        {% endif %}
    {% endfor %}
    {% if test_case.raw_code and actual.get('query_length', 0) != test_case.raw_code | length %}
        {% do failed_tests.append(test_case.name ~ '.query_length: Expected "' ~ test_case.raw_code | length ~ '", got "' ~ actual.get('query_length') ~ '"') %}
    {% endif %}
{% endfor %}

{# Report results #}
{% if failed_tests | length > 0 %}
    {{ dbt_macro_polo.logging(message="Failed tests:\n" ~ failed_tests | join('\n'), level='ERROR') }}
{% endif %}

select {{ 'true' if failed_tests | length == 0 else 'false' }} as test_passed
