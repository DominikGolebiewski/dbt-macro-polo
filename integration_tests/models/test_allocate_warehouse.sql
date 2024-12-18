{% set test_cases = [
    {
        'name': 'Basic warehouse allocation',
        'incremental_size': 'xs',
        'fullrefresh_size': none,
        'expected': var('macro_polo').get('warehouse_config').get('environment').get(target.name, target.name).get('warehouse_name_prefix') ~ '_xs'
    },
    {
        'name': 'Full refresh warehouse allocation',
        'incremental_size': 'xs',
        'fullrefresh_size': 's',
        'expected': var('macro_polo').get('warehouse_config').get('environment').get(target.name, target.name).get('warehouse_name_prefix') ~ ('_s' if dbt_macro_polo.should_full_refresh() else '_xs')
    },
    {
        'name': 'Full refresh with default size',
        'incremental_size': 'm',
        'fullrefresh_size': none,
        'expected': var('macro_polo').get('warehouse_config').get('environment').get(target.name, target.name).get('warehouse_name_prefix') ~ '_m'
    }
] %}


{# Process test results #}
{% set failed_tests = [] %}
{% for test_case in test_cases %}
    {% set actual = dbt_macro_polo.allocate_warehouse(
        incremental_size=test_case.incremental_size,
        fullrefresh_size=test_case.fullrefresh_size
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
        '{{ dbt_macro_polo.allocate_warehouse(
            incremental_size=test_case.incremental_size,
            fullrefresh_size=test_case.fullrefresh_size
        ) }}' as actual,
        '{{ test_case.expected }}' as expected
    where actual != expected
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
)