{% macro test_get_warehouse() %}
    -- Test case 1: Basic incremental size
    {% set result = dbt_macro_polo.get_warehouse('xs') %}
    {{ test_get_warehouse_case_1(result) }}

    -- Test case 2: Different sizes for incremental and full refresh
    {% set result = dbt_macro_polo.get_warehouse('L', 'XL') %}
    {{ test_get_warehouse_case_2(result) }}

    -- Test case 3: Full refresh size (only when full_refresh is true)
    {% if var('full_refresh', false) %}
        {% do log("Running full refresh test case...", info=True) %}
        {% set result = dbt_macro_polo.get_warehouse('S', '2XL') %}
        {{ test_get_warehouse_case_3(result) }}
    {% endif %}

{% endmacro %}

--------------------------------------------------------------------------------

{% macro test_get_warehouse_case_1(result) %}
    {% set expected_result = 'ci_xs' %}
    {% if result != expected_result %}
        {{ exceptions.raise_compiler_error("Test failed: Expected '" ~ expected_result ~ "', got '" ~ result ~ "'") }}
    {% endif %}
    {{ log("✓ Test case 1 passed!", info=True) }}
{% endmacro %}

--------------------------------------------------------------------------------

{% macro test_get_warehouse_case_2(result) %}
    {% set expected_result = 'ci_l' %}
    {% if result != expected_result %}
        {{ exceptions.raise_compiler_error("Test failed: Expected '" ~ expected_result ~ "', got '" ~ result ~ "'") }}
    {% endif %}
    {{ log("✓ Test case 2 passed!", info=True) }}
{% endmacro %}

--------------------------------------------------------------------------------

{% macro test_get_warehouse_case_3(result) %}
    {% set expected_result = 'ci_2xl' %}
    {% if result != expected_result %}
        {{ exceptions.raise_compiler_error("Test failed: Expected '" ~ expected_result ~ "', got '" ~ result ~ "'") }}
    {% endif %}
    {{ log("✓ Test case 3 passed!", info=True) }}
{% endmacro %}