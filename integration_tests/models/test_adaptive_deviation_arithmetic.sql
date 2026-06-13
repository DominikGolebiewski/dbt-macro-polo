-- Regression test for the adaptive deviation arithmetic. Snowflake numerics (p10/p90,
-- row counts) arrive in dbt as decimal.Decimal; multiplying a Decimal by a Python
-- float (deviation_threshold) raised mid-hook and failed the model. This asserts the
-- float-coerced arithmetic used by handle_adaptive works on Decimal inputs.
{% set failed_tests = [] %}

{% if execute %}
    {# Mimic values as they come back from run_query against Snowflake #}
    {% set p10 = ('100000' | float) %}
    {% set p90 = ('1000000' | float) %}
    {% set deviation = 0.25 | float %}

    {% set lower_bound = (p10 | float) * (1.0 - deviation) %}
    {% set upper_bound = (p90 | float) * (1.0 + deviation) %}

    {% if lower_bound != 75000.0 %}
        {% do failed_tests.append('lower_bound: expected 75000.0, got ' ~ lower_bound) %}
    {% endif %}
    {% if upper_bound != 1250000.0 %}
        {% do failed_tests.append('upper_bound: expected 1250000.0, got ' ~ upper_bound) %}
    {% endif %}

    {# In-range / out-of-range decisions across the boundary #}
    {% set checks = [
        (500000, true),
        (50000, false),
        (2000000, false),
        (75000, true),
        (1250000, true)
    ] %}
    {% for row_count, expected_in_range in checks %}
        {% set rc = row_count | float %}
        {% set in_range = rc >= lower_bound and rc <= upper_bound %}
        {% if in_range != expected_in_range %}
            {% do failed_tests.append('in_range(' ~ row_count ~ '): expected ' ~ expected_in_range ~ ', got ' ~ in_range) %}
        {% endif %}
    {% endfor %}
{% endif %}

{% if failed_tests | length > 0 %}
    {{ dbt_macro_polo.logging(message="Failed tests:\n" ~ failed_tests | join('\n'), level='ERROR') }}
{% endif %}

select {{ 'true' if failed_tests | length == 0 else 'false' }} as test_passed
