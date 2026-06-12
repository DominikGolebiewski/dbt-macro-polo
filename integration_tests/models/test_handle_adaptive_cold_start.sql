-- Adaptive mode is enabled at project level but no recommendations table exists in
-- the CI account (no training has run): handle_adaptive must degrade gracefully and
-- return '' so the optimiser falls back to configured thresholds. This is the key
-- degradation test for adaptive mode.
{% set failed_tests = [] %}

{% if execute %}
    {% set adaptive_config = dbt_macro_polo.get_adaptive_config() %}
    {% if not adaptive_config.enabled %}
        {% do failed_tests.append('project_config: Expected adaptive enabled in integration test vars, got disabled') %}
    {% endif %}

    {% set actual = dbt_macro_polo.handle_adaptive('integration_tests.test_handle_adaptive_cold_start', 'ctas', 1000) %}
    {% if actual != '' %}
        {% do failed_tests.append('handle_adaptive_cold_start: Expected "" (graceful fallback), got "' ~ actual ~ '"') %}
    {% endif %}
{% endif %}

{# Report results #}
{% if failed_tests | length > 0 %}
    {{ dbt_macro_polo.logging(message="Failed tests:\n" ~ failed_tests | join('\n'), level='ERROR') }}
{% endif %}

select {{ 'true' if failed_tests | length == 0 else 'false' }} as test_passed
