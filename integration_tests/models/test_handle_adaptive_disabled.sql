-- Adaptive mode is enabled at project level but disabled in this model's meta
-- (see test_handle_adaptive_disabled.yaml): handle_adaptive must return '' so the
-- optimiser falls through to the existing thresholds/scheduling/default chain.
{% set failed_tests = [] %}

{% set adaptive_config = dbt_macro_polo.get_adaptive_config() %}
{% if adaptive_config.enabled %}
    {% do failed_tests.append('model_meta_override: Expected adaptive disabled via meta, got enabled') %}
{% endif %}

{% set actual = dbt_macro_polo.handle_adaptive('integration_tests.test_handle_adaptive_disabled', 'ctas', 1000) %}
{% if actual != '' %}
    {% do failed_tests.append('handle_adaptive_disabled: Expected "", got "' ~ actual ~ '"') %}
{% endif %}

{# Report results #}
{% if failed_tests | length > 0 %}
    {{ dbt_macro_polo.logging(message="Failed tests:\n" ~ failed_tests | join('\n'), level='ERROR') }}
{% endif %}

select {{ 'true' if failed_tests | length == 0 else 'false' }} as test_passed
