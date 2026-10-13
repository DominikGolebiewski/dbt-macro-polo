{% set target_watermark = "'2026-09-30 00:00:00'::timestamp_ntz" %}
{% set incremental_probe_watermark = dbt_macro_polo.get_upstream_dependency_maximum_timestamp(target_watermark) %}
{% set historical_probe_watermark = dbt_macro_polo.get_upstream_dependency_maximum_timestamp(target_watermark, true) %}

{% if incremental_probe_watermark != target_watermark %}
    {{ exceptions.raise_compiler_error('Normal upstream dependencies must retain the target watermark.') }}
{% endif %}

{% if historical_probe_watermark != "'1900-01-01 00:00:00'::timestamp_ntz" %}
    {{ exceptions.raise_compiler_error('ignore_timestamp must include historical source rows in the probe.') }}
{% endif %}

select 1 as test_passed
