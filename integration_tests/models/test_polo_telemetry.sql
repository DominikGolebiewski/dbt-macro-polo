-- Test the adaptive telemetry audit table DDL: creation is idempotent (safe to run
-- in every CI build) and repeated calls within one invocation are served from cache.
{% set failed_tests = [] %}

{% if execute %}
    {% set first_call = dbt_macro_polo.polo_ensure_audit_table() %}
    {% set second_call = dbt_macro_polo.polo_ensure_audit_table() %}

    {% if not first_call %}
        {% do failed_tests.append('ensure_audit_table: Expected audit table FQN, got "' ~ first_call ~ '"') %}
    {% endif %}
    {% if first_call != second_call %}
        {% do failed_tests.append('ensure_audit_table_cache: Expected identical result on repeat call, got "'
            ~ first_call ~ '" then "' ~ second_call ~ '"') %}
    {% endif %}
{% endif %}

{# Report results #}
{% if failed_tests | length > 0 %}
    {{ dbt_macro_polo.logging(message="Failed tests:\n" ~ failed_tests | join('\n'), level='ERROR') }}
{% endif %}

{% if execute and failed_tests | length == 0 %}
select case when (select count(*) from {{ dbt_macro_polo.polo_ensure_audit_table() }}) >= 0 then true else false end as test_passed
{% else %}
select {{ 'true' if failed_tests | length == 0 else 'false' }} as test_passed
{% endif %}
