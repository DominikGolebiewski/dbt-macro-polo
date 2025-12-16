{% macro provision_compute(incremental_size, fullrefresh_size=none) %}
    {{ return(adapter.dispatch('provision_compute', 'dbt_macro_polo')(incremental_size, fullrefresh_size)) }}
{% endmacro %}

{% macro snowflake__provision_compute(incremental_size, fullrefresh_size=none) %}

    {% set macro_name = 'provision_compute' %}
    {% set config = dbt_macro_polo._get_compute_config() %}

    {% if not incremental_size %}
        {% set msg = "incremental_size not defined. Defualting to: " ~ config.default_size ~ " for environment: " ~ target.name %}
        {{ dbt_macro_polo.log_event(message=msg, level='WARN', model_id=this, macro_name=macro_name) }}
    {% endif %}

    {#-- Normalise inputs, fall back to defaults #}
    {% set inc_size = incremental_size | trim | lower if incremental_size else config.default_size %}
    {% set fr_size  = fullrefresh_size | trim | lower if fullrefresh_size else inc_size %}

    {% set allowed = config.allowed_sizes | map('lower') | map('trim') | list %}
    {% if inc_size not in allowed %}
        {{ dbt_macro_polo.log_error("Invalid incremental_size: " ~ inc_size ~ ". Allowed: " ~ allowed | join(', '), macro_name) }}
    {% endif %}
    {% if fr_size not in allowed %}
        {{ dbt_macro_polo.log_error("Invalid fullrefresh_size: " ~ fr_size ~ ". Allowed: " ~ allowed | join(', '), macro_name) }}
    {% endif %}

    {% set target_size = fr_size if dbt_macro_polo.should_full_refresh() else inc_size %}
    {% set warehouse_id = config.prefix ~ '_' ~ target_size %}

    {{ dbt_macro_polo.log_event(message="Provisioned warehouse", status=warehouse_id | upper, model_id=this, macro_name=macro_name) }}

    {{ return(warehouse_id) }}

{% endmacro %}
