{# Override of dbt-snowflake's set_query_tag/unset_query_tag, resolved through the
   dispatch search order consumers already configure for this package. When adaptive
   telemetry is enabled, each optimiser-enabled model run is tagged with a JSON
   query_tag so its queries can be joined back from
   snowflake.account_usage.query_history during training. In every other case the
   stock dbt-snowflake behaviour (config/profile query_tag) is preserved. #}

{% macro polo_is_adaptive_tagging_enabled() %}
    {% if not execute or model is not defined %}
        {{ return(false) }}
    {% endif %}
    {% set adaptive_config = dbt_macro_polo.get_adaptive_config() %}
    {% set optimiser_enabled = model.config.get('meta', {}).get('warehouse_optimiser', {}).get('enabled', false) %}
    {{ return(adaptive_config.enabled and optimiser_enabled and model.resource_type == 'model') }}
{% endmacro %}

{% macro snowflake__set_query_tag() %}
    {% if dbt_macro_polo.polo_is_adaptive_tagging_enabled() %}
        {% set original_query_tag = get_current_query_tag() %}
        {% set tag = {
            'app': 'dbt_macro_polo',
            'invocation_id': invocation_id,
            'node_id': model.unique_id,
            'full_refresh': flags.FULL_REFRESH
        } | tojson %}
        {% do run_query("alter session set query_tag = '" ~ tag | replace("'", "''") ~ "'") %}
        {{ dbt_macro_polo.logging(message="Adaptive telemetry query tag set", model_id=model.unique_id, level='DEBUG') }}
        {{ return(original_query_tag) }}
    {% endif %}

    {# Stock dbt-snowflake behaviour #}
    {% if config.get('query_tag') %}
        {% set original_query_tag = get_current_query_tag() %}
        {% do run_query("alter session set query_tag = '" ~ config.get('query_tag') | replace("'", "''") ~ "'") %}
        {{ return(original_query_tag) }}
    {% endif %}
    {{ return(none) }}
{% endmacro %}

{% macro snowflake__unset_query_tag(original_query_tag) %}
    {% if dbt_macro_polo.polo_is_adaptive_tagging_enabled() or config.get('query_tag') %}
        {% if original_query_tag %}
            {% do run_query("alter session set query_tag = '" ~ original_query_tag | replace("'", "''") ~ "'") %}
        {% else %}
            {% do run_query("alter session unset query_tag") %}
        {% endif %}
    {% endif %}
{% endmacro %}
