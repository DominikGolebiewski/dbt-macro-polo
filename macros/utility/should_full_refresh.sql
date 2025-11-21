{% macro should_full_refresh() %}
    {#
    Determines if the current run should be treated as a full refresh.
    Checks the --full-refresh flag, if the relation exists, and materialization type.

    Returns:
        bool: True if full refresh logic should apply.
    #}
    {{ return(adapter.dispatch('should_full_refresh', 'dbt_macro_polo')()) }}
{% endmacro %}

{% macro default__should_full_refresh() %}
    {% set is_relation_exist = load_relation(this) is not none %}
    {% set materialisation = config.get('materialized', 'undefined') | lower %}
    {% set is_full_refresh = flags.FULL_REFRESH or not is_relation_exist or materialisation == 'table' %}
    {{ return(is_full_refresh) }}
{% endmacro %}
