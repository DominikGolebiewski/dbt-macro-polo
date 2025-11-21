{% macro should_full_refresh() %}
    {#
    Determines if the current run should be treated as a full refresh.
    Checks the --full-refresh flag, if the relation exists, and materialization type.

    Returns:
        bool: True if full refresh logic should apply.
    #}
    {{ return(adapter.dispatch('should_full_refresh', 'dbt_macro_polo')()) }}
{% endmacro %}

{% macro default__should_full_refresh(model, model_id) %}
    {% set is_relation_exist = load_relation(model) is not none %}
    {% set is_full_refresh = flags.FULL_REFRESH or not is_relation_exist %}

    {{ dbt_macro_polo.log_event(
        message="Full refresh logic should apply: " ~ is_full_refresh,
        level='DEBUG',
        model_id=model_id,
        macro_name='should_full_refresh'
    ) }}

    {{ return(is_full_refresh) }}
{% endmacro %}
