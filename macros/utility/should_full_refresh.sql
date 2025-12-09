{% macro should_full_refresh() %}
    {{ return(adapter.dispatch('should_full_refresh', 'dbt_macro_polo')()) }}
{% endmacro %}

{% macro default__should_full_refresh() %}

    {% set macro_name = 'should_full_refresh' %}

    {% set is_table_materialisation = config.get('materialized', 'undefined') | trim | lower == 'table' %}
    {% set is_relation_exist = load_relation(this) is not none %}
    {% set is_full_refresh = flags.FULL_REFRESH or not is_relation_exist or is_table_materialisation %}

    {% set msg = "Full refresh logic should apply: " ~ is_full_refresh 
                    ~ " is_relation_exist: " ~ is_relation_exist ~ " is_table_materialisation: " 
                    ~ is_table_materialisation ~ " flags.FULL_REFRESH: " ~ flags.FULL_REFRESH %}
                    
    {{ dbt_macro_polo.log_event(message=msg, level='DEBUG', model_id=this, macro_name=macro_name) }}

    {{ return(is_full_refresh) }}

{% endmacro %}
