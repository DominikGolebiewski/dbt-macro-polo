{% macro create_macro_context(macro_name) %}
    {{ return(adapter.dispatch('create_macro_context', 'dbt_macro_polo')(macro_name)) }}
{% endmacro %}

{% macro default__create_macro_context(macro_name) %}
    {% set macro_polo = var('macro_polo', {}) %}
    {% set logging_level = macro_polo.get('logging_level', 'info') %}
    {% set context = namespace(
        macro_name = macro_name | upper,
        model_id = this.schema | lower ~ '.' ~ this.name | lower,
        materialisation = config.get('materialized', 'undefined'),
        logging_level = logging_level,
        is_warehouse_optimiser_enabled = var('warehouse_optimiser.enabled', false)
    ) %}
    {{ return(context) }}
{% endmacro %}
