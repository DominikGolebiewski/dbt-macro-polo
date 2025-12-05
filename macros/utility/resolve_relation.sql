{% macro resolve_relation(model_name, context_relation=none) %}
  {{ return(adapter.dispatch('resolve_relation', 'dbt_macro_polo')(model_name, context_relation)) }}
{% endmacro %}

{% macro default__resolve_relation(model_name, context_relation=none) %}

    {% set relation = none %}

    {% if model_name is string %}
        {% if context_relation %}
            {% set relation = adapter.get_relation(context_relation.database, context_relation.schema, model_name) %}
        {% else %}
            {# Fallback if no context is provided, potentially risky without explicit db/schema #}
             {% set relation = adapter.get_relation(target.database, target.schema, model_name) %}
        {% endif %}
    {% else %}
        {% set relation = model_name %}
    {% endif %}

    {% if not relation and context_relation is defined and context_relation %}
        {% set relation = context_relation %}
    {% endif %}

    {{ return(relation) }}
{% endmacro %}
