{% macro _determine_compute_size(incremental_size, fullrefresh_size, relation) %}
  {{ return(adapter.dispatch('_determine_compute_size', 'dbt_macro_polo')(incremental_size, fullrefresh_size, relation)) }}
{% endmacro %}

{% macro default___determine_compute_size(incremental_size, fullrefresh_size, relation) %}
    {% set incremental = incremental_size | trim | lower %}
    {% set fullrefresh = (fullrefresh_size or incremental_size) | trim | lower %}

    {% if relation is defined and relation %}
        {% set is_relation_exist = load_relation(relation) is not none %}
    {% else %}
        {% set is_relation_exist = false %}
    {% endif %}

    {% set materialisation = config.get('materialized', 'undefined') | lower %}
    {% set is_full_refresh = flags.FULL_REFRESH or not is_relation_exist or materialisation == 'table' %}

    {% set size_suffix = fullrefresh if is_full_refresh else incremental %}

    {{ return(size_suffix) }}
{% endmacro %}
