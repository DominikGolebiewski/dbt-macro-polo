{% macro _determine_compute_size(incremental_size, fullrefresh_size, relation) %}
  {{ return(adapter.dispatch('_determine_compute_size', 'dbt_macro_polo')(incremental_size, fullrefresh_size, relation)) }}
{% endmacro %}

{% macro default___determine_compute_size(incremental_size, fullrefresh_size, relation) %}

    {% set incremental = incremental_size | trim | lower %}
    {% set fullrefresh = (fullrefresh_size | trim | lower) or incremental %}

    {#/* We need logic to decide when full refresh size should be use.
        Either relation does not exists, materialistaion is table or we run explicitly with the --full-refresh flag. */#}    
    {% set is_relation_exist = load_relation(this) is not none %}
    {% set is_table_materialisation = config.get('materialized', 'undefined') | trim | lower == 'table' %}
    {% set is_full_refresh = flags.FULL_REFRESH or not is_relation_exist or is_table_materialisation %}

    {% set size_suffix = fullrefresh if is_full_refresh else incremental %}

    {{ return(size_suffix) }}
    
{% endmacro %}
