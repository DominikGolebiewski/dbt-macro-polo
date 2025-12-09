{% macro _build_volume_query(relation, timestamp_column, high_water_mark) %}
  {{ return(adapter.dispatch('_build_volume_query', 'dbt_macro_polo')(relation, timestamp_column, high_water_mark)) }}
{% endmacro %}

{% macro default___build_volume_query(relation, timestamp_column, high_water_mark) %}

    {% set query %}
        select count(1) as row_count
        from {{ relation }}
        {% if high_water_mark != '0' %}
        where {{ timestamp_column }} > '{{ high_water_mark }}'::timestamp
        {% endif %}
    {% endset %}

    {{ return(query) }}

{% endmacro %}
