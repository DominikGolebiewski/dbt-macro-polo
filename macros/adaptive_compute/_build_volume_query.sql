{% macro _build_volume_query(relation, timestamp_column, high_water_mark_value) %}
  {{ return(adapter.dispatch('_build_volume_query', 'dbt_macro_polo')(relation, timestamp_column, high_water_mark_value)) }}
{% endmacro %}

{% macro default___build_volume_query(relation, timestamp_column, high_water_mark_value) %}

    {% set query %}
        select count(1) as row_count
        from {{ relation }}
        {% if high_water_mark_value != '0' %}
        where {{ timestamp_column }} > '{{ high_water_mark_value }}'::timestamp
        {% endif %}
    {% endset %}

    {{ return(query) }}

{% endmacro %}
