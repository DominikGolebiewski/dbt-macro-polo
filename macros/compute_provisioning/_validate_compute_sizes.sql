{% macro _validate_compute_sizes(incremental_size, fullrefresh_size, allowed_sizes) %}
  {{ return(adapter.dispatch('_validate_compute_sizes', 'dbt_macro_polo')(incremental_size, fullrefresh_size, allowed_sizes)) }}
{% endmacro %}

{% macro default___validate_compute_sizes(incremental_size, fullrefresh_size, allowed_sizes) %}

    {% set macro_name = '_validate_compute_sizes' %}

    {#/* Validate requested sizes - trim and lowercase to avoid case sensitivity and any empty chars */#}
    {% set incremental = incremental_size | trim | lower %}

    {#/* If fullrefresh size is not provided, use incremental size instead */#}
    {% set fullrefresh = (fullrefresh_size | trim | lower) or incremental %}

    {#/* Validate requested sizes against allowed sizes. To make it case insensitive, itterate over both lists and compare 
         each size to each other so that the trim and lower filters can be use. These filter are not allowed on lists or sets. */#}
    {% set invalid_requested_sizes = [] %}
    {% for size, label in [(incremental, 'incremental'), (fullrefresh, 'fullrefresh')] %}
        {% for allowed_size in allowed_sizes %}
            {% if size  == allowed_size | trim | lower %}
                {% do invalid_requested_sizes.append(label ~ ': ' ~ size) %}
            {% endif %}
        {% endfor %}
    {% endfor %}

    {{ return(invalid_requested_sizes) }}

{% endmacro %}
