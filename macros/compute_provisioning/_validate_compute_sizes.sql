{% macro _validate_compute_sizes(incremental_size, fullrefresh_size, allowed_sizes) %}
  {{ return(adapter.dispatch('_validate_compute_sizes', 'dbt_macro_polo')(incremental_size, fullrefresh_size, allowed_sizes)) }}
{% endmacro %}

{% macro default___validate_compute_sizes(incremental_size, fullrefresh_size, allowed_sizes) %}

    {% set macro_name = '_validate_compute_sizes' %}

    {#/* Validate requested sizes - trim and lowercase to avoid case sensitivity and any empty chars */#}
    {% set incremental = incremental_size | trim | lower %}

    {#/* If fullrefresh size is not provided, use incremental size instead */#}
    {% set fullrefresh = (fullrefresh_size | trim | lower) or incremental %}
    


    {#/* Before validation, convert allowed sizes to a list of lowercase trimmed strings */#}
    {% set normalised_allowed_sizes = allowed_sizes | map(attribute='trim') | map(attribute='lower') | list %}

    {#/* Validate requested sizes against allowed sizes */#}
    {% set invalid_requested_sizes = [] %}
    {% for size, label in [(incremental, 'incremental'), (fullrefresh, 'fullrefresh')] %}
        {% if size not in normalised_allowed_sizes %}
            {% do invalid_requested_sizes.append(label ~ ': ' ~ size) %}
        {% endif %}
    {% endfor %}

    {{ return(invalid_requested_sizes) }}

{% endmacro %}
