{% macro _validate_compute_sizes(incremental_size, fullrefresh_size, allowed_sizes) %}
  {{ return(adapter.dispatch('_validate_compute_sizes', 'dbt_macro_polo')(incremental_size, fullrefresh_size, allowed_sizes)) }}
{% endmacro %}

{% macro default___validate_compute_sizes(incremental_size, fullrefresh_size, allowed_sizes) %}

    {% set macro_name = '_validate_compute_sizes' %}

    {#/* Before validation, convert allowed sizes to a list of lowercase trimmed strings */#}
    {% set normalised_allowed_sizes = allowed_sizes | map('trim') | map('lower') | list %}

    {#/* Validate requested sizes against allowed sizes */#}
    {% set invalid_requested_sizes = [] %}
    {% for size, label in [(incremental_size, 'incremental'), (fullrefresh_size, 'fullrefresh')] %}
        {% if size not in normalised_allowed_sizes %}
            {% do invalid_requested_sizes.append(label ~ ': ' ~ size) %}
        {% endif %}
    {% endfor %}

    {% if invalid_requested_sizes != [] %}
        {% set msg = "Configuration Error: Requested size(s) not in configured allowed_sizes list: " ~ invalid_requested_sizes ~ ". Configured sizes: " ~ allowed_sizes %}
        {{ dbt_macro_polo.log_event(message=msg, level='ERROR', model_id=this, macro_name=macro_name) }}
        {{ return(false) }}
    {% endif %}

    {{ return(true) }}

{% endmacro %}
