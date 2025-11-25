{% macro _validate_compute_sizes(incremental_size, fullrefresh_size, allowed_sizes, model_id, macro_name) %}
  {{ return(adapter.dispatch('_validate_compute_sizes', 'dbt_macro_polo')(incremental_size, fullrefresh_size, allowed_sizes, model_id, macro_name)) }}
{% endmacro %}

{% macro default___validate_compute_sizes(incremental_size, fullrefresh_size, allowed_sizes, model_id, macro_name) %}
    {% set incremental = incremental_size | trim | lower %}
    {% set fullrefresh = (fullrefresh_size or incremental_size) | trim | lower %}

    {% set invalid_requested_sizes = [] %}
    {% for size, label in [(incremental, 'incremental'), (fullrefresh, 'fullrefresh')] %}
        {% if size not in allowed_sizes %}
            {% do invalid_requested_sizes.append(label ~ ': ' ~ size) %}
        {% endif %}
    {% endfor %}

    {% if invalid_requested_sizes %}
        {% set msg = "Configuration Error: Requested size(s) not in configured allowed_sizes list: " ~ invalid_requested_sizes ~ ". Configured sizes: " ~ allowed_sizes %}
        {{ dbt_macro_polo.log_event(message=msg, level='ERROR', model_id=model_id, macro_name=macro_name) }}
        {{ return(false) }}
    {% endif %}

    {{ return(true) }}
{% endmacro %}
