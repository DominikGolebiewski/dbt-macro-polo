{% macro provision_compute(incremental_size, fullrefresh_size=none) %}
    {{ return(adapter.dispatch('provision_compute', 'dbt_macro_polo')(incremental_size, fullrefresh_size)) }}
{% endmacro %}

{% macro snowflake__provision_compute(incremental_size, fullrefresh_size=none) %}

    {% set macro_name = 'provision_compute' %}
    {% set macro_polo = var('macro_polo', {}) %}

    {% if this is not defined or not this %}
        {% set msg = "Configuration Error: provision_compute macro requires a valid model context. The 'this' variable is not defined." %}
        {{ dbt_macro_polo.log_event(message=msg, level='ERROR', macro_name=macro_name, model_id='unknown_model') }}
        {{ return(none) }}
    {% endif %}
    {% set model_id = this.schema ~ "." ~ this.name %}

    {# Validate input parameters #}
    {% if not incremental_size %}
        {% set msg = "Configuration Error: incremental_size parameter is required" %}
        {{ dbt_macro_polo.log_event(message=msg, level='ERROR', model_id=model_id, macro_name=macro_name) }}
        {{ return(none) }}
    {% endif %}

    {# 1. Get and validate infrastructure configuration #}
    {% set infrastructure_def = dbt_macro_polo._get_infrastructure_config(model_id, macro_name) %}
    {% if not infrastructure_def %}
        {{ return(none) }}
    {% endif %}

    {# 2. Get and validate environment configuration #}
    {% set warehouse_prefix = dbt_macro_polo._get_environment_config(infrastructure_def, model_id, macro_name) %}
    {% if not warehouse_prefix %}
        {{ return(none) }}
    {% endif %}

    {# 3. Determine size suffix based on run context #}
    {% set size_suffix = dbt_macro_polo._determine_compute_size(incremental_size, fullrefresh_size, this) %}

    {# Cache handling #}
    {% set state_key = '_macro_polo_provision_compute_' ~  warehouse_prefix ~ '_' ~ size_suffix %}
    {% set state_value = dbt_macro_polo.get_runtime_state(state_key) %}

    {% if state_value %}
        {{ dbt_macro_polo.log_event(
            message="Provisioned warehouse from runtime state",
            level='DEBUG',
            model_id=model_id,
            status=state_value | upper,
            macro_name=macro_name
        ) }}
        {{ return(state_value) }}
    {% endif %}

    {# 4. Validate requested sizes #}
    {% set allowed_sizes = infrastructure_def.get('allowed_sizes') %}
    {% set is_valid_sizes = dbt_macro_polo._validate_compute_sizes(incremental_size, fullrefresh_size, allowed_sizes, model_id, macro_name) %}
    {% if not is_valid_sizes %}
        {{ return(none) }}
    {% endif %}

    {# 5. Generate and validate warehouse identifier #}
    {% set warehouse_id = warehouse_prefix ~ "_" ~ size_suffix %}
    {% set is_valid_id = dbt_macro_polo._validate_warehouse_id(warehouse_id, model_id, macro_name) %}
    {% if not is_valid_id %}
        {{ return(none) }}
    {% endif %}

    {# Cache and return result #}
    {{ dbt_macro_polo.log_event(
        message="Saving warehouse '" ~ warehouse_id ~ "' to runtime state with key '" ~ state_key ~ "'",
        level='DEBUG',
        macro_name=macro_name
    ) }}

    {% do macro_polo.get('runtime_state', {}).update({state_key: warehouse_id}) %}

    {{ dbt_macro_polo.log_event(
        message="Provisioned warehouse",
        model_id=model_id,
        status=warehouse_id | upper,
        macro_name=macro_name
    ) }}
    {{ return(warehouse_id) }}

{% endmacro %}
