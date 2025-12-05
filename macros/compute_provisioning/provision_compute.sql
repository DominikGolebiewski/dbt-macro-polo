{% macro provision_compute(incremental_size, fullrefresh_size=none) %}
    {{ return(adapter.dispatch('provision_compute', 'dbt_macro_polo')(incremental_size, fullrefresh_size)) }}
{% endmacro %}

{% macro snowflake__provision_compute(incremental_size, fullrefresh_size=none) %}

    {#/* Get and validate infrastructure definition */#}
    {% set infrastructure_definition = dbt_macro_polo._get_infrastructure_config() %}

    {% set macro_polo = dbt_macro_polo.validate_macro_polo_var() %}
    {% set macro_name = 'provision_compute' %}
    {% set model_id = this.schema ~ "." ~ this.name %}
    {% set warehouse_id = none %}

    {#/* Parameters validation */#}
    {% if incremental_size is none or incremental_size == '' %}
        {% set msg = "Configuration Error: incremental_size parameter is required" %}
        {{ dbt_macro_polo.log_event(message=msg, level='ERROR', model_id=this, macro_name=macro_name) }}
        {{ return(none) }}
    {% endif %}

    {#/* Normalise sizes */#}
    {% set incremental = incremental_size | trim | lower %}
    {% set fullrefresh = incremental if fullrefresh_size is none else (fullrefresh_size | trim | lower)  %}

    {#/* Get warehouse prefix */#}
    {% set warehouse_prefix = infrastructure_definition.get('environment_context', {}).get(target.name, {}).get('warehouse_name_prefix', none) %}

    {% if warehouse_prefix is none or warehouse_prefix == '' %}
        {% set msg = "Configuration Error (dbt_project.yml): warehouse_name_prefix value cannot be none or empty for environment: " ~ target.name %}
        {{ dbt_macro_polo.log_event(message=msg, level='ERROR', macro_name=macro_name) }}
        {{ return(none) }}
    {% endif %}

    {#/* Validate requested sizes */#}
    {% set allowed_sizes = infrastructure_definition.get('allowed_sizes', []) %}
    {% set invalid_requested_sizes = dbt_macro_polo._validate_compute_sizes(incremental, fullrefresh, allowed_sizes) %}

    {% if invalid_requested_sizes != [] %}
        {% set msg = "Configuration Error: Requested size(s) not in configured allowed_sizes list: " ~ invalid_requested_sizes ~ ". Configured sizes: " ~ allowed_sizes %}
        {{ dbt_macro_polo.log_event(message=msg, level='ERROR', model_id=this, macro_name=macro_name) }}
        {{ return([]) }}
    {% endif %}

    {#/* Determine size suffix based on incremental or fullrefresh mode */#}
    {% set size_suffix = fullrefresh if dbt_macro_polo.should_full_refresh() else incremental %}

    {#/* Cache handling */#}
    {% set state_key = '_macro_polo_provision_compute_' ~  warehouse_prefix ~ '_' ~ size_suffix %}
    {% set warehouse_id = dbt_macro_polo.get_runtime_state(state_key) %}

    {% if warehouse_id is none %}

        {% set warehouse_id = warehouse_prefix ~ '_' ~ size_suffix %}
        {#/* Cache and return result */#}
        {% set msg = "Saving warehouse '" ~ warehouse_id ~ "' to runtime state with key '" ~ state_key ~ "'" %}
        {{ dbt_macro_polo.log_event(message=msg,level='DEBUG',macro_name=macro_name) }}
        {% do macro_polo.get('runtime_state', {}).update({state_key: warehouse_id}) %}

    {% endif %}

    {#/* Log and return Result */#}
    {{ dbt_macro_polo.log_event(message="Provisioned warehouse",model_id=this,status=warehouse_id | upper,macro_name=macro_name) }}
    {{ return(warehouse_id) }}

{% endmacro %}
