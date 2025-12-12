{% macro provision_compute(incremental_size, fullrefresh_size=none) %}
    {{ return(adapter.dispatch('provision_compute', 'dbt_macro_polo')(incremental_size, fullrefresh_size)) }}
{% endmacro %}

{% macro snowflake__provision_compute(incremental_size, fullrefresh_size=none) %}

    {#/* Set variables */#}
    {% set macro_name = 'provision_compute' %}
    {% set warehouse_id = none %}

    {#/* Get and validate infrastructure definition and validate macro_polo variable */#}
    {% set infrastructure_definition = dbt_macro_polo._get_infrastructure_config() %}
    {% set macro_polo = dbt_macro_polo.validate_macro_polo_var() %}

    {#/* Arguments validation */#}
    {#/* Incremental size is the only required argument and cannot be empty or none */#}
    {% if incremental_size is none or incremental_size == '' %}
        {% set msg = "Configuration Error: incremental_size parameter is required" %}
        {{ dbt_macro_polo.log_event(message=msg, level='ERROR', model_id=this, macro_name=macro_name) }}
        {{ return(none) }}
    {% endif %}

    {#/* Normalise sizes and set fullrefresh size if not provided */#}
    {% set incremental = incremental_size | trim | lower %}
    {% set fullrefresh = incremental if fullrefresh_size is none else (fullrefresh_size | trim | lower)  %}

    {#/* Determine if fullrefresh mode is needed but only if fullrefresh size is provided and is different from incremental size */#}
    {% set is_fullrefresh = dbt_macro_polo.should_full_refresh() if (fullrefresh_size is not none and fullrefresh != incremental) else false %}

    {#/* Validate requested sizes */#}
    {% set allowed_sizes = infrastructure_definition.get('allowed_sizes', []) %}
    {% set is_valid_sizes = dbt_macro_polo._validate_compute_sizes(incremental, fullrefresh, allowed_sizes) %}

    {#/* Determine size suffix based on incremental or fullrefresh mode */#}
    {% set size_suffix = fullrefresh if is_fullrefresh else incremental %}

    {#/* Get warehouse prefix */#}
    {% set warehouse_prefix = infrastructure_definition.get('environment_context', {}).get(target.name, {}).get('warehouse_name_prefix', none) %}

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
    {{ dbt_macro_polo.log_event(message="Provisioned warehouse", model_id=this, status=warehouse_id | upper, macro_name=macro_name) }}
    
    {{ return(warehouse_id) }}

{% endmacro %}
