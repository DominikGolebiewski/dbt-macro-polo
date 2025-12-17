{% macro _get_infra_config(config_root) %}
    {{ return(adapter.dispatch('_get_infra_config', 'dbt_macro_polo')(config_root)) }}
{% endmacro %}

{% macro default___get_infra_config(config_root) %}
    
    {% set macro_name = '_get_infra_config' %}
    {% set message_prefix = "Configuration Warning (dbt_project.yml): " %}

    {#-- Validate all required configuration elements #}
    {% set infra = dbt_macro_polo.require(config_root.get('infrastructure_definition'), message_prefix ~ "'infrastructure_definition' must be defined in macro_polo", macro_name) %}
    {% set env_ctx = dbt_macro_polo.require(infra.get('environment_context'), message_prefix ~ "environment_context missing in infrastructure_definition", macro_name) if infra %}
    {% set allowed_sizes = dbt_macro_polo.require(infra.get('allowed_sizes'), message_prefix ~ "allowed_sizes must be defined in infrastructure_definition", macro_name) if infra %}
    {% set target_env = dbt_macro_polo.require(env_ctx.get(target.name), message_prefix ~ "environment '" ~ target.name ~ "' not found in environment_context", macro_name) if env_ctx %}
    {% set prefix = dbt_macro_polo.require(target_env.get('warehouse_name_prefix'), message_prefix ~ "warehouse_name_prefix must be defined for environment '" ~ target.name ~ "'", macro_name) if target_env %}

    {#-- Return empty dict if any validation failed, otherwise return proper config #}
    {% if not infra or not env_ctx or not allowed_sizes or not target_env or not prefix %}
        {{ return({}) }}
    {% else %}
        {{ return({
            'environment': target.name,
            'allowed_sizes': allowed_sizes,
            'default_size': 'xs',
            'prefix': prefix
        }) }}
    {% endif %}

{% endmacro %}
