{% macro get_infra_config(config_root) %}
    {{ return(adapter.dispatch('get_infra_config', 'dbt_macro_polo')(config_root)) }}
{% endmacro %}

{% macro default__get_infra_config(config_root) %}
    
    {% set macro_name = 'get_infra_config' %}
    {% set message_prefix = "Configuration Warning (dbt_project.yml): " %}

    {% set infra = dbt_macro_polo.require(config_root.get('infrastructure_definition'), message_prefix ~ "'infrastructure_definition' must be defined in macro_polo", macro_name) %}
    {{ return({}) if not infra }}

    {% set env_ctx = dbt_macro_polo.require(infra.get('environment_context'), message_prefix ~ "environment_context missing in infrastructure_definition", macro_name) %}
    {{ return({}) if not env_ctx }}

    {% set target_env = dbt_macro_polo.require(env_ctx.get(target.name), message_prefix ~ "environment '" ~ target.name ~ "' not found in environment_context", macro_name) %}
    {{ return({}) if not target_env }}

    {% set prefix = dbt_macro_polo.require(target_env.get('warehouse_name_prefix'), message_prefix ~ "warehouse_name_prefix missing for environment '" ~ target.name ~ "'", macro_name) %}
    {{ return({}) if not prefix }}

    {{ return({
        'environment': target.name,
        'allowed_sizes': infra.get('allowed_sizes', ['xs', 's', 'm', 'l']),
        'default_size': 'xs',
        'prefix': prefix
    }) }}
{% endmacro %}
