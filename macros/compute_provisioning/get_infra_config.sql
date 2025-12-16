{% macro get_infra_config(config_root) %}
    {{ return(adapter.dispatch('get_infra_config', 'dbt_macro_polo')(config_root)) }}
{% endmacro %}

{% macro default__get_infra_config(config_root) %}

    {% set macro_name = 'get_infra_config' %}
    {% set main_config = {} %}

    {% if 'infrastructure_definition' in config_root %}
        {% set infra_definition = config_root.get('infrastructure_definition', {}) %}

        {% if 'environment_context' in infra_definition %}
            {% set env_ctx = infra_definition.get('environment_context', {}) %}

            {% if target.name in env_ctx %}
                {% set target_env = env_ctx.get(target.name, {}) %}
            {% else %}
                {% set msg = "Configuration Warning (dbt_project.yml): environment '" ~ target.name ~ "' not found in environment_context" %}
                {{ dbt_macro_polo.log_event(message=msg, level='WARN', macro_name=macro_name) }}
                {{ return({}) }}
            {% endif %}

        {% else %}
            {% set msg = "Configuration Warning (dbt_project.yml): environment_context missing in infrastructure_definition" %}
            {{ dbt_macro_polo.log_event(message=msg, level='WARN', macro_name=macro_name) }}
            {{ return({}) }}
        {% endif %}

        {% set prefix = target_env.get('warehouse_name_prefix') %}

        {% if not prefix %}
            {% set msg = "Configuration Warning (dbt_project.yml): warehouse_name_prefix missing for environment '" ~ target.name ~ "'" %}
            {{ dbt_macro_polo.log_event(message=msg, level='WARN', macro_name=macro_name) }}
            {{ return({}) }}
        {% endif %}

        {% set main_config = {
            'environment': target.name,
            'allowed_sizes': infra_definition.get('allowed_sizes', ['xs', 's', 'm', 'l']),
            'default_size': 'xs', 
            'prefix': prefix
        } %}
    {% else %}
        {% set msg = "Configuration Warning (dbt_project.yml): Provide 'infrastructure_definition' in macro_polo" %}
        {{ dbt_macro_polo.log_event(message=msg, level='WARN', macro_name=macro_name) }}
        {{ return({}) }}
    {% endif %}

    {{ return(main_config) }}

{% endmacro %}
