{% macro _get_compute_config() %}
    {{ return(adapter.dispatch('_get_compute_config', 'dbt_macro_polo')()) }}
{% endmacro %}

{% macro default___get_compute_config() %}

    {% set macro_name = '_get_compute_config' %}
    {% set config_root = dbt_macro_polo.validate_macro_polo_var() %}

    {% if 'infrastructure_definition' in config_root %}
        {% set infra_config = config_root.get('infrastructure_definition', {}) %}

        {% if 'environment_context' in infra_config %}
            {% set env_ctx = infra_config.get('environment_context', {}) %}

            {% if target.name in env_ctx %}
                {% set target_env = env_ctx.get(target.name, {}) %}
            {% else %}
                {{ dbt_macro_polo.log_error("environment '" ~ target.name ~ "' not found in environment_context", macro_name) }}
            {% endif %}

        {% else %}
            {{ dbt_macro_polo.log_error("environment_context missing in infrastructure_definition", macro_name) }}
        {% endif %}

        {% set prefix = target_env.get('warehouse_name_prefix') %}

        {% if not prefix %}
            {{ dbt_macro_polo.log_error("warehouse_name_prefix missing for environment '" ~ target.name ~ "'", macro_name) }}
        {% endif %}

        {% set main_config = {
            'environment': target.name,
            'allowed_sizes': infra_config.get('allowed_sizes', ['xs', 's', 'm', 'l']),
            'defaults': {'incremental': 'xs', 'full_refresh': 'xs'}, 
            'prefix': prefix
        } %}
    {% else %}
        {{ dbt_macro_polo.log_error("Provide 'infrastructure_definition' in macro_polo", macro_name) }}
    {% endif %}

    {{ return(main_config) }}
    
{% endmacro %}
