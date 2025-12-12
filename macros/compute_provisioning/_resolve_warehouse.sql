{#-
    _resolve_warehouse.sql
    
    Private macro that resolves and validates warehouse configuration.
    Combines infrastructure config retrieval, validation, and warehouse ID construction.
    
    Design Principles:
    - Single Responsibility: One macro handles all resolution logic
    - Fail Fast: Validates configuration upfront with clear error messages
    - Schema-Driven: Declarative validation using configuration schema
    - DRY: No repeated validation patterns
-#}

{% macro _resolve_warehouse(size) %}
    {{ return(adapter.dispatch('_resolve_warehouse', 'dbt_macro_polo')(size)) }}
{% endmacro %}


{% macro default___resolve_warehouse(size) %}

    {#-- Configuration Schema: Define expected structure once --#}
    {% set required_config = [
        {'path': ['infrastructure_definition'], 'name': 'infrastructure_definition'},
        {'path': ['infrastructure_definition', 'allowed_sizes'], 'name': 'allowed_sizes'},
        {'path': ['infrastructure_definition', 'environment_context'], 'name': 'environment_context'}
    ] %}

    {#-- Get base configuration --#}
    {% set config_root = dbt_macro_polo.validate_macro_polo_var() %}

    {#-- Schema-driven validation: iterate once, validate all --#}
    {% for field in required_config %}

        {{ dbt_macro_polo.log_event(message="Validating configuration: " ~ field, level='DEBUG', macro_name='_resolve_warehouse') }}

        {% set current = config_root %}
        {% for key in field.path %}
            {% set current = current.get(key, none) %}
        {% endfor %}
        {% if current is none %}
            {{ _config_error(field.name ~ " is required in macro_polo configuration. Macro Polo is disabled.") }}
        {% endif %}
    {% endfor %}

    {#-- Extract validated configuration --#}
    {% set infra = config_root.infrastructure_definition %}
    {% set env_config = infra.environment_context.get(target.name, none) %}
    
    {% if env_config is none %}
        {{ _config_error("No configuration found for target environment: " ~ target.name) }}
    {% endif %}

    {% set prefix = env_config.get('warehouse_name_prefix', none) %}
    
    {% if not prefix %}
        {{ _config_error("warehouse_name_prefix is required for environment: " ~ target.name) }}
    {% endif %}

    {#-- Validate requested size against allowed sizes --#}
    {% set allowed = infra.allowed_sizes | map('lower') | map('trim') | list %}
    
    {% if size not in allowed %}
        {{ _config_error("Size '" ~ size ~ "' not in allowed_sizes: " ~ allowed | join(', ')) }}
    {% endif %}

    {#-- Construct and return warehouse identifier --#}
    {{ return(prefix ~ '_' ~ size) }}

{% endmacro %}


{#-- Helper: Raises configuration error with consistent formatting --#}
{% macro _config_error(message) %}
    {{ dbt_macro_polo.log_event(
        message="Configuration Error: " ~ message,
        level='ERROR',
        macro_name='provision_compute'
    ) }}
{% endmacro %}
