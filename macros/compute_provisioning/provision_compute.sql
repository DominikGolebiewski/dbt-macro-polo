{% macro provision_compute(incremental_size, fullrefresh_size=none) %}
    {{ return(adapter.dispatch('provision_compute', 'dbt_macro_polo')(incremental_size, fullrefresh_size)) }}
{% endmacro %}

{% macro snowflake__provision_compute(incremental_size, fullrefresh_size=none) %}

    {% set macro_polo = var('macro_polo', {}) %}
    {% set model_id = this.schema ~ "." ~ this.name if this else 'unknown_model' %}
    {% set macro_name = 'provision_compute' %}

    {# Validate input parameters #}
    {% if not incremental_size %}
        {% set msg = "Configuration Error: incremental_size parameter is required" %}
        {{ dbt_macro_polo.log_event(message=msg, level='ERROR', model_id=model_id, macro_name=macro_name) }}
    {% endif %}

    {% set incremental = incremental_size | trim | lower %}
    {% set fullrefresh = (fullrefresh_size or incremental_size) | trim | lower %}

    {# Validate relation and materialisation #}
    {% set is_relation_exist = load_relation(this) is not none %}
    {% set materialisation = config.get('materialized', 'undefined') | lower %}
    {% set is_full_refresh = flags.FULL_REFRESH or not is_relation_exist or materialisation == 'table' %}
    {% set size_suffix = fullrefresh if is_full_refresh else incremental %}

    {# Get and validate configuration #}
    {% set infrastructure_def = macro_polo.get('infrastructure_definitions', {}) %}
    {% if not infrastructure_def %}
        {% set msg = "Configuration Error (dbt_project.yml): infrastructure_definitions project variable must be defined." %}
        {{ dbt_macro_polo.log_event(message=msg, level='ERROR', model_id=model_id, macro_name=macro_name) }}
    {% endif %}

    {# Get and validate environment configuration #}
    {% set environments = infrastructure_def.get('environment_contexts', {}) %}
    {% set env_config = environments.get(target.name) %}

    {% if not env_config %}
        {% set msg = "Configuration Error (dbt_project.yml): No configuration found for target environment: " ~ target.name %}
        {{ dbt_macro_polo.log_event(message=msg, level='ERROR', model_id=model_id, macro_name=macro_name) }}
    {% endif %}

    {% set warehouse_prefix = env_config.get('resource_prefix') %}
    {% if not warehouse_prefix %}
        {% set msg = "Configuration Error (dbt_project.yml): resource_prefix missing for environment: " ~ target.name %}
        {{ dbt_macro_polo.log_event(message=msg, level='ERROR', model_id=model_id, macro_name=macro_name) }}
    {% endif %}

    {# Cache handling #}
    {% set state_key = '_macro_polo_provision_compute_' ~  warehouse_prefix ~ '_' ~ size_suffix %}
    {% set state_value = dbt_macro_polo.get_runtime_state(state_key) %}
    
    {% if state_value %}
        {{ dbt_macro_polo.log_event(message="Allocated warehouse from runtime state", level='DEBUG', model_id=model_id, status=state_value | upper, macro_name=macro_name) }}
        {{ return(state_value) }}
    {% endif %}

    {# Get and validate configured sizes #}
    {% set available_sizes = ['xs', 's', 'm', 'l', 'xl', '2xl', '3xl', '4xl', '5xl', '6xl'] %}
    {% set allowed_sizes = infrastructure_def.get('allowed_sizes') %}

    {% if not allowed_sizes or not (allowed_sizes is sequence and allowed_sizes is not string and allowed_sizes is not mapping) %}
        {% set msg = "Configuration Error (dbt_project.yml): allowed_sizes parameter must be a list of strings. Current value: " ~ allowed_sizes %}
        {{ dbt_macro_polo.log_event(message=msg, level='ERROR', model_id=model_id, macro_name=macro_name) }}
    {% endif %}

    {% set invalid_allowed_sizes = [] %}
    {% for size in allowed_sizes %}
        {% if size not in available_sizes %}
            {% do invalid_allowed_sizes.append(size) %}
        {% endif %}
    {% endfor %}

    {% if invalid_allowed_sizes %}
        {% set msg = "Configuration Error (dbt_project.yml): Invalid warehouse size(s) configured: " ~ invalid_allowed_sizes ~ ". Valid sizes are: " ~ available_sizes %}
        {{ dbt_macro_polo.log_event(message=msg, level='ERROR', model_id=model_id, macro_name=macro_name) }}
    {% endif %}

    {# Validate requested sizes against configured sizes #}
    {% set invalid_requested_sizes = [] %}
    {% for size, label in [(incremental, 'incremental'), (fullrefresh, 'fullrefresh')] %}
        {% if size not in allowed_sizes %}
            {% do invalid_requested_sizes.append(label ~ ': ' ~ size) %}
        {% endif %}
    {% endfor %}

    {% if invalid_requested_sizes %}
        {% set msg = "Configuration Error: Requested size(s) not in configured allowed_sizes list: " ~ invalid_requested_sizes ~ ". Configured sizes: " ~ allowed_sizes %}
        {{ dbt_macro_polo.log_event(message=msg, level='ERROR', model_id=model_id, macro_name=macro_name) }}
    {% endif %}

    {# Generate and validate warehouse identifier #}
    {% set warehouse_id = warehouse_prefix ~ "_" ~ size_suffix %}
    {% if warehouse_id | length > 255 %}
        {% set msg = "Configuration Error: Generated warehouse_id exceeds 255 characters: " ~ warehouse_id %}
        {{ dbt_macro_polo.log_event(message=msg, level='ERROR', model_id=model_id, macro_name=macro_name) }}
    {% endif %}

    {# Cache and return result #}
    {{ dbt_macro_polo.log_event(message="Saving warehouse '" ~ warehouse_id ~ "' to runtime state with key '" ~ state_key ~ "'", level='DEBUG', macro_name=macro_name) }}
    {% do macro_polo.get('runtime_state', {}).update({state_key: warehouse_id}) %}
    {{ dbt_macro_polo.log_event(message="Allocated warehouse", model_id=model_id, status=warehouse_id | upper, macro_name=macro_name) }}
    {{ return(warehouse_id) }}
    
{% endmacro %}
