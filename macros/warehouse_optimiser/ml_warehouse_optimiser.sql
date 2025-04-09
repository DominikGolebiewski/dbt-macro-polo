{% macro ml_warehouse_optimiser(query_operation='ctas', model_type='default') %}
    {{ return(adapter.dispatch('ml_warehouse_optimiser', 'dbt_macro_polo')(query_operation, model_type)) }}
{% endmacro %}

{% macro default__ml_warehouse_optimiser(query_operation='ctas', model_type='default') %}
    {# Get model configuration #}
    {% set model_config = model.config.get('meta', {}).get('warehouse_optimiser', {}) %}
    {% set project_config = var('macro_polo', {}).get('warehouse_optimiser', {}) %}
    {% set ml_config = var('macro_polo', {}).get('ml_warehouse_optimiser', {}) %}
    
    {# Check if ML optimiser is enabled #}
    {% if not (project_config.get('enabled', false) and model_config.get('enabled', false) and ml_config.get('enabled', false)) %}
        {{ log("ML Warehouse Optimiser is disabled", info=true) }}
        {{ return(dbt_macro_polo.warehouse_optimiser(query_operation)) }}
    {% endif %}

    {# Get operation data #}
    {% set is_full_refresh = dbt_macro_polo.should_full_refresh() %}
    {% set row_count = var('macro_polo', {}).get('cache', {}).get('_upstream_row_count_' ~ this.identifier, 0) %}
    {% set timestamp_column = model.config.get('timestamp_column', 'loaded_timestamp') %}
    
    {# Fetch historical execution data #}
    {% set query_history %}
        SELECT
            query_id,
            warehouse_size,
            execution_time,
            rows_processed,
            bytes_scanned,
            compilation_time,
            queued_provisioning_time,
            queued_repair_time,
            query_load_percent,
            query_type,
            query_text
        FROM {{ ml_config.get('query_history_table', 'snowflake.account_usage.query_history') }}
        WHERE DATABASE_NAME = '{{ this.database }}' 
        AND SCHEMA_NAME = '{{ this.schema }}'
        AND QUERY_TEXT ILIKE '%{{ this.identifier }}%'
        AND EXECUTION_STATUS = 'SUCCESS'
        ORDER BY start_time DESC
        LIMIT {{ ml_config.get('history_limit', 100) }}
    {% endset %}
    
    {# Feature extraction #}
    {% set features = {} %}
    {% if execute %}
        {% set query_results = run_query(query_history) %}
        
        {% if query_results and query_results.columns|length > 0 %}
            {% set features = {
                'avg_execution_time': query_results.columns[2].values() | sum / query_results.columns[2].values() | length if query_results.columns[2].values() | length > 0 else 0,
                'avg_rows_processed': query_results.columns[3].values() | sum / query_results.columns[3].values() | length if query_results.columns[3].values() | length > 0 else 0,
                'avg_bytes_scanned': query_results.columns[4].values() | sum / query_results.columns[4].values() | length if query_results.columns[4].values() | length > 0 else 0,
                'current_row_count': row_count,
                'is_full_refresh': is_full_refresh,
                'hour_of_day': modules.datetime.datetime.now().hour,
                'day_of_week': modules.datetime.datetime.now().weekday()
            } %}
        {% endif %}
    {% endif %}
    
    {# Model selection based on specified model_type #}
    {% set warehouse_size = "" %}
    
    {% if model_type == 'llm' and ml_config.get('llm_enabled', false) %}
        {# Use LLM-based decision making via external API #}
        {% set llm_recommendation = dbt_macro_polo.query_llm_api(features, ml_config.get('llm_config', {})) %}
        {% set warehouse_size = llm_recommendation %}
    
    {% elif model_type == 'regression' and ml_config.get('regression_enabled', false) %}
        {# Use regression model - this is a simplified version #}
        {% set weights = ml_config.get('regression_weights', {
            'intercept': 1.0,
            'avg_execution_time': 0.001,
            'avg_rows_processed': 0.000001, 
            'avg_bytes_scanned': 0.0000001,
            'current_row_count': 0.000002,
            'is_full_refresh': 1.0,
            'hour_of_day_factor': 0.05,
            'day_of_week_factor': 0.05
        }) %}
        
        {% set score = weights.get('intercept') %}
        {% set score = score + weights.get('avg_execution_time') * features.get('avg_execution_time', 0) %}
        {% set score = score + weights.get('avg_rows_processed') * features.get('avg_rows_processed', 0) %}
        {% set score = score + weights.get('avg_bytes_scanned') * features.get('avg_bytes_scanned', 0) %}
        {% set score = score + weights.get('current_row_count') * features.get('current_row_count', 0) %}
        {% if features.get('is_full_refresh') %}
            {% set score = score + weights.get('is_full_refresh') %}
        {% endif %}
        
        {# Map score to warehouse size #}
        {% set warehouse_sizes = ['xs', 's', 'm', 'l', 'xl', '2xl', '3xl', '4xl'] %}
        {% set index = [[(score / 10)|int, 0]|max, warehouse_sizes|length - 1]|min %}
        {% set warehouse_size = warehouse_sizes[index] %}
    
    {% else %}
        {# Fallback to rule-based decision #}
        {{ log("Using fallback rule-based optimiser", info=true) }}
        {{ return(dbt_macro_polo.warehouse_optimiser(query_operation)) }}
    {% endif %}
    
    {# Allocate the warehouse #}
    {% set allocated_warehouse = dbt_macro_polo.allocate_warehouse(warehouse_size) %}
    {{ log("ML-based warehouse selection: " ~ allocated_warehouse ~ " (model: " ~ model_type ~ ")", info=true) }}
    {{ return('use warehouse ' ~ allocated_warehouse) }}
{% endmacro %}

{% macro query_llm_api(features, config) %}
    {{ return(adapter.dispatch('query_llm_api', 'dbt_macro_polo')(features, config)) }}
{% endmacro %}

{% macro default__query_llm_api(features, config) %}
    {# This would be implemented to call your LLM API #}
    {# For now, returning a fallback value #}
    {{ log("LLM API not implemented, using fallback size", info=true) }}
    {{ return('m') }}
{% endmacro %} 