{% macro query_llm_api(features, config) %}
    {{ return(adapter.dispatch('query_llm_api', 'dbt_macro_polo')(features, config)) }}
{% endmacro %}

{% macro default__query_llm_api(features, config) %}
    {# This macro calls the Python script to query an LLM API #}
    {% set python_exec = config.get('python_path', 'python') %}
    {% set script_path = config.get('script_path', '/usr/local/bin/llm_integration.py') %}
    {% set api_key = config.get('api_key', env_var('DBT_LLM_API_KEY', '')) %}
    {% set model_type = config.get('model_type', 'regression') %}
    {% set weights_json = config.get('weights', {}) | tojson %}
    
    {# Convert features to JSON #}
    {% set features_json = features | tojson %}
    
    {# Construct command #}
    {% set cmd %}
        {{ python_exec }} {{ script_path }} --features '{{ features_json }}' --model {{ model_type }} --api-key {{ api_key }}{% if model_type == 'regression' %} --weights '{{ weights_json }}'{% endif %}
    {% endset %}
    
    {# Execute command and capture output #}
    {% set result = run_shell_command(cmd) %}
    
    {% if result.returncode == 0 %}
        {% set warehouse_size = result.stdout.strip() %}
        {{ log("LLM API recommendation: " ~ warehouse_size, info=true) }}
        {{ return(warehouse_size) }}
    {% else %}
        {{ log("Error calling LLM API: " ~ result.stderr, info=true) }}
        {{ return('m') }}  {# Default fallback #}
    {% endif %}
{% endmacro %}

{# This is a placeholder macro that might need to be implemented based on your DBT adapter #}
{% macro run_shell_command(cmd) %}
    {{ return(adapter.dispatch('run_shell_command', 'dbt_macro_polo')(cmd)) }}
{% endmacro %}

{% macro default__run_shell_command(cmd) %}
    {# This is a placeholder - you'll need to implement this for your specific DBT adapter #}
    {% set result = {'stdout': 'm', 'stderr': '', 'returncode': 0} %}
    {{ log("Shell command execution not implemented in this adapter. Command: " ~ cmd, info=true) }}
    {{ return(result) }}
{% endmacro %}

{# Snowflake-specific implementation using JavaScript API #}
{% macro snowflake__run_shell_command(cmd) %}
    {% set js %}
    var process = require('child_process');
    var response = {};
    
    try {
        // Execute command and capture output
        var result = process.execSync("{{ cmd | replace("'", "''") }}", {encoding: 'utf8'});
        response = {
            stdout: result.trim(),
            stderr: '',
            returncode: 0
        };
    } catch (error) {
        response = {
            stdout: '',
            stderr: error.message,
            returncode: error.status || 1
        };
    }
    
    return JSON.stringify(response);
    {% endset %}
    
    {% set result = run_js(js) %}
    {% set result_obj = fromjson(result) %}
    {{ return(result_obj) }}
{% endmacro %} 