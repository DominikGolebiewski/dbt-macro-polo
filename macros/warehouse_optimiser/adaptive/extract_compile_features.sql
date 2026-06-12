{% macro extract_compile_features(raw_code, upstream_count=0) %}
    {{ return(adapter.dispatch('extract_compile_features', 'dbt_macro_polo')(raw_code, upstream_count)) }}
{% endmacro %}

{% macro default__extract_compile_features(raw_code, upstream_count=0) %}

    {# Compile-time query complexity features shared by telemetry capture and
       live prediction so that training and inference inputs always match #}
    {% set sql = (raw_code or '') | lower %}

    {% set features = {
        'join_count': modules.re.findall('\\bjoin\\b', sql) | length,
        'cte_count': modules.re.findall('(?:\\bwith|,)\\s*[a-z0-9_]+\\s+as\\s*\\(', sql) | length,
        'union_count': modules.re.findall('\\bunion\\b', sql) | length,
        'window_fn_count': modules.re.findall('\\bover\\s*\\(', sql) | length,
        'query_length': sql | length,
        'upstream_count': upstream_count or 0
    } %}

    {{ return(features) }}
{% endmacro %}
