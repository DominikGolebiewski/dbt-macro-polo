{#--
    Resolves a model's column attribute -- timestamp_column, date_column,
    retailer_column, supplier_column, product_column or any other column key --
    from either config layout:

        config:
          timestamp_column: loaded_timestamp       # top level
          meta:
            columns:
              timestamp_column: loaded_timestamp   # nested under meta.columns

    dbt deprecates custom top-level config keys, so models can move these keys
    under meta.columns without any reader changing. A top-level key that is set,
    even to an empty value, wins and is returned as set, so existing models
    resolve exactly as before. Under meta only meta.columns.<key> is read.

    Returns `default` when neither layout sets the key, and always while parsing:
    dbt's parse-time config returns '' for every key. With required=true the key must
    be configured, non-empty, in one of the layouts, otherwise execution raises; a
    `default` does not satisfy it. Parsing never raises.
--#}

{% macro get_column_config(name, default=none, required=false) %}
    {{ return(adapter.dispatch('get_column_config', 'dbt_macro_polo')(name, default, required)) }}
{% endmacro %}

{% macro default__get_column_config(name, default=none, required=false) %}
    {# A fresh object marks "absent" and is compared by identity, so no configured value
       can be mistaken for it. #}
    {%- set unset = [] -%}
    {%- set configured = unset -%}
    {%- if execute -%}
        {%- set top_level = config.get(name, unset) -%}
        {%- set meta_columns = (config.get('meta') or {}).get('columns') or {} -%}
        {%- if top_level is not sameas unset -%}
            {%- set configured = top_level -%}
        {%- elif name in meta_columns -%}
            {%- set configured = meta_columns[name] -%}
        {%- endif -%}
    {%- endif -%}
    {# required checks the configuration itself: a default does not satisfy it. #}
    {%- if required and execute and (configured is sameas unset or not configured) -%}
        {{ exceptions.raise_compiler_error(
            "Macro Polo: " ~ name ~ " must be set in config or config.meta.columns for " ~ model.unique_id
        ) }}
    {%- endif -%}
    {{ return(default if configured is sameas unset else configured) }}
{% endmacro %}
