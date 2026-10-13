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
    dbt's parse-time config returns '' for every key. With required=true a missing
    or empty key raises at execution time; parsing never raises.
--#}

{% macro get_column_config(name, default=none, required=false) %}
    {{ return(adapter.dispatch('get_column_config', 'dbt_macro_polo')(name, default, required)) }}
{% endmacro %}

{% macro default__get_column_config(name, default=none, required=false) %}
    {%- set unset = '__macro_polo_unset__' -%}
    {%- set value = default -%}
    {%- if execute -%}
        {%- set top_level = config.get(name, unset) -%}
        {%- set meta_columns = (config.get('meta') or {}).get('columns') or {} -%}
        {%- if top_level != unset -%}
            {%- set value = top_level -%}
        {%- elif name in meta_columns -%}
            {%- set value = meta_columns[name] -%}
        {%- endif -%}
    {%- endif -%}
    {%- if required and not value and execute -%}
        {{ exceptions.raise_compiler_error(
            "Macro Polo: " ~ name ~ " must be set in config or config.meta.columns for " ~ model.unique_id
        ) }}
    {%- endif -%}
    {{ return(value) }}
{% endmacro %}
