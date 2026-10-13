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
    under meta.columns without any reader changing. The top level wins when both
    are set, so existing models resolve exactly as before.

    Returns `default` when neither layout sets the key. With required=true a
    missing key raises at execution time; parsing never raises, because dbt has
    not merged the model's YAML config into it yet.
--#}

{% macro get_column_config(name, default=none, required=false) %}
    {{ return(adapter.dispatch('get_column_config', 'dbt_macro_polo')(name, default, required)) }}
{% endmacro %}

{% macro default__get_column_config(name, default=none, required=false) %}
    {%- set meta_columns = (config.get('meta') or {}).get('columns') or {} -%}
    {%- set value = config.get(name) or meta_columns.get(name) or default -%}
    {%- if required and not value and execute -%}
        {{ exceptions.raise_compiler_error(
            "Macro Polo: " ~ name ~ " must be set in config or config.meta.columns for " ~ model.unique_id
        ) }}
    {%- endif -%}
    {{ return(value) }}
{% endmacro %}
