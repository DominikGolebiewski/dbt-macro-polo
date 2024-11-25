{% set warehouse_size_incremental_and_full_refresh_fallback = dbt_macro_polo.get_warehouse('xs') %}
{% set warehouse_size_incremental_and_full_refresh = dbt_macro_polo.get_warehouse('s', 'l') %}
{% set warehouse_size_full_refresh = dbt_macro_polo.get_warehouse('m', 'xl') %}

select 
    md5(current_timestamp()) as id,
    '{{ warehouse_size_incremental_and_full_refresh_fallback }}' as warehouse_size_incremental_and_full_refresh_fallback,
    '{{ warehouse_size_incremental_and_full_refresh }}' as warehouse_size_incremental_and_full_refresh,
    '{{ warehouse_size_full_refresh }}' as warehouse_size_full_refresh,
    current_timestamp() as loaded_at
