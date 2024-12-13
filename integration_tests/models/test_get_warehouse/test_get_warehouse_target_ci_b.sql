{% set warehouse_size_incremental_and_full_refresh_fallback = dbt_macro_polo.allocate_warehouse('xs') %}
{% set warehouse_size_incremental_and_full_refresh = dbt_macro_polo.allocate_warehouse('s', 'l') %}
{% set warehouse_size_full_refresh = dbt_macro_polo.allocate_warehouse('m', 'xl') %}

select 
    '{{ warehouse_size_incremental_and_full_refresh_fallback }}' as warehouse_size_incremental_and_full_refresh_fallback,
    '{{ warehouse_size_incremental_and_full_refresh }}' as warehouse_size_incremental_and_full_refresh,
    '{{ warehouse_size_full_refresh }}' as warehouse_size_full_refresh
