{% macro get_delete_insert_merge_sql(target, source, unique_key, dest_columns, incremental_predicates) %}
    {{ return(adapter.dispatch('get_delete_insert_merge_sql', 'dbt_macro_polo')(target, source, unique_key, dest_columns, incremental_predicates)) }}
{% endmacro %}

{% macro default__get_delete_insert_merge_sql(target, source, unique_key, dest_columns, incremental_predicates) %}

    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

    {% if unique_key %}
        {{ dbt_macro_polo.handle_warehouse_switch('delete') }}
        {% if dbt_macro_polo.polo_is_selective_refresh() %}
            {#--
                Parameter-driven delete: the WHERE expression is built from CLI
                vars by the project-provided filter macro, not from a join against
                source. This makes the delete window equal to the reprocess window
                even when upstream removed rows that source no longer covers.

                Gated on `meta.selective_refresh_enabled` so models that havent
                opted in fall through to the standard sequence-key / in-clause
                delete — protects against a global --vars selective_refresh=true
                accidentally wiping rectangles in non-fact tables.
            --#}
            delete from {{ target }}
            where {{ dbt_macro_polo.polo_selective_refresh_filter() }};
        {% elif unique_key is sequence and unique_key is not string %}
            delete from {{ target }}
            using {{ source }}
            where (
                {% for key in unique_key %}
                    {{ source }}.{{ key }} = {{ target }}.{{ key }}
                    {{ "and " if not loop.last}}
                {% endfor %}
                {% if incremental_predicates %}
                    {% for predicate in incremental_predicates %}
                        and {{ predicate }}
                    {% endfor %}
                {% endif %}
            );
        {% else %}
            delete from {{ target }}
            where 
                {{ unique_key }} in (
                select {{ unique_key }}
                from {{ source }}
            )
            {%- if incremental_predicates %}
                {% for predicate in incremental_predicates %}
                    and {{ predicate }}
                {% endfor %}
            {%- endif -%};

        {% endif %}
    {% endif %}
    
    {{ dbt_macro_polo.handle_warehouse_switch('insert') }}
    insert into {{ target }} ({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from {{ source }}
    )

{% endmacro %}