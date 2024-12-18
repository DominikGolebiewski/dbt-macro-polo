{% macro get_delete_insert_merge_sql(target, source, unique_key, dest_columns, incremental_predicates) %}
    {{ return(adapter.dispatch('get_delete_insert_merge_sql', 'dbt_macro_polo')(target, source, unique_key, dest_columns, incremental_predicates)) }}
{% endmacro %}

{% macro default__get_delete_insert_merge_sql(target, source, unique_key, dest_columns, incremental_predicates) %}

    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

    {% if unique_key %}
        {{ dbt_macro_polo.handle_warehouse_switch('delete') }}
        {% if unique_key is sequence and unique_key is not string %}
            delete from {{ target }}
            -- TODO: Add a comment to the query to indicate that it is a delete operation
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