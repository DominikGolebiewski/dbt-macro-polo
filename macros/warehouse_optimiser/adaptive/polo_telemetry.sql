{# Telemetry for adaptive warehouse scaling. Captures per-model run features into an
   audit table that the training pipeline (polo_train_warehouse_model) later joins
   with snowflake.account_usage.query_history.

   Both macros are safe to call from on-run-end (no `this` / model context required)
   and wrap their DML in Snowflake Scripting blocks so telemetry can never fail a run. #}

{% macro polo_ensure_audit_table() %}
    {{ return(adapter.dispatch('polo_ensure_audit_table', 'dbt_macro_polo')()) }}
{% endmacro %}

{% macro snowflake__polo_ensure_audit_table() %}

    {% if not execute %}
        {{ return(false) }}
    {% endif %}

    {% set macro_polo = var('macro_polo', {}) %}
    {% set adaptive_config = dbt_macro_polo.get_adaptive_config(include_model_meta=false) %}

    {% if not adaptive_config.enabled %}
        {{ return(false) }}
    {% endif %}

    {# Run the DDL once per invocation #}
    {% set cache_key = '_polo_audit_table_ready' %}
    {% set cache_value = dbt_macro_polo.get_cache_value(cache_key) %}
    {% if cache_value == 'unavailable' %}
        {{ return(false) }}
    {% elif cache_value %}
        {{ return(cache_value) }}
    {% endif %}

    {% set audit_schema_fqn = target.database ~ '.' ~ adaptive_config.audit_schema %}
    {% set audit_table_fqn = audit_schema_fqn ~ '.' ~ adaptive_config.audit_table %}

    {% set ddl %}
        execute immediate $$
        begin
            create schema if not exists {{ audit_schema_fqn }};
            create table if not exists {{ audit_table_fqn }} (
                schema_version          number,
                invocation_id           varchar,
                node_id                 varchar,
                model_id                varchar,
                target_name             varchar,
                run_started_at          timestamp_ntz,
                is_full_refresh         boolean,
                upstream_row_count      number,
                warehouse_size_ctas     varchar,
                warehouse_size_delete   varchar,
                warehouse_size_insert   varchar,
                join_count              number,
                cte_count               number,
                union_count             number,
                window_fn_count         number,
                query_length            number,
                upstream_count          number,
                column_count            number,
                status                  varchar,
                execution_time_s        float,
                rows_affected           number,
                loaded_at               timestamp_ntz default current_timestamp()
            );
            return 'ok';
        exception
            when other then
                return 'failed: ' || sqlerrm;
        end;
        $$
    {% endset %}

    {% set results = run_query(ddl) %}
    {% set ddl_status = results.columns[0].values()[0] %}

    {% if ddl_status != 'ok' %}
        {{ dbt_macro_polo.logging(message="Could not create adaptive telemetry audit table " ~ audit_table_fqn
            ~ ". Telemetry disabled for this invocation. Reason: " ~ ddl_status, level='WARN') }}
        {% do macro_polo.get('cache', {}).update({cache_key: 'unavailable'}) %}
        {{ return(false) }}
    {% endif %}

    {% do macro_polo.get('cache', {}).update({cache_key: audit_table_fqn}) %}
    {{ dbt_macro_polo.logging(message="Adaptive telemetry audit table ready", status=audit_table_fqn | upper, level='DEBUG') }}
    {{ return(audit_table_fqn) }}
{% endmacro %}


{% macro polo_log_telemetry(results) %}
    {{ return(adapter.dispatch('polo_log_telemetry', 'dbt_macro_polo')(results)) }}
{% endmacro %}

{% macro snowflake__polo_log_telemetry(results) %}

    {% if not execute %}
        {{ return('') }}
    {% endif %}

    {% set adaptive_config = dbt_macro_polo.get_adaptive_config(include_model_meta=false) %}
    {% if not adaptive_config.enabled %}
        {{ return('') }}
    {% endif %}

    {% set macro_polo = var('macro_polo', {}) %}
    {% set cache = macro_polo.get('cache', {}) %}
    {% set telemetry_rows = [] %}

    {% for result in results %}
        {% set node = result.node %}
        {% if node.resource_type == 'model' and node.config.get('meta', {}).get('warehouse_optimiser', {}).get('enabled', false) %}

            {% set model_id = node.schema | lower ~ '.' ~ node.name | lower %}
            {% set cache_id = model_id | replace('.', '_') %}
            {% set upstream_row_count = cache.get('_upstream_row_count_' ~ cache_id, 0) or 0 %}
            {% set size_ctas = cache.get('_polo_selected_size_ctas_' ~ cache_id) %}
            {% set size_delete = cache.get('_polo_selected_size_delete_' ~ cache_id) %}
            {% set size_insert = cache.get('_polo_selected_size_insert_' ~ cache_id) %}
            {% set features = dbt_macro_polo.extract_compile_features(node.raw_code, node.depends_on.nodes | length) %}

            {% set relation = adapter.get_relation(node.database, node.schema, node.name) %}
            {% set column_count = adapter.get_columns_in_relation(relation) | length if relation is not none else 0 %}

            {% set rows_affected = result.adapter_response.get('rows_affected') %}

            {% set row %}
                select
                    1 as schema_version,
                    '{{ invocation_id }}' as invocation_id,
                    '{{ node.unique_id | replace("'", "''") }}' as node_id,
                    '{{ model_id | replace("'", "''") }}' as model_id,
                    '{{ target.name | replace("'", "''") }}' as target_name,
                    '{{ run_started_at.strftime('%Y-%m-%d %H:%M:%S') }}'::timestamp_ntz as run_started_at,
                    {{ 'true' if flags.FULL_REFRESH else 'false' }} as is_full_refresh,
                    {{ upstream_row_count }} as upstream_row_count,
                    {{ "'" ~ size_ctas ~ "'" if size_ctas else 'null' }} as warehouse_size_ctas,
                    {{ "'" ~ size_delete ~ "'" if size_delete else 'null' }} as warehouse_size_delete,
                    {{ "'" ~ size_insert ~ "'" if size_insert else 'null' }} as warehouse_size_insert,
                    {{ features.join_count }} as join_count,
                    {{ features.cte_count }} as cte_count,
                    {{ features.union_count }} as union_count,
                    {{ features.window_fn_count }} as window_fn_count,
                    {{ features.query_length }} as query_length,
                    {{ features.upstream_count }} as upstream_count,
                    {{ column_count }} as column_count,
                    '{{ result.status }}' as status,
                    {{ result.execution_time if result.execution_time is not none else 'null' }} as execution_time_s,
                    {{ rows_affected if rows_affected is not none else 'null' }} as rows_affected
            {% endset %}
            {% do telemetry_rows.append(row) %}
        {% endif %}
    {% endfor %}

    {% if telemetry_rows | length == 0 %}
        {{ dbt_macro_polo.logging(message="No optimiser-enabled models in this run, skipping telemetry", level='DEBUG') }}
        {{ return('') }}
    {% endif %}

    {% set audit_table_fqn = dbt_macro_polo.polo_ensure_audit_table() %}
    {% if not audit_table_fqn %}
        {{ return('') }}
    {% endif %}

    {% set insert_query %}
        execute immediate $$
        begin
            delete from {{ audit_table_fqn }} where invocation_id = '{{ invocation_id }}';
            insert into {{ audit_table_fqn }} (
                schema_version, invocation_id, node_id, model_id, target_name, run_started_at,
                is_full_refresh, upstream_row_count,
                warehouse_size_ctas, warehouse_size_delete, warehouse_size_insert,
                join_count, cte_count, union_count, window_fn_count, query_length,
                upstream_count, column_count, status, execution_time_s, rows_affected
            )
            {{ telemetry_rows | join(' union all ') }};
            return 'ok';
        exception
            when other then
                return 'failed: ' || sqlerrm;
        end;
        $$
    {% endset %}

    {% set insert_results = run_query(insert_query) %}
    {% set insert_status = insert_results.columns[0].values()[0] %}

    {% if insert_status == 'ok' %}
        {{ dbt_macro_polo.logging(message="Adaptive telemetry captured for " ~ telemetry_rows | length ~ " model(s)", status='LOGGED') }}
    {% else %}
        {{ dbt_macro_polo.logging(message="Adaptive telemetry write skipped. Reason: " ~ insert_status, level='WARN') }}
    {% endif %}

    {{ return('') }}
{% endmacro %}
