{# Training pipeline for adaptive warehouse scaling. Operator-invoked:

       dbt run-operation polo_train_warehouse_model
       dbt run-operation polo_train_warehouse_model --args '{dry_run: true}'

   Builds a labelled training view (telemetry audit table joined to
   snowflake.account_usage.query_history via the JSON query_tag), trains a
   SNOWFLAKE.ML.CLASSIFICATION model on it, and materialises the per-model
   recommendations + feature-range table consumed by handle_adaptive at run time.

   Unlike the model-hook macros, this one is allowed to raise on misconfiguration
   (level='ERROR') because it never runs inside a dbt build. #}

{% macro polo_train_warehouse_model(dry_run=false) %}
    {{ return(adapter.dispatch('polo_train_warehouse_model', 'dbt_macro_polo')(dry_run)) }}
{% endmacro %}

{% macro snowflake__polo_train_warehouse_model(dry_run=false) %}

    {% if not execute %}
        {{ return('') }}
    {% endif %}

    {% set macro_polo = var('macro_polo', {}) %}
    {% set adaptive_config = dbt_macro_polo.get_adaptive_config(include_model_meta=false) %}

    {% if not adaptive_config.enabled %}
        {{ dbt_macro_polo.logging(message="Adaptive mode is disabled. Enable it under"
            ~ " vars: macro_polo: warehouse_optimiser: adaptive: enabled: true before training.", level='ERROR') }}
        {{ return('') }}
    {% endif %}

    {# Resolve the consumer's configured sizes, ordered smallest to largest #}
    {% set size_order = ['xs', 's', 'm', 'l', 'xl', '2xl', '3xl', '4xl', '5xl', '6xl'] %}
    {% set qh_size_names = {
        'xs': 'X-Small', 's': 'Small', 'm': 'Medium', 'l': 'Large', 'xl': 'X-Large',
        '2xl': '2X-Large', '3xl': '3X-Large', '4xl': '4X-Large', '5xl': '5X-Large', '6xl': '6X-Large'
    } %}
    {% set configured_sizes = macro_polo.get('warehouse_config', {}).get('warehouse_size') or [] %}
    {% set ordered_sizes = size_order | select('in', configured_sizes) | list %}

    {% if ordered_sizes | length < 2 %}
        {{ dbt_macro_polo.logging(message="warehouse_config.warehouse_size must list at least two valid sizes"
            ~ " to train an adaptive model. Got: " ~ configured_sizes, level='ERROR') }}
        {{ return('') }}
    {% endif %}

    {% set audit_schema_fqn = target.database ~ '.' ~ adaptive_config.audit_schema %}
    {% set audit_table_fqn = audit_schema_fqn ~ '.' ~ adaptive_config.audit_table %}
    {% set view_fqn = audit_schema_fqn ~ '.polo_training_v' %}
    {% set model_fqn = audit_schema_fqn ~ '.' ~ adaptive_config.model_name %}
    {% set rec_fqn = audit_schema_fqn ~ '.' ~ adaptive_config.recommendations_table %}

    {% if adapter.get_relation(target.database, adaptive_config.audit_schema, adaptive_config.audit_table) is none %}
        {{ dbt_macro_polo.logging(message="Telemetry audit table " ~ audit_table_fqn ~ " not found."
            ~ " Add on-run-end: [\"{{ dbt_macro_polo.polo_log_telemetry(results) }}\"] to dbt_project.yml"
            ~ " and complete at least one run with adaptive enabled before training.", level='ERROR') }}
        {{ return('') }}
    {% endif %}

    {% set lookback = adaptive_config.account_usage_lookback_days %}
    {% set target_duration = adaptive_config.target_duration_seconds %}
    {% set fast_duration = target_duration * 0.25 %}
    {% set max_ordinal = ordered_sizes | length %}

    {# 1. Labelled training view: telemetry features + query_history derived label.
         The label is the 'optimal' size: upsize on spilling or SLA breach, downsize
         when comfortably fast, otherwise keep the size that was used. #}
    {% set create_view_sql %}
        create or replace view {{ view_fqn }} as
        with audit as (
            select *
            from {{ audit_table_fqn }}
            where loaded_at >= dateadd(day, -{{ lookback }}, current_timestamp())
              and lower(status) like 'success%'
        ),
        history as (
            select
                try_parse_json(query_tag):invocation_id::varchar as invocation_id,
                try_parse_json(query_tag):node_id::varchar as node_id,
                sum(total_elapsed_time) / 1000 as elapsed_s,
                sum(coalesce(bytes_spilled_to_local_storage, 0)
                    + coalesce(bytes_spilled_to_remote_storage, 0)) as spill_bytes,
                max(case warehouse_size
                    {%- for size in ordered_sizes %}
                    when '{{ qh_size_names[size] }}' then {{ loop.index }}
                    {%- endfor %}
                    end) as used_ordinal
            from snowflake.account_usage.query_history
            where start_time >= dateadd(day, -{{ lookback }}, current_timestamp())
              and execution_status = 'SUCCESS'
              and try_parse_json(query_tag):app::varchar = 'dbt_macro_polo'
            group by 1, 2
        ),
        labelled as (
            select
                a.model_id,
                a.upstream_row_count,
                a.is_full_refresh,
                a.join_count,
                a.cte_count,
                a.union_count,
                a.window_fn_count,
                a.query_length,
                a.upstream_count,
                a.column_count,
                case
                    when h.spill_bytes > 0 or h.elapsed_s > {{ target_duration }}
                        then least(h.used_ordinal + 1, {{ max_ordinal }})
                    when h.elapsed_s < {{ fast_duration }} and h.spill_bytes = 0
                        then greatest(h.used_ordinal - 1, 1)
                    else h.used_ordinal
                end as optimal_ordinal
            from audit a
            inner join history h
                on h.invocation_id = a.invocation_id
               and h.node_id = a.node_id
            where h.used_ordinal is not null
        )
        select
            model_id,
            upstream_row_count,
            is_full_refresh,
            join_count,
            cte_count,
            union_count,
            window_fn_count,
            query_length,
            upstream_count,
            column_count,
            case optimal_ordinal
                {%- for size in ordered_sizes %}
                when {{ loop.index }} then '{{ size }}'
                {%- endfor %}
            end as optimal_size
        from labelled
    {% endset %}

    {{ dbt_macro_polo.logging(message="Building training view " ~ view_fqn
        ~ " (lookback: " ~ lookback ~ " days). Requires IMPORTED PRIVILEGES on the SNOWFLAKE database."
        ~ " Note: ACCOUNT_USAGE has up to ~45 min latency; the most recent runs may not be joinable yet.") }}
    {% do run_query(create_view_sql) %}

    {# 2. Minimum-sample guard #}
    {% set count_results = run_query('select count(*) from ' ~ view_fqn) %}
    {% set sample_count = count_results.columns[0].values()[0] %}

    {% if sample_count < adaptive_config.min_training_samples %}
        {{ dbt_macro_polo.logging(message="Only " ~ sample_count ~ " joinable training sample(s) found"
            ~ " (minimum: " ~ adaptive_config.min_training_samples ~ "). Keep collecting telemetry and retry later."
            ~ " No model was trained.", level='WARN') }}
        {{ return('') }}
    {% endif %}

    {{ dbt_macro_polo.logging(message="Training samples available", status=sample_count) }}

    {# 3. Label distribution (always shown; the only output in dry-run mode) #}
    {% set dist_results = run_query('select optimal_size, count(*) from ' ~ view_fqn ~ ' group by 1 order by 2 desc') %}
    {% for row in dist_results.rows %}
        {{ dbt_macro_polo.logging(message="Label distribution: " ~ row[0], status=row[1]) }}
    {% endfor %}

    {% if dry_run %}
        {{ dbt_macro_polo.logging(message="Dry run complete. Inspect " ~ view_fqn ~ " - no model was trained.", status='DRY RUN') }}
        {{ return('') }}
    {% endif %}

    {# 4. Train the Cortex ML classifier #}
    {{ dbt_macro_polo.logging(message="Training classification model " ~ model_fqn ~ " (this can take a few minutes)") }}
    {% set train_sql %}
        create or replace snowflake.ml.classification {{ model_fqn }}(
            input_data => system$reference('VIEW', '{{ view_fqn }}'),
            target_colname => 'OPTIMAL_SIZE'
        )
    {% endset %}
    {% do run_query(train_sql) %}

    {# 5. Per-model recommendations + feature ranges for hybrid inference.
         Recommendations target typical incremental batches; full refreshes are
         handled earlier in the optimiser precedence by static configuration. #}
    {% set rec_sql %}
        create or replace table {{ rec_fqn }} as
        with stats as (
            select
                model_id,
                median(upstream_row_count) as upstream_row_count,
                median(join_count) as join_count,
                median(cte_count) as cte_count,
                median(union_count) as union_count,
                median(window_fn_count) as window_fn_count,
                median(query_length) as query_length,
                median(upstream_count) as upstream_count,
                median(column_count) as column_count,
                percentile_cont(0.1) within group (order by upstream_row_count) as row_count_p10,
                percentile_cont(0.5) within group (order by upstream_row_count) as row_count_p50,
                percentile_cont(0.9) within group (order by upstream_row_count) as row_count_p90,
                count(*) as training_samples
            from {{ view_fqn }}
            where not is_full_refresh
            group by model_id
        ),
        preds as (
            select
                model_id,
                {{ model_fqn }}!PREDICT(INPUT_DATA => object_construct(
                    'MODEL_ID', model_id,
                    'UPSTREAM_ROW_COUNT', upstream_row_count,
                    'IS_FULL_REFRESH', false,
                    'JOIN_COUNT', join_count,
                    'CTE_COUNT', cte_count,
                    'UNION_COUNT', union_count,
                    'WINDOW_FN_COUNT', window_fn_count,
                    'QUERY_LENGTH', query_length,
                    'UPSTREAM_COUNT', upstream_count,
                    'COLUMN_COUNT', column_count
                )) as prediction,
                row_count_p10,
                row_count_p50,
                row_count_p90,
                training_samples
            from stats
        )
        select
            model_id,
            lower(prediction:class::varchar) as recommended_warehouse_size,
            get(prediction:probability, prediction:class::varchar)::float as confidence,
            row_count_p10,
            row_count_p50,
            row_count_p90,
            training_samples,
            current_timestamp() as trained_at
        from preds
    {% endset %}
    {% do run_query(rec_sql) %}

    {% set rec_count_results = run_query('select count(*) from ' ~ rec_fqn) %}
    {{ dbt_macro_polo.logging(message="Adaptive training complete. Recommendations written to " ~ rec_fqn
        ~ " for " ~ rec_count_results.columns[0].values()[0] ~ " model(s)", status='TRAINED') }}

    {{ return('') }}
{% endmacro %}
