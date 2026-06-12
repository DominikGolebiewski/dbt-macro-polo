{# Training pipeline for adaptive warehouse scaling. Operator-invoked:

       dbt run-operation polo_train_warehouse_model
       dbt run-operation polo_train_warehouse_model --args '{dry_run: true}'

   Builds a labelled training view, trains a SNOWFLAKE.ML.CLASSIFICATION model on it,
   and materialises the per-model recommendations + feature-range table consumed by
   handle_adaptive at run time.

   Training data sources (adaptive.training_source):
   - 'telemetry':      package audit table joined to account_usage.query_history via
                       the JSON query_tag (exact features captured at run time)
   - 'query_history':  historical dbt runs mined directly from
                       account_usage.query_history via dbt's default JSON query
                       comment ("node_id": ...) - no telemetry required. Batch volume
                       is proxied by rows_inserted/rows_updated; complexity features
                       come from the current graph; column counts from
                       account_usage.columns
   - 'auto' (default): both, unioned. Runs already captured by telemetry (tagged with
                       app=dbt_macro_polo) are excluded from the history source so
                       nothing is double counted

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

    {% set training_source = adaptive_config.training_source | trim | lower %}
    {% if training_source not in ['telemetry', 'query_history', 'auto'] %}
        {{ dbt_macro_polo.logging(message="Invalid adaptive.training_source: '" ~ training_source
            ~ "'. Expected: 'telemetry', 'query_history' or 'auto'.", level='ERROR') }}
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

    {# Determine which sources are actually usable #}
    {% set ns = namespace(
        use_telemetry = training_source in ['telemetry', 'auto'],
        use_history = training_source in ['query_history', 'auto']
    ) %}

    {% if ns.use_telemetry and adapter.get_relation(target.database, adaptive_config.audit_schema, adaptive_config.audit_table) is none %}
        {% if training_source == 'telemetry' %}
            {{ dbt_macro_polo.logging(message="Telemetry audit table " ~ audit_table_fqn ~ " not found."
                ~ " Add on-run-end: [\"{{ dbt_macro_polo.polo_log_telemetry(results) }}\"] to dbt_project.yml"
                ~ " and complete at least one run with adaptive enabled before training,"
                ~ " or set adaptive.training_source: query_history to train from history alone.", level='ERROR') }}
            {{ return('') }}
        {% else %}
            {{ dbt_macro_polo.logging(message="Telemetry audit table " ~ audit_table_fqn ~ " not found,"
                ~ " training from account_usage.query_history only.", level='WARN') }}
            {% set ns.use_telemetry = false %}
        {% endif %}
    {% endif %}

    {% set graph_features = dbt_macro_polo.polo_get_graph_model_features() %}
    {% if ns.use_history and graph_features | length == 0 %}
        {% if ns.use_telemetry %}
            {{ dbt_macro_polo.logging(message="No optimiser-enabled models found in the current graph,"
                ~ " skipping the query_history source and training from telemetry only.", level='WARN') }}
            {% set ns.use_history = false %}
        {% else %}
            {{ dbt_macro_polo.logging(message="No optimiser-enabled models found in the current graph."
                ~ " The query_history source needs models with meta.warehouse_optimiser.enabled: true"
                ~ " to map historical runs onto.", level='ERROR') }}
            {{ return('') }}
        {% endif %}
    {% endif %}

    {% set lookback = adaptive_config.account_usage_lookback_days %}
    {% set target_duration = adaptive_config.target_duration_seconds %}
    {% set fast_duration = target_duration * 0.25 %}
    {% set max_ordinal = ordered_sizes | length %}

    {# Shared SQL fragments: query_history size name -> ordinal, label heuristic
       (upsize on spilling or SLA breach, downsize when comfortably fast) #}
    {% set used_ordinal_case %}
        max(case warehouse_size
            {%- for size in ordered_sizes %}
            when '{{ qh_size_names[size] }}' then {{ loop.index }}
            {%- endfor %}
            end)
    {% endset %}
    {% set label_case %}
        case
            when spill_bytes > 0 or elapsed_s > {{ target_duration }}
                then least(used_ordinal + 1, {{ max_ordinal }})
            when elapsed_s < {{ fast_duration }} and spill_bytes = 0
                then greatest(used_ordinal - 1, 1)
            else used_ordinal
        end as optimal_ordinal
    {% endset %}

    {% set model_databases = graph_features | map(attribute='database') | map('upper') | unique | list %}

    {# 1. Labelled training view #}
    {% set create_view_sql %}
        create or replace view {{ view_fqn }} as
        with
        {% if ns.use_history %}
        graph_features (node_id, model_id, join_count, cte_count, union_count, window_fn_count,
                        query_length, upstream_count, table_catalog, table_schema, table_name) as (
            select * from values
            {%- for entry in graph_features %}
            ('{{ entry.node_id | replace("'", "''") }}',
             '{{ entry.model_id | replace("'", "''") }}',
             {{ entry.features.join_count }}, {{ entry.features.cte_count }}, {{ entry.features.union_count }},
             {{ entry.features.window_fn_count }}, {{ entry.features.query_length }}, {{ entry.features.upstream_count }},
             upper('{{ entry.database | replace("'", "''") }}'),
             upper('{{ entry.schema | replace("'", "''") }}'),
             upper('{{ entry.alias | replace("'", "''") }}')){{ ',' if not loop.last }}
            {%- endfor %}
        ),
        column_counts as (
            select table_catalog, table_schema, table_name, count(*) as column_count
            from snowflake.account_usage.columns
            where deleted is null
              and table_catalog in ({{ "'" ~ model_databases | join("', '") ~ "'" }})
            group by 1, 2, 3
        ),
        {# Historical dbt runs identified by dbt's default JSON query comment.
           One training row per model per session (a session groups the ctas/delete/
           insert phases of one run). Batch volume proxied by rows inserted/updated. #}
        history_runs as (
            select
                regexp_substr(query_text, '"node_id"\\s*:\\s*"([^"]+)"', 1, 1, 'e', 1) as node_id,
                session_id,
                sum(total_elapsed_time) / 1000 as elapsed_s,
                sum(coalesce(bytes_spilled_to_local_storage, 0)
                    + coalesce(bytes_spilled_to_remote_storage, 0)) as spill_bytes,
                {{ used_ordinal_case }} as used_ordinal,
                sum(coalesce(rows_inserted, 0) + coalesce(rows_updated, 0)) as rows_processed,
                boolor_agg(query_type = 'CREATE_TABLE_AS_SELECT') as is_full_refresh
            from snowflake.account_usage.query_history
            where start_time >= dateadd(day, -{{ lookback }}, current_timestamp())
              and execution_status = 'SUCCESS'
              and query_type in ('CREATE_TABLE_AS_SELECT', 'INSERT', 'DELETE', 'MERGE')
              and query_text like '%"node_id"%'
              {% if ns.use_telemetry %}
              and try_parse_json(query_tag):app::varchar is distinct from 'dbt_macro_polo'
              {% endif %}
            group by 1, 2
            having node_id is not null and used_ordinal is not null
        ),
        history_labelled as (
            select
                f.model_id,
                h.rows_processed as upstream_row_count,
                h.is_full_refresh,
                f.join_count,
                f.cte_count,
                f.union_count,
                f.window_fn_count,
                f.query_length,
                f.upstream_count,
                coalesce(c.column_count, 0) as column_count,
                h.elapsed_s,
                h.spill_bytes,
                h.used_ordinal
            from history_runs h
            inner join graph_features f
                on f.node_id = h.node_id
            left join column_counts c
                on c.table_catalog = f.table_catalog
               and c.table_schema = f.table_schema
               and c.table_name = f.table_name
        ),
        {% endif %}
        {% if ns.use_telemetry %}
        audit as (
            select *
            from {{ audit_table_fqn }}
            where loaded_at >= dateadd(day, -{{ lookback }}, current_timestamp())
              and lower(status) like 'success%'
        ),
        tagged_history as (
            select
                try_parse_json(query_tag):invocation_id::varchar as invocation_id,
                try_parse_json(query_tag):node_id::varchar as node_id,
                sum(total_elapsed_time) / 1000 as elapsed_s,
                sum(coalesce(bytes_spilled_to_local_storage, 0)
                    + coalesce(bytes_spilled_to_remote_storage, 0)) as spill_bytes,
                {{ used_ordinal_case }} as used_ordinal
            from snowflake.account_usage.query_history
            where start_time >= dateadd(day, -{{ lookback }}, current_timestamp())
              and execution_status = 'SUCCESS'
              and try_parse_json(query_tag):app::varchar = 'dbt_macro_polo'
            group by 1, 2
        ),
        telemetry_labelled as (
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
                h.elapsed_s,
                h.spill_bytes,
                h.used_ordinal
            from audit a
            inner join tagged_history h
                on h.invocation_id = a.invocation_id
               and h.node_id = a.node_id
            where h.used_ordinal is not null
        ),
        {% endif %}
        unioned as (
            {% if ns.use_telemetry %}
            select * from telemetry_labelled
            {% endif %}
            {% if ns.use_telemetry and ns.use_history %}
            union all
            {% endif %}
            {% if ns.use_history %}
            select * from history_labelled
            {% endif %}
        ),
        labelled as (
            select
                model_id, upstream_row_count, is_full_refresh, join_count, cte_count,
                union_count, window_fn_count, query_length, upstream_count, column_count,
                {{ label_case }}
            from unioned
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

    {% set active_sources = (['telemetry'] if ns.use_telemetry else []) + (['query_history'] if ns.use_history else []) %}
    {{ dbt_macro_polo.logging(message="Building training view " ~ view_fqn
        ~ " (sources: " ~ active_sources | join(' + ')
        ~ ", lookback: " ~ lookback ~ " days). Requires IMPORTED PRIVILEGES on the SNOWFLAKE database."
        ~ " Note: ACCOUNT_USAGE has up to ~45 min latency; the most recent runs may not be joinable yet.") }}
    {% do run_query(create_view_sql) %}

    {# 2. Minimum-sample guard #}
    {% set count_results = run_query('select count(*) from ' ~ view_fqn) %}
    {% set sample_count = count_results.columns[0].values()[0] %}

    {% if sample_count < adaptive_config.min_training_samples %}
        {% set hint = " Consider training_source: auto to also mine historical runs from query_history."
            if not ns.use_history
            else " Consider increasing adaptive.account_usage_lookback_days (account_usage retains 365 days)." %}
        {{ dbt_macro_polo.logging(message="Only " ~ sample_count ~ " joinable training sample(s) found"
            ~ " (minimum: " ~ adaptive_config.min_training_samples ~ ")." ~ hint
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
