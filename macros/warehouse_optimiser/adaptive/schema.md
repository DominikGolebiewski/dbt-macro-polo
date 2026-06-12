{% docs warehouse_optimiser_adaptive %}

# warehouse_optimiser — adaptive mode (ML)

**Snowflake Only** 🏔️
Machine-learning based warehouse sizing for the `warehouse_optimiser`. The package collects query parameters from past runs (data volume, join complexity, query complexity, column counts, execution outcomes) into a telemetry table, trains a Snowflake Cortex ML classifier on that history enriched with `snowflake.account_usage.query_history`, and then sizes the warehouse adaptively for each incoming batch.

#### Status: Beta 🚧

#### How it fits into the optimiser precedence

```
full refresh > zero incoming rows > adaptive (ML) > monitoring thresholds > scheduling > default
```

Adaptive mode is strictly additive: whenever it is disabled, untrained (cold start), below its confidence threshold, lacking privileges, or hits any error, it returns nothing and the existing thresholds/scheduling/default chain decides — a dbt run can never fail because of adaptive mode.

#### Architecture

1. **Telemetry capture** (every run, automatic once enabled)
   - `set_query_tag`/`unset_query_tag` overrides tag each optimiser-enabled model's queries with a JSON query tag (`app`, `invocation_id`, `node_id`, `full_refresh`) so they can be joined back from `account_usage.query_history`.
   - The `polo_log_telemetry(results)` on-run-end hook writes one row per optimiser-enabled model into `{database}.{audit_schema}.{audit_table}`: upstream/incoming row count, selected warehouse sizes per phase (ctas/delete/insert), compile-time complexity features (`join_count`, `cte_count`, `union_count`, `window_fn_count`, `query_length`, `upstream_count`, `column_count`), run status and execution time. The write is a single batched insert wrapped in an exception-safe block.

2. **Training** (operator-invoked, scheduled e.g. weekly)
   - `dbt run-operation polo_train_warehouse_model` builds a labelled training set and derives the **optimal size label** per run: upsize when the run spilled to storage or breached `target_duration_seconds`; downsize when it finished in under 25% of the target without spilling; otherwise keep the size used.
   - **Training data sources** (`adaptive.training_source`):
     - `telemetry` — only the package's audit table joined to `account_usage.query_history` via the JSON query tag. Exact features, but requires telemetry to have been collecting.
     - `query_history` — **no telemetry needed**: historical dbt runs are mined directly from `account_usage.query_history` using dbt's default JSON query comment (`"node_id": "model...."`, enabled by default in dbt for years), so existing production history is usable immediately. Batch volume is proxied by `rows_inserted + rows_updated`, complexity features come from the current project graph, and column counts from `account_usage.columns`. Runs are grouped per model per session (one session covers the ctas/delete/insert phases of a run).
     - `auto` (default) — both sources unioned; history rows already captured by telemetry (query tag `app=dbt_macro_polo`) are excluded so nothing is double counted. Telemetry naturally takes over as it accumulates.
   - To leverage years of production history, raise `account_usage_lookback_days` (ACCOUNT_USAGE retains 365 days) and run the training operation against the same target environment that produced the history (the graph supplies each model's database/schema mapping).
   - Caveat for the `query_history` source: it requires dbt query comments (on by default — only absent if your project sets `query-comment: null`), and models renamed since the historical runs won't match their old `node_id`.
   - A `SNOWFLAKE.ML.CLASSIFICATION` model is trained on the labelled view (`model_id` is included as a categorical feature so predictions are model-aware).
   - A per-model recommendations table is materialised with the predicted size, its confidence, and the p10/p50/p90 of the trained row-count range.
   - Use `--args '{dry_run: true}'` to build and inspect the training view and label distribution without training.

3. **Hybrid inference** (per model, per run, in the existing pre-hook)
   - The recommendations table is read **once per dbt invocation** and cached.
   - If the incoming batch's row count falls within the trained range (p10–p90 widened by `deviation_threshold`), the precomputed recommendation is used — no extra query.
   - If the batch deviates and `live_predict_enabled: true`, a live `!PREDICT` call is made with the current batch's features (wrapped in an exception-safe block); otherwise adaptive mode falls back to the static thresholds.
   - Recommendations below `confidence_threshold`, outside the project's configured `warehouse_size` list, or above the model-level `max_warehouse_size` cap are rejected or capped.

#### Prerequisites ⚠️

- Snowflake region with Cortex ML functions (`SNOWFLAKE.ML.CLASSIFICATION`) available.
- The training role needs `IMPORTED PRIVILEGES` on the `SNOWFLAKE` database (for `account_usage.query_history`), plus `CREATE SCHEMA`/`CREATE TABLE`/`CREATE VIEW` and `CREATE SNOWFLAKE.ML.CLASSIFICATION` in the target database.
- `account_usage.query_history` has up to ~45 minutes of latency — train on settled history, not on the run that just finished.
- The existing `warehouse_optimiser` prerequisites (dispatch search order, `cache: {}`, `warehouse_config`) apply.

#### Configuration

Project level (`dbt_project.yml`):

```yaml
{% raw %}
on-run-end: [ "{{ dbt_macro_polo.polo_log_telemetry(results) }}" ] # telemetry capture
{% endraw %}

vars:
  macro_polo:
    warehouse_optimiser:
      enabled: true
      default_warehouse_size: xs
      adaptive:
        enabled: false                  # master switch for telemetry + inference (default: false)
        audit_schema: polo_audit        # created lazily in the target database
        audit_table: warehouse_optimiser_runs
        recommendations_table: warehouse_optimiser_recommendations
        model_name: polo_warehouse_classifier
        training_source: auto           # telemetry | query_history | auto (default)
        target_duration_seconds: 300    # SLA used to label the optimal size
        confidence_threshold: 0.7       # minimum predicted-class probability
        deviation_threshold: 0.25       # widening of the trained p10-p90 row-count band
        min_training_samples: 50        # training aborts below this
        live_predict_enabled: false     # allow live !PREDICT on deviating batches
        account_usage_lookback_days: 30
```

Model level (optional overrides):

```yaml
{% raw %}
config:
  meta:
    warehouse_optimiser:
      enabled: true
      adaptive:
        enabled: true        # default: project setting
        max_warehouse_size: l # hard cap on the ML recommendation for this model
{% endraw %}
```

#### Rollout

**Fast path (existing production history):** with `training_source: auto` (or `query_history`) you can train immediately from past runs — set `account_usage_lookback_days` to cover the period you want to learn from, run the dry-run to inspect the training set, then train. No telemetry warm-up needed.

1. Set `adaptive.enabled: true` and add the `on-run-end` hook. Runs behave exactly as before; telemetry starts accumulating alongside any mined history.
2. Run `dbt run-operation polo_train_warehouse_model --args '{dry_run: true}'` and inspect the training view, sample counts and label distribution. If you have no usable history, keep collecting telemetry until `min_training_samples` runs have settled in `account_usage` (allow for latency).
3. Run `dbt run-operation polo_train_warehouse_model`. Subsequent dbt runs log `Adaptive warehouse size selected` / `Adaptive warehouse recommendation applied` and size warehouses from the model.
4. Re-train on a schedule (e.g. weekly) from your orchestrator so recommendations track drift.

#### Cost notes 💰

- Telemetry: one small batched insert per dbt invocation plus free `alter session set query_tag` statements.
- Inference: one small lookup query per dbt invocation; live `!PREDICT` (if enabled) adds one short query only for deviating batches.
- Training: a Cortex ML training job per invocation of the run-operation — schedule it deliberately, on a small/medium warehouse.

{% enddocs %}
