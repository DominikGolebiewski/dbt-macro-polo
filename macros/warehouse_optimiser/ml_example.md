# ML-Based Warehouse Optimiser Example

This document provides practical examples for implementing machine learning-based warehouse optimisation in your dbt projects.

## Configuration

### 1. DBT Project Configuration

Add the following to your `dbt_project.yml`:

```yaml
vars:
  macro_polo:
    warehouse_optimiser:
      enabled: true
    ml_warehouse_optimiser:
      enabled: true
      query_history_table: "snowflake.account_usage.query_history"
      history_limit: 100
      llm_enabled: true
      regression_enabled: true
      llm_config:
        api_key: "{{ env_var('DBT_LLM_API_KEY', '') }}"
        model_type: "llm"
        python_path: "python"
        script_path: "/path/to/llm_integration.py"
      regression_weights:
        intercept: 1.0
        avg_execution_time: 0.001
        avg_rows_processed: 0.000001
        avg_bytes_scanned: 0.0000001
        current_row_count: 0.000002
        is_full_refresh: 1.0
        hour_of_day_factor: 0.05
        day_of_week_factor: 0.05
```

### 2. Model Configuration

For each model you want to use ML-based warehouse optimisation, add the following to the model configuration:

```sql
{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    meta={
      'warehouse_optimiser': {
        'enabled': true,
        'use_ml': true,
        'ml_model_type': 'llm',  -- or 'regression'
        'operation_type': {
          'incremental': {
            'default_warehouse_size': 'm',
            'thresholds': [
              { 'threshold': 1000000, 'scale': 'l' },
              { 'threshold': 100000, 'scale': 'm' }
            ]
          },
          'full_refresh': {
            'warehouse_size': 'xl'
          }
        }
      }
    }
  )
}}
```

## Usage Examples

### Example 1: Using LLM for Warehouse Optimisation

```sql
-- models/my_model.sql
{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    meta={
      'warehouse_optimiser': {
        'enabled': true,
        'use_ml': true,
        'ml_model_type': 'llm'
      }
    }
  )
}}

SELECT * FROM source_table
```

For this model, the LLM will be used to determine the warehouse size based on historical query patterns.

### Example 2: Using Regression for Warehouse Optimisation

```sql
-- models/another_model.sql
{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    meta={
      'warehouse_optimiser': {
        'enabled': true,
        'use_ml': true,
        'ml_model_type': 'regression'
      }
    }
  )
}}

SELECT * FROM another_source_table
```

This model will use the regression model to determine warehouse size.

### Example 3: Fallback to Rule-Based Optimisation

```sql
-- models/fallback_model.sql
{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    meta={
      'warehouse_optimiser': {
        'enabled': true,
        'use_ml': false,  -- Falls back to rule-based
        'operation_type': {
          'incremental': {
            'default_warehouse_size': 'm',
            'thresholds': [
              { 'threshold': 1000000, 'scale': 'l' },
              { 'threshold': 100000, 'scale': 'm' }
            ]
          }
        }
      }
    }
  )
}}

SELECT * FROM fallback_source_table
```

This model will use the traditional rule-based optimisation.

## How It Works

1. When a model runs, the macro checks if ML-based optimisation is enabled
2. If enabled, it:
   - Fetches historical query data from Snowflake query history
   - Extracts features like average execution time, rows processed, etc.
   - Passes these features to the selected ML model (LLM or regression)
   - Receives a warehouse size recommendation
   - Applies that warehouse size for the current operation

## Monitoring and Training

To improve ML-based warehouse sizing over time:

1. Monitor query performance with the recommended warehouse sizes
2. Collect feedback on whether sizes were optimal
3. For the regression model, adjust weights based on performance
4. For the LLM model, consider fine-tuning with your specific data patterns

## Setup Requirements

1. The Python script `llm_integration.py` must be available on the server running dbt
2. For LLM integration, set the `DBT_LLM_API_KEY` environment variable
3. For Snowflake, ensure JavaScript API access is enabled for shell command execution

## Limitations

- LLM API calls add latency to model execution
- Historical performance data requires time to accumulate
- Initial recommendations may need tuning 