# Warehouse Optimizer with Cron Scheduling

This document provides examples of how to use the warehouse optimizer with cron-based scheduling.

## Model Configuration Example

The SQL model configuration can include cron-based scheduling as shown below:

```yaml
config:
  tags: ["sainsburys", "morrisons", "staging"]
  materialized: '{{ "atheon_insert_by_replace" if var("is_replace", False) == True else "incremental" }}'
  incremental_strategy: "delete+insert"
  unique_key: ["source_db_id", "day_date", "unique_key"]
  timestamp_column: "loaded_timestamp"
  date_column: "day_date"
  retailer_column: "source_db_id"
  cluster_by: ["source_db_id", "day_date", "loaded_timestamp"]
  pre_hook: ["{{ dbt_macro_polo.warehouse_optimiser() }}"]

  meta:
    warehouse_optimiser:
      enabled: true
      operation_type:
        full_refresh:
          warehouse: xl
        incremental:
          ctas:
            default: l
            schedule:
              - cron: "0 7-19 * * 1-5"  # Every hour from 7am to 7pm, Monday to Friday
                scale: xl
              - cron: "0 0 * * *"       # At midnight every day
                scale: xxl
            monitor:
              - threshold: 1000000
                scale: xl
              - threshold: 5000000
                scale: xxl
          delete:
            default: l
            schedule:
              - cron: "0 */4 * * *"     # Every 4 hours
                scale: xl
            monitor:
              - threshold: 1000000
                scale: xl
              - threshold: 5000000
                scale: xxl
          insert:
            default: s
            schedule:
              - cron: "*/30 * * * *"    # Every 30 minutes
                scale: s
              - cron: "0 */2 * * *"     # Every 2 hours
                scale: l
            monitor:
              - threshold: 500000
                scale: l
              - threshold: 1000000
                scale: xl
        dry_run:
          upstream_dependency: dsr_input.stream_act_mvt_daysku
```

## Cron Expression Format

Cron expressions use the standard 5-field format:

```
┌───────────── minute (0 - 59)
│ ┌───────────── hour (0 - 23)
│ │ ┌───────────── day of the month (1 - 31)
│ │ │ ┌───────────── month (1 - 12)
│ │ │ │ ┌───────────── day of the week (0 - 6) (Sunday to Saturday)
│ │ │ │ │                                   
│ │ │ │ │
│ │ │ │ │
* * * * *
```

### Common Patterns

- `* * * * *` - Every minute
- `*/15 * * * *` - Every 15 minutes
- `0 * * * *` - Every hour
- `0 */2 * * *` - Every 2 hours
- `0 0 * * *` - Every day at midnight
- `0 8-17 * * 1-5` - Every hour from 8am to 5pm, Monday to Friday
- `*/30 9-16 * * 1-5` - Every 30 minutes between 9am and 4pm, Monday to Friday

## How Cron Matching Works

The warehouse optimizer will:

1. Parse the cron expression into its components
2. Compare the current time with the cron expression
3. If matched, use the corresponding warehouse size (scale)
4. If no cron expression matches, fall back to the default warehouse size
5. Apply monitoring thresholds if configured

The cron expression is evaluated at the time the model runs, allowing for dynamic scaling based on schedule, which is especially useful for optimizing resource usage during peak and off-peak times. 