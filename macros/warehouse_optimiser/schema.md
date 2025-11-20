{% docs warehouse_optimiser %}

# optimise_warehouse

**Snowflake Only** 🏔️
A sophisticated macro for dynamically optimising warehouse sizes based on operation type, data volume, and time-based scheduling, featuring built-in monitoring capabilities and intelligent resource allocation.

#### Status: Beta 🚧

This macro is currently in beta. Whilst functional, there are several known limitations and areas for improvement:
- Time-based scheduling currently only supports 24-hour format
- Warehouse size validation needs enhancement
- Cache invalidation strategy needs refinement
- Error handling could be more graceful
- Testing coverage needs expansion

#### Critical Prerequisites ⚠️

**IMPORTANT**: This macro has been specifically designed and tested for:
- Incremental materialisation **only**
- Delete+Insert strategy **only**
- Models with defined unique key(s)

Usage with any other materialisation strategy or configuration is **not recommended** without thorough testing and validation.

#### Configuration Requirements 📋

1. **dbt Version Requirements**:
   - dbt-core >= 1.8.6
   - dbt-snowflake >= 1.8.4

2. **Required Project-Level Setup** (dbt_project.yml):
```yaml
dispatch:
  - macro_namespace: dbt
    search_order: ['dbt_macro_polo', 'dbt']

vars:
  macro_polo:
    cache: {} # Required for caching functionality
    warehouse_optimiser: # Required for warehouse optimiser functionality
      enabled: true # Enable warehouse optimiser in your project - global setting
      default_warehouse_size: 'xs' # Default warehouse size to use if no specific settings are provided
    warehouse_config: # Required for warehouse config functionality
      warehouse_size: ['xs', 's', 'm', 'l', 'xl', '2xl'] # Explicit list of available warehouse sizes in your project
      environment:
        prod: # Matches your profiles.yml target
          warehouse_name_prefix: prod_wh # Prefix for warehouse names in your production environment
        dev:
          warehouse_name_prefix: dev_wh
        <target_name>:
          warehouse_name_prefix: <warehouse_name_prefix>
```

3. **Required Model-Level Configuration**:
```yaml
{% raw %}
config:
    materialized: 'incremental'
    incremental_strategy: 'delete+insert'
    unique_key: 'unique_key'
    timestamp_column: 'loaded_timestamp'
    pre_hook: ["{{ dbt_macro_polo.optimise_warehouse() }}"]
    
    meta:
      warehouse_optimiser:
        enabled: true # Enable warehouse optimiser in your model
        operation_type:
          on_full_refresh:
            warehouse_size: xl # Warehouse size to use for full refresh operations
          on_run:
            ctas: # Warehouse size to use for incremental ctasoperations
              warehouse_size: xs
              monitoring:
                enabled: true # Enable source row count monitoring for warehouse size
                thresholds:
                  - rows: 10000000
                    warehouse_size: s # Warehouse size to use if source row count is greater than threshold
                  - rows: 100000000
                    warehouse_size: m
            delete: # Warehouse size to use for incremental delete operations
              warehouse_size: xs
              monitoring:
                enabled: true
                thresholds:
                  - rows: 10000000
                    warehouse_size: s
            insert:
              warehouse_size: xs
          on_dry_run: # Upstream dependencies for dry run operations
            upstream_dependency: # List of upstream models to monitor for row count
              - model1
              - model2
{% endraw %}
```

#### Core Features 🎯

- **Dynamic Warehouse Sizing**:
  - Operation-specific sizing (CTAS, DELETE, INSERT)
  - Data volume-based scaling
  - Time-based allocation
  - Day-of-week scheduling

- **Intelligent Monitoring**:
  - Row count-based thresholds
  - Automatic warehouse scaling

- **Flexible Scheduling**:
  - Time-based warehouse allocation
  - Day-of-week scheduling
  - Multiple schedule support
  - Peak/Off-peak handling

#### Parameters 🔧

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| query_operation | Optional | 'ctas' | Operation type: 'ctas', 'delete', 'insert' |

#### Usage Examples 📝

1. **Basic Implementation**:
```sql
{% raw %}
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='unique_key',
    timestamp_column='loaded_timestamp',
    pre_hook=[
        '{{ dbt_macro_polo.optimise_warehouse() }}'
    ]
) }}

select * from {{ ref('my_source_table') }}
{% endraw %}
```

#### Monitoring & Troubleshooting 🔍

1. **Logging**:
   - All operations are logged with model ID
   - Check dbt logs for "Macro Polo" entries
   - Monitor warehouse switches and sizes

{% enddocs %}
