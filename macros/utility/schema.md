{% docs create_macro_context %}

# create_macro_context ([source](macros/utility/create_macro_context.sql))

Creates a standardised context object for macro execution, providing consistent access to macro and model information.

## Arguments

| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| macro_name | string | Yes | Name of the macro being executed |

## Returns

Dictionary containing:
```python
{
    'macro_name': str,           # Uppercase macro name
    'model_id': str,             # Schema.model_name in lowercase
    'materialisation': str,      # Current model materialisation
    'logging_level': str,        # Configured logging level
    'is_adaptive_compute_enabled': bool  # Adaptive compute status
}
```

## Configuration

```yaml
vars:
  macro_polo:
    observability:
      log_level: 'info'  # Optional, defaults to 'info'
    adaptive_compute:
      enabled: true  # Optional, defaults to false
```

## Usage Example

```sql
{% raw %}
{% set ctx = dbt_macro_polo.create_macro_context('my_macro') %}
{{ log("Executing " ~ ctx.macro_name ~ " for " ~ ctx.model_id) }}
{% endraw %}
```

{% enddocs %}

{% docs get_runtime_state %}

# get_runtime_state ([source](macros/utility/get_runtime_state.sql))

Retrieves values from the macro_polo runtime state system. Essential for the adaptive compute's state persistence mechanism.

## Arguments

| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| key | string | Yes | Unique identifier for the stored value |

## Returns

Stored value (any type) or empty dict if not found.

## Configuration

```yaml
vars:
  macro_polo:
    runtime_state: {}  # Required for state persistence functionality
```

## Usage Example

```sql
{% raw %}
{% set state_key = '_macro_polo_' ~ this.name %}
{% set stored_value = dbt_macro_polo.get_runtime_state(state_key) %}
{% endraw %}
```

## Integration with Adaptive Compute

The runtime state system is crucial for:
- Storing row counts for warehouse sizing decisions
- Persisting maximum timestamps for incremental processing
- Maintaining warehouse allocation states

{% enddocs %}

{% docs get_delete_insert_merge_sql %}

# get_delete_insert_merge_sql ([source](macros/utility/get_delete_insert_merge_sql.sql))

Enhanced version of dbt's delete+insert strategy with integrated adaptive compute support.

## Integration with Adaptive Compute

The macro automatically:
1. Scales warehouse for prune (delete) and append (insert) operations

## Configuration Requirements

1. **Model Configuration**:
```yaml
config:
  meta:
    adaptive_compute:
      enabled: true
      execution_strategies:
        incremental:
          prune:
            warehouse_size: 's'
            volume_based_scaling:
              enabled: true
              thresholds:
                - rows: 1000000
                  warehouse_size: 'm'
          append:
            warehouse_size: 's'
            volume_based_scaling:
              enabled: true
              thresholds:
                - rows: 1000000
                  warehouse_size: 'm'
```

2. **Project Configuration**:
```yaml
vars:
  macro_polo:
    adaptive_compute:
      enabled: true
      baseline_size: 'xs'
```

## Usage Example

```sql
{% raw %}
-- In your model SQL
{{ config(
    materialised='incremental',
    incremental_strategy='delete+insert',
    unique_key='order_id',
    pre_hook=["{{ dbt_macro_polo.adaptive_compute() }}"]
) }}

select * from {{ ref('source_table') }}
{% endraw %}
```

{% enddocs %}

{% docs should_full_refresh %}

# should_full_refresh ([source](macros/utility/should_full_refresh.sql))

Determines whether a model should perform a full refresh based on various conditions.

## Returns

Boolean indicating if full refresh is needed.

## Conditions Checked

1. DBT's `--full-refresh` flag is set
2. Relation does not exist
3. Materialisation is 'table'

## Usage Example

```sql
{% raw %}
{% set do_full_refresh = dbt_macro_polo.should_full_refresh() %}
{% if do_full_refresh %}
    {{ log("Performing full refresh") }}
{% endif %}
{% endraw %}
```

## Integration with Adaptive Compute

When used with adaptive compute, helps determine:
- Whether to use full refresh warehouse sizes
- When to apply different monitoring thresholds
- Whether to skip incremental optimisations

{% enddocs %}

{% docs is_within_time_range %}

# is_within_time_range ([source](macros/utility/is_within_time_range.sql))

Helper macro to determine if the current time falls within a scheduled time range.

## Arguments

| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| schedule_name | string | Yes | Name of the schedule for logging |
| current_time | datetime | Yes | Current timestamp |
| start_time | string | Yes | Start time of the range (HH:MM) |
| end_time | string | Yes | End time of the range (HH:MM) |

## Returns

Boolean indicating if the current time is within the range.

{% enddocs %}
