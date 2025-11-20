{% docs get_high_water_mark %}

# Get High Water Mark Macro

## Overview
The `get_high_water_mark` macro is designed to efficiently retrieve the high water mark (maximum timestamp) from a specified column in a model. It includes features such as result persistence, warehouse management, and comprehensive error handling.

## Arguments
- `timestamp_column` (optional): Column name containing timestamps (default: 'loaded_timestamp')
- `predicate` (optional): WHERE clause for filtering records
- `warehouse_size` (optional): Warehouse size to use (default: 'xs')
- `model_name` (optional): Target model name (default: current model)

## Features
### State Persistence
- Implements automatic persistence of results to runtime state
- State keys are generated based on model ID and predicate
- Persisted results are returned without re-querying

### Warehouse Management
- Dynamically allocates warehouses based on specified size
- Automatically reverts to original warehouse after query execution
- Handles warehouse allocation failures gracefully

### Error Handling
Comprehensive validation and error checking for:
- Missing or invalid timestamp columns
- Non-existent relations
- Failed queries
- Null timestamp results

## Usage Examples

```sql
{% raw %} -- Raw block to prevent jinja rendering

-- Basic usage
{% set max_ts = dbt_macro_polo.get_high_water_mark() %}

-- With custom timestamp column and predicate
{% set max_ts = dbt_macro_polo.get_high_water_mark(
timestamp_column='created_at',
predicate="status = 'active'",
warehouse_size='s'
) %}

-- With all parameters
{% set max_ts = dbt_macro_polo.get_high_water_mark(
timestamp_column='updated_at',
predicate='category = "sales"',
warehouse_size='m',
model_name='my_model'
) %}

{% endraw %}
```

## Return Value
- Returns a properly typed timestamp literal (`'timestamp'::timestamp`)
- Returns `false` if any errors occur during execution

## Dependencies
- Requires the `logging` macro for status reporting
- Requires the `create_macro_context` macro for execution context
- Requires the `provision_compute` macro for warehouse management

## Configuration
The macro utilises these configuration elements:
- State management through the `runtime_state` variable
- Warehouse allocation through the warehouse management system
- Debug logging through the logging macro

## Notes
- Always validates input parameters before execution
- Implements safe coalescing to handle null timestamps
- Maintains warehouse state throughout execution
- Provides detailed logging at various execution stages

{% enddocs %}
