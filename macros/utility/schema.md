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
    'is_warehouse_optimiser_enabled': bool  # Warehouse optimiser status
}
```

## Configuration

```yaml
vars:
  macro_polo:
    logging_level: 'info'  # Optional, defaults to 'info'
    warehouse_optimiser:
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

{% docs get_cache_value %}

# get_cache_value ([source](macros/utility/get_cache_value.sql))

Retrieves values from the macro_polo cache system. Essential for the warehouse optimiser's caching mechanism.

## Arguments

| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| cache_key | string | Yes | Unique identifier for the cached value |

## Returns

Cached value (any type) or empty dict if not found.

## Configuration

```yaml
vars:
  macro_polo:
    cache: {}  # Required for caching functionality
```

## Usage Example

```sql
{% raw %}
{% set cache_key = '_macro_polo_' ~ this.name %}
{% set cached_value = dbt_macro_polo.get_cache_value(cache_key) %}
{% endraw %}
```

## Integration with Warehouse Optimiser

The cache system is crucial for:
- Storing row counts for warehouse sizing decisions
- Caching maximum timestamps for incremental processing
- Maintaining warehouse allocation states

{% enddocs %}

{% docs get_delete_insert_merge_sql %}

# get_delete_insert_merge_sql ([source](macros/utility/get_delete_insert_merge_sql.sql))

Enhanced version of dbt's delete+insert strategy with integrated warehouse optimisation support.

## Integration with Warehouse Optimiser

The macro automatically:
1. Scales warehouse for delete and insert operations

## Configuration Requirements

1. **Model Configuration**:
```yaml
config:
  meta:
    warehouse_optimiser:
      enabled: true
      operation_type:
        on_run:
          delete:
            warehouse_size: 's'
            monitoring:
              enabled: true
              thresholds:
                - rows: 1000000
                  warehouse_size: 'm'
          insert:
            warehouse_size: 's'
            monitoring:
                enabled: true
                thresholds:
                - rows: 1000000
                    warehouse_size: 'm'
```

2. **Project Configuration**:
```yaml
vars:
  macro_polo:
    warehouse_optimiser:
      enabled: true
      default_warehouse: 'xs'
```

## Usage Example

```sql
{% raw %}
-- In your model SQL
{{ config(
    materialised='incremental',
    incremental_strategy='delete+insert',
    unique_key='order_id',
    pre_hook=["{{ dbt_macro_polo.warehouse_optimiser() }}"]
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

## Integration with Warehouse Optimiser

When used with warehouse optimiser, helps determine:
- Whether to use full refresh warehouse sizes
- When to apply different monitoring thresholds
- Whether to skip incremental optimisations

{% enddocs %}