{% docs allocate_warehouse %}

# allocate_warehouse

This macro dynamically allocates a Snowflake warehouse based on specified size parameters and environment configuration. It includes caching functionality to optimise warehouse allocation across multiple calls.

## Overview

The macro determines the appropriate warehouse size based on whether the model is running in incremental or full-refresh mode, and constructs a warehouse identifier using environment-specific prefixes.

## Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| incremental_size | Yes | The warehouse size to use for incremental runs. Must be one of the configured available sizes (typically xs, s, m, l) |
| fullrefresh_size | No | The warehouse size to use for full-refresh runs. If not specified, defaults to the incremental_size value |
| calling_macro | No | Name of the calling macro for logging purposes |

## Returns

Returns a string containing the fully qualified warehouse identifier (e.g., "DEV_WH_XS", "PROD_WH_L").

## Configuration Requirements

The macro expects the following configuration in your `dbt_project.yml`:


```yaml
{%- raw -%}
vars:
warehouse_config:
    warehouse_size: ['xs', 's', 'm', 'l']
    environment:
        dev:
            warehouse_name_prefix: dev_wh
        prod:
            warehouse_name_prefix: prod_wh
{%- endraw -%}
```

## Features

- **Caching**: Implements caching to avoid redundant warehouse allocation calculations
- **Environment Awareness**: Uses environment-specific configurations
- **Size Validation**: Validates warehouse sizes against configured available options
- **Logging**: Comprehensive logging for debugging and monitoring
- **Full-refresh Detection**: Automatically detects full-refresh runs

## Usage Example

### Example Usage

```jinja
{%- raw -%}
use warehouse {{ allocate_warehouse(incremental_size='xs', fullrefresh_size='m') }}
{%- endraw -%}
```

## Error Handling

The macro will raise errors in the following scenarios:
- Missing incremental_size parameter
- Invalid warehouse sizes specified
- Missing or invalid environment configuration
- Missing warehouse prefix configuration

## Dependencies

- Requires the `logging` macro for debug and error logging
- Requires the `create_macro_context` macro for execution context

{% enddocs %}
