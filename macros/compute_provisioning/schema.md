{% docs provision_compute %}

# provision_compute

Dynamically provisions a Snowflake warehouse based on run mode and environment configuration.

## Overview

This macro determines the appropriate warehouse size based on whether the model is running in incremental or full-refresh mode, validates the configuration, and returns a fully qualified warehouse identifier.

## Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| incremental_size | Yes | Warehouse size for incremental runs (e.g., 'xs', 's', 'm', 'l') |
| fullrefresh_size | No | Warehouse size for full-refresh runs. Defaults to `incremental_size` |

## Returns

A string containing the warehouse identifier (e.g., `dev_wh_xs`, `prd_wh_l`).

## Configuration

Add to your `dbt_project.yml`:

```yaml
vars:
  macro_polo:
    infrastructure_definition:
      allowed_sizes: ['xs', 's', 'm', 'l']
      environment_context:
        dev:
          warehouse_name_prefix: dev_wh
        prod:
          warehouse_name_prefix: prd_wh
```

## Usage

**In model config:**

```sql
{{
    config(
        pre_hook="USE WAREHOUSE {{ dbt_macro_polo.provision_compute('xs', 'l') }}"
    )
}}
```

**Same size for all modes:**

```sql
{{ dbt_macro_polo.provision_compute('m') }}
```

## Features

- **Memoised**: Caches results to avoid redundant resolution
- **Environment-Aware**: Uses target-specific warehouse prefixes
- **Validated**: Checks sizes against configured `allowed_sizes`
- **Observable**: Structured logging for debugging

## Error Handling

The macro raises an error when:
- `incremental_size` is not provided
- Requested size is not in `allowed_sizes`
- Configuration is missing required fields
- Target environment is not configured

{% enddocs %}
