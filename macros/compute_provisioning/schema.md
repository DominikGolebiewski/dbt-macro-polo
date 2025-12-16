{% docs provision_compute %}

# provision_compute

A production-ready macro that provisions Snowflake warehouses with intelligent size selection and load balancing based on execution mode and environment context.

## Overview

Dynamically allocates compute resources by determining the appropriate warehouse size for incremental vs. full-refresh operations. Supports advanced load balancing strategies to distribute workload across multiple warehouses.

## Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| incremental_size | Yes | Warehouse size for incremental runs (e.g., 'xs', 's', 'm', 'l'). Must be in configured allowed_sizes |
| fullrefresh_size | No | Warehouse size for full-refresh runs. Defaults to configured `full_refresh` default, otherwise `incremental_size` |

## Returns

Fully qualified warehouse identifier string (e.g., `dev_wh_xs`, `prd_wh_l`, `dev_wh_xs_1`).

## Configuration

Add to your `dbt_project.yml` variable `macro_polo`.

### Modern Configuration (Recommended)

```yaml
{%- raw -%}
vars:
  macro_polo:
    compute:
      environment: dev
      allowed_sizes: ['xs', 's', 'm', 'l', 'xl']
      # Optional: Intelligent Load Balancing
      # Map sizes to pools of warehouses
      warehouses:
        xs: ['dev_wh_xs_1', 'dev_wh_xs_2']
        s:  ['dev_wh_s']
      # Optional: Default Strategy is 'prefix' if warehouses map is empty
      defaults:
        incremental: xs
        full_refresh: s
{%- endraw -%}
```

### Legacy Configuration (Supported)

```yaml
{%- raw -%}
vars:
  macro_polo:
    infrastructure_definition:
      allowed_sizes: ['xs', 's', 'm', 'l']
      environment_context:
        dev:
          warehouse_name_prefix: dev_wh
{%- endraw -%}
```

## Features

- **Intelligent Load Balancing**: Automatically distributes queries across a pool of warehouses if configured.
- **Fail-Fast Validation**: Early parameter and configuration validation.
- **Environment Isolation**: Target-specific warehouse allocation.
- **Execution Mode Detection**: Automatic full-refresh detection with size switching and sensible defaults.
- **Backwards Compatibility**: Supports legacy `infrastructure_definition` config.

## Usage

```jinja
{%- raw -%}
{{ config(
    pre_hook="use warehouse {{ dbt_macro_polo.provision_compute('xs', 'm') }}"
) }}
{%- endraw -%}
```

{% enddocs %}
