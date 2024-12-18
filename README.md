![macro_polo_logo](https://github.com/user-attachments/assets/797407eb-73bf-449f-8de2-01a2a533ab06)

> A sophisticated exploration of dbt macro capabilities, pushing the boundaries of what's possible with dbt's macro system.

[![dbt](https://img.shields.io/badge/dbt-1.8+-FF694B.svg)](https://github.com/dbt-labs/dbt-core)
[![Snowflake](https://img.shields.io/badge/Snowflake-Ready-29B5E8.svg)](https://www.snowflake.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

https://github.com/user-attachments/assets/bc16e27f-84f4-4ec5-b9a0-c0cf2944201f
> The video demonstrates the package in its alpha version, so the appearance, parameter names, and functionality may differ from the current release.

## 📚 Table of Contents

- [Overview](#overview)
- [Installation](#installation)
- [Macro Collection](#macro-collection)
  - [allocate_warehouse](#allocate_warehouse)
  - [get_max_timestamp](#get_max_timestamp)
  - [warehouse_optimiser](#warehouse_optimiser)
<!-- - [Contributing](#contributing-) -->
- [Resources](#resources-)


## Overview

This project aims to:
- Explore the full potential of dbt macros
- Experiment with novel solutions
- Share sophisticated solutions with the dbt community


>**⚠️ IMPORTANT**: 
>This repository is actively being developed and some features may not be fully functional or may produce unexpected results in your environment. 
> 
> List of known limitations and caveats:
>- warehouse_optimiser may produce unexpected results if setup incorrectly
>- Logging is not yet fully implemented withing warehouse_optimiser, especially DEBUG level
>- Compilation and docs generation is taking longer due to timestamp retrival

## Installation

1. **Add to packages.yml**
```yaml
packages:
  - git: "https://github.com/DominikGolebiewski/dbt-macro-polo.git"
    revision: 0.1.1-beta.1
```

2. **Install the package**
```bash
dbt deps
```

3. **Add Macro Polo to your dbt_project.yml (includes all required vars)**
```yaml
vars:
  macro_polo:
    cache: {} # Required for caching functionality
    warehouse_optimiser: # Required for warehouse optimiser functionality
      enabled: true # Enable warehouse optimiser in your project - global setting
      default_warehouse: 'xs' # Default warehouse size to use if no specific settings are provided
    warehouse_config: # Required for warehouse config functionality
      warehouse_size: ['xs', 's', 'm', 'l', 'xl', '2xl'] # Explicit list of available warehouse sizes in your project
      environment:
        prod: # Matches your profiles.yml target
          warehouse_name_prefix: prod_wh # Prefix for warehouse names in your production environment
        dev:
          warehouse_name_prefix: dev_wh
        <target_name>:
          warehouse_name_prefix: <warehouse_name_prefix>
        ...
```

4. **Add dispatch order to your dbt_project.yml to ensure Macro Polo will use custom delete_insert strategy**
```yaml
dispatch:
  - macro_namespace: dbt
    search_order: ['dbt_macro_polo', 'dbt']

vars:
  ...
```

## Macro Collection

### allocate_warehouse

> **Snowflake Only** - Dynamically sets warehouse size based on operation context.

<details>
<summary><b>Click to expand configuration & usage details</b></summary>

#### Configuration
```yaml
vars:
  macro_polo:
    cache: {} # Required for caching functionality
    warehouse_config:
      warehouse_size: ['xs', 's', 'm', 'l', 'xl', '2xl']
      environment:
        <target_name>:
          warehouse_name_prefix: <warehouse_name_prefix>
        ...
```

#### Usage
In your model:
```sql
{{ config(
    pre_hook=[
        'use warehouse {{ dbt_macro_polo.allocate_warehouse(incremental_size="s", full_refresh_size="xl") }}'
    ]
) }}
```

In your model property file:
```yaml
config:
    materialized: 'incremental'
    incremental_strategy: 'delete+insert'
    unique_key: 'unique_key'
    timestamp_column: 'loaded_timestamp'
    pre_hook: ['use warehouse {{ dbt_macro_polo.allocate_warehouse(incremental_size="s", full_refresh_size="xl") }}']
```

[View Full Documentation →](/macros/allocate_warehouse/schema.md)
</details>

### get_max_timestamp

> **Snowflake Only** - Efficiently retrieves and caches maximum timestamps with built-in warehouse management.

<details>
<summary><b>Click to expand configuration & usage details</b></summary>

#### Configuration
```yaml
vars:
  macro_polo:
    cache: {}  # Required for caching
```

#### Usage
```sql
{% set max_timestamp = dbt_macro_polo.get_max_timestamp(
    timestamp_column='created_at',
    predicate="status = 'active'",
    warehouse_size='m'
) %}
```

[View Full Documentation →](/macros/get_max_timestamp/schema.md)
</details>

### warehouse_optimiser 

This is a beta version of the warehouse optimiser. It is currently in development and some features may not be fully functional or produce unexpected results.

> **Snowflake Only** - Advanced warehouse optimisation and load balancing with resource allocation.

<details>
<summary><b>Click to expand configuration & usage details</b></summary>

#### Key Features
- Dynamic Warehouse Sizing
- Source Monitoring
- Flexible Scheduling

#### Basic Usage
In your model:
```sql
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='unique_key',
    timestamp_column='loaded_timestamp',
    pre_hook=[
        '{{ dbt_macro_polo.warehouse_optimiser() }}'
    ]
) }}
```

In your model property file:
```yaml
config:
    materialized: 'incremental' 
    incremental_strategy: 'delete+insert'
    unique_key: 'unique_key'
    timestamp_column: 'loaded_timestamp'
    pre_hook: ['{{ dbt_macro_polo.warehouse_optimiser() }}']
```

[View Full Documentation →](/macros/warehouse_optimiser/schema.md)
</details>

<!-- #### Contributing 🤝

We welcome contributions! Areas that need attention:

1. **Testing**:
   - Integration tests
   - Performance benchmarks
   - Edge case handling

2. **Documentation**:
   - Performance tuning guide
   - Troubleshooting guide
   - Additional use cases

3. **Features**:
   - Enhanced monitoring
   - Advanced scheduling
   - Custom warehouse parameters -->

#### Resources 📚

- [GitHub Issues](https://github.com/DominikGolebiewski/dbt-macro-polo/issues)

For bugs, questions, or suggestions, please open an issue on GitHub.
