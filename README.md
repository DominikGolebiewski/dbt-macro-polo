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
  - [provision_compute](#provision_compute)
  - [get_high_water_mark](#get_high_water_mark)
  - [adaptive_compute](#adaptive_compute)
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
> - adaptive_compute may produce unexpected results if setup incorrectly
> - Logging is not yet fully implemented withing adaptive_compute, especially DEBUG level
> - Compilation and docs generation is taking longer due to timestamp retrieval

## Installation

1. **Add to packages.yml**
```yaml
packages:
  - git: "https://github.com/DominikGolebiewski/dbt-macro-polo.git"
    revision: 1.0.0
```

2. **Install the package**
```bash
dbt deps
```

3. **Add Macro Polo to your dbt_project.yml (includes all required vars)**
```yaml
vars:
  macro_polo:
    runtime_state: {} # Required for caching functionality (formerly 'cache')
    
    # Project-level configuration for warehouse optimisation
    adaptive_compute:
      enabled: true # Enable/Disable globally
      baseline_size: 'xs' # Default warehouse size
    
    # Infrastructure definitions (Warehouses)
    infrastructure_definition: 
      allowed_sizes: ['xs', 's', 'm', 'l', 'xl', '2xl'] # Valid warehouse sizes
      environment_context:
        prod: 
          resource_prefix: prod_wh # Warehouse name prefix for prod
        dev:
          resource_prefix: dev_wh # Warehouse name prefix for dev
        <target_name>:
          resource_prefix: <resource_prefix>
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

### provision_compute

> **Snowflake Only** - Dynamically sets warehouse size based on operation context.

<details>
<summary><b>Click to expand configuration & usage details</b></summary>

#### Configuration
```yaml
vars:
  macro_polo:
    runtime_state: {} 
    infrastructure_definition:
      allowed_sizes: ['xs', 's', 'm', 'l', 'xl', '2xl']
      environment_context:
        <target_name>:
          resource_prefix: <resource_prefix>
        ...
```

#### Usage
In your model:
```sql
{{ config(
    pre_hook=[
        'use warehouse {{ dbt_macro_polo.provision_compute(incremental_size="s", fullrefresh_size="xl") }}'
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
    pre_hook: ['use warehouse {{ dbt_macro_polo.provision_compute(incremental_size="s", fullrefresh_size="xl") }}']
```

[View Full Documentation →](/macros/compute_provisioning/schema.md)
</details>

### get_high_water_mark

> **Snowflake Only** - Efficiently retrieves and caches maximum timestamps with built-in warehouse management.

<details>
<summary><b>Click to expand configuration & usage details</b></summary>

#### Configuration
```yaml
vars:
  macro_polo:
    runtime_state: {}  # Required for caching
```

#### Usage
```sql
{% set max_timestamp = dbt_macro_polo.get_high_water_mark(
    timestamp_column='created_at',
    predicate="status = 'active'",
    warehouse_size='m'
) %}
```

[View Full Documentation →](/macros/high_water_mark/schema.md)
</details>

### adaptive_compute

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
        '{{ dbt_macro_polo.adaptive_compute() }}'
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
    meta:
      adaptive_compute:
        enabled: true
        execution_strategies:
          incremental:
            build:
              warehouse_size: 'm'
              volume_based_scaling:
                 enabled: true
                 thresholds:
                   - rows: 1000000
                     warehouse_size: 'xl'
              time_based_overrides:
                enabled: true
                windows:
                  - name: 'Morning Rush'
                    days: ['monday', 'tuesday', 'wednesday', 'thursday', 'friday']
                    time_range:
                      start: '08:00'
                      end: '11:00'
                    warehouse_size: 'l'
                    volume_based_scaling:
                      enabled: true
                      thresholds:
                        - rows: 2000000
                          warehouse_size: '2xl'
    pre_hook: ['{{ dbt_macro_polo.adaptive_compute() }}']
```

[View Full Documentation →](/macros/adaptive_compute/schema.md)
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
