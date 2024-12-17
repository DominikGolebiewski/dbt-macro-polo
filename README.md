# DBT Macro Polo 🎯

> A sophisticated exploration of dbt macro capabilities, pushing the boundaries of what's possible with dbt's macro system.

[![dbt](https://img.shields.io/badge/dbt-1.8+-FF694B.svg)](https://github.com/dbt-labs/dbt-core)
[![Snowflake](https://img.shields.io/badge/Snowflake-Ready-29B5E8.svg)](https://www.snowflake.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## 📚 Table of Contents

- [Overview](#overview)
- [Installation](#installation)
- [Macro Collection](#macro-collection)
  - [allocate_warehouse](#allocate_warehouse)
  - [get_max_timestamp](#get_max_timestamp)
  - [warehouse_optimiser](#warehouse-optimiser-)
- [Contributing](#contributing-)
- [Resources](#resources)

## Overview

This project aims to:
- Explore the full potential of dbt macros
- Experiment with novel solutions
- Share sophisticated solutions with the dbt community

> **⚠️ IMPORTANT**: This repository is actively being developed. The macros are being rigorously tested before their public release. Please test thoroughly in your environment before production deployment.

## Installation

1. **Add to packages.yml**
```yaml
packages:
  - git: "https://github.com/DominikGolebiewski/dbt-macro-polo.git"
    revision: 0.1.0  # Specify your desired version
```

2. **Install the package**
```bash
dbt deps
```

## Macro Collection

### allocate_warehouse

> **Snowflake Only** - Dynamically sets warehouse size based on operation context.

<details>
<summary><b>Click to expand configuration & usage details</b></summary>

#### Configuration
```yaml
vars:
  warehouse_config:
    warehouse_size: ['xs', 's', 'm', 'l', 'xl', '2xl']
    environment:
      prod:
        warehouse_name_prefix: prod_wh
      dev:
        warehouse_name_prefix: dev_wh
```

#### Usage
```sql
{{ config(
    pre_hook=[
        'use warehouse {{ dbt_macro_polo.allocate_warehouse(incremental_size="s", full_refresh_size="xl") }}'
    ]
) }}
```

[View Full Documentation →](docs/allocate_warehouse.md)
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

[View Full Documentation →](docs/get_max_timestamp.md)
</details>

### Warehouse Optimiser 🏭

> **Snowflake Only** - Advanced warehouse optimization with intelligent resource allocation.

<details>
<summary><b>Click to expand configuration & usage details</b></summary>

#### Key Features
- Dynamic Warehouse Sizing
- Intelligent Monitoring
- Flexible Scheduling
- Performance Tracking

#### Basic Usage
```sql
{{ config(
    pre_hook=[
        '{{ dbt_macro_polo.warehouse_optimiser() }}'
    ]
) }}
```

[View Full Documentation →](docs/warehouse_optimiser.md)
</details>

## Contributing 🤝

### Prerequisites
- Python 3.11+
- Poetry
- dbt-core 1.8+
- dbt-snowflake 1.8+
- Snowflake access

### Quick Start
```bash
# Clone repository
git clone https://github.com/yourusername/dbt-macro-polo.git
cd dbt-macro-polo

# Install dependencies
poetry install

# Setup integration tests
cd integration_tests
chmod +x run.sh

# Configure environment
cp .env.template .env
# Edit .env with your credentials

# Run tests
./run.sh
```

## Resources

- 📚 [dbt Documentation](https://docs.getdbt.com/)
- 💬 [dbt Discourse](https://discourse.getdbt.com/)
- 🤝 [dbt Slack Community](https://community.getdbt.com/)

---

<p align="center">
Licensed under <a href="LICENSE">MIT</a><br>
Created with ❤️ by the dbt community
</p>
