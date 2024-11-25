# DBT Macro Polo 🎯

A sophisticated exploration of dbt macro capabilities, pushing the boundaries of what's possible with dbt's macro system.

## Why This Project Exists

As a passionate dbt practitioner, I noticed a gap in the ecosystem for sophisticated solutions. This project was born from the desire to:

- Explore the full potential of dbt macros
- Experiment with novel solutions
- Share novel solutions with the dbt community

## Enable in Your Project 🔌

1. **Add to packages.yml**
   ```yaml
   packages:
     - git: "https://github.com/DominikGolebiewski/dbt-macro-polo.git"
       revision: 0.0.2  # Specify the git release version you want to use
   ```

2. **Install the package**
   ```bash
   dbt deps
   ```

## Macro Collection 📚

### [get_warehouse](macros/get_warehouse/get_warehouse.sql)

** Snowflake Only **
Dynamically sets warehouse size based on operation context (incremental and full-refresh). Perfect for optimising compute costs.

#### Configuration Requirements

1. **Configure warehouse settings in dbt_project.yml**

```yaml
vars:
    warehouse_config:
        # Define available warehouse sizes for validation
        warehouse_size: ['xs', 's', 'm', 'l', 'xl', '2xl']
        # Map your dbt targets to warehouse configurations
        environments:
            production:
                target_name: prod # Matches your profiles.yml target
                warehouse_name_prefix: production_warehouse
            development:
                target_name: dev
                warehouse_name_prefix: development_warehouse
```

Check out the [integration tests](integration_tests/dbt_project.yml) for example.

#### Usage Example

1. **Configure in your model**

```sql
{{ config(
        pre_hook=[
            'use warehouse {{ dbt_macro_polo.get_warehouse(incremental_size="s", full_refresh_size="xl") }}'
        ]
    )
) }}
```

2. **Run your model**

Incremental run:

```bash
dbt run --select my_model
```

Full refresh run:

```bash
dbt run --select my_model --full-refresh
```

3. **Resolution Examples**

For initial run:

```sql
-- Development environment (target: dev)
use warehouse development_warehouse_xl;
-- Production environment (target: prod)
use warehouse production_warehouse_xl;
```

For incremental runs:

```sql
-- Development environment (target: dev)
use warehouse development_warehouse_s;
-- Production environment (target: prod)
use warehouse production_warehouse_s;
```

For full refresh runs:

```sql
-- Development environment (target: dev)
use warehouse development_warehouse_xl;
-- Production environment (target: prod)
use warehouse production_warehouse_xl;
```

The warehouse name is dynamically constructed using:
- Environment prefix (from `warehouse_config`)
- Warehouse size (based on run type)

If full refresh size is not provided, the warehouse size will be the same as the incremental size.
View materialisation will always use the incremental size even if full refresh size is provided.
Any other materialisation type other then `view`, `table` or `incremental` will use the target warehouse size.

## Contributing 🤝

### Prerequisites

- Python 3.11+
- Poetry (Python package manager)
- dbt-core 1.8+
- dbt-snowflake 1.8+
- Access to a data warehouse (Snowflake)

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/dbt-macro-polo.git
   cd dbt-macro-polo
   ```

2. **Install Poetry** (if not already installed)
   ```bash
   curl -sSL https://install.python-poetry.org | python3 -
   ```

3. **Install dependencies**
   ```bash
   poetry install
   ```

4. **Make the run script executable**
   ```bash
   cd integration_tests
   chmod +x run.sh
   ```

5. **Configure your data warehouse**
   - Copy `.env.template` to `.env`
   - Update with your warehouse credentials

6. **Run the tests**
   ```bash
   ./run.sh
   ```

## Resources
- [dbt Documentation](https://docs.getdbt.com/)
- [dbt Discourse](https://discourse.getdbt.com/)
- [dbt Slack Community](https://community.getdbt.com/)

## License

MIT