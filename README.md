# DBT Macro Polo 🎯

A sophisticated exploration of dbt macro capabilities, pushing the boundaries of what's possible with dbt's macro system.

## Why This Project Exists

As a passionate dbt practitioner, I noticed a gap in the ecosystem for sophisticated warehouse management solutions. This project was born from the desire to:

- Explore the full potential of dbt macros
- Experiment with novel solutions
- Share novel solutions with the dbt community

## Enable in Your Project 🔌

1. **Add to packages.yml**
   ```yaml
   packages:
     - git: "https://github.com/DominikGolebiewski/dbt-macro-polo.git"
       revision: x.x.x  # Specify the git release version you want to use
   ```

2. **Install the package**
   ```bash
   dbt deps
   ```

## Macro Collection 📚

### [get_warehouse](macros/get_warehouse/get_warehouse.sql)

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
                warehouse_name_prefix: production # Used in final warehouse name
            development:
                target_name: dev
                warehouse_name_prefix: development
```

Check out the [integration tests](integration_tests/dbt_project.yml) for example.

#### Usage Example

```sql
{{ config(
    warehouse=get_warehouse(
        incremental_size='s',
        full_refresh_size='xl'
    )
) }}
```


#### How It Works

1. **Configuration Validation**:
   - Verifies warehouse_config presence in dbt_project.yml
   - Validates warehouse sizes against allowed options
   - Ensures proper environment mappings exist

2. **Size Selection Logic**:
   - Incremental runs: Utilises specified `incremental_size`
   - Full refreshes: Prioritises `full_refresh_size` if provided
   - Normalises size formatting (e.g., 'XL' → 'xl')
   - Validates against configured size options

3. **Environment Handling**:
   - Identifies current dbt target
   - Maps target to environment settings
   - Extracts corresponding warehouse prefix
   - Example: target 'dev' → prefix 'dev_wh'

4. **Warehouse Name Construction**:
   - Combines environment prefix with chosen size
   - Format: `{environment_prefix}_{size}`
   - Examples:
     - Development: `dev_wh_s`
     - Production: `prod_etl_xl`

#### Benefits
- Standardised warehouse naming across environments
- Automated size optimisation based on operation type
- Environment-aware configuration
- Cost optimisation through appropriate sizing
- Centralised warehouse management

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