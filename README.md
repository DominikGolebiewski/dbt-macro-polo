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

### Warehouse Management
| Macro | Description | Source |
|-------|-------------|--------|
| **get_warehouse** | Dynamically sets warehouse size based on operation context (incremental vs full-refresh). Perfect for optimising compute costs.<br><br>**Usage:**<br>```sql<br>{{ config(<br>    warehouse=get_warehouse(<br>        incremental_size='s',<br>        full_refresh_size='xl'<br>    )<br>) }}<br>```<br><br>**Parameters:**<br>- `incremental_size`: Size for incremental runs (xs, s, m, l, xl, 2xl)<br>- `full_refresh_size`: Optional. Size for full refresh runs. Falls back to incremental_size if not specified. | [Source](macros/get_warehouse/get_warehouse.sql) |

## Usage Examples 🚀

### Warehouse Management
```sql
{{ config(
    warehouse=get_warehouse(
        incremental_size='s',
        full_refresh_size='xl'
    )
) }}
```

## Contributing 🤝

We welcome contributions! Here are some areas we're particularly interested in:
- New macro ideas for common dbt challenges
- Performance improvements to existing macros
- Additional test coverage
- Documentation improvements
- Real-world use cases and examples

Please check our [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## Prerequisites

- Python 3.8+
- Poetry (Python package manager)
- dbt-core
- Access to a data warehouse (Snowflake)

## Installation

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
   chmod +x run.sh
   ```

5. **Configure your data warehouse**
   - Copy `profiles.yml.example` to `~/.dbt/profiles.yml`
   - Update with your warehouse credentials



## Resources
- [dbt Documentation](https://docs.getdbt.com/)
- [dbt Discourse](https://discourse.getdbt.com/)
- [dbt Slack Community](https://community.getdbt.com/)

## License

MIT