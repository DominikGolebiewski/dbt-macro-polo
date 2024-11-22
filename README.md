# DBT Macro Polo 🎯

A sophisticated exploration of dbt macro capabilities, pushing the boundaries of what's possible with dbt's macro system. This project demonstrates advanced warehouse configuration management through dynamic macro execution.

## Why This Project Exists

As a passionate dbt practitioner, I noticed a gap in the ecosystem for sophisticated warehouse management solutions. This project was born from the desire to:

- Explore the full potential of dbt macros
- Create dynamic warehouse sizing based on operation context
- Implement robust testing patterns for macro behaviour
- Share novel solutions with the dbt community

The `get_warehouse` macro specifically addresses the common need to dynamically adjust warehouse sizes based on whether an operation is running incrementally or as a full refresh, something not commonly found in existing dbt resources.

## Macro Collection 📚

### Warehouse Management
| Macro | Description | Source |
|-------|-------------|--------|
| **get_warehouse** | Dynamically sets warehouse size based on operation context (incremental vs full-refresh). Perfect for optimising compute costs. | [Source](macros/get_warehouse/get_warehouse.sql) |


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

### Performance Optimisation
```sql
{{ config(
    cluster_by=smart_clustering(this)
) }}
```

### Testing & Validation
```sql
{{ data_quality_suite(
    model_name='my_model',
    severity='warn'
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

## Roadmap 🗺️

Upcoming macros and features:
- [ ] Query optimisation analyser
- [ ] Dynamic materialisation selector
- [ ] Advanced dependency tracker
- [ ] Custom metric generator
- [ ] Cross-database compatibility layer

## Resources
- [dbt Documentation](https://docs.getdbt.com/)
- [dbt Discourse](https://discourse.getdbt.com/)
- [dbt Slack Community](https://community.getdbt.com/)

## License

MIT