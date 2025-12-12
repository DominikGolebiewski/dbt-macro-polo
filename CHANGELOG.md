# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- **Refactor**: Redesigned `provision_compute` macro set with 50% less code
  - Consolidated `_get_infrastructure_config` and `_validate_compute_sizes` into single `_resolve_warehouse` private macro
  - Implemented schema-driven configuration validation (declarative, DRY)
  - Adopted fail-fast pattern with clear error messages
  - Leveraged existing `set_runtime_state` utility for memoisation
  - Improved code documentation with design principles

### Removed
- **Macros**: Removed `_get_infrastructure_config` (merged into `_resolve_warehouse`)
- **Macros**: Removed `_validate_compute_sizes` (merged into `_resolve_warehouse`)

## [1.0.0] - 2025-11-21

### Added
- **Testing**: Implemented a comprehensive SQL-based Integration Test suite (`integration_tests/models/integration`) verifying end-to-end macro execution against Snowflake.
- **Testing**: Added Unit Logic Tests (`integration_tests/models/unit_tests`) to verify Jinja macro logic using dbt models and `dbt-utils` equality tests.
- **Tooling**: Added `.sqlfluff` configuration for consistent SQL formatting (Snowflake dialect).
- **Tooling**: Added `.pre-commit-config.yaml` for local code quality checks.
- **Documentation**: Added comprehensive docstrings to all core macros.

### Changed
- **Core**: Promoted project to Production/Stable status (v1.0.0).
- **Refactor**: Polished `adaptive_compute` and `provision_compute` macros for better readability and error handling.
- **Refactor**: Standardized `log_event` macro for consistent logging across the package.
- **Configuration**: Explicitly disabled database/schema/identifier quoting in integration tests to ensure compatibility with generated SQL.
- **Dependencies**: Updated `pyproject.toml` to include `sqlfluff`, `yamllint` and `pre-commit`.

### Fixed
- **Macros**: Fixed `measure_upstream_volume` to correctly handle full-refresh scenarios and missing target relations by skipping High Water Mark calculation.
- **Macros**: Hardened `get_high_water_mark` and `provision_compute` to safely handle contexts where `this` is undefined (e.g., during `run-operation`).
- **CI/CD**: Removed GitHub Actions CI in favor of robust local integration testing.
- Standardized `adapter.dispatch` calls across all macros.

## [0.1.1-beta.1] - Unreleased
- Initial beta release with Warehouse Optimiser features.
