# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2025-11-21

### Added
- **Tooling**: Added `.sqlfluff` configuration for consistent SQL formatting (Snowflake dialect).
- **Tooling**: Added `.pre-commit-config.yaml` for local code quality checks (linting, trailing whitespace, etc.).
- **CI**: Added GitHub Actions workflow (`.github/workflows/ci.yml`) for automated linting and project parsing.
- **Documentation**: Added comprehensive docstrings to all core macros (`adaptive_compute`, `provision_compute`, `get_high_water_mark`).

### Changed
- **Core**: Promoted project to Production/Stable status (v1.0.0).
- **Refactor**: Polished `adaptive_compute` and `provision_compute` macros for better readability and error handling.
- **Refactor**: Standardized `log_event` macro for consistent logging across the package.
- **Documentation**: Updated `README.md` to reflect production status, removing alpha/beta warnings and clarifying installation.
- **Dependencies**: Updated `pyproject.toml` to include `sqlfluff`, `yamllint` and `pre-commit` as development dependencies.

### Fixed
- Standardized `adapter.dispatch` calls across all macros to ensure cross-database compatibility layers are correctly structured.
- Unified error message formatting and logging levels.

## [0.1.1-beta.1] - Unreleased
- Initial beta release with Warehouse Optimiser features.
