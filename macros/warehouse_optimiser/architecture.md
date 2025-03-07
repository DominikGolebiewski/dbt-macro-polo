# Warehouse Optimizer Architecture

This document provides a comprehensive overview of the Warehouse Optimizer architecture, including component relationships and process flows.

## High-Level Architecture

The Warehouse Optimizer is a dbt pre-hook that dynamically adjusts the warehouse size based on the operation type, schedule, and data volume.

```mermaid
graph TD
    A[dbt Model] -->|pre-hook| B[Warehouse Optimizer]
    B -->|Check Config| C{Is Enabled?}
    C -->|Yes| D[Determine Operation Type]
    C -->|No| Z[Use Default Warehouse]
    D -->|CTAS/INSERT/DELETE| E[Handle Operation]
    E -->|Check Schedule| F[Handle Scheduling]
    E -->|Check Row Count| G[Handle Monitoring]
    F --> H[Cron Matching]
    G --> I[Threshold Matching]
    H --> J[Allocate Warehouse]
    I --> J
    J --> K[Return SQL Statement]
    K -->|use warehouse X| A
```

## Component Architecture

The Warehouse Optimizer is composed of several macros, each with a specific responsibility:

```mermaid
graph TD
    A[warehouse_optimiser] --> B[handle_operation]
    B --> C[handle_scheduling]
    B --> D[get_upstream_row_count]
    C --> E[cron_match]
    C --> F[is_within_time_range]
    C --> G[handle_monitoring]
    D --> H[check_upstream_row_count]
    E --> I[field_matches]
    
    classDef main fill:#f9f,stroke:#333,stroke-width:2px;
    classDef core fill:#bbf,stroke:#333,stroke-width:1px;
    classDef helper fill:#bfb,stroke:#333,stroke-width:1px;
    
    class A main;
    class B,C,D,G core;
    class E,F,H,I helper;
```

## Macro Responsibilities

| Macro | Description |
|-------|-------------|
| `warehouse_optimiser` | Main entry point that validates configuration and determines operation type |
| `handle_operation` | Processes the operation type (CTAS, INSERT, DELETE) and determines warehouse size |
| `handle_scheduling` | Checks if current time matches any configured schedules |
| `handle_monitoring` | Checks if row count exceeds any configured thresholds |
| `cron_match` | Determines if current time matches a cron expression |
| `field_matches` | Helper for cron matching to check individual cron fields |
| `get_upstream_row_count` | Gets row count from upstream dependencies |
| `check_upstream_row_count` | Executes query to count rows in a specific relation |
| `is_within_time_range` | Legacy helper for time-based scheduling |

## Process Flow

The following diagram shows the detailed process flow for the Warehouse Optimizer:

```mermaid
sequenceDiagram
    participant Model as dbt Model
    participant WO as warehouse_optimiser
    participant HO as handle_operation
    participant HS as handle_scheduling
    participant CM as cron_match
    participant HM as handle_monitoring
    participant GUR as get_upstream_row_count
    participant AW as allocate_warehouse
    
    Model->>WO: Call pre-hook
    WO->>WO: Check if enabled
    WO->>WO: Determine operation type
    
    alt Full Refresh
        WO->>HO: Pass full_refresh config
        HO->>WO: Return warehouse size
    else Incremental
        WO->>GUR: Get upstream row count
        GUR->>WO: Return row count
        WO->>HO: Pass incremental config & row count
        
        HO->>HS: Check scheduling
        HS->>CM: Match cron expressions
        CM->>HS: Return match result
        
        alt Schedule Matched
            HS->>HM: Check thresholds
            HM->>HS: Return warehouse size
        else No Schedule Match
            HS->>HM: Check default thresholds
            HM->>HS: Return warehouse size
        end
        
        HS->>HO: Return warehouse size
        HO->>WO: Return warehouse size
    end
    
    WO->>AW: Allocate warehouse
    AW->>WO: Return warehouse name
    WO->>Model: Return SQL statement
```

## Configuration Flow

This diagram shows how configuration is processed:

```mermaid
graph TD
    A[dbt_project.yml] -->|Global Config| B[warehouse_optimiser]
    C[Model Config] -->|Model-specific Config| B
    B -->|Operation Type| D{Full Refresh?}
    D -->|Yes| E[full_refresh config]
    D -->|No| F[incremental config]
    F -->|Operation| G{CTAS/INSERT/DELETE}
    G -->|Config| H[Schedule & Monitoring]
    H -->|Cron Expressions| I[Time-based Scaling]
    H -->|Thresholds| J[Volume-based Scaling]
    I --> K[Final Warehouse Size]
    J --> K
```

## Simplified Implementation

The implementation has been simplified to:

1. Reduce code complexity and redundancy
2. Establish clear boundaries between macros
3. Simplify logging
4. Make the code more maintainable
5. Improve readability

Key improvements include:

- Consolidated cron matching into a single, efficient macro
- Simplified parameter passing between macros
- Reduced logging verbosity
- Improved error handling
- Better handling of time ranges in cron expressions 