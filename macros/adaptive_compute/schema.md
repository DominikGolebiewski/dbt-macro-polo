{% docs adaptive_compute %}

# adaptive_compute

This macro intelligently allocates Snowflake warehouse resources based on data volume, time schedules, and operation type. It serves as the entry point for the adaptive compute system.

## Overview

The macro inspects the current model's configuration and run context to determine the optimal warehouse size. It supports:
- **Operation-based sizing**: Different sizes for build (CTAS), prune (DELETE), and append (INSERT) operations.
- **Volume-based scaling**: Dynamically scales up warehouse size if the incoming data volume exceeds configured thresholds.
- **Time-based scaling**: Overrides sizes or scaling rules during specific time windows (e.g., peak hours).

## Configuration Requirements

The macro relies on the `adaptive_compute` project configuration and `compute_provisioning` model configuration.

### Project Config (dbt_project.yml)
```yaml
vars:
  macro_polo:
    adaptive_compute:
      enabled: true
      baseline_size: 'xs'
```

### Model Config (schema.yml or config block)
```yaml
config:
  meta:
    compute_provisioning:
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
```

## Logic Flow

1.  **Validation**: Checks if the operation is valid and if adaptive compute is enabled.
2.  **Strategy Resolution**: Determines which configuration strategy to use based on the operation (build, prune, append) and run mode (incremental vs. full refresh).
3.  **Volume Determination**: Calculates the volume of incoming data from upstream dependencies.
4.  **Size Determination**:
    *   Checks for time-based overrides.
    *   Evaluates volume-based scaling rules.
    *   Falls back to the baseline size.
5.  **Allocation**: Calls `provision_compute` to set the warehouse.

## Dependencies

- `provision_compute`: For actual warehouse allocation.
- `measure_upstream_volume`: For calculating data volume.
- `log_event`: For observability.

{% enddocs %}
