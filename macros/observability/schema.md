{% docs log_event %}

# log_event ([source](macros/observability/log_event.sql))

Standardised logging utility for the dbt-macro-polo package. Provides consistent log formatting, level control, and error handling.

## Arguments

| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| message | string | Yes | The message content to log |
| level | string | No | Log level (DEBUG, INFO, WARN, ERROR). Default: INFO |
| model_id | string | No | Identifier of the model context |
| status | string | No | Right-aligned status indicator (e.g., "SUCCESS", "SKIPPED") |
| macro_name | string | No | Name of the calling macro for context |

## Returns

Nothing (Outputs to dbt log/stdout).

## Configuration

```yaml
vars:
  macro_polo:
    observability:
        log_level: 'INFO'  # Controls verbosity. Options: DEBUG, INFO, WARN, ERROR
```

## Usage Example

```sql
{% raw %}
{{ dbt_macro_polo.log_event(
    message="Processing completed", 
    level='INFO', 
    model_id=this.name, 
    status='SUCCESS',
    macro_name='my_macro'
) }}
{% endraw %}
```

## Output Format

```text
[INFO ] Macro-Polo model_name (macro_name) Message text ... [SUCCESS]
```

{% enddocs %}
