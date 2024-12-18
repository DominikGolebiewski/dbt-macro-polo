{% docs logging %}

# Logging Macro

## Overview
The `logging` macro provides a standardised way to output formatted log messages with different severity levels in your dbt project. It supports colour-coded output and hierarchical logging levels.

## Arguments
- `macro_name` (optional): Name of the macro generating the log
- `message` (optional): The message to be logged
- `level` (optional): Logging level ('DEBUG', 'INFO', 'WARN', 'ERROR'). Defaults to 'INFO'
- `model_id` (optional): Identifier for the model being processed
- `status` (optional): Custom status message

## Configuration
The macro respects two variables that can be set in your `dbt_project.yml`:
- `global_debug_mode`: Boolean flag to enable/disable logging (default: false)
- `logging_level`: Minimum logging level to display (default: 'INFO')

## Log Levels
1. DEBUG (Grey) - Detailed information for debugging
2. INFO (Cyan) - General information about process execution
3. WARN (Yellow) - Warning messages for potential issues
4. ERROR (Red) - Critical errors that halt execution

## Usage Examples

```sql
-- Basic usage
{% raw %}
{{ logging(message="Starting process") }}
-- With all parameters
{{ logging(
macro_name="my_macro",
message="Process completed",
level="INFO",
model_id="my_model",
status="COMPLETE"
) }}
-- Error logging
{{ logging(
macro_name="validation",
message="Failed to validate data",
level="ERROR"
) }}
{% endraw %}
```

## Output Format
The macro produces formatted output with the following structure:
[macro_name] message [model_id] .......... [STATUS]

## Notes
- Messages are only displayed when `global_debug_mode` is set to true
- The logging level hierarchy ensures that only messages at or above the configured `logging_level` are displayed
- ANSI colour codes are used to enhance readability in the terminal

{% enddocs %}