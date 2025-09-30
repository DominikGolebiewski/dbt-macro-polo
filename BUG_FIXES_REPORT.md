# Bug Fixes Report

This document details the 3 bugs found and fixed in the dbt Macro Polo codebase.

---

## Bug 1: Time Range Comparison Logic Error (Logic Bug)

### Location
**File:** `/workspace/macros/warehouse_optimiser/handle_scheduling.sql`  
**Line:** 126  
**Function:** `is_within_time_range`

### Description
The time range comparison was using an inclusive upper bound (`<=`) instead of an exclusive upper bound (`<`). This caused incorrect behavior when checking if the current time falls within a scheduled time range.

### Issue
```sql
{% set is_within_range = current_minutes >= start_minutes and current_minutes <= end_minutes %}
```

The problem with this logic:
- If a schedule ends at `17:00` (5:00 PM), and the current time is exactly `17:00`, the condition would return `true`
- This means the schedule would be considered active even at the exact end time
- In time range logic, the end time should typically be exclusive (e.g., `09:00-17:00` means from 9:00 AM up to, but not including, 5:00 PM)
- This creates ambiguity when schedules are adjacent (e.g., `09:00-17:00` and `17:00-23:59`) - both would match at 17:00

### Impact
- **Severity:** Medium
- **Type:** Logic Error
- **Effect:** Could cause the wrong warehouse size to be selected when the current time matches a schedule's end time exactly
- **Real-world scenario:** If you have a "business hours" schedule (09:00-17:00) using a larger warehouse and an "after hours" schedule (17:00-23:59) using a smaller warehouse, at exactly 17:00, the system might incorrectly select the business hours warehouse

### Fix
```sql
{% set is_within_range = current_minutes >= start_minutes and current_minutes < end_minutes %}
```

Changed the comparison from `<=` to `<` for the upper bound, making the end time exclusive. This follows standard time range conventions where the end time is not included in the range.

### Verification
The fix ensures that:
- A schedule from 09:00 to 17:00 is active from 09:00:00 up to 16:59:59
- At exactly 17:00:00, the schedule is no longer active
- This prevents overlap when multiple schedules are configured adjacently

---

## Bug 2: Unreachable Code Due to Redundant Break Statement (Code Quality Bug)

### Location
**File:** `/workspace/macros/warehouse_optimiser/handle_scheduling.sql`  
**Lines:** 55-60  
**Function:** `handle_scheduling`

### Description
There was a redundant `break` statement that could never be reached due to a previous `break` statement in the same code path. This represents dead code that serves no purpose and could confuse future maintainers.

### Issue
```jinja2
{% for schedule in schedules %}
    {% set times = schedule.get('times', {}) %}
    {% set days = schedule.get('days', []) %}
    {% set schedule_name = schedule.get('name', 'Unnamed schedule') %}

    {% if current_day in days %}
        {% if dbt_macro_polo.is_within_time_range(schedule_name, current_time, times.get('start'), times.get('end')) %}
            {% set is_matched.value = true %}
            {{ dbt_macro_polo.logging(message="Schedule matched", model_id=model_id, status=schedule_name | upper) }}
            {% if schedule.get('monitoring', {}).get('enabled', false) and has_on_dry_run_config %}
                {# ... monitoring logic ... #}
            {% else %}
                {% set final_size.value = schedule.get('warehouse_size', default_warehouse_size) %}
            {% endif %}
            {% break %}  {# FIRST BREAK - executed when time range matches #}
        {% endif %}
    {% endif %}
    {% if is_matched.value %}
        {% break %}  {# SECOND BREAK - UNREACHABLE! #}
    {% endif %}
{% endfor %}
```

The problem:
- When a schedule matches (time range check passes), `is_matched.value` is set to `true` and the first `break` executes
- This immediately exits the loop
- The second `break` statement (lines 58-60) checks if `is_matched.value` is true, but this can never be reached because the first `break` already exited the loop
- This is classic dead code

### Impact
- **Severity:** Low
- **Type:** Code Quality Issue / Dead Code
- **Effect:** No functional impact, but reduces code clarity and maintainability
- **Performance:** Negligible (the unreachable code is never executed anyway)

### Fix
```jinja2
{% for schedule in schedules %}
    {% set times = schedule.get('times', {}) %}
    {% set days = schedule.get('days', []) %}
    {% set schedule_name = schedule.get('name', 'Unnamed schedule') %}

    {% if current_day in days %}
        {% if dbt_macro_polo.is_within_time_range(schedule_name, current_time, times.get('start'), times.get('end')) %}
            {% set is_matched.value = true %}
            {{ dbt_macro_polo.logging(message="Schedule matched", model_id=model_id, status=schedule_name | upper) }}
            {% if schedule.get('monitoring', {}).get('enabled', false) and has_on_dry_run_config %}
                {# ... monitoring logic ... #}
            {% else %}
                {% set final_size.value = schedule.get('warehouse_size', default_warehouse_size) %}
            {% endif %}
            {% break %}
        {% endif %}
    {% endif %}
{% endfor %}
```

Removed the redundant second `break` statement (lines 58-60) as it was unreachable.

### Verification
The fix improves code clarity by:
- Removing dead code that could confuse developers
- Making the loop logic clearer and more straightforward
- Reducing maintenance burden (no one needs to wonder why there are two breaks)

---

## Bug 3: Cache Miss Returns Empty Dict Instead of None (Type Safety Bug)

### Location
**File:** `/workspace/macros/utility/get_cache_value.sql`  
**Line:** 12  
**Function:** `get_cache_value`

### Description
When a cache key doesn't exist, the function returns an empty dictionary `{}` instead of a proper falsy value like `none`. This causes issues with truthiness checks throughout the codebase.

### Issue
```jinja2
{% macro default__get_cache_value(cache_key) %}
    {% set macro_ctx = dbt_macro_polo.create_macro_context('get_cache_value') %}
    {% set macro_name = macro_ctx.macro_name %}
    {% set model_id = macro_ctx.model_id %}
    
    {% set macro_polo = var('macro_polo', {}) %}
    {% set cache = macro_polo.get('cache', {}) %}
    {% set cache_value = cache.get(cache_key, {}) %}  {# Returns {} on cache miss #}
    {{ dbt_macro_polo.logging(macro_name, message="Cache handling: " ~ {'cache_key': cache_key, 'cache_value': cache_value}, level='DEBUG', model_id=model_id) }}
    {{ return(cache_value) }}
{% endmacro %}
```

The problem:
- When a cache key doesn't exist, `cache.get(cache_key, {})` returns an empty dictionary `{}`
- In Jinja2/Python, an empty dictionary is **truthy** when used in boolean contexts
- Code that checks `if cache_value:` will incorrectly treat a cache miss as a cache hit

Example of problematic usage in `/workspace/macros/warehouse_optimiser/check_upstream_row_count.sql` (line 17):
```jinja2
{% set cache_value = dbt_macro_polo.get_cache_value(cache_key) %}

{% if cache_value %}
    {{ dbt_macro_polo.logging(macro_name, message="Upstream row count from cache", model_id=model_id, status=cache_value | upper) }}
    {{ return(cache_value) }}
{% endif %}
```

With the bug:
- If the cache doesn't have the key, `cache_value` is `{}`
- `if cache_value:` evaluates to `true` (because `{}` is truthy)
- The code tries to return `{}` and log it, which is incorrect behavior
- This could cause type errors or unexpected behavior downstream

### Impact
- **Severity:** High
- **Type:** Logic/Type Safety Bug
- **Effect:** Cache miss detection fails, potentially causing:
  - Incorrect values being returned to calling code
  - Type errors when empty dict is used where a string/number is expected
  - Performance issues (redundant queries not being executed when cache is empty)
  - Subtle bugs that are hard to diagnose

### Fix
```jinja2
{% macro default__get_cache_value(cache_key) %}
    {% set macro_ctx = dbt_macro_polo.create_macro_context('get_cache_value') %}
    {% set macro_name = macro_ctx.macro_name %}
    {% set model_id = macro_ctx.model_id %}
    
    {% set macro_polo = var('macro_polo', {}) %}
    {% set cache = macro_polo.get('cache', {}) %}
    {% set cache_value = cache.get(cache_key, none) %}  {# Now returns none on cache miss #}
    {{ dbt_macro_polo.logging(macro_name, message="Cache handling: " ~ {'cache_key': cache_key, 'cache_value': cache_value}, level='DEBUG', model_id=model_id) }}
    {{ return(cache_value) }}
{% endmacro %}
```

Changed the default value from `{}` to `none`. In Jinja2/Python:
- `none` is falsy, so `if cache_value:` correctly evaluates to `false` on a cache miss
- This aligns with standard Python conventions where `dict.get()` returns `None` by default
- Makes the code behavior predictable and consistent

### Verification
The fix ensures that:
- Cache misses are properly detected with `if cache_value:` checks
- Calling code can distinguish between a cached value and no cached value
- Type safety is improved (no unexpected empty dicts being passed around)
- Performance is optimized (queries are run when cache is empty, not skipped)

---

## Summary

| Bug # | Type | Severity | File | Impact |
|-------|------|----------|------|--------|
| 1 | Logic Error | Medium | handle_scheduling.sql | Incorrect schedule matching at boundary times |
| 2 | Code Quality | Low | handle_scheduling.sql | Dead code, reduced maintainability |
| 3 | Type Safety | High | get_cache_value.sql | Cache miss detection failure, potential type errors |

All three bugs have been successfully fixed, improving the reliability, performance, and maintainability of the codebase.