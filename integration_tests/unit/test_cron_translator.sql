{% test test_cron_translator() %}

    {% set cron_expression = "*/15 9-17 * * 1-5" %}
    {% set monday_10am = modules.datetime.datetime(2023, 1, 2, 10, 15, 0) %}  -- Monday at 10:15 AM
    {% set saturday_noon = modules.datetime.datetime(2023, 1, 7, 12, 0, 0) %}  -- Saturday at 12:00 PM
    {% set tuesday_8am = modules.datetime.datetime(2023, 1, 3, 8, 15, 0) %}  -- Tuesday at 8:15 AM
    
    {% set test_cases = [
        {'time': monday_10am, 'expected': true},
        {'time': saturday_noon, 'expected': false},
        {'time': tuesday_8am, 'expected': false}
    ] %}
    
    {% set results = [] %}
    
    -- Test cron expression parsing
    {% set cron_parts = dbt_macro_polo.parse_cron_expression(cron_expression) %}
    {% if cron_parts.minute != "*/15" %}
        {% do results.append("FAIL: minute should be */15, got " ~ cron_parts.minute) %}
    {% endif %}
    
    {% if cron_parts.hour != "9-17" %}
        {% do results.append("FAIL: hour should be 9-17, got " ~ cron_parts.hour) %}
    {% endif %}
    
    -- Test cron matching against test cases
    {% for test_case in test_cases %}
        {% set match_result = dbt_macro_polo.is_cron_match(cron_expression, test_case.time) %}
        {% if match_result != test_case.expected %}
            {% do results.append("FAIL: " ~ cron_expression ~ " at " ~ test_case.time ~ " should be " ~ test_case.expected ~ ", got " ~ match_result) %}
        {% endif %}
    {% endfor %}
    
    -- Test field expansion
    {% set expanded_minutes = dbt_macro_polo.expand_cron_field("*/15", 0, 59) %}
    {% if expanded_minutes | length != 4 %}
        {% do results.append("FAIL: */15 should expand to 4 values (0,15,30,45), got " ~ expanded_minutes | length) %}
    {% endif %}
    
    {% if 0 not in expanded_minutes or 15 not in expanded_minutes or 30 not in expanded_minutes or 45 not in expanded_minutes %}
        {% do results.append("FAIL: */15 should expand to 0,15,30,45, got " ~ expanded_minutes) %}
    {% endif %}
    
    {% set expanded_days = dbt_macro_polo.expand_cron_field("1-5", 1, 7) %}
    {% if expanded_days | length != 5 %}
        {% do results.append("FAIL: 1-5 should expand to 5 values (1,2,3,4,5), got " ~ expanded_days | length) %}
    {% endif %}
    
    {% if 1 not in expanded_days or 2 not in expanded_days or 3 not in expanded_days or 4 not in expanded_days or 5 not in expanded_days %}
        {% do results.append("FAIL: 1-5 should expand to 1,2,3,4,5, got " ~ expanded_days) %}
    {% endif %}
    
    {% if results | length == 0 %}
        {% do results.append("PASS: All cron translator tests passed") %}
    {% endif %}
    
    select 1 as id, '{{ results | join(", ") }}' as test_result

{% endtest %} 