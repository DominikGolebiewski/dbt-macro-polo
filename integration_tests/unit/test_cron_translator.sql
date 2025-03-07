{% test test_cron_translator() %}

    {% set cron_expression_step = "*/15 9-17 * * 1-5" %}
    {% set cron_expression_range = "0 7-19 * * 1-5" %}
    
    {# Test dates for step pattern #}
    {% set monday_10am_15min = modules.datetime.datetime(2023, 1, 2, 10, 15, 0) %}  -- Monday at 10:15 AM
    {% set saturday_noon = modules.datetime.datetime(2023, 1, 7, 12, 0, 0) %}  -- Saturday at 12:00 PM
    {% set tuesday_8am = modules.datetime.datetime(2023, 1, 3, 8, 15, 0) %}  -- Tuesday at 8:15 AM
    
    {# Test dates for time range pattern #}
    {% set wednesday_noon = modules.datetime.datetime(2023, 1, 4, 12, 30, 0) %}  -- Wednesday at 12:30 PM
    {% set friday_8am = modules.datetime.datetime(2023, 1, 6, 8, 45, 0) %}  -- Friday at 8:45 AM
    {% set monday_6am = modules.datetime.datetime(2023, 1, 2, 6, 0, 0) %}  -- Monday at 6:00 AM
    {% set sunday_noon = modules.datetime.datetime(2023, 1, 8, 12, 0, 0) %}  -- Sunday at 12:00 PM
    
    {% set step_test_cases = [
        {'time': monday_10am_15min, 'expected': true},
        {'time': saturday_noon, 'expected': false},
        {'time': tuesday_8am, 'expected': false}
    ] %}
    
    {% set range_test_cases = [
        {'time': wednesday_noon, 'expected': true},
        {'time': friday_8am, 'expected': true},
        {'time': monday_6am, 'expected': false},
        {'time': sunday_noon, 'expected': false}
    ] %}
    
    {% set results = [] %}
    
    -- Test cron expression parsing
    {% set cron_parts = dbt_macro_polo.parse_cron_expression(cron_expression_step) %}
    {% if cron_parts.minute != "*/15" %}
        {% do results.append("FAIL: minute should be */15, got " ~ cron_parts.minute) %}
    {% endif %}
    
    {% if cron_parts.hour != "9-17" %}
        {% do results.append("FAIL: hour should be 9-17, got " ~ cron_parts.hour) %}
    {% endif %}
    
    -- Test step-based cron matching against test cases
    {% for test_case in step_test_cases %}
        {% set match_result = dbt_macro_polo.is_cron_match(cron_expression_step, test_case.time) %}
        {% if match_result != test_case.expected %}
            {% do results.append("FAIL: Step pattern " ~ cron_expression_step ~ " at " ~ test_case.time ~ " should be " ~ test_case.expected ~ ", got " ~ match_result) %}
        {% endif %}
    {% endfor %}
    
    -- Test time range cron matching against test cases
    {% for test_case in range_test_cases %}
        {% set match_result = dbt_macro_polo.is_cron_match(cron_expression_range, test_case.time) %}
        {% if match_result != test_case.expected %}
            {% do results.append("FAIL: Range pattern " ~ cron_expression_range ~ " at " ~ test_case.time ~ " should be " ~ test_case.expected ~ ", got " ~ match_result) %}
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
    
    -- Test time range detection
    {% set hour_range_minute_specific = dbt_macro_polo.parse_cron_expression("0 7-19 * * *") %}
    {% set is_time_range = hour_range_minute_specific.hour.find('-') >= 0 and hour_range_minute_specific.minute != '*' and hour_range_minute_specific.minute.find('-') < 0 %}
    {% if not is_time_range %}
        {% do results.append("FAIL: '0 7-19 * * *' should be detected as a time range pattern") %}
    {% endif %}
    
    {% if results | length == 0 %}
        {% do results.append("PASS: All cron translator tests passed") %}
    {% endif %}
    
    select 1 as id, '{{ results | join(", ") }}' as test_result

{% endtest %} 