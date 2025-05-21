-- Test the handle_scheduling macro
{% set test_cases = [
    {
        'name': 'peak_hours_schedule',
        'operation_config': {
            'scheduling': {
                'enabled': true,
                'schedules': [
                    {
                        'name': 'peak_hours',
                        'warehouse_size': 's',
                        'times': {'start': '08:00', 'end': '17:00'},
                        'days': ['monday', 'tuesday', 'wednesday', 'thursday', 'friday']
                    }
                ]
            }
        },
        'current_time': modules.datetime.datetime(2025, 1, 1, 9, 30),
        'expected': 's'
    },
    {
        'name': 'off_peak_hours_schedule',
        'operation_config': {
            'scheduling': {
                'enabled': true,
                'schedules': [
                    {
                        'name': 'off_peak_hours',
                        'warehouse_size': 'xs',
                        'times': {'start': '12:01', 'end': '23:59'},
                        'days': ['monday', 'tuesday', 'wednesday', 'thursday', 'friday']
                    }
                ]
            }
        },
        'current_time': modules.datetime.datetime(2025, 1, 1, 14, 30),
        'expected': 'xs'
    },
    {
        'name': 'weekend_schedule',
        'operation_config': {
            'scheduling': {
                'enabled': true,
                'schedules': [
                    {
                        'name': 'weekend_hours',
                        'warehouse_size': 'm',
                        'times': {'start': '00:00', 'end': '23:59'},
                        'days': ['saturday', 'sunday']
                    }
                ]
            }
        },
        'current_time': modules.datetime.datetime(2025, 1, 4, 14, 30),
        'expected': 'm'
    },
    {
        'name': 'cron_schedule_match',
        'operation_config': {
            'scheduling': {
                'enabled': true,
                'schedules': [
                    {
                        'name': 'lunchtime',
                        'warehouse_size': 'm',
                        'cron': '30 12 * * 1-5'
                    }
                ]
            }
        },
        'current_time': modules.datetime.datetime(2025, 1, 1, 12, 30),
        'expected': 'm'
    }
] %}

{# Process test results #}
{% set failed_tests = [] %}
{% for test_case in test_cases %}
    {% set actual = dbt_macro_polo.handle_scheduling(test_case.operation_config, 0, false, test_case.current_time, 'xs') %}
    {% set expected = test_case.expected %}
    {% if actual != expected %}
        {% do failed_tests.append(
            test_case.name ~ ': Expected "' ~ expected ~ '", got "' ~ actual ~ '"'
        ) %}
    {% endif %}
{% endfor %}

{# Report results #}
{% if failed_tests | length > 0 %}
    {{ dbt_macro_polo.logging(message="Failed tests:\n" ~ failed_tests | join('\n'), level='ERROR') }}
{% endif %}

select 
    case when count(*) = 0 then true else false end as test_passed
from (
    {% for test_case in test_cases %}
    select 
        '{{ test_case.name }}' as test_name,
        '{{ dbt_macro_polo.handle_scheduling(
            test_case.operation_config,
            0,
            false,
            test_case.current_time,
            "xs"
        ) }}' as actual,
        '{{ test_case.expected }}' as expected
    where actual != expected
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
)