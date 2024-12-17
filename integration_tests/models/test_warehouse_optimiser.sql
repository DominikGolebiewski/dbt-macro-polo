-- depends_on: {{ ref('seed_dummy_data_incremental_run') }}
-- depends_on: {{ ref('seed_dummy_data_initial_run') }}

{% set test_results = [] %}
{% set test_model_name_initial_run = 'seed_dummy_data_initial_run' %}
{% set test_model_name_incremental_run = 'seed_dummy_data_incremental_run' %}
{% set test_model_conditional = ref(test_model_name_initial_run) if flags.FULL_REFRESH else ref(test_model_name_incremental_run) %}
{% set is_full_refresh = dbt_macro_polo.should_full_refresh() %}

{% if not is_full_refresh %}
{% set test_cases = [
    {
        'test_name': 'basic_ctas_operation',
        'model_name': 'test_model',
        'config': {
            'timestamp_column': 'run_started_time',
            'meta': {
                'warehouse_optimiser': {
                    'enabled': true,
                    'operation_type': {
                        'on_run': {
                            'ctas': {'warehouse_size': 'xs'}
                        }
                    }
                }
            }
        }
    },
    {
        'test_name': 'scheduled_operation',
        'model_name': 'test_model_scheduled',
        'config': {
            'timestamp_column': 'run_started_time',
            'meta': {
                'warehouse_optimiser': {
                    'enabled': true,
                    'operation_type': {
                        'on_run': {
                            'ctas': {
                                'scheduling': {
                                    'enabled': true,
                                    'schedules': [
                                        {
                                            'name': 'peak_hours',
                                            'warehouse_size': 'l',
                                            'times': {'start': '09:00', 'end': '17:00'},
                                            'days': ['monday', 'tuesday', 'wednesday', 'thursday', 'friday']
                                        }
                                    ]
                                }
                            }
                        }
                    }
                }
            }
        }
    },
    {
        'test_name': 'monitored_operation',
        'model_name': 'test_model_monitored',
        'config': {
            'timestamp_column': 'run_started_time',
            'meta': {
                'warehouse_optimiser': {
                    'enabled': true,
                    'operation_type': {
                        'on_dry_run': {
                            'upstream_dependency': ['seed_dummy_data_initial_run'],
                            'monitoring': {
                                'enabled': true,
                                'thresholds': [
                                    {'rows': 1000000, 'warehouse_size': 'l'},
                                    {'rows': 100000, 'warehouse_size': 'm'},
                                    {'rows': 10000, 'warehouse_size': 's'}
                                ]
                            }
                        }
                    }
                }
            }
        }
    }
] %}

with test_results as (
    {% for test_case in test_cases %}
    select
        '{{ test_case.test_name }}' as test_name,
        '{{ test_case.model_name }}' as model_name,
        '{{ test_case.config.meta.warehouse_optimiser | tojson }}' as config,
        case 
            when '{{ test_case.test_name }}' = 'basic_ctas_operation' then 'xs'
            when '{{ test_case.test_name }}' = 'scheduled_operation' 
                and {{ dbt_macro_polo.is_within_time_range('peak_hours', modules.datetime.datetime.now(), '09:00', '17:00') }}
                then 'l'
            when '{{ test_case.test_name }}' = 'monitored_operation' 
                and {{ dbt_macro_polo.get_upstream_row_count(test_case.model_name, 'seed_dummy_data_initial_run', 'loaded_timestamp') }} > 1000000
                then 'l'
            else 'xs'
        end as expected,
        {{ dbt_macro_polo.warehouse_optimiser('ctas') }} as actual,
        case 
            when expected = actual then 'PASS'
            else 'FAIL'
        end as status
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
)
{% endif %}
select
    md5(concat(date, product)) as id,
    seed.*,
    {% if not is_full_refresh %}
    test_name,
    model_name,
    expected,
    actual,
    status,
    {% endif %}
from {{ test_model_conditional }} seed
{% if not is_full_refresh %}
cross join test_results
{% endif %}

{% if is_incremental() %}
where run_started_time > {{ dbt_macro_polo.get_max_timestamp(timestamp_column='run_started_time') }}
{% endif %}