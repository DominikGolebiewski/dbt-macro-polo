select
    md5(concat(date, product)) as id,
    seed.*
from {{ ref('seed_dummy_data') }} seed

{% if is_incremental() %}
where run_started_time > {{ dbt_macro_polo.get_max_timestamp(timestamp_column='run_started_time') }}
{% endif %}