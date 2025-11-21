{% macro measure_upstream_volume(model_id, upstream_dependency, timestamp_column) %}
    {#
    Calculates the volume (row count) of upstream dependencies.
    Used for adaptive compute to scale resources based on incoming data volume.

    Args:
        model_id (str): The model identifier.
        upstream_dependency (str or list): One or more upstream model names or source references.
        timestamp_column (str): The column to filter by (e.g., for incremental loads).

    Returns:
        int: The total number of rows to process.
    #}
    {{ return(adapter.dispatch('measure_upstream_volume', 'dbt_macro_polo')(model_id, upstream_dependency, timestamp_column)) }}
{% endmacro %}

{% macro default__measure_upstream_volume(model_id, upstream_dependency, timestamp_column) %}

    {% set macro_name = 'measure_upstream_volume' %}
    {% set state_key = '_macro_polo_upstream_volume_' ~ model_id | replace('.', '_') %}
    {% set state_value = dbt_macro_polo.get_runtime_state(state_key) %}

    {% if state_value %}
        {{ dbt_macro_polo.log_event(
            message="Resolved volume from runtime state",
            model_id=model_id,
            status=state_value,
            level='DEBUG',
            macro_name=macro_name
        ) }}
        {{ return(state_value) }}
    {% endif %}

    {% set dependencies = [upstream_dependency] if upstream_dependency is string else upstream_dependency %}
    {% if not dependencies %}
        {{ dbt_macro_polo.log_event(
            message="No upstream dependencies provided",
            level='DEBUG',
            model_id=model_id,
            macro_name=macro_name
        ) }}
        {{ return(0) }}
    {% endif %}

    {% set total_rows = namespace(value=0) %}

    {# Check if target exists to determine incremental scan #}
    {# Handle case where 'this' is not defined (e.g. run-operation) #}
    {% if this is defined and this %}
        {% set target_exists = load_relation(this) is not none %}
    {% else %}
        {% set target_exists = false %}
    {% endif %}

    {# If is_full_refresh logic is triggered (e.g. flags.FULL_REFRESH), we should skip filtering and scan everything #}
    {% set is_full_refresh = dbt_macro_polo.should_full_refresh() %}

    {# Important: The macro calculates volume based on *upstream* data.
       If it's an incremental run, we only want to count rows in upstream that are NEWER than the max timestamp in the CURRENT (target) model.
       If the target model doesn't exist (target_exists is false) OR it's a full refresh, we scan everything (max_value = '0').
    #}

    {% set max_value = '0' %}

    {# BUG FIX: We should only try to get the High Water Mark if the target relation exists AND it's NOT a full refresh. #}
    {# The original logic was correct, BUT when running 'dbt run' for a View (like the integration test), dbt might resolve 'target_exists' to True
       if the view was already created in a previous run, but since it's a VIEW, it doesn't store data and might not have the column physically readable in the same way,
       or more likely, the test view 'int_test_measure_upstream_volume' DOES NOT HAVE the 'run_started_time' column in its select list yet because it's being built!

       Wait, if 'target_exists' is true, dbt checks the existing object in the DB.
       If the existing object is the view from a previous run, it has columns: test_case, expected_value, actual_value.
       It DOES NOT have 'run_started_time'.

       So when get_high_water_mark tries to select max(run_started_time) from the EXISTING view, it fails because that column doesn't exist on the target view!

       We should only use the timestamp filter if we are sure the TARGET has that column.
       However, we can't easily check column existence on the target relation inside Jinja without a query.

       For adaptive compute on Incremental models, the target IS the incremental table, which definitely has the timestamp column.
       For this integration test, we are simulating the macro call. The 'this' context is the view being built.
       The view being built (int_test_measure_upstream_volume) does NOT simulate the incremental table structure, it simulates the RESULT of the test.

       We need to handle this case.
    #}

    {% if target_exists and timestamp_column and not is_full_refresh %}

        {# Safety check: verify column exists in target? Or wrap in try/catch?
           dbt adapter.get_columns_in_relation(this) return list of columns.
        #}
        {% set cols = adapter.get_columns_in_relation(this) %}
        {% set col_names = cols | map(attribute='name') | map('lower') | list %}

        {% if timestamp_column | lower in col_names %}
            {# Get high water mark using small warehouse #}
            {% set max_value = dbt_macro_polo.get_high_water_mark(column_name=timestamp_column) %}
            {% if not max_value %}
                 {% set max_value = '0' %}
            {% endif %}
        {% else %}
             {{ dbt_macro_polo.log_event(
                 message="Timestamp column " ~ timestamp_column ~ " not found in target " ~ this.name ~ ". Skipping incremental volume calculation.",
                 level='DEBUG',
                 model_id=model_id,
                 macro_name=macro_name
             ) }}
        {% endif %}
    {% endif %}

    {# Use XS warehouse for counting #}
    {% set wh = dbt_macro_polo.provision_compute('xs') %}

    {% for dep in dependencies %}
        {% set dep_relation = ref(dep) if '.' not in dep else source(dep.split('.')[0], dep.split('.')[1]) %}

        {% set query %}
            use warehouse {{ wh }};
            select count(1) from {{ dep_relation }}
            {% if target_exists and timestamp_column and not is_full_refresh %}
                {# Only apply filter if we successfully retrieved a valid HWM (meaning target has the column) #}
                {% if max_value != '0' %}
                    where {{ timestamp_column }} > {{ max_value }}
                {% endif %}
            {% endif %}
        {% endset %}

        {% if execute %}
            {% set res = run_query(query) %}
            {% if res and res.rows %}
                {% set count = res.columns[0].values()[0] %}
                {% set total_rows.value = total_rows.value + count %}
            {% endif %}
        {% endif %}
    {% endfor %}

    {# Ensure we log an explicit 0 if value is None or 0 #}
    {% set final_volume = total_rows.value if total_rows.value is not none else 0 %}
    {{ dbt_macro_polo.log_event(
        message="Total upstream volume calculated",
        status=final_volume | int,
        model_id=model_id,
        level='DEBUG',
        macro_name=macro_name
    ) }}

    {# Update: Use runtime_state instead of cache #}
    {% do var('macro_polo', {}).get('runtime_state', {}).update({state_key: final_volume}) %}

    {{ return(final_volume) }}

{% endmacro %}
