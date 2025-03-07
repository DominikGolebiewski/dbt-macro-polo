{% macro register_macros() %}
    {# This macro doesn't do anything except ensure all warehouse optimizer macros are loaded #}
    {{ return('') }}
{% endmacro %}

{# Import all warehouse optimizer macros #}
{% import 'warehouse_optimiser/warehouse_optimiser.sql' as _ %}
{% import 'warehouse_optimiser/handle_scheduling.sql' as _ %}
{% import 'warehouse_optimiser/handle_monitoring.sql' as _ %}
{% import 'warehouse_optimiser/handle_operation.sql' as _ %}
{% import 'warehouse_optimiser/check_upstream_row_count.sql' as _ %}
{% import 'warehouse_optimiser/cron_translator.sql' as _ %} 