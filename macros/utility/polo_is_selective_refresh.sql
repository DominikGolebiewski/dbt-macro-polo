{#--
    Extension point: "is selective_refresh active for this model on this run?"

    polo's delete+insert strategy (get_delete_insert_merge_sql) and its
    warehouse-optimiser upstream probe (check_upstream_row_count) both branch on
    this question. polo does NOT own the selective_refresh feature and must not
    depend on the package that does, so it asks through this hook. Consumers
    override `default__polo_is_selective_refresh` (via the dispatch search_order)
    to delegate to their own implementation — e.g. a feature macro in another
    package — without forcing polo to depend on it.

    The default returns false: a polo consumer that has not wired up
    selective_refresh simply gets standard incremental behaviour. Unlike the
    filter hook, this MUST NOT raise — it is evaluated on every incremental
    delete and every upstream probe, not only when selective_refresh is requested.
--#}

{% macro polo_is_selective_refresh() %}
    {{ return(adapter.dispatch('polo_is_selective_refresh', 'dbt_macro_polo')()) }}
{% endmacro %}

{% macro default__polo_is_selective_refresh() %}
    {{ return(false) }}
{% endmacro %}
