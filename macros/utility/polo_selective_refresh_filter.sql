{#--
    Extension point for selective_refresh.

    When `--vars '{"selective_refresh": true, ...}'` is passed, the delete branch
    of `default__get_delete_insert_merge_sql` calls this macro to build the SQL
    WHERE expression that scopes the delete. Consumers override
    `default__polo_selective_refresh_filter` in their own project (via the
    dispatch search_order) to plug in their own filter implementation — e.g.
    delegating to a filter-builder in another package — without forcing polo to
    depend on that package.

    Optional `keys` and `columns` arguments are passed through opaquely to the
    consumer's implementation (polo never interprets them). The delete branch calls
    this with no arguments (the consumer decides the defaults); the upstream
    row-count probe passes a per-upstream `keys` (and optional `columns`) so each
    upstream is filtered with the key type — and column-name set — it actually holds.

    The default raises a compile-time error so misconfiguration is loud and early.
--#}

{% macro polo_selective_refresh_filter(keys=none, relation=none, columns=none) %}
    {{ return(adapter.dispatch('polo_selective_refresh_filter', 'dbt_macro_polo')(keys, relation, columns)) }}
{% endmacro %}

{% macro default__polo_selective_refresh_filter(keys=none, relation=none, columns=none) %}
    {{- exceptions.raise_compiler_error(
        "selective_refresh=true was passed but no filter macro is configured. "
        "Define `default__polo_selective_refresh_filter` in your project's macros "
        "and add `dbt_macro_polo` to your dispatch search_order in dbt_project.yml. "
        "The macro must return a SQL WHERE expression (without the WHERE keyword)."
    ) -}}
{% endmacro %}
