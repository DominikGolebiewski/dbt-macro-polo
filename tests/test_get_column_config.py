"""Unit tests for column attributes read from config or config.meta.columns."""

from pathlib import Path
from types import SimpleNamespace
import unittest

from jinja2 import Environment, nodes

MACROS = Path(__file__).resolve().parents[1] / "macros"
COLUMN_KEYS = (
    "timestamp_column",
    "date_column",
    "retailer_column",
    "supplier_column",
    "product_column",
    "target_date_column",
    "source_date_column",
)


class Returned(Exception):
    """dbt's return(): stops the macro and hands its value to the caller."""

    def __init__(self, value):
        super().__init__(value)
        self.value = value


def dbt_return(value):
    raise Returned(value)


def call(macro, *args, **kwargs):
    try:
        macro(*args, **kwargs)
    except Returned as returned:
        return returned.value
    return None


def load_macro(relative_path, name, context):
    environment = Environment(extensions=["jinja2.ext.do"])
    parsed = environment.parse((MACROS / relative_path).read_text())
    macro_node = next(
        node for node in parsed.body if isinstance(node, nodes.Macro) and node.name == name
    )
    module = environment.from_string(nodes.Template([macro_node])).make_module(context)
    return getattr(module, name)


def raise_compiler_error(message):
    raise ValueError(message)


def column_config(config, execute=True):
    """default__get_column_config bound to a model config, called like a dbt macro."""
    macro = load_macro(
        "utility/get_column_config.sql",
        "default__get_column_config",
        {
            "return": dbt_return,
            "config": config,
            "execute": execute,
            "model": SimpleNamespace(unique_id="model.my_project.my_model"),
            "exceptions": SimpleNamespace(raise_compiler_error=raise_compiler_error),
        },
    )
    return lambda name, default=None, required=False: call(macro, name, default, required)


class GetColumnConfigTests(unittest.TestCase):
    def test_every_column_key_resolves_from_either_layout(self):
        for key in COLUMN_KEYS:
            layouts = {
                "top level": {key: "the_column"},
                "meta.columns": {"meta": {"columns": {key: "the_column"}}},
                "meta.columns beside other meta": {
                    "meta": {"warehouse_optimiser": {"enabled": True}, "columns": {key: "the_column"}}
                },
            }
            for layout, config in layouts.items():
                with self.subTest(key=key, layout=layout):
                    self.assertEqual(column_config(config)(key), "the_column")

    def test_top_level_wins_when_both_layouts_set_the_key(self):
        for key in COLUMN_KEYS:
            config = {key: "top_level", "meta": {"columns": {key: "nested"}}}
            with self.subTest(key=key):
                self.assertEqual(column_config(config)(key), "top_level")

    def test_missing_key_returns_the_default(self):
        configs = ({}, {"meta": None}, {"meta": {}}, {"meta": {"columns": None}}, {"meta": {"columns": {}}})
        for config in configs:
            with self.subTest(config=config):
                resolve = column_config(config)
                self.assertIsNone(resolve("date_column"))
                self.assertEqual(resolve("date_column", "day_date"), "day_date")

    def test_a_set_top_level_key_is_returned_as_set_even_when_empty(self):
        # Parity with the former model.config.get(key, default): a key that is present
        # is not replaced by meta.columns or by the default.
        for empty in ("", None):
            with self.subTest(value=empty):
                config = {"timestamp_column": empty, "meta": {"columns": {"timestamp_column": "nested"}}}
                self.assertEqual(column_config(config)("timestamp_column", "loaded_timestamp"), empty)

    def test_required_is_not_satisfied_by_a_default(self):
        # A key must be configured in one of the layouts; a fallback value does not count.
        for config in ({}, {"timestamp_column": ""}, {"meta": {"columns": {"timestamp_column": ""}}}):
            with self.subTest(config=config):
                with self.assertRaisesRegex(ValueError, "timestamp_column must be set"):
                    column_config(config)("timestamp_column", "loaded_timestamp", required=True)

    def test_required_returns_the_configured_value_not_the_default(self):
        for config in (
            {"timestamp_column": "runstartedtime"},
            {"meta": {"columns": {"timestamp_column": "runstartedtime"}}},
        ):
            with self.subTest(config=config):
                self.assertEqual(
                    column_config(config)("timestamp_column", "loaded_timestamp", required=True),
                    "runstartedtime",
                )

    def test_required_default_is_ignored_while_parsing(self):
        self.assertEqual(
            column_config({}, execute=False)("timestamp_column", "loaded_timestamp", required=True),
            "loaded_timestamp",
        )

    def test_a_top_level_value_equal_to_any_internal_marker_is_still_a_set_value(self):
        # The "absent" marker is an object compared by identity, so no configured string,
        # including the former literal marker, can be mistaken for an absent key.
        for value in ("__macro_polo_unset__", "unset", "None"):
            with self.subTest(value=value):
                config = {"timestamp_column": value, "meta": {"columns": {"timestamp_column": "nested"}}}
                self.assertEqual(column_config(config)("timestamp_column", "loaded_timestamp"), value)

    def test_only_meta_columns_is_read_under_meta(self):
        config = {"meta": {"timestamp_column": "not_read"}}
        self.assertEqual(column_config(config)("timestamp_column", "loaded_timestamp"), "loaded_timestamp")

    def test_parsing_returns_the_default(self):
        # dbt's parse-time config answers '' for every key.
        parse_config = {"timestamp_column": "", "meta": ""}
        self.assertEqual(column_config(parse_config, execute=False)("timestamp_column", "loaded_timestamp"), "loaded_timestamp")

    def test_other_keys_do_not_leak_into_the_requested_one(self):
        config = {"date_column": "day_date", "meta": {"columns": {"retailer_column": "organisation_id"}}}
        resolve = column_config(config)
        self.assertEqual(resolve("date_column"), "day_date")
        self.assertEqual(resolve("retailer_column"), "organisation_id")
        self.assertIsNone(resolve("timestamp_column"))

    def test_required_key_raises_only_at_execution_time(self):
        with self.assertRaisesRegex(ValueError, "timestamp_column must be set .*model.my_project.my_model"):
            column_config({})("timestamp_column", required=True)
        # dbt parses model SQL before merging YAML config, so parsing must not fail.
        self.assertIsNone(column_config({}, execute=False)("timestamp_column", required=True))
        self.assertEqual(
            column_config({"meta": {"columns": {"timestamp_column": "loaded"}}})("timestamp_column", required=True),
            "loaded",
        )


class WarehouseOptimiserTimestampColumnTests(unittest.TestCase):
    """warehouse_optimiser hands the resolved timestamp_column to its upstream probe."""

    def probed_timestamp_column(self, column_config_entries):
        model_config = {
            "materialized": "incremental",
            "incremental_strategy": "delete+insert",
            **column_config_entries,
        }
        meta = model_config.setdefault("meta", {})
        meta["warehouse_optimiser"] = {
            "enabled": True,
            "operation_type": {
                "on_full_refresh": {"warehouse_size": "xl"},
                "on_run": {"ctas": {"warehouse_size": "s"}},
                "on_dry_run": {"upstream_dependency": ["upstream_model"]},
            },
        }
        probes = []
        package = SimpleNamespace(
            create_macro_context=lambda name: SimpleNamespace(model_id="model.my_project.my_model"),
            logging=lambda *args, **kwargs: "",
            should_full_refresh=lambda: False,
            get_upstream_row_count=lambda model_id, dependency, timestamp_column, size: probes.append(
                timestamp_column
            )
            or 10,
            handle_operation=lambda *args: "s",
            allocate_warehouse=lambda size: "WH_" + size.upper(),
        )
        context = {
            "return": dbt_return,
            "execute": True,
            "config": model_config,
            "model": SimpleNamespace(config=model_config, unique_id="model.my_project.my_model"),
            "var": lambda name, default=None: {"warehouse_optimiser": {"enabled": True}, "cache": {}}
            if name == "macro_polo"
            else default,
            "exceptions": SimpleNamespace(raise_compiler_error=raise_compiler_error),
            "dbt_macro_polo": package,
        }
        package.get_column_config = lambda *args: call(
            load_macro("utility/get_column_config.sql", "default__get_column_config", context), *args
        )
        optimiser = load_macro("warehouse_optimiser/warehouse_optimiser.sql", "default__warehouse_optimiser", context)
        self.assertEqual(call(optimiser, "ctas"), "use warehouse WH_S")
        return probes

    def test_probe_uses_top_level_timestamp_column(self):
        self.assertEqual(self.probed_timestamp_column({"timestamp_column": "runstartedtime"}), ["runstartedtime"])

    def test_probe_uses_meta_columns_timestamp_column(self):
        self.assertEqual(
            self.probed_timestamp_column({"meta": {"columns": {"timestamp_column": "runstartedtime"}}}),
            ["runstartedtime"],
        )

    def test_probe_prefers_the_top_level_and_defaults_to_loaded_timestamp(self):
        both = {"timestamp_column": "top_level", "meta": {"columns": {"timestamp_column": "nested"}}}
        self.assertEqual(self.probed_timestamp_column(both), ["top_level"])
        self.assertEqual(self.probed_timestamp_column({}), ["loaded_timestamp"])


class LoggingTests(unittest.TestCase):
    """default__logging pads short messages with dots and clamps padding at 0 when > 80 chars."""

    def render_log(self, **kwargs):
        logged = []
        macro = load_macro(
            "logging/logging.sql",
            "default__logging",
            {
                "execute": True,
                "var": lambda name, default=None: default,
                "log": lambda msg, info=False: logged.append(msg) or "",
                "exceptions": SimpleNamespace(
                    warn=lambda msg: logged.append(msg) or "",
                    raise_compiler_error=raise_compiler_error,
                ),
            },
        )
        call(macro, **kwargs)
        return logged

    def test_message_over_80_chars_clamps_dot_padding_to_zero(self):
        logged = self.render_log(
            message="Allocated warehouse",
            model_id="account_82166_staging.output_aia_fact_day_depot_sku",
            status="DEVELOPER_XS",
        )
        self.assertEqual(len(logged), 1)
        self.assertIn("account_82166_staging.output_aia_fact_day_depot_sku\x1b[0m [\x1b[34mDEVELOPER_XS\x1b[0m]", logged[0])


if __name__ == "__main__":
    unittest.main()
