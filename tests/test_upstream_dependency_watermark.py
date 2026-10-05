"""Unit tests for per-upstream watermark overrides in Macro Polo."""

from pathlib import Path
from types import SimpleNamespace
import unittest

from jinja2 import Environment, nodes


class UpstreamDependencyWatermarkTests(unittest.TestCase):
    def setUp(self):
        macro_path = (
            Path(__file__).resolve().parents[1]
            / "macros"
            / "warehouse_optimiser"
            / "check_upstream_row_count.sql"
        )
        environment = Environment(extensions=["jinja2.ext.do"])
        parsed = environment.parse(macro_path.read_text())
        macro_node = next(
            node
            for node in parsed.body
            if isinstance(node, nodes.Macro)
            and node.name == "default__get_upstream_row_count"
        )
        self.template = environment.from_string(nodes.Template([macro_node]))

    def test_ignore_timestamp_is_applied_only_to_the_opted_in_dependency(self):
        observed_dependencies = []
        target_watermark = "'2026-09-30 00:00:00'::timestamp_ntz"
        history_watermark = "'1900-01-01 00:00:00'::timestamp_ntz"

        def check_upstream_row_count(
            target_exists,
            upstream_relation,
            timestamp_column,
            warehouse,
            maximum_timestamp,
            keys,
            columns,
            predicate,
        ):
            observed_dependencies.append(
                {
                    "name": upstream_relation,
                    "maximum_timestamp": maximum_timestamp,
                    "predicate": predicate,
                }
            )
            return 10

        def get_upstream_dependency_predicate(model_name, dependency):
            if isinstance(dependency, dict) and dependency.get("predicate_provider"):
                return f"resolved {dependency['predicate_provider']} for {model_name}"
            return dependency.get("predicate") if isinstance(dependency, dict) else None

        macro_polo = SimpleNamespace(
            create_macro_context=lambda name: SimpleNamespace(
                macro_name=name,
                model_id="model.DSR.output_aia_beta_fact_day_store_sku",
            ),
            get_cache_value=lambda key: None,
            allocate_warehouse=lambda size: "DEVELOPER_XS",
            get_max_timestamp=lambda **kwargs: target_watermark,
            get_upstream_dependency_maximum_timestamp=lambda maximum, ignore: (
                history_watermark if ignore else maximum
            ),
            get_upstream_dependency_predicate=get_upstream_dependency_predicate,
            check_upstream_row_count=check_upstream_row_count,
            logging=lambda *args, **kwargs: "",
        )
        cache = {}
        module = self.template.make_module(
            {
                "dbt_macro_polo": macro_polo,
                "var": lambda name, default=None: {"cache": cache}
                if name == "macro_polo"
                else default,
                "load_relation": lambda relation: object(),
                "this": object(),
                "model": SimpleNamespace(name="output_aia_beta_fact_day_store_sku"),
                "return": lambda value: value,
            }
        )

        module.default__get_upstream_row_count(
            "ignored_model_id",
            [
                {
                    "name": "fact_storedaysku",
                    "predicate": "organisation_id = 'existing retailer'",
                },
                {
                    "name": "fact_storedaysku",
                    "ignore_timestamp": True,
                    "predicate_provider": "pending_history",
                },
            ],
            "runstartedtime",
        )

        self.assertEqual(
            observed_dependencies,
            [
                {
                    "name": "fact_storedaysku",
                    "maximum_timestamp": target_watermark,
                    "predicate": "organisation_id = 'existing retailer'",
                },
                {
                    "name": "fact_storedaysku",
                    "maximum_timestamp": history_watermark,
                    "predicate": (
                        "resolved pending_history for output_aia_beta_fact_day_store_sku"
                    ),
                },
            ],
        )

    def render_probe_with_allocations(self, *size_argument):
        allocated_sizes = []
        macro_polo = SimpleNamespace(
            create_macro_context=lambda name: SimpleNamespace(
                macro_name=name, model_id="model.DSR.probe_model"
            ),
            get_cache_value=lambda key: None,
            allocate_warehouse=lambda size: allocated_sizes.append(size)
            or "DEVELOPER_" + size.upper(),
            get_max_timestamp=lambda **kwargs: "'2026-09-30'::timestamp_ntz",
            get_upstream_dependency_maximum_timestamp=lambda maximum, ignore: maximum,
            get_upstream_dependency_predicate=lambda model_name, dependency: None,
            check_upstream_row_count=lambda *args: 1,
            logging=lambda *args, **kwargs: "",
        )
        module = self.template.make_module(
            {
                "dbt_macro_polo": macro_polo,
                "var": lambda name, default=None: {"cache": {}}
                if name == "macro_polo"
                else default,
                "load_relation": lambda relation: object(),
                "this": object(),
                "model": SimpleNamespace(name="probe_model"),
                "return": lambda value: value,
            }
        )
        module.default__get_upstream_row_count(
            "ignored_model_id", ["upstream"], "runstartedtime", *size_argument
        )
        return allocated_sizes

    def test_probe_warehouse_defaults_to_xs(self):
        self.assertEqual(self.render_probe_with_allocations(), ["xs"])

    def test_probe_warehouse_uses_requested_size(self):
        self.assertEqual(self.render_probe_with_allocations("l"), ["l"])

    def test_default_predicate_resolver_returns_literal_sql(self):
        macro_path = (
            Path(__file__).resolve().parents[1]
            / "macros"
            / "warehouse_optimiser"
            / "check_upstream_row_count.sql"
        )
        environment = Environment(extensions=["jinja2.ext.do"])
        parsed = environment.parse(macro_path.read_text())
        macro_node = next(
            node
            for node in parsed.body
            if isinstance(node, nodes.Macro)
            and node.name == "default__get_upstream_dependency_predicate"
        )
        template = environment.from_string(nodes.Template([macro_node]))
        module = template.make_module({"return": lambda value: value})

        self.assertEqual(
            module.default__get_upstream_dependency_predicate(
                "output_aia_beta_fact_day_store_sku",
                {"predicate": "organisation_id = 42"},
            ).strip(),
            "organisation_id = 42",
        )

    def test_default_predicate_resolver_rejects_unhandled_provider(self):
        macro_path = (
            Path(__file__).resolve().parents[1]
            / "macros"
            / "warehouse_optimiser"
            / "check_upstream_row_count.sql"
        )
        environment = Environment(extensions=["jinja2.ext.do"])
        parsed = environment.parse(macro_path.read_text())
        macro_node = next(
            node
            for node in parsed.body
            if isinstance(node, nodes.Macro)
            and node.name == "default__get_upstream_dependency_predicate"
        )

        def raise_compiler_error(message):
            raise ValueError(message)

        module = environment.from_string(nodes.Template([macro_node])).make_module(
            {
                "return": lambda value: value,
                "exceptions": SimpleNamespace(
                    raise_compiler_error=raise_compiler_error
                ),
            }
        )
        with self.assertRaisesRegex(ValueError, "no consumer implementation"):
            module.default__get_upstream_dependency_predicate(
                "output_aia_beta_fact_day_sku",
                {"predicate_provider": "pending_history"},
            )


if __name__ == "__main__":
    unittest.main()
