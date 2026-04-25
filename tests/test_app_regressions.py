import json
import os
import subprocess
import sys
import unittest
from pathlib import Path

from app.formatting import fmt_usd


ROOT = Path(__file__).resolve().parents[1]
APP_PATH = ROOT / "app" / "streamlit_app.py"
APPTEST_SCRIPT = """
import datetime as dt
import json
import os
import sys

try:
    import snowflake.connector as snowflake_connector
except ModuleNotFoundError:
    snowflake_connector = None

today = dt.date.today()


def fake_query(sql):
    query = sql.lower()
    dates = [today - dt.timedelta(days=i) for i in range(1, 6)]
    if "information_schema.tables" in query:
        return ["1"], []
    if "fct_daily_costs" in query:
        rows = [
            (day, "COMPUTE_WH", 100.0, 5.0, 105.0, 30.0, dt.datetime.combine(day, dt.time()))
            for day in dates
        ]
        return ["usage_date", "warehouse_name", "compute_cost", "cloud_services_cost", "total_cost", "idle_cost", "_loaded_at"], rows
    if "fct_cost_by_department" in query:
        rows = []
        for day in dates:
            rows.append(("Analytics", day, 65.0))
            rows.append(("Data Platform", day, 40.0))
        return ["department", "usage_date", "total_cost_usd"], rows
    if "budget_daily" in query:
        rows = []
        for day in dates:
            rows.append((day, "Analytics", 70.0))
            rows.append((day, "Data Platform", 45.0))
        return ["date", "department", "budget_usd"], rows
    if "fct_budget_vs_actual" in query:
        return ["usage_date"], [(dates[0],)]
    if "fct_cost_forecast" in query:
        rows = [
            (today + dt.timedelta(days=i), "COMPUTE_WH", 95.0, 80.0, 115.0, i)
            for i in range(1, 4)
        ]
        return ["forecast_date", "warehouse_name", "forecasted_cost_usd", "confidence_band_low", "confidence_band_high", "days_ahead"], rows
    if "fct_daily_storage_costs" in query:
        rows = [
            (day, "RAW_DB", 10.0, 7.0, 5.0, 1.0, 1.0, 35.0)
            for day in dates
        ]
        return ["usage_date", "database_name", "total_storage_tb", "estimated_storage_cost_usd", "estimated_active_cost_usd", "estimated_failsafe_cost_usd", "estimated_stage_cost_usd", "mtd_storage_cost_usd"], rows
    if "fct_top_spenders" in query:
        rows = [
            (day, "analyst", "COMPUTE_WH", 12, 3600.0, 42.0, 18.0, True, 1, 1, 1, 30.0)
            for day in dates
        ]
        return ["usage_date", "user_name", "primary_warehouse_name", "query_count", "total_runtime_seconds", "gb_scanned", "estimated_cost_usd", "has_cost_estimate", "rank_by_query_count", "rank_by_runtime", "rank_by_cost", "pct_of_daily_query_total"], rows
    if "fct_total_cost_summary" in query:
        rows = []
        for day in dates:
            rows.append((day, "COMPUTE", 105.0, 92.0, 525.0))
            rows.append((day, "STORAGE", 7.0, 8.0, 35.0))
        return ["usage_date", "cost_category", "cost_usd", "pct_of_daily_total", "mtd_cost_usd"], rows
    if "int_hourly_compute_costs" in query or "show warehouses" in query:
        return ["warehouse_name"], []
    return ["value"], []


class FakeCursor:
    description = []

    def execute(self, sql):
        columns, rows = fake_query(sql)
        self.description = [(column,) for column in columns]
        self._rows = rows

    def fetchall(self):
        return self._rows

    def close(self):
        return None


class FakeConnection:
    def cursor(self):
        return FakeCursor()

    def is_closed(self):
        return False

    def close(self):
        return None


stub_mode = sys.argv[3]
if snowflake_connector is not None:
    if stub_mode == "nonempty":
        snowflake_connector.connect = lambda **kwargs: FakeConnection()
    else:
        snowflake_connector.connect = lambda **kwargs: None

from streamlit.testing.v1 import AppTest

app_path = sys.argv[1]
demo_mode = sys.argv[2] == "true"

at = AppTest.from_file(app_path)
at.run(timeout=30)
if not demo_mode:
    at.toggle[0].set_value(False)
    at.run(timeout=30)

payload = {
    "exceptions": [str(exc.value) for exc in at.exception],
    "toggle_labels": [toggle.label for toggle in at.toggle],
    "toggle_values": [toggle.value for toggle in at.toggle],
    "markdown": [str(getattr(markdown, "value", "")) for markdown in at.markdown],
    "errors": [str(getattr(error, "value", "")) for error in at.error],
    "buttons": [button.label for button in at.button],
}
print("RESULT_JSON=" + json.dumps(payload))
sys.stdout.flush()
os._exit(0)
"""


class AppRegressionTests(unittest.TestCase):
    def run_apptest(self, *, demo_mode: bool, stub_mode: str = "empty"):
        env = {
            **os.environ,
            "PYTHONPATH": str(ROOT),
            "ENABLE_PRO_PACK": "false",
            "SNOWFLAKE_ACCOUNT": "",
            "SNOWFLAKE_USER": "",
            "SNOWFLAKE_PASSWORD": "",
            "SNOWFLAKE_DATABASE": "",
            "SNOWFLAKE_SCHEMA": "",
            "SNOWFLAKE_WAREHOUSE": "",
            "SNOWFLAKE_ROLE": "",
        }
        result = subprocess.run(
            [sys.executable, "-c", APPTEST_SCRIPT, str(APP_PATH), "true" if demo_mode else "false", stub_mode],
            cwd=ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr or result.stdout)
        payload_lines = [line for line in result.stdout.splitlines() if line.startswith("RESULT_JSON=")]
        self.assertTrue(payload_lines, result.stdout)
        return json.loads(payload_lines[-1].split("=", 1)[1])

    def test_fmt_usd_places_negative_sign_before_dollar(self):
        self.assertEqual(fmt_usd(-11760), "-$11,760")
        self.assertEqual(fmt_usd(-12.34, 2), "-$12.34")

    def test_fmt_usd_shows_sub_dollar_values(self):
        self.assertEqual(fmt_usd(2.67), "$2.67")
        self.assertEqual(fmt_usd(0.47), "$0.47")
        self.assertEqual(fmt_usd(0.004), "<$0.01")

    def test_streamlit_app_does_not_pass_module_to_kpi(self):
        source = (ROOT / "app" / "streamlit_app.py").read_text(encoding="utf-8")
        self.assertNotIn("kpi(st", source)
        self.assertNotIn("with container:", source)

    def test_streamlit_chrome_css_preserves_sidebar_expand_control(self):
        source = (ROOT / "app" / "styles.py").read_text(encoding="utf-8")
        self.assertNotIn('[data-testid="stToolbar"], [data-testid="stDecoration"]', source)
        self.assertNotIn('[data-testid="stStatusWidget"], [data-testid="stHeader"],', source)
        self.assertIn("stExpandSidebarButton", source)

    def test_streamlit_app_defines_visible_spendscope_page_header(self):
        source = (ROOT / "app" / "streamlit_app.py").read_text(encoding="utf-8")
        self.assertIn("spendscope-page-title", source)
        self.assertIn(">Spendscope<", source)
        self.assertNotIn("## FinOps for Snowflake + dbt", source)

    def test_demo_data_unavailable_state_renders_for_empty_critical_tables(self):
        payload = self.run_apptest(demo_mode=True, stub_mode="empty")
        rendered_text = "\n".join(payload["markdown"] + payload["errors"])
        self.assertEqual(payload["exceptions"], [])
        self.assertIn("Demo data unavailable", rendered_text)
        self.assertIn("fct_daily_costs", rendered_text)
        self.assertIn("fct_cost_by_department", rendered_text)
        self.assertIn("Retry data load", payload["buttons"])

    def test_stubbed_nonempty_demo_data_renders_core_surfaces(self):
        payload = self.run_apptest(demo_mode=True, stub_mode="nonempty")
        rendered_text = "\n".join(payload["markdown"] + payload["errors"])
        self.assertEqual(payload["exceptions"], [])
        self.assertNotIn("Demo data unavailable", rendered_text)
        self.assertIn("Idle Wasted", rendered_text)
        self.assertIn("Top Departments", rendered_text)
        self.assertIn("Analytics", rendered_text)

    def test_streamlit_app_apptest_demo_mode_renders_without_exceptions(self):
        payload = self.run_apptest(demo_mode=True, stub_mode="nonempty")
        self.assertEqual(payload["toggle_labels"], ["Demo data"])
        self.assertEqual(payload["toggle_values"], [True])
        self.assertEqual(payload["exceptions"], [])

    def test_streamlit_app_apptest_live_mode_renders_without_exceptions(self):
        payload = self.run_apptest(demo_mode=False)
        self.assertEqual(payload["toggle_labels"], ["Demo data"])
        self.assertEqual(payload["toggle_values"], [False])
        self.assertEqual(payload["exceptions"], [])


if __name__ == "__main__":
    unittest.main()
