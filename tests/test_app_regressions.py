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
import json
import os
import sys

try:
    import snowflake.connector as snowflake_connector
except ModuleNotFoundError:
    snowflake_connector = None

if snowflake_connector is not None:
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
}
print("RESULT_JSON=" + json.dumps(payload))
sys.stdout.flush()
os._exit(0)
"""


class AppRegressionTests(unittest.TestCase):
    def run_apptest(self, *, demo_mode: bool):
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
            [sys.executable, "-c", APPTEST_SCRIPT, str(APP_PATH), "true" if demo_mode else "false"],
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

    def test_streamlit_app_apptest_demo_mode_renders_without_exceptions(self):
        payload = self.run_apptest(demo_mode=True)
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
