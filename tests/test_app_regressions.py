import unittest
from pathlib import Path

from app.formatting import fmt_usd


ROOT = Path(__file__).resolve().parents[1]


class AppRegressionTests(unittest.TestCase):
    def test_fmt_usd_places_negative_sign_before_dollar(self):
        self.assertEqual(fmt_usd(-11760), "-$11,760")
        self.assertEqual(fmt_usd(-12.34, 2), "-$12.34")

    def test_streamlit_app_does_not_pass_module_to_kpi(self):
        source = (ROOT / "app" / "streamlit_app.py").read_text(encoding="utf-8")
        self.assertNotIn("kpi(st", source)
        self.assertNotIn("with container:", source)


if __name__ == "__main__":
    unittest.main()
