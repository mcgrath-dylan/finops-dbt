STYLES = """
<style>
:root {
  --fs-hero-value: 2.6rem;
  --fs-section-h: 0.95rem;
  --fs-kpi-value: 1.35rem;
  --fs-kpi-title: 0.78rem;
  --fs-body: 0.9rem;
  --fs-caption: 0.75rem;
  --lh-tight: 1.15;
  --lh-body: 1.5;

  --bg-page: #0e1117;
  --bg-section: #13161c;
  --bg-section-2: #181c23;
  --border: rgba(255, 255, 255, 0.06);
  --border-2: rgba(255, 255, 255, 0.10);
  --text-1: #f0f6fc;
  --text-2: #c9d1d9;
  --text-3: #8b949e;
  --accent: #2dd4bf;
  --success: #34d399;
  --danger: #f87171;
  --sidebar-width: 300px;
}

[data-testid="stToolbar"], [data-testid="stDecoration"],
[data-testid="stStatusWidget"], [data-testid="stHeader"],
[data-testid="stAppDeployButton"], #MainMenu, footer {
  display: none !important;
}

[data-testid="stAppViewContainer"] > .main {
  padding-top: 0;
}

html, body, [data-testid="stAppViewContainer"], .stApp {
  background: var(--bg-page);
  color: var(--text-1);
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Inter, sans-serif;
}

body {
  font-size: var(--fs-body);
  line-height: var(--lh-body);
}

[data-testid="stAppViewContainer"] > .main > .block-container {
  max-width: 1240px !important;
  margin: 0 auto;
  padding: 40px 32px 80px !important;
}

[data-testid="stSidebar"] {
  background: #0b0e13;
  border-right: 1px solid var(--border);
}

[data-testid="stSidebar"] [data-testid="stVerticalBlock"] {
  gap: 1rem;
}

[data-testid="stSidebar"] label,
[data-testid="stSidebar"] .stMarkdown,
[data-testid="stSidebar"] .stCaption,
[data-testid="stSidebar"] [data-baseweb="select"] * {
  color: var(--text-2);
}

[data-testid="stSidebar"] .stToggle label,
[data-testid="stSidebar"] .stSelectbox label,
[data-testid="stSidebar"] .stSlider label,
[data-testid="stSidebar"] .stExpander summary {
  font-size: var(--fs-caption);
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: var(--text-3);
}

[data-testid="stSidebar"] [data-baseweb="select"] > div,
[data-testid="stSidebar"] .stSlider [data-baseweb="slider"],
[data-testid="stSidebar"] button[kind],
[data-testid="stSidebar"] [data-testid="stExpander"] {
  background: var(--bg-section);
  border: 1px solid var(--border);
  border-radius: 10px;
  box-shadow: none !important;
}

[data-testid="stSidebar"] button[kind] {
  color: var(--text-1);
}

[data-testid="stSidebar"] [data-testid="stExpander"] details {
  border: none;
  background: transparent;
}

a {
  color: var(--text-2);
}

a:hover {
  color: var(--accent);
}

.spendscope-brand {
  display: flex;
  flex-direction: column;
  gap: 0.35rem;
  padding: 0.15rem 0 0.75rem;
}

.spendscope-brand-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 0.75rem;
}

.spendscope-brand-name {
  color: var(--text-1);
  font-size: 1.1rem;
  font-weight: 700;
  line-height: 1;
}

.spendscope-brand-subtitle {
  color: var(--text-3);
  font-size: var(--fs-caption);
  line-height: 1.45;
}

.mode-pill {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-width: 54px;
  padding: 0.28rem 0.6rem;
  border-radius: 999px;
  border: 1px solid var(--border-2);
  font-size: 0.68rem;
  font-weight: 700;
  letter-spacing: 0.08em;
}

.mode-pill-demo {
  background: rgba(45, 212, 191, 0.12);
  color: var(--accent);
}

.mode-pill-live {
  background: rgba(255, 255, 255, 0.05);
  color: var(--text-2);
}

.kpi {
  background: var(--bg-section);
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 18px 20px;
  min-height: 116px;
}

.kpi-title,
.inline-stat-title,
.hero-support-label {
  color: var(--text-3);
  font-size: var(--fs-kpi-title);
  font-weight: 600;
  letter-spacing: 0.06em;
  line-height: var(--lh-tight);
  text-transform: uppercase;
}

.kpi-value {
  color: var(--text-1);
  font-size: var(--fs-kpi-value);
  font-weight: 700;
  letter-spacing: -0.02em;
  line-height: var(--lh-tight);
}

.kpi-value-success {
  color: var(--success);
}

.kpi-value-danger {
  color: var(--danger);
}

.kpi-value-neutral {
  color: var(--text-2);
}

.kpi-note {
  color: var(--text-3);
  font-size: var(--fs-body);
  line-height: 1.45;
  margin-top: 0.55rem;
}

.hero-kpi {
  background: var(--bg-section);
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 24px 28px;
  min-height: 100%;
}

.hero-value {
  color: var(--text-1);
  font-size: var(--fs-hero-value);
  font-weight: 700;
  letter-spacing: -0.04em;
  line-height: 1;
  margin: 0.85rem 0 0.95rem;
}

.hero-support {
  color: var(--text-2);
  font-size: 0.92rem;
  line-height: 1.45;
}

.hero-caption {
  color: var(--text-3);
  font-size: var(--fs-caption);
  line-height: 1.5;
  margin-top: 0.55rem;
}

.spendscope-context {
  color: var(--text-3);
  font-size: var(--fs-caption);
  line-height: 1.5;
  margin-top: 0.65rem;
}

.inline-stat-strip {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 0.65rem;
  margin-top: 1rem;
}

.inline-stat {
  display: inline-flex;
  align-items: baseline;
  gap: 0.45rem;
}

.inline-stat-value {
  color: var(--text-1);
  font-size: 0.98rem;
  font-weight: 600;
  letter-spacing: -0.01em;
}

.inline-stat-value-success {
  color: var(--success);
}

.inline-stat-value-danger {
  color: var(--danger);
}

.inline-stat-separator {
  color: var(--text-3);
  font-size: 0.92rem;
}

.spendscope-section-title {
  color: var(--text-2);
  font-size: var(--fs-section-h);
  font-weight: 700;
  letter-spacing: 0.01em;
  line-height: var(--lh-tight);
  margin-bottom: 14px;
}

div[data-testid="stVerticalBlockBorderWrapper"]:has(.spendscope-section-title) {
  background: var(--bg-section) !important;
  border: 1px solid var(--border) !important;
  border-radius: 10px !important;
  box-shadow: none !important;
  padding: 24px 28px !important;
}

div[data-testid="stVerticalBlockBorderWrapper"]:has(.spendscope-section-title) > div[data-testid="stVerticalBlock"] {
  gap: 1rem;
}

.spendscope-gap {
  height: 48px;
}

.spendscope-empty {
  color: var(--text-3);
  font-size: var(--fs-body);
  line-height: 1.5;
}

.legend-pills {
  display: flex;
  flex-wrap: wrap;
  gap: 0.5rem;
  margin-top: 0.5rem;
}

.legend-pill {
  display: inline-flex;
  align-items: center;
  gap: 0.45rem;
  padding: 0.38rem 0.65rem;
  background: var(--bg-section-2);
  border: 1px solid var(--border);
  border-radius: 999px;
  color: var(--text-2);
  font-size: 0.78rem;
  line-height: 1;
}

.legend-dot {
  width: 8px;
  height: 8px;
  border-radius: 999px;
  background: #3a3f47;
}

.legend-pill-primary .legend-dot {
  background: var(--accent);
}

.ranked-list {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.ranked-row {
  display: grid;
  grid-template-columns: 28px minmax(0, 1fr) minmax(72px, 120px) auto auto;
  align-items: center;
  gap: 0.9rem;
  padding: 10px 14px;
  border: 1px solid transparent;
  border-radius: 6px;
  transition: background 120ms ease, border-color 120ms ease;
}

.ranked-row:hover {
  background: var(--bg-section-2);
  border-color: var(--border);
}

.ranked-rank {
  color: var(--text-3);
  font-family: ui-monospace, SFMono-Regular, "SF Mono", Consolas, monospace;
  font-size: 0.78rem;
  font-variant-numeric: tabular-nums;
}

.ranked-name {
  color: var(--text-1);
  font-size: 0.92rem;
  font-weight: 500;
  line-height: 1.35;
}

.ranked-meta {
  color: var(--text-3);
  font-size: 0.76rem;
  line-height: 1.45;
  margin-top: 2px;
}

.ranked-bar {
  width: 120px;
  max-width: 120px;
  height: 4px;
  border-radius: 999px;
  overflow: hidden;
  background: rgba(255, 255, 255, 0.06);
}

.ranked-bar-fill {
  display: block;
  height: 100%;
  border-radius: 999px;
  background: var(--accent);
}

.ranked-value {
  color: var(--text-1);
  font-family: ui-monospace, SFMono-Regular, "SF Mono", Consolas, monospace;
  font-size: 0.84rem;
  font-variant-numeric: tabular-nums;
  white-space: nowrap;
}

.ranked-chips {
  display: inline-flex;
  justify-content: flex-end;
  flex-wrap: wrap;
  gap: 0.35rem;
}

.rank-chip {
  display: inline-flex;
  align-items: center;
  padding: 2px 8px;
  border-radius: 6px;
  border: 1px solid transparent;
  font-size: 0.72rem;
  font-weight: 600;
  line-height: 1.3;
  white-space: nowrap;
}

.rank-chip-share {
  background: var(--bg-section-2);
  border-color: var(--border);
  color: var(--text-2);
}

.rank-chip-success {
  background: rgba(52, 211, 153, 0.18);
  color: #ffffff;
}

.rank-chip-danger {
  background: rgba(248, 113, 113, 0.18);
  color: #ffffff;
}

.rank-chip-neutral {
  background: rgba(255, 255, 255, 0.08);
  color: #ffffff;
}

[data-testid="stPlotlyChart"] {
  background: transparent !important;
}

[data-testid="stAlert"] {
  background: var(--bg-section);
  border: 1px solid var(--border);
}

@media (max-width: 1024px) {
  [data-testid="stAppViewContainer"] > .main > .block-container {
    padding: 32px 24px 72px !important;
  }

  .ranked-row {
    grid-template-columns: 28px minmax(0, 1fr);
  }

  .ranked-bar,
  .ranked-value,
  .ranked-chips {
    grid-column: 2;
  }
}

@media (max-width: 640px) {
  [data-testid="stAppViewContainer"] > .main > .block-container {
    padding: 24px 18px 64px !important;
  }

  .hero-value {
    font-size: 2.2rem;
  }

  .inline-stat-strip {
    gap: 0.45rem;
  }

  .inline-stat-separator {
    display: none;
  }

  .ranked-row {
    gap: 0.7rem;
    padding: 10px 12px;
  }
}
</style>
"""
