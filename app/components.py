import html

import streamlit as st


def section_open(title: str):
    section = st.container(border=True)
    with section:
        st.markdown(
            f'<div class="spendscope-section-title">{html.escape(str(title))}</div>',
            unsafe_allow_html=True,
        )
    return section


def section_close():
    return None


def kpi_hero(title: str, value: str, support_line: str = "", caption: str = ""):
    support_html = f'<div class="hero-support">{html.escape(str(support_line))}</div>' if support_line else ""
    caption_html = f'<div class="hero-caption">{html.escape(str(caption))}</div>' if caption else ""
    st.markdown(
        (
            '<div class="hero-kpi">'
            f'<div class="kpi-title">{html.escape(str(title))}</div>'
            f'<div class="hero-value">{html.escape(str(value))}</div>'
            f"{support_html}"
            f"{caption_html}"
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def inline_stat_strip(stats):
    if not stats:
        return

    items = []
    for stat in stats:
        tone = stat.get("tone") or ""
        tone_class = f" inline-stat-value-{tone}" if tone else ""
        items.append(
            (
                '<div class="inline-stat">'
                f'<span class="inline-stat-title">{html.escape(str(stat.get("title", "")))}</span>'
                f'<span class="inline-stat-value{tone_class}">{html.escape(str(stat.get("value", "")))}</span>'
                "</div>"
            )
        )

    separator = '<span class="inline-stat-separator">&bull;</span>'
    st.markdown(
        f'<div class="inline-stat-strip">{separator.join(items)}</div>',
        unsafe_allow_html=True,
    )


def ranked_list(rows, *, with_delta: bool = False):
    if not rows:
        st.markdown('<div class="spendscope-empty">No rows in the selected window.</div>', unsafe_allow_html=True)
        return

    max_value = max(float(row.get("value") or 0.0) for row in rows) if rows else 0.0
    html_rows = []
    for idx, row in enumerate(rows, start=1):
        raw_value = float(row.get("value") or 0.0)
        bar_width = 0.0 if max_value <= 0 else (raw_value / max_value) * 100.0
        share = row.get("share")
        share_label = ""
        if share is not None:
            share_label = f"{round(float(share)):.0f}% share"

        meta_html = ""
        if not with_delta and share is not None:
            meta_html = f'<div class="ranked-meta">Share: {round(float(share)):.0f}%</div>'

        chips = []
        if with_delta and share is not None:
            chips.append(f'<span class="rank-chip rank-chip-share">{share_label}</span>')

        delta_pct = row.get("delta_pct")
        if with_delta and delta_pct is not None:
            tone = "neutral"
            if float(delta_pct) > 0:
                tone = "danger"
            elif float(delta_pct) < 0:
                tone = "success"
            chips.append(f'<span class="rank-chip rank-chip-{tone}">{float(delta_pct):+.0f}%</span>')

        chip_html = f'<div class="ranked-chips">{"".join(chips)}</div>' if chips else '<div class="ranked-chips"></div>'
        html_rows.append(
            (
                '<div class="ranked-row">'
                f'<div class="ranked-rank">{idx:02d}</div>'
                '<div>'
                f'<div class="ranked-name">{html.escape(str(row.get("name", "")))}</div>'
                f"{meta_html}"
                "</div>"
                f'<div class="ranked-bar"><span class="ranked-bar-fill" style="width: {bar_width:.1f}%"></span></div>'
                f'<div class="ranked-value">{html.escape(str(row.get("display_value", row.get("value", ""))))}</div>'
                f"{chip_html}"
                "</div>"
            )
        )

    st.markdown(
        f'<div class="ranked-list">{"".join(html_rows)}</div>',
        unsafe_allow_html=True,
    )


def apply_chart_theme(fig):
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(
            family='Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif',
            size=12,
            color="#c9d1d9",
        ),
        margin=dict(l=20, r=20, t=8, b=8),
        hoverlabel=dict(
            bgcolor="#13161c",
            bordercolor="rgba(255,255,255,0.10)",
            font=dict(color="#f0f6fc", size=12),
        ),
    )
    fig.update_xaxes(
        showgrid=False,
        linecolor="rgba(255,255,255,0.08)",
        tickcolor="rgba(255,255,255,0.08)",
        title="",
        zeroline=False,
    )
    fig.update_yaxes(
        showgrid=True,
        gridcolor="rgba(255,255,255,0.04)",
        linecolor="rgba(0,0,0,0)",
        tickfont=dict(color="#8b949e"),
        title="",
        zeroline=False,
    )
    return fig
