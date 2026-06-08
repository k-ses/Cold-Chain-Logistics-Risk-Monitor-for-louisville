"""
Cold-Chain Logistics Risk Monitor
app.py — Plotly Dash MVP Dashboard

Three-page interactive analytics dashboard connected to PostgreSQL views.
All charts update dynamically when filters change — no restart required.

    Page 1 — Executive Overview      /
    Page 2 — Warehouse Operations    /warehouse
    Page 3 — Cargo & Threshold Analysis  /cargo

Business Insights Delivered:
    • Daily temperature and humidity risk across all Louisville cold-chain sites
    • Which warehouses and cargo types are closest to regulatory thresholds
    • 7-day precipitation probability context alongside compliance risk
    • Recommended operational actions prioritised by severity

Run:
    python app.py
Then open http://127.0.0.1:8050

Dependencies:
    pip install dash plotly pandas sqlalchemy psycopg2-binary python-dotenv
"""

from __future__ import annotations
import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, Input, Output, callback, dcc, html, dash_table, State
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

# =============================================================================
# DATABASE
# =============================================================================

def get_engine():
    return create_engine(
        "postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}?sslmode=require".format(
            user=os.getenv("user"), pw=os.getenv("password"),
            host=os.getenv("host"), port=os.getenv("port", "5432"),
            db=os.getenv("dbname"),
        )
    )

engine = get_engine()

def query(sql: str, params: dict | None = None) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)

def load_risk() -> pd.DataFrame:
    return query("SELECT * FROM public.vw_risk_assessments ORDER BY forecast_date")

def load_exec() -> pd.DataFrame:
    return query("SELECT * FROM vw_executive_kpis ORDER BY forecast_date")

# =============================================================================
# CONSTANTS
# =============================================================================

RISK_COLOURS = {
    "Very High": "#C00000", "High": "#FF0000",
    "Moderate":  "#FFC000", "Low":  "#70AD47", "Unknown": "#808080",
}
PRECIP_COLOURS = {
    "Very High": "#1F3864", "High": "#2E75B6",
    "Moderate":  "#9DC3E6", "Low":  "#D5E8F0", "Unknown": "#D9D9D9",
}
RISK_ORDER    = ["Very High", "High", "Moderate", "Low", "Unknown"]
RISK_PRIORITY = {"Very High": 4, "High": 3, "Moderate": 2, "Low": 1, "Unknown": 0}

BRAND_DARK = "#1F3864"
BRAND_MID  = "#2E75B6"
BG         = "#F8FAFD"
CARD_BG    = "#FFFFFF"
TEXT_DARK  = "#1A2340"
TEXT_MUTED = "#6B7A99"
BORDER     = "#E2EAF4"

# ── Shared table styles ────────────────────────────────────────────────────────
TBL_HEADER = {
    "backgroundColor": BRAND_DARK, "color": "white", "fontWeight": "600",
    "fontSize": "11px", "fontFamily": "IBM Plex Sans, sans-serif",
    "border": "none", "textTransform": "uppercase", "letterSpacing": "0.05em",
}
TBL_CELL = {
    "fontFamily": "IBM Plex Sans, sans-serif", "fontSize": "12px",
    "padding": "10px 14px", "border": f"1px solid {BORDER}",
    "color": TEXT_DARK, "textAlign": "left",
}
TBL_CELL_WRAP = {
    **TBL_CELL, "minWidth": "120px", "maxWidth": "400px",
    "whiteSpace": "normal", "height": "auto", "lineHeight": "1.4",
}

def risk_conditionals(col_id: str) -> list:
    return [
        {"if": {"filter_query": f'{{{col_id}}} = "Very High"', "column_id": col_id},
         "backgroundColor": "#C0000020", "color": "#C00000", "fontWeight": "700"},
        {"if": {"filter_query": f'{{{col_id}}} = "High"',      "column_id": col_id},
         "backgroundColor": "#FF000015", "color": "#CC0000", "fontWeight": "700"},
        {"if": {"filter_query": f'{{{col_id}}} = "Moderate"',  "column_id": col_id},
         "backgroundColor": "#FFC00020", "color": "#B38600", "fontWeight": "700"},
        {"if": {"filter_query": f'{{{col_id}}} = "Low"',       "column_id": col_id},
         "backgroundColor": "#70AD4720", "color": "#3D7A1F", "fontWeight": "700"},
        {"if": {"row_index": "odd"}, "backgroundColor": "#F8FAFD"},
    ]

_BASE_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="IBM Plex Sans, sans-serif", color=TEXT_DARK, size=12),
    margin=dict(l=16, r=16, t=36, b=16),
    legend=dict(orientation="h", yanchor="bottom", y=1.02,
                xanchor="right", x=1, bgcolor="rgba(0,0,0,0)"),
)

def fig_layout(fig, title="", height=280, xaxis=None, yaxis=None, **extra):
    fig.update_layout(**_BASE_LAYOUT,
                      title=dict(text=title, font=dict(size=13, color=BRAND_DARK)),
                      height=height, **extra)
    fig.update_xaxes(**{"gridcolor": BORDER, "showgrid": True, "zeroline": False, **(xaxis or {})})
    fig.update_yaxes(**{"gridcolor": BORDER, "showgrid": True, "zeroline": False, **(yaxis or {})})
    return fig

# =============================================================================
# UI HELPERS
# =============================================================================

def card(children, style=None):
    base = {"background": CARD_BG, "borderRadius": "12px", "padding": "20px 24px",
            "boxShadow": "0 2px 12px rgba(31,56,100,0.08)", "border": f"1px solid {BORDER}"}
    if style:
        base.update(style)
    return html.Div(children, style=base)

def kpi_card(label, value, colour=BRAND_MID, subtitle=None):
    return html.Div([
        html.P(label, style={"margin": "0 0 6px 0", "fontSize": "11px", "fontWeight": "600",
                              "letterSpacing": "0.08em", "textTransform": "uppercase",
                              "color": TEXT_MUTED, "fontFamily": "IBM Plex Sans, sans-serif"}),
        html.H2(str(value), style={"margin": "0", "fontSize": "32px", "fontWeight": "700",
                                    "color": colour, "fontFamily": "IBM Plex Mono, monospace",
                                    "lineHeight": "1"}),
        html.P(subtitle or "", style={"margin": "6px 0 0 0", "fontSize": "11px",
                                       "color": TEXT_MUTED, "fontFamily": "IBM Plex Sans, sans-serif"}),
    ], style={"background": CARD_BG, "borderRadius": "12px", "padding": "20px 24px",
              "border": f"2px solid {colour}20", "boxShadow": f"0 4px 16px {colour}15"})

def section_title(text):
    return html.H3(text, style={
        "margin": "0 0 16px 0", "fontSize": "13px", "fontWeight": "600",
        "letterSpacing": "0.06em", "textTransform": "uppercase", "color": BRAND_DARK,
        "fontFamily": "IBM Plex Sans, sans-serif",
        "borderLeft": f"3px solid {BRAND_MID}", "paddingLeft": "10px",
    })

def insight_box(text):
    """Business insight callout shown below charts."""
    return html.Div([
        html.Span("💡 Insight: ", style={"fontWeight": "700", "color": BRAND_MID}),
        html.Span(text, style={"color": TEXT_DARK}),
    ], style={"background": "#EBF3FB", "borderLeft": f"3px solid {BRAND_MID}",
              "borderRadius": "0 8px 8px 0", "padding": "10px 16px",
              "fontSize": "12px", "fontFamily": "IBM Plex Sans, sans-serif",
              "marginTop": "8px"})

def page_header(title, subtitle):
    return html.Div([
        html.H1(title, style={"margin": "0 0 4px 0", "fontSize": "26px", "fontWeight": "700",
                               "color": BRAND_DARK, "fontFamily": "IBM Plex Sans, sans-serif"}),
        html.P(subtitle, style={"margin": "0", "fontSize": "13px",
                                  "color": TEXT_MUTED, "fontFamily": "IBM Plex Sans, sans-serif"}),
    ], style={"marginBottom": "24px"})

def row(*children, mb="24px"):
    return html.Div(list(children),
                    style={"display": "flex", "gap": "16px", "marginBottom": mb})

def make_table(data, columns, col_id="risk_level", extra_cell=None, **kwargs):
    return dash_table.DataTable(
        data=data, columns=columns,
        style_table={"overflowX": "auto"},
        style_header=TBL_HEADER,
        style_cell=extra_cell or TBL_CELL,
        style_data_conditional=risk_conditionals(col_id),
        **kwargs,
    )

def filter_dropdown(id_, label, options, value=None, multi=False):
    """Labelled dropdown filter block."""
    return html.Div([
        html.Label(label, style={"fontSize": "11px", "fontWeight": "600",
                                  "textTransform": "uppercase", "letterSpacing": "0.06em",
                                  "color": TEXT_MUTED, "fontFamily": "IBM Plex Sans, sans-serif",
                                  "marginBottom": "6px", "display": "block"}),
        dcc.Dropdown(id=id_, options=options, value=value, multi=multi,
                     clearable=True, placeholder="All",
                     style={"fontFamily": "IBM Plex Sans, sans-serif", "fontSize": "13px"}),
    ], style={"flex": "1"})

def sidebar():
    link_style = {"display": "flex", "alignItems": "center", "padding": "10px 16px",
                  "borderRadius": "8px", "cursor": "pointer", "fontSize": "13px",
                  "fontFamily": "IBM Plex Sans, sans-serif", "fontWeight": "500",
                  "color": TEXT_DARK, "textDecoration": "none"}
    nav_items = [("📊", "Executive Overview", "/"),
                 ("🏭", "Warehouse Ops",      "/warehouse"),
                 ("📦", "Cargo Analysis",     "/cargo")]
    return html.Div([
        html.Div([
            html.Div("❄️", style={"fontSize": "28px"}),
            html.Div([
                html.P("Cold Chain",   style={"margin": "0", "fontSize": "13px", "fontWeight": "700",
                                               "color": BRAND_DARK, "fontFamily": "IBM Plex Sans, sans-serif"}),
                html.P("Risk Monitor", style={"margin": "0", "fontSize": "11px",
                                               "color": TEXT_MUTED, "fontFamily": "IBM Plex Sans, sans-serif"}),
            ]),
        ], style={"display": "flex", "alignItems": "center", "gap": "10px",
                  "padding": "20px 20px 16px", "borderBottom": f"1px solid {BORDER}"}),
        html.Div([
            dcc.Link(
                html.Div([html.Span(icon, style={"fontSize": "16px"}),
                           html.Span(label, style={"marginLeft": "10px"})], style=link_style),
                href=href, style={"textDecoration": "none"}
            )
            for icon, label, href in nav_items
        ], style={"padding": "12px 10px"}),
        html.Div([
            html.P("Data refreshes on each page load. Run run_pipeline.py to update forecasts.",
                   style={"fontSize": "10px", "color": TEXT_MUTED, "padding": "0 16px",
                          "fontFamily": "IBM Plex Sans, sans-serif", "lineHeight": "1.5"})
        ], style={"position": "absolute", "bottom": "20px"}),
    ], style={"width": "220px", "minHeight": "100vh", "background": CARD_BG,
              "borderRight": f"1px solid {BORDER}", "flexShrink": "0",
              "position": "sticky", "top": "0", "height": "100vh"})

# =============================================================================
# PAGE 1 — EXECUTIVE OVERVIEW
# Layout: filter bar → KPI row → charts → weather strip
# Callback: date-range filter updates KPIs + bar chart + donut
# =============================================================================

def page_executive():
    exec_df = load_exec()
    dates   = sorted(exec_df["forecast_date"].astype(str).unique().tolist())

    filter_bar = card([
        html.Div([
            filter_dropdown("exec-date-start", "From Date",
                            [{"label": d, "value": d} for d in dates], value=dates[0]),
            filter_dropdown("exec-date-end",   "To Date",
                            [{"label": d, "value": d} for d in dates], value=dates[-1]),
            filter_dropdown("exec-risk-filter", "Risk Level",
                            [{"label": l, "value": l} for l in RISK_ORDER[:-1]],
                            multi=True),
        ], style={"display": "flex", "gap": "16px", "alignItems": "flex-end"}),
    ], style={"marginBottom": "20px"})

    return html.Div([
        page_header("Executive Overview",
                    "Operation-wide 7-day cold-chain risk position — Louisville, KY"),
        filter_bar,
        html.Div(id="exec-kpis"),
        html.Div(id="exec-charts", style={"marginBottom": "24px"}),
        html.Div(id="exec-strip"),
    ])


@callback(
    Output("exec-kpis",   "children"),
    Output("exec-charts", "children"),
    Output("exec-strip",  "children"),
    Input("exec-date-start",  "value"),
    Input("exec-date-end",    "value"),
    Input("exec-risk-filter", "value"),
)
def update_executive(date_start, date_end, risk_filter):
    exec_df = load_exec()
    risk_df = load_risk()

    # Apply date filter
    if date_start:
        exec_df = exec_df[exec_df["forecast_date"].astype(str) >= date_start]
        risk_df = risk_df[risk_df["forecast_date"].astype(str) >= date_start]
    if date_end:
        exec_df = exec_df[exec_df["forecast_date"].astype(str) <= date_end]
        risk_df = risk_df[risk_df["forecast_date"].astype(str) <= date_end]
    if risk_filter:
        risk_df = risk_df[risk_df["risk_level"].isin(risk_filter)]

    today     = exec_df["forecast_date"].max() if not exec_df.empty else None
    today_row = exec_df[exec_df["forecast_date"] == today].iloc[0] if today is not None else {}
    today_risk = risk_df[risk_df["forecast_date"] == today] if today is not None else pd.DataFrame()

    def fmt(key, fmt_str, fallback="N/A"):
        v = today_row.get(key)
        return fmt_str.format(v) if v is not None else fallback

    worst    = max(today_risk["risk_level"].tolist(),
                   key=lambda r: RISK_PRIORITY.get(r, 0)) if not today_risk.empty else "N/A"
    high_pct = round(today_risk[today_risk["risk_level"].isin(["High","Very High"])].shape[0]
                     / max(len(today_risk), 1) * 100) if not today_risk.empty else 0
    total    = len(risk_df)

    # ── KPI cards ─────────────────────────────────────────────────────────────
    kpis = html.Div([
        kpi_card("Max Temp Today",       fmt("temp_max_f", "{:.1f}°F"), BRAND_MID,
                 f"{today_row.get('weather_icon','🌡️')} {today_row.get('weather_description','')}"),
        kpi_card("Humidity Today",       fmt("humidity_avg_pct", "{:.0f}%"), "#0EA5E9"),
        kpi_card("Precip Probability",   fmt("precipitation_prob_pct", "{:.0f}%"), BRAND_MID),
        kpi_card("Worst Risk Today",     worst, RISK_COLOURS.get(worst, TEXT_MUTED)),
        kpi_card("% Elevated Risk",      f"{high_pct}%",
                 RISK_COLOURS["High"] if high_pct > 0 else RISK_COLOURS["Low"],
                 f"{total} assessments in window"),
    ], style={"display": "grid", "gridTemplateColumns": "repeat(5,1fr)",
              "gap": "16px", "marginBottom": "24px"})

    # ── 7-day stacked bar ─────────────────────────────────────────────────────
    bar_fig = go.Figure()
    col_map = {"Very High": "very_high_count", "High": "high_count",
               "Moderate": "moderate_count",   "Low":  "low_count"}
    for level in reversed(RISK_ORDER[:-1]):
        col = col_map[level]
        if col in exec_df.columns:
            bar_fig.add_trace(go.Bar(
                name=level, x=exec_df["forecast_date"].astype(str),
                y=exec_df[col], marker_color=RISK_COLOURS[level], marker_line_width=0,
            ))
    fig_layout(bar_fig, "", barmode="stack")

    # ── Donut ─────────────────────────────────────────────────────────────────
    if not today_risk.empty:
        counts = today_risk["risk_level"].value_counts().reindex(
            RISK_ORDER, fill_value=0).reset_index()
        counts.columns = ["risk_level", "count"]
        donut_fig = go.Figure(go.Pie(
            labels=counts["risk_level"], values=counts["count"], hole=0.55,
            marker_colors=[RISK_COLOURS.get(r, "#808080") for r in counts["risk_level"]],
            textinfo="percent+label", textfont=dict(size=11),
        ))
        fig_layout(donut_fig, "", showlegend=False)
    else:
        donut_fig = go.Figure()

    charts = row(
        card([section_title("7-Day Risk Timeline"),
              dcc.Graph(figure=bar_fig, config={"displayModeBar": False}),
              insight_box("Peaks in Very High or High bars indicate days where warehouse "
                          "managers should pre-stage contingency cooling equipment.")],
             style={"flex": "2"}),
        card([section_title("Today's Risk Split"),
              dcc.Graph(figure=donut_fig, config={"displayModeBar": False}),
              insight_box("A high proportion of Very High/High segments means most cargo "
                          "types are outside safe thresholds today.")],
             style={"flex": "1"}),
    )

    # ── Weather strip ─────────────────────────────────────────────────────────
    weather_rows = []
    for _, wx_row in exec_df.iterrows():
        day_risk = risk_df[risk_df["forecast_date"] == wx_row["forecast_date"]]
        w_risk   = max(day_risk["risk_level"].tolist(),
                       key=lambda r: RISK_PRIORITY.get(r, 0)) if not day_risk.empty else "Low"
        weather_rows.append({
            "Date":        str(wx_row["forecast_date"]),
            "Icon":        wx_row.get("weather_icon", "🌡️"),
            "Condition":   wx_row.get("weather_description", ""),
            "Max Temp °F": f"{wx_row['temp_max_f']:.1f}",
            "Humidity %":  f"{wx_row['humidity_avg_pct']:.0f}",
            "Precip %":    f"{wx_row['precipitation_prob_pct']:.0f}",
            "Risk Level":  w_risk,
        })
    weather_df = pd.DataFrame(weather_rows) if weather_rows else pd.DataFrame()

    strip = card([
        section_title("7-Day Weather & Risk Strip"),
        make_table(weather_df.to_dict("records") if not weather_df.empty else [],
                   [{"name": c, "id": c} for c in weather_df.columns] if not weather_df.empty else [],
                   col_id="Risk Level"),
        insight_box("Cross-reference high Precip % days with cargo dispatch schedules "
                    "to reduce dock-door exposure risk."),
    ])

    return kpis, charts, strip


# =============================================================================
# PAGE 2 — WAREHOUSE OPERATIONS
# Callback: warehouse filter + risk level filter → all charts + actions table
# =============================================================================

def page_warehouse():
    risk_df    = load_risk()
    warehouses = sorted(risk_df["warehouse_name"].unique().tolist()) if not risk_df.empty else []

    filter_bar = card([
        html.Div([
            filter_dropdown("wh-warehouse", "Warehouse",
                            [{"label": w, "value": w} for w in warehouses], multi=True),
            filter_dropdown("wh-risk",      "Risk Level",
                            [{"label": l, "value": l} for l in RISK_ORDER[:-1]], multi=True),
        ], style={"display": "flex", "gap": "16px"}),
    ], style={"marginBottom": "20px"})

    return html.Div([
        page_header("Warehouse Operations",
                    "Daily risk status per warehouse and cargo type"),
        filter_bar,
        html.Div(id="wh-charts"),
    ])


@callback(
    Output("wh-charts", "children"),
    Input("wh-warehouse", "value"),
    Input("wh-risk",      "value"),
)
def update_warehouse(wh_filter, risk_filter):
    risk_df = load_risk()
    wh_df   = query("SELECT * FROM public.vw_warehouse_summary ORDER BY forecast_date")

    if wh_filter:
        risk_df = risk_df[risk_df["warehouse_name"].isin(wh_filter)]
        wh_df   = wh_df[wh_df["warehouse_name"].isin(wh_filter)]
    if risk_filter:
        risk_df = risk_df[risk_df["risk_level"].isin(risk_filter)]

    today      = risk_df["forecast_date"].max() if not risk_df.empty else None
    today_risk = risk_df[risk_df["forecast_date"] == today] if today else pd.DataFrame()

    # ── Heatmap ───────────────────────────────────────────────────────────────
    if not today_risk.empty:
        matrix = today_risk.groupby(["warehouse_name","cargo_name"])["risk_level"].first().reset_index()
        matrix["risk_score"] = matrix["risk_level"].map(RISK_PRIORITY)
        pivot  = matrix.pivot(index="warehouse_name", columns="cargo_name",
                              values="risk_score").fillna(0)
        text   = [[matrix[(matrix["warehouse_name"]==y)&(matrix["cargo_name"]==x)]["risk_level"].values[0]
                   if len(matrix[(matrix["warehouse_name"]==y)&(matrix["cargo_name"]==x)]) > 0 else ""
                   for x in pivot.columns] for y in pivot.index]
        heatmap_fig = go.Figure(go.Heatmap(
            z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(),
            colorscale=[[0,"#70AD47"],[0.25,"#70AD47"],[0.26,"#FFC000"],[0.5,"#FFC000"],
                        [0.51,"#FF0000"],[0.75,"#FF0000"],[0.76,"#C00000"],[1,"#C00000"]],
            zmin=0, zmax=4, showscale=False,
            text=text, texttemplate="%{text}", textfont=dict(size=10, color="white"),
        ))
        fig_layout(heatmap_fig, "", height=300,
                   xaxis={"side": "top", "gridcolor": "rgba(0,0,0,0)", "showgrid": False})
    else:
        heatmap_fig = go.Figure()

    # ── Warehouse stacked bar ─────────────────────────────────────────────────
    today_wh    = wh_df[wh_df["forecast_date"] == today] if today else wh_df
    wh_bar_fig  = go.Figure()
    col_map     = {"Very High": "very_high_count", "High": "high_count",
                   "Moderate":  "moderate_count",  "Low":  "low_count"}
    for level in reversed(RISK_ORDER[:-1]):
        col = col_map[level]
        if col in today_wh.columns:
            wh_bar_fig.add_trace(go.Bar(name=level, x=today_wh["warehouse_name"],
                                        y=today_wh[col], marker_color=RISK_COLOURS[level],
                                        marker_line_width=0))
    fig_layout(wh_bar_fig, "", height=300, barmode="stack", xaxis={"tickangle": -20})

    # ── Threshold proximity ───────────────────────────────────────────────────
    if not today_risk.empty and "actual_temp" in today_risk.columns:
        thresh = today_risk[["cargo_name","actual_temp","cargo_temp_threshold"]].drop_duplicates()
        prox_fig = go.Figure([
            go.Bar(name="Actual Temp °F", x=thresh["cargo_name"], y=thresh["actual_temp"],
                   marker_color=BRAND_MID, marker_line_width=0),
            go.Bar(name="Safe Max °F",    x=thresh["cargo_name"], y=thresh["cargo_temp_threshold"],
                   marker_color="#E2EAF4", marker_line_color=BRAND_DARK, marker_line_width=1),
        ])
        fig_layout(prox_fig, "", height=260, barmode="group")
    else:
        prox_fig = go.Figure()

    # ── Actions table ─────────────────────────────────────────────────────────
    high_actions = today_risk[today_risk["risk_level"].isin(["Very High","High","Moderate"])].copy() \
                   if not today_risk.empty else pd.DataFrame()
    if not high_actions.empty:
        high_actions = high_actions.sort_values(
            "risk_level", key=lambda s: s.map(RISK_PRIORITY), ascending=False)
    action_cols = [c for c in ["warehouse_name","cargo_name","risk_level",
                                "temp_risk","humidity_risk","precip_risk","recommended_action"]
                   if c in high_actions.columns]

    return html.Div([
        row(
            card([section_title("Risk Matrix — Warehouse × Cargo"),
                  dcc.Graph(figure=heatmap_fig, config={"displayModeBar": False}),
                  insight_box("Red cells show cargo types exceeding safe temperature or humidity "
                              "thresholds today. Cross-reference with dispatch schedules.")],
                 style={"flex": "3"}),
            card([section_title("Risk Count per Warehouse"),
                  dcc.Graph(figure=wh_bar_fig, config={"displayModeBar": False}),
                  insight_box("Warehouses with the tallest red/orange bars have the most "
                              "cargo types outside regulatory limits today.")],
                 style={"flex": "2"}),
        ),
        card([section_title("Actual Temperature vs Safe Threshold by Cargo"),
              dcc.Graph(figure=prox_fig, config={"displayModeBar": False}),
              insight_box("Bars where Actual Temp approaches or exceeds Safe Max indicate "
                          "cargo at immediate regulatory compliance risk.")],
             style={"marginBottom": "24px"}),
        card([section_title("Recommended Actions — Elevated Risk Only"),
              make_table(
                  high_actions[action_cols].to_dict("records") if not high_actions.empty else [],
                  [{"name": c.replace("_"," ").title(), "id": c} for c in action_cols],
                  extra_cell=TBL_CELL_WRAP,
                  tooltip_data=[{c: {"value": str(r[c]), "type": "markdown"} for c in action_cols}
                                for r in high_actions[action_cols].to_dict("records")]
                  if not high_actions.empty else [],
                  tooltip_duration=None,
              )]),
    ])


# =============================================================================
# PAGE 3 — CARGO & THRESHOLD ANALYSIS
# Callback: cargo type + regulatory body filters → all charts + table
# =============================================================================

def page_cargo():
    risk_df     = load_risk()
    if risk_df.empty:
        return html.Div("No data available.", style={"padding": "40px", "color": TEXT_MUTED})
    cargo_opts  = [{"label": c, "value": c} for c in sorted(risk_df["cargo_name"].unique())]
    reg_opts    = [{"label": r, "value": r} for r in sorted(risk_df["regulatory_body"].unique())]

    filter_bar = card([
        html.Div([
            filter_dropdown("cargo-type",   "Cargo Type",      cargo_opts, multi=True),
            filter_dropdown("cargo-reg",    "Regulatory Body", reg_opts,   multi=True),
            filter_dropdown("cargo-risk",   "Risk Level",
                            [{"label": l, "value": l} for l in RISK_ORDER[:-1]], multi=True),
        ], style={"display": "flex", "gap": "16px"}),
    ], style={"marginBottom": "20px"})

    return html.Div([
        page_header("Cargo & Threshold Analysis",
                    "Which cargo types drive risk and what is the dominant factor"),
        filter_bar,
        html.Div(id="cargo-charts"),
    ])


@callback(
    Output("cargo-charts", "children"),
    Input("cargo-type", "value"),
    Input("cargo-reg",  "value"),
    Input("cargo-risk", "value"),
)
def update_cargo(cargo_filter, reg_filter, risk_filter):
    risk_df = load_risk()
    if risk_df.empty:
        return html.Div("No data.", style={"color": TEXT_MUTED})

    if cargo_filter:
        risk_df = risk_df[risk_df["cargo_name"].isin(cargo_filter)]
    if reg_filter:
        risk_df = risk_df[risk_df["regulatory_body"].isin(reg_filter)]
    if risk_filter:
        risk_df = risk_df[risk_df["risk_level"].isin(risk_filter)]

    risk_df["risk_score"] = risk_df["risk_level"].map(RISK_PRIORITY)
    today    = risk_df["forecast_date"].max()
    today_df = risk_df[risk_df["forecast_date"] == today]

    # ── Cargo risk history ────────────────────────────────────────────────────
    history  = risk_df.groupby(["forecast_date","cargo_name"])["risk_score"].max().reset_index()
    hist_fig = px.line(history, x="forecast_date", y="risk_score", color="cargo_name",
                       labels={"forecast_date":"Date","risk_score":"Risk Score","cargo_name":"Cargo"},
                       color_discrete_sequence=px.colors.qualitative.Set2)
    hist_fig.update_layout(legend_title_text="")
    fig_layout(hist_fig, "", height=300,
               yaxis={"tickvals": [1,2,3,4],
                      "ticktext": ["Low","Moderate","High","Very High"],
                      "range": [0.5, 4.5]})

    # ── Temp vs humidity risk split ───────────────────────────────────────────
    def elevated(df, col):
        return df[df[col].isin(["High","Very High"])].groupby("cargo_name").size().reset_index(name="count")

    split_df = pd.concat([elevated(today_df,"temp_risk").assign(type="Temp Risk"),
                          elevated(today_df,"humidity_risk").assign(type="Humidity Risk")])
    split_fig = go.Figure() if split_df.empty else px.bar(
        split_df, x="cargo_name", y="count", color="type", barmode="group",
        color_discrete_map={"Temp Risk":"#C00000","Humidity Risk":BRAND_MID},
        labels={"cargo_name":"Cargo","count":"Count","type":"Risk Type"},
    )
    fig_layout(split_fig, "", xaxis={"tickangle": -20})

    # ── Precipitation risk by day ─────────────────────────────────────────────
    precip_df = (risk_df.groupby("forecast_date")
                 .agg(actual_precip_prob=("actual_precip_prob","mean"),
                      precip_risk=("precip_risk", lambda x: x.mode()[0] if not x.empty else "Unknown"))
                 .reset_index())
    precip_fig = go.Figure(go.Bar(
        x=precip_df["forecast_date"].astype(str), y=precip_df["actual_precip_prob"],
        marker_color=[PRECIP_COLOURS.get(r,"#D9D9D9") for r in precip_df["precip_risk"]],
        marker_line_width=0, name="Precip Probability %",
    ))
    for threshold, label in [(80,"Very High"),(60,"High"),(40,"Moderate")]:
        precip_fig.add_hline(y=threshold, line_dash="dot",
                             line_color=PRECIP_COLOURS[label],
                             annotation_text=label, annotation_position="right")
    fig_layout(precip_fig, "", height=260, yaxis={"title":"Probability %","range":[0,105]})

    # ── Scatter ───────────────────────────────────────────────────────────────
    scatter_df  = today_df[["cargo_name","actual_temp","actual_humidity",
                             "risk_score","risk_level"]].drop_duplicates()
    scatter_fig = go.Figure() if scatter_df.empty else px.scatter(
        scatter_df, x="actual_temp", y="actual_humidity",
        color="risk_level", size="risk_score", hover_data=["cargo_name"],
        color_discrete_map=RISK_COLOURS,
        labels={"actual_temp":"Actual Temp °F","actual_humidity":"Humidity %","risk_level":"Risk Level"},
    )
    fig_layout(scatter_fig, "")

    # ── Full table ────────────────────────────────────────────────────────────
    table_cols = [c for c in ["forecast_date","warehouse_name","cargo_name","regulatory_body",
                               "actual_temp","actual_humidity","actual_precip_prob",
                               "temp_risk","humidity_risk","precip_risk","risk_level"]
                  if c in risk_df.columns]

    return html.Div([
        row(
            card([section_title("Cargo Risk History — 7 Days"),
                  dcc.Graph(figure=hist_fig, config={"displayModeBar": False}),
                  insight_box("Flat lines at Very High across the full window signal cargo types "
                              "that consistently breach thresholds and may need revised storage conditions.")],
                 style={"flex":"1"}),
            card([section_title("Temperature vs Humidity — Risk Driver"),
                  dcc.Graph(figure=split_fig, config={"displayModeBar": False}),
                  insight_box("If temp bars are consistently taller, focus cooling capacity. "
                              "If humidity bars dominate, review dehumidification equipment.")],
                 style={"flex":"1"}),
        ),
        row(
            card([section_title("Temp vs Humidity Scatter (Today)"),
                  dcc.Graph(figure=scatter_fig, config={"displayModeBar": False}),
                  insight_box("Dots in the top-right quadrant are closest to breaching both "
                              "temperature and humidity thresholds simultaneously.")],
                 style={"flex":"1"}),
            card([section_title("Daily Precipitation Probability"),
                  dcc.Graph(figure=precip_fig, config={"displayModeBar": False}),
                  insight_box("Days above the 80% line should trigger covered-loading protocols "
                              "for all cargo types regardless of ambient temperature risk.")],
                 style={"flex":"1"}),
        ),
        card([section_title("Full Assessment Detail — Filterable & Sortable"),
              make_table(
                  risk_df[table_cols].sort_values(
                      "risk_level", key=lambda s: s.map(RISK_PRIORITY), ascending=False
                  ).to_dict("records"),
                  [{"name": c.replace("_"," ").title(), "id": c} for c in table_cols],
                  page_size=15, sort_action="native", filter_action="native",
              )]),
    ])


# =============================================================================
# APP
# =============================================================================

app = Dash(__name__, suppress_callback_exceptions=True)
app.title = "Cold Chain Risk Monitor"

app.layout = html.Div([
    dcc.Location(id="url", refresh=False),
    html.Link(rel="stylesheet", href=(
        "https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600"
        "&family=IBM+Plex+Sans:wght@400;500;600;700&display=swap"
    )),
    html.Div([
        sidebar(),
        html.Div(id="page-content", style={
            "flex": "1", "padding": "32px 36px",
            "background": BG, "minHeight": "100vh", "overflowY": "auto",
        }),
    ], style={"display": "flex", "minHeight": "100vh"}),
], style={"fontFamily": "IBM Plex Sans, sans-serif", "background": BG})


@callback(Output("page-content", "children"), Input("url", "pathname"))
def render_page(pathname):
    if pathname == "/warehouse": return page_warehouse()
    if pathname == "/cargo":     return page_cargo()
    return page_executive()


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8050, debug=True)