from __future__ import annotations

import os
import math

from datetime import datetime
from typing import Optional

import plotly.graph_objects as go
from plotly.subplots import make_subplots


def export_cloudsql_cpu_plot_html(
    metrics,
    out_dir_name: str = "reports",
    filename: Optional[str] = None,
    title: str = "Cloud SQL CPU Metrics",
    auto_open: bool = False,
) -> go.Figure:
    """
    Generate an interactive Plotly HTML report containing:
      - cpu_utilization
      - cpu_reserved_cores

    Uses the directory of this file as the base ("mother dir").
    Returns the absolute path to the generated HTML file.
    """

    mother_dir = os.path.dirname(os.path.abspath(__file__))
    out_dir = os.path.join(mother_dir, out_dir_name)
    os.makedirs(out_dir, exist_ok=True)

    # ------------------------------------------------------------
    # Resolve filename
    # ------------------------------------------------------------
    if filename is None:
        ts0 = (
            metrics.cpu_utilization.values[0][0]
            if metrics.cpu_utilization.values
            else datetime.now()
        )
        stamp = ts0.strftime("%Y%m%d_%H%M%S")
        filename = f"cloudsql_cpu_metrics_{stamp}.html"

    out_path = os.path.abspath(os.path.join(out_dir, filename))

    # ------------------------------------------------------------
    # Extract time series
    # ------------------------------------------------------------
    util_x = metrics.cpu_utilization.timestamps()
    util_y = metrics.cpu_utilization.data()

    cores_y = metrics.cpu_reserved_cores.data()

    cut_x = metrics.cpu_usage_time.timestamps()
    cut_y = metrics.cpu_usage_time.data()

    # for i in range(cut_y)

    # ------------------------------------------------------------
    # Build Plotly figure
    # ------------------------------------------------------------
    fig = go.Figure()

    # ------------------------------------------------------------
    # Add lines
    # ------------------------------------------------------------
    fig.add_trace(
        go.Scatter(
            x=util_x,
            y=util_y,
            mode="lines",
            name="cpu_utilization",
            yaxis="y",
            hovertemplate="Utilization: %{y:.1%}<extra></extra>",
        )
    )

    fig.add_trace(
        go.Bar(
        x=cut_x,
        y=cut_y,
        name="cpu_usage_time",
        yaxis="y3",
        opacity=0.35,
        hovertemplate="CPU usage time: %{y}<extra></extra>",
    )
    )

    # ------------------------------------------------------------
    # Layout
    # ------------------------------------------------------------
    fig.update_layout(
        title=dict(
            text=title,
            font=dict(
                size=24,  # bigger
                family="Arial",  # optional
                color="black",
            ),
            x=0.5,  # center title
            y=0.97,
            xanchor="center",
        ),
        hovermode="x unified",
        margin=dict(l=70, r=70, t=80, b=60),

        xaxis=dict(
            title="time",
            type="date",
        ),

        yaxis=dict(
            title="CPU Utilization (%)",
            tickformat=".0%",
            rangemode="tozero",
        ),


        yaxis3=dict(
            title="CPU Usage Time (CPU-seconds)",
            overlaying="y",
            side="right",
            position=1,  # push it slightly outward
            rangemode="tozero",
        ),

        legend=dict(
            orientation="v",
            yanchor="bottom",
            y=0.65,
            xanchor="left",
            x=1.03,
        ),
    )

    cores = sum(cores_y) / len(cores_y)
    is_integer = math.isclose(cores, round(cores), rel_tol=1e-9)
    warning = ""
    if not is_integer:
        warning = "<br><span style='color:red'><b>⚠ resize happened</b></span>"

    fig.add_annotation(
        xref="paper",
        yref="paper",
        x=1.10,
        y=0.99,
        showarrow=False,
        align="left",
        text=f"<b>CPU capacity</b><br>{cores:.2f} vCPUs{warning}",
    )

    # for x, y, label in [
    #     (0, 0, "(0,0)"),
    #     (1, 0, "(1,0)"),
    #     (0, 1, "(0,1)"),
    #     (1, 1, "(1,1)"),
    # ]:
    #     fig.add_annotation(
    #         xref="paper",
    #         yref="paper",
    #         x=x,
    #         y=y,
    #         text=label,
    #         showarrow=False,
    #         font=dict(color="red"),
    #     )

    # ------------------------------------------------------------
    # Save HTML
    # ------------------------------------------------------------
    # fig.write_html(
    #     out_path,
    #     include_plotlyjs="cdn",
    #     full_html=True,
    #     auto_open=auto_open,
    # )

    return fig

# def sql_wal_heavy_job(metrics: CloudSQLMetrics) -> go.Figure:
#     df = pd.DataFrame(metrics.pg_stat_statements_top_queries).copy()
#     if df.empty:
#         fig = go.Figure()
#         fig.update_layout(title_text="pg_stat_statements: Total Exec Time Pareto (90%)", height=650)
#         return fig