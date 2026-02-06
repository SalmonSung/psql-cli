from __future__ import annotations

from pathlib import Path

from datetime import datetime
from typing import Optional, Dict

from utils import bytes_to_unit
import config as config

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from metrics import CloudSQLMetrics, TimeSeries


def general_database_overview(metrics: CloudSQLMetrics) -> go.Figure:
    # --- Make Fig ---
    fig = make_subplots(
        rows=3,
        cols=1,
        specs=[
            [{"type": "xy", "secondary_y": True}],
            [{"type": "xy"}],
            [{"type": "xy"}],
        ],
        row_heights=[0.3, 0.35, 0.35],
        column_widths=[1],
        horizontal_spacing=0.08,
        vertical_spacing=0.04,
        subplot_titles=[
            "CPU",
            "Disk",
            "Memory"
        ]
    )

    # --- Figure I: CPU ---
    x_ts = metrics.cpu_utilization.timestamps()
    y_cpu_u = metrics.cpu_utilization.data()
    y_cut = metrics.cpu_usage_time.data()
    grouped_sql_count: Dict[datetime, int] = {}
    for item in metrics.perquery_latency_metrics:
        for ts in x_ts:
            count = item.perquery_count.get_by_ts(ts)
            grouped_sql_count[ts] = grouped_sql_count.get(ts, 0) + count
    sorted_counts = [
        count
        for _, count in sorted(grouped_sql_count.items())
    ]

    fig.add_trace(
        go.Bar(
            x=x_ts,
            y=sorted_counts,
            name="sql_counts",
            marker=dict(color="lightcoral"),
            opacity=0.5,
            hovertemplate="<b>Count:</b> %{y}<extra></extra>",
            showlegend=False,
        ),
        secondary_y=True,
        row=1, col=1
    )

    fig.add_trace(
        go.Scatter(
            x=x_ts,
            y=y_cpu_u,
            mode="lines",
            line=dict(color="blue"),
            name="cpu_utilization",
            hovertemplate="%{y:.1%}",
            showlegend=False,
        ),
        secondary_y=False,
        row=1, col=1
    )
    # --- Figure II: Disk ---
    x_ts = metrics.disk_utilization.timestamps()

    for d_type, values in metrics.disk_bytes_used_by_type.items():
        fig.add_trace(
            go.Scatter(
                x=values.timestamps(),
                y=[bytes_to_unit(v) for v in values.data()],
                name=d_type,
                mode="lines",
                # line=dict(color=CONNECTION_STATE_COLORS[state]),
                stackgroup="one",
                hovertemplate=(
                    "<b>%{y:.2f} GiB</b>"
                ),
                showlegend=False,
                visible=True,
            ),
            row=2, col=1,
        )

    fig.add_trace(
        go.Scatter(
            x=x_ts,
            y=[bytes_to_unit(v) for v in metrics.disk_quota.data()],
            mode="lines",
            line=dict(
                color="lightcoral",
                dash="dash",
                width=2
            ),
            hovertemplate=(
                "<b>Time:</b> %{x|%H:%M} - "
                "%{x|%Y/%m/%d} - "
                "%{x|%a}<br>"
                "<b>%{y:.2f} GiB</b><extra></extra>"
            )
            ,
            showlegend=False,
        ),
        secondary_y=False,
        row=2, col=1
    )

    fig.add_annotation(
        x=x_ts[-15],
        y=bytes_to_unit(metrics.disk_quota.data()[-1]),
        text="Quota",
        showarrow=False,
        font=dict(
            color="white",
            size=12,
        ),
        bgcolor="lightcoral",  # 填滿背景
        bordercolor="lightcoral",  # 邊框顏色
        borderwidth=0.5,  # 邊框粗細
        borderpad=1,  # 文字與框的內距
        xanchor="left",
        yanchor="bottom",
        row=2,col=1,
    )

    # --- Figure III: Memory ---
    x_memory = metrics.memory_quota.timestamps()
    for component_type, values in metrics.memory_components.items():
        if component_type == "Free":
            continue
        bytes_values = [a * b / 100 for a, b in zip(values.data(), metrics.memory_quota.data())]
        fig.add_trace(
                go.Scatter(
                    x=x_memory,
                    y=[bytes_to_unit(v) for v in bytes_values],
                    name=component_type,
                    mode="lines",
                    # line=dict(color=CONNECTION_STATE_COLORS[state]),
                    stackgroup="one",
                    hovertemplate=(
                        "<b>%{y:.2f} GiB</b>"
                    ),
                    showlegend=False,
                    visible=True,
                ),
                row=3, col=1,
            )

    fig.add_trace(
        go.Scatter(
            x=x_memory,
            y=[bytes_to_unit(v) for v in metrics.memory_quota.data()],
            mode="lines",
            line=dict(
                color="lightcoral",
                dash="dash",
                width=2
            ),
            hovertemplate=(
                "<b>Time:</b> %{x|%H:%M} - "
                "%{x|%Y/%m/%d} - "
                "%{x|%a}<br>"
                "<b>%{y:.2f} GiB</b><extra></extra>"
            )
            ,
            showlegend=False,
        ),
        secondary_y=False,
        row=3, col=1
    )

    fig.add_annotation(
        x=x_memory[-15],
        y=bytes_to_unit(metrics.memory_quota.data()[-1]),
        text="Quota",
        showarrow=False,
        font=dict(
            color="white",
            size=12,
        ),
        bgcolor="lightcoral",  # 填滿背景
        bordercolor="lightcoral",  # 邊框顏色
        borderwidth=0.5,  # 邊框粗細
        borderpad=1,  # 文字與框的內距
        xanchor="left",
        yanchor="bottom",
        row=3,col=1,
    )
    # --- Formatting ---
    fig.update_xaxes(
        showticklabels=False,
        ticks="",
        row=1, col=1,
    )
    fig.update_xaxes(
        showticklabels=False,
        ticks="",
        row=2, col=1,
    )
    fig.update_xaxes(
        tickformat="%H:%M<br>%Y/%m/%d<br>%a",
        row=3, col=1
    )

    fig.update_yaxes(
        title_text="CPU Utilization (%)",
        title_font=dict(color="blue"),
        tickformat=".0%",
        secondary_y=False,
        row=1, col=1
    )

    fig.update_yaxes(
        title_text="SQL Statement",
        title_font=dict(color="lightcoral"),
        # tickfont=dict(color="grey"),
        secondary_y=True,
        row=1, col=1
    )

    fig.update_yaxes(
        title_text="GiB",
        row=2, col=1
    )

    fig.update_yaxes(
        title_text="GiB",
        row=3, col=1
    )


    fig.update_layout(
        hovermode="x",  # <- one hover box containing ALL traces at that x
        hoverdistance=-1,
        # hoverdistance=50,  # optional: how far from the cursor Plotly will look for points
    )

    fig.update_layout(
        height=800,
        margin=dict(l=20, r=20, t=60, b=120),
        legend=dict(
            orientation="h",
            xanchor="left",
            x=0.0,
            yanchor="top",
            y=-0.10
        ))

    return fig
