from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional, Tuple

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from metrics import CloudSQLMetrics
from utils import bytes_to_unit



def _safe_xy(ts) -> Tuple[List[datetime], List[float]]:
    """Return (x, y) sorted by time from a TimeSeries-like object."""
    try:
        vals = list(ts.values)  # List[(datetime, value)]
        if not vals:
            return [], []
        vals.sort(key=lambda x: x[0])
        x = [t for t, _ in vals]
        y = [float(v) for _, v in vals]
        return x, y
    except Exception:
        return [], []

def disk_usage_pie_overview(
    metrics: "CloudSQLMetrics",
    title: str = "Cloud SQL Disk Usage (Current + Trend)",
) -> go.Figure:
    """
    Two subfigures SIDE-BY-SIDE:

    Left:
      - Donut pie showing ALL used types + optional remainder + Available
      - NO legend

    Right:
      - Disk used over time (total + by-type)
      - Legend shown on the RIGHT side
      - Toggleable 90% quota warning line
    """
    cur_quota = metrics.disk_quota.data()[-1]
    cur_used = metrics.disk_bytes_used.data()[-1]
    cur_avail_bytes = max(cur_quota - cur_used, 0.0)

    by_type_b: Dict[str, float] = {}
    for k, ts in metrics.disk_bytes_used_by_type.items():
        cur_used_k_bytes = ts.data()[-1]
        if cur_used_k_bytes is not None and cur_used_k_bytes >= 0:
            by_type_b[k] = cur_used_k_bytes

    labels: List[str] = []
    values: List[float] = []

    for item in sorted(by_type_b.items(), key=lambda x: x[1]):
        labels.append(item[0])
        values.append(bytes_to_unit(item[1]))

    labels.append("Available")
    values.append(bytes_to_unit(cur_avail_bytes))


    fig = make_subplots(
        rows=1,
        cols=2,
        specs=[[{"type": "domain"}, {"type": "xy"}]],
        column_widths=[0.35, 0.65],
        horizontal_spacing=0.10,
        subplot_titles=(
            "Current disk usage",
            "Disk used over time",
        ),
    )

    fig.add_trace(
        go.Pie(
            labels=labels,
            values=values,
            hole=0.35,
            sort=False,
            textinfo="label",
            textposition="inside",
            insidetextorientation="radial",
            hovertemplate="<b>%{label}</b><br>%{value:.2f} GiB<extra></extra>",
            showlegend=False,
        ),
        row=1,
        col=1,
    )

    # -------------------
    # RIGHT: Time series (legend ON)
    # -------------------
    x_used = metrics.disk_quota.timestamps()
    y_used = metrics.disk_quota.data()
    fig.add_trace(
        go.Scatter(
            x=x_used,
            y=[bytes_to_unit(v) for v in y_used],
            mode="lines",
            name="quota",
            hovertemplate="%{x}<br>%{y:.2f} GiB<extra></extra>",
        ),
        row=1,
        col=2,
    )




    x_used = metrics.disk_bytes_used.timestamps()
    y_used = metrics.disk_bytes_used.data()
    fig.add_trace(
        go.Scatter(
            x=x_used,
            y=[bytes_to_unit(v) for v in y_used],
            mode="lines",
            name="disk_bytes_used",
            hovertemplate="%{x}<br>%{y:.2f} GiB<extra></extra>",
        ),
        row=1,
        col=2,
    )

    for type_name, ts in metrics.disk_bytes_used_by_type.items():
        fig.add_trace(
            go.Scatter(
                x=ts.timestamps(),
                y=[bytes_to_unit(v) for v in ts.data()],
                mode="lines",
                name=f"Type: {type_name}",
                hovertemplate="%{x}<br>%{y:.2f} GiB<extra></extra>",
            ),
            row=1,
            col=2,
        )

    # -------------------
    # Warning line + toggle
    # -------------------
    warn_x = metrics.disk_quota.timestamps()
    warn_y = [bytes_to_unit(v* 0.9)  for v in metrics.disk_quota.data()]  # 90% in GiB

    fig.add_trace(
        go.Scatter(
            x=warn_x,
            y=warn_y,
            mode="lines",
            name="Safe line (90% quota)",
            line=dict(color="red", dash="dash"),
            visible=False,
            hovertemplate="%{x}<br>%{y:.2f} GiB (90% quota)<extra></extra>",
        ),
        row=1,
        col=2,
    )

    # -------------------
    # Button to toggle the safe line
    # -------------------
    safe_trace_index = len(fig.data) - 1  # last trace we just added

    fig.update_layout(
        updatemenus=[
            dict(
                type="buttons",
                direction="left",
                x=0.62,  # position (tweak as you like)
                y=1.15,
                buttons=[
                    dict(
                        label="Safe line",
                        method="restyle",
                        args=[{"visible": [True]}, [safe_trace_index]],
                    ),
                    dict(
                        label="Hide safe line",
                        method="restyle",
                        args=[{"visible": [False]}, [safe_trace_index]],
                    ),
                ],
            )
        ]
    )

    fig.update_layout(
        # title_text="Cloud SQL Disk Usage",
        height=650,
        margin=dict(l=20, r=20, t=70, b=20),
    )

    fig.update_yaxes(
        title_text="GiB",
        # ticks="outside",
        # tickformat=".2f",
        rangemode="tozero",
        row=1,
        col=2,
    )

    return fig


def disk_io_and_usage_timeseries(
    metrics: CloudSQLMetrics,
    title: str = "Cloud SQL Disk IO Overview",
) -> go.Figure:
    """
    Two-row figure:

    Row 1:
      - read/write bytes (GiB per interval) as lines
      - disk_utilization as light-blue bars on secondary y-axis

    Row 2:
      - read/write ops as lines
      - disk_utilization as light-blue bars on secondary y-axis
    """
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.10,
        specs=[
            [{"type": "xy", "secondary_y": True}],
            [{"type": "xy", "secondary_y": True}],
        ],
        subplot_titles=(
            "Disk IO bytes (read/write) + utilization (bar)",
            "Disk IO ops (read/write) + utilization (bar)",
        ),
    )

    # Utilization (used in both rows)
    x_util = metrics.disk_utilization.timestamps()
    y_util = metrics.disk_utilization.data()

    # --- Row 1: bytes (convert to GiB for y) + utilization bars
    fig.add_trace(
        go.Scatter(
            x=metrics.disk_read_bytes.timestamps(),
            y=[bytes_to_unit(v, "GiB") for v in metrics.disk_read_bytes.data()],
            mode="lines",
            name="read_bytes_count",
            hovertemplate="%{x}<br>%{y:.4f} GiB/interval<extra></extra>",
        ),
        row=1,
        col=1,
        secondary_y=False,
    )

    fig.add_trace(
        go.Scatter(
            x=metrics.disk_write_bytes.timestamps(),
            y=[bytes_to_unit(v, "GiB") for v in metrics.disk_write_bytes.data()],
            mode="lines",
            name="write_bytes_count",
            hovertemplate="%{x}<br>%{y:.4f} GiB/interval<extra></extra>",
        ),
        row=1,
        col=1,
        secondary_y=False,
    )

    fig.add_trace(
        go.Bar(
            x=x_util,
            y=y_util,
            name="disk_utilization",
            opacity=0.35,
            marker_color="lightblue",
            hovertemplate="%{x}<br>%{y:.3f}<extra></extra>",
        ),
        row=1,
        col=1,
        secondary_y=True,
    )

    # --- Row 2: ops + utilization bars (same series, no legend duplicate)
    fig.add_trace(
        go.Scatter(
            x=metrics.disk_read_ops.timestamps(),
            y=metrics.disk_read_ops.data(),
            mode="lines",
            name="read_ops_count",
            hovertemplate="%{x}<br>%{y:.0f} ops/interval<extra></extra>",
        ),
        row=2,
        col=1,
        secondary_y=False,
    )

    fig.add_trace(
        go.Scatter(
            x=metrics.disk_write_ops.timestamps(),
            y=metrics.disk_write_ops.data(),
            mode="lines",
            name="write_ops_count",
            hovertemplate="%{x}<br>%{y:.0f} ops/interval<extra></extra>",
        ),
        row=2,
        col=1,
        secondary_y=False,
    )

    fig.add_trace(
        go.Bar(
            x=x_util,
            y=y_util,
            name="disk_utilization (bar)",
            opacity=0.35,
            marker_color="lightblue",
            showlegend=False,
            hovertemplate="%{x}<br>%{y:.3f}<extra></extra>",
        ),
        row=2,
        col=1,
        secondary_y=True,
    )

    # Axes + layout
    fig.update_yaxes(title_text="GiB / interval", row=1, col=1, secondary_y=False)
    fig.update_yaxes(title_text="Utilization (ratio)", row=1, col=1, secondary_y=True, range=[0, 1])

    fig.update_yaxes(title_text="Ops / interval", row=2, col=1, secondary_y=False)
    fig.update_yaxes(title_text="Utilization (ratio)", row=2, col=1, secondary_y=True, range=[0, 1])

    fig.update_layout(
        title_text=title,
        height=780,
        margin=dict(l=30, r=30, t=90, b=30),
        barmode="overlay",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    )

    return fig
