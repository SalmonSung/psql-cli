from __future__ import annotations
import os
import numpy as np
import math
import sqlparse
import textwrap

from datetime import datetime
from collections import defaultdict
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from metrics import CloudSQLMetrics, WALFlushedBytesCountMetric
import config as config

def wal_overview(metrics: CloudSQLMetrics) -> go.Figure:
    wal_fbc = metrics.wal_flushed_bytes_metrics.wal_flushed_bytes_count
    wal_fbc.sort()
    wal_fbc.group_by_minutes(config.GROUP_BY_MINUTES, mode="sum")

    wal_ibc = metrics.wal_inserted_bytes_metrics.wal_inserted_bytes_count
    wal_ibc.sort()
    wal_ibc.group_by_minutes(config.GROUP_BY_MINUTES, mode="sum")

    x_ts = wal_fbc.timestamps()
    y_flushed_bytes_count = []
    y_inserted_bytes_count = []
    customdata = []
    for flushed_byte, inserted_byte in zip(wal_fbc.data(), wal_ibc.data()):
        y_flushed_bytes_count.append(flushed_byte)
        y_inserted_bytes_count.append(inserted_byte)
        customdata.append([flushed_byte/1024, inserted_byte/1024])


    # --- Make Fig ---
    fig = make_subplots(
        rows=3,
        cols=1,
        specs=[
            [{"type": "xy"}],
            [{"type": "xy"}],
            [{"type": "xy"}],
        ],
        row_heights=[0.3, 0.35, 0.35],
        horizontal_spacing=0.08,
        vertical_spacing=0.08,
        subplot_titles=[
            "Flush History",
            "PLACEHOLDER",
            "PLACEHOLDER"
        ]
    )


    # --- Fig: Overview ---
    fig.add_trace(
        go.Scatter(
            x=x_ts,
            y=y_flushed_bytes_count,
            name="flushed_bytes_count",
            mode="lines",
            line=dict(
                color="royalblue",
                # width=2,
                dash="solid"  # solid | dash | dot | dashdot | longdash
            ),
            customdata=customdata,
            legendgroup="flushed_bytes_count",
            hovertemplate=(
                "<b>Time:</b> %{x|%H:%M} - "
                "%{x|%Y/%m/%d} - "
                "%{x|%a}<br><br>"
                "<b>Inserted Bytes:</b> %{customdata[1]:.2f} KiB<br>"
                "<b>Flushed Bytes:</b> %{customdata[0]:.2f} KiB<br>"
                "<extra></extra>"
            ),
            showlegend=True,
            visible=True,
        ),
        row=1, col=1
    )

    fig.add_trace(
        go.Scatter(
            x=x_ts,
            y=y_inserted_bytes_count,
            name="inserted_bytes_count",
            mode="lines",
            line=dict(
                color="orange",
                # width=2,
                dash="solid"  # solid | dash | dot | dashdot | longdash
            ),
            customdata=customdata,
            legendgroup="inserted_bytes_count",
            hovertemplate=(
                "<b>Time:</b> %{x|%H:%M} - "
                "%{x|%Y/%m/%d} - "
                "%{x|%a}<br><br>"
                "<b>Inserted Bytes:</b> %{customdata[1]:.2f} KiB<br>"
                "<b>Flushed Bytes:</b> %{customdata[0]:.2f} KiB<br>"
                "<extra></extra>"
            ),
            showlegend=True,
            visible=True,
        ),
        row=1, col=1
    )

    # --- Axis formatting ---
    fig.update_xaxes(
        tickformat="%H:%M<br>%Y/%m/%d<br>%a",
        row=1, col=1
    )

    fig.update_yaxes(
        title_text=f"bytes/{config.GROUP_BY_MINUTES} mins",
        # color="darkblue",
        row=1, col=1
    )

    fig.update_layout(
        height=1250,
        margin=dict(l=20, r=20, t=60, b=120),
        legend=dict(
            orientation="h",
            xanchor="left",
            x=0.0,
            yanchor="top",
            y=-0.10
        ))

    return fig
