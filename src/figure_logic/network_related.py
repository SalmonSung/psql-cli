from __future__ import annotations

from datetime import datetime
from collections import defaultdict
import pandas as pd
import plotly.graph_objects as go
from dns.query import default_socket_factory
from plotly.subplots import make_subplots
from metrics import TimeSeries, CloudSQLMetrics, PSQLNumBackendsByStateMetric
import config as config

CONNECTION_STATE_COLORS = {
    "active": "red",
    "idle": "lightblue",
    "idle in transaction": "orange",
    "idle in transaction aborted": "darkred",
    "disabled": "gray",
    "fastpath function call": "purple",
}

def network_overview(metrics: CloudSQLMetrics) -> go.Figure:
    x_set = set()
    for item in metrics.psql_num_backends_by_state_metrics:
        # item.psql_num_backends_by_state.group_by_minutes(config.GROUP_BY_MINUTES)
        x_set.update(item.psql_num_backends_by_state.timestamps())


    fig = make_subplots(
        rows=2,
        cols=1,
        specs=[
            [{"type": "xy"}],
            [{"type": "xy"}],
        ],
        row_heights=[0.5, 0.5],
        column_widths=[1],
        horizontal_spacing=0.08,
        vertical_spacing=0.05,
        subplot_titles=[
            "Connection State Overview",
            "Each Database",
        ]
    )

    x_ts = sorted(x_set)
    # --- Figure I ---
    grouped_state: dict[str, TimeSeries] = {}
    for item in metrics.psql_num_backends_by_state_metrics:
        # item.psql_num_backends_by_state.group_by_minutes(config.GROUP_BY_MINUTES, mode="avg")
        y_values: TimeSeries = TimeSeries(unit="counts")
        for ts in x_ts:
            if ts not in item.psql_num_backends_by_state.timestamps():
                y_values.add(ts, 0)
            else:
                y_values.add(ts, item.psql_num_backends_by_state.get_by_ts(ts))

        if grouped_state.get(item.state):
            grouped_state[item.state] = grouped_state[item.state].combine(y_values, mode="sum")
        else:
            grouped_state[item.state] = y_values

    for state, values in grouped_state.items():
        fig.add_trace(
            go.Scatter(
                x=x_ts,
                y=values.data(),
                name=state,
                mode="lines",
                line=dict(color=CONNECTION_STATE_COLORS[state]),
                stackgroup="one",
                hovertemplate=(
                    "<b>Time:</b> %{x|%H:%M} - "
                    "%{x|%Y/%m/%d} - "
                    "%{x|%a}<br><br>"
                    "<b>Connection Counts:</b> %{y}<br>"
                ),
                showlegend=False,
                visible=True,
            ),
            row=1, col=1,
        )





    # --- Figure II ---
    unique_db: list[str] = []
    fig_db_order: list[str] = []

    for item in metrics.psql_num_backends_by_state_metrics:
        # item.psql_num_backends_by_state.group_by_minutes(config.GROUP_BY_MINUTES, mode="avg")
        unique_id = item.database+f"({item.state})"
        if item.database not in unique_db:
            unique_db.append(item.database)

        y_values: list[int] = []
        for ts in x_ts:
            if ts not in item.psql_num_backends_by_state.timestamps():
                y_values.append(0)
            else:
                y_values.append(item.psql_num_backends_by_state.get_by_ts(ts))

        fig_db_order.append(item.database)
        fig.add_trace(
            go.Scatter(
                x=x_ts,
                y=y_values,
                name=unique_id,
                mode="lines",
                line=dict(color=CONNECTION_STATE_COLORS[item.state]),
                stackgroup="one",
                legendgroup=unique_id,
                hovertemplate=(
                    "<b>Time:</b> %{x|%H:%M} - "
                    "%{x|%Y/%m/%d} - "
                    "%{x|%a}<br><br>"
                    "<b>Connection Counts:</b> %{y}<br>"
                ),
                showlegend=True,
                visible=True,
            ),
            row=2, col=1,
        )

    # --- Update Buttons logic
    buttons: list[dict] = []

    n = len(fig.data)
    row1_count = len(grouped_state)  # number of traces in row 1
    row2_count = len(fig_db_order)  # number of traces in row 2
    # row3_count = n - row1_count - row2_count      # if needed
    for db in unique_db:
        vis = [True] * n
        # Keep row1 always visible:
        # (row1 traces are first row1_count traces)
        # Filter row2 traces:
        for i, db_name in enumerate(fig_db_order):
            vis[row1_count + i] = (db_name == db)
        # If you have row3 traces and want them always visible, keep them True (already True)

        buttons.append(dict(label=db, method="update", args=[{"visible": vis}]))

    default_db = unique_db[0]  # first button
    row1_count = len(grouped_state)

    # Set initial visibility: keep row1 visible, show only row2 traces for default_db
    for i, db_name in enumerate(fig_db_order):
        fig.data[row1_count + i].visible = (db_name == default_db)

    # row=2,col=1 is typically xaxis2 / yaxis2
    x0 = fig.layout.xaxis2.domain[0]  # left edge of subplot 2 in paper coords
    y1 = fig.layout.yaxis2.domain[1]  # top edge of subplot 2 in paper coords
    fig.update_layout(
        updatemenus=[
            dict(
                type="buttons",
                direction="right",
                x=x0,  # left edge of col=2 (tweak slightly)
                y=y1,  # above the subplot title
                xanchor="left",
                yanchor="bottom",
                buttons=buttons,
                pad={"t": 0, "r": 0, "b": 0, "l": 0},
            )
        ]
    )



    # --- Axis formatting (multi-line ticks) ---
    fig.update_xaxes(
        showticklabels=False,
        ticks="",
        row=1, col=1,
    )
    fig.update_xaxes(
        tickformat="%H:%M<br>%Y/%m/%d<br>%a",
        row=2, col=1
    )

    fig.update_yaxes(
        title_text="Counts",
        row=1, col=1
    )
    fig.update_yaxes(
        title_text="Counts",
        row=2, col=1
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
