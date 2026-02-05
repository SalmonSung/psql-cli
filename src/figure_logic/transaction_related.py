from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional, Tuple

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from metrics import CloudSQLMetrics


def transaction_ops(metrics: CloudSQLMetrics) -> go.Figure:
    fig = go.Figure()

    first_trace = True
    for item in  metrics.psql_transaction_count:
        if sum(item.psql_transaction_count.data()) < 5:
            continue

        if first_trace:
            hovertemplate = (
                "<b>Time:</b> %{x|%H:%M} - "
                "%{x|%Y/%m/%d} - "
                "%{x|%a}<br><br>"
                "<b>Counts:</b> %{y}<br>"
            )
            first_trace = False
        else:
            hovertemplate = (
                        "<b>Counts:</b> %{y}<br>"
                    )

        unique_id = item.database + "(" + item.transaction_type + ")"
        fig.add_trace(
            go.Scatter(
                x=item.psql_transaction_count.timestamps(),
                y=item.psql_transaction_count.data(),
                name=unique_id,
                mode="lines",
                # line=dict(color=CONNECTION_STATE_COLORS[item.state]),
                stackgroup="one",
                legendgroup=unique_id,
                hovertemplate=hovertemplate,
                showlegend=True,
                visible=True,
            ),
        )

        fig.update_xaxes(
            tickformat="%H:%M<br>%Y/%m/%d<br>%a",
        )

        fig.update_yaxes(
            title_text="Counts",
        )
        fig.update_layout(
            hovermode="x",  # <- one hover box containing ALL traces at that x
            hoverdistance=-1,
            # hoverdistance=50,  # optional: how far from the cursor Plotly will look for points
        )
        fig.update_layout(
            height=400,
            margin=dict(l=20, r=20, t=60, b=150),
            legend=dict(
                orientation="h",
                xanchor="left",
                x=0.0,
                yanchor="top",
                y=-0.25
            ))
    return fig