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
from src.metrics import CloudSQLMetrics, PerqueryLockTimeMetric, PerqueryLatencyMetric, PerqueryIOTimeMetric
import src.config as config

PALETTE_20 = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
    "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
    "#aec7e8", "#ffbb78", "#98df8a", "#ff9896", "#c5b0d5",
    "#c49c94", "#f7b6d2", "#c7c7c7", "#dbdb8d", "#9edae5",
]

OTHERS_COLOR = "#808080"


def _format_sql_for_hover(sql, width=40, max_lines=10):
    if not isinstance(sql, str):
        return ""

    formatted = sqlparse.format(
        sql,
        reindent=True,
        keyword_case="upper",
        strip_comments=True,
    )

    wrapped_lines = []
    for line in formatted.splitlines():
        wrapped_lines.extend(textwrap.wrap(line, width=width) or [""])

    if len(wrapped_lines) > max_lines:
        wrapped_lines = wrapped_lines[:max_lines]
        wrapped_lines.append("…")

    return "<br>".join(wrapped_lines)


def sql_perquery_latency_metrics(metrics: CloudSQLMetrics) -> go.Figure:
    group_query_hash: dict[str, PerqueryLatencyMetric] = {}
    for q in metrics.perquery_latency_metrics:
        group_query_hash[q.query_hash] = q

    # --- Compute totals (microseconds) ---
    items_with_totals: list[tuple[str, PerqueryLatencyMetric, int]] = []
    for key, item in group_query_hash.items():
        group_query_hash[key].perquery_count.sort()
        group_query_hash[key].perquery_latency_mean.sort()

        group_query_hash[key].perquery_count.group_by_minutes(config.GROUP_BY_MINUTES, mode="sum")
        group_query_hash[key].perquery_latency_mean.group_by_minutes(config.GROUP_BY_MINUTES, mode="avg")
        group_query_hash[key].perquery_latency_pr75.group_by_minutes(config.GROUP_BY_MINUTES, mode="sum")

        ts_count = group_query_hash[key].perquery_count.values
        ts_mean = group_query_hash[key].perquery_latency_mean.values
        total_latency_ms = 0.0
        for (dt1, count), (dt2, mean_us) in zip(ts_count, ts_mean):
            total_latency_ms += count * mean_us / 1000

        items_with_totals.append((key, item, total_latency_ms))

    # --- top 90% ---
    items_with_totals.sort(key=lambda x: x[2], reverse=True)

    total_latency_all = sum(total for _, _, total in items_with_totals)

    kept: list[tuple[str, PerqueryLatencyMetric, int]] = []
    cumulative = 0.0
    threshold = 0.9 * total_latency_all

    for item in items_with_totals:
        kept.append(item)
        cumulative += item[2]
        if cumulative >= threshold:
            break

    rest = total_latency_all - cumulative

    # --- Build Pie Data ---
    pie_labels = []
    pie_values_ms = []
    custom_data: list[list] = []
    pie_colors = []
    color_map: dict[str, str] = {}

    for i, (key, _item, _total_us) in enumerate(kept):
        color_map[key] = PALETTE_20[i % len(PALETTE_20)]

    for query_hash, item, total_latency_ms in kept:
        pie_labels.append(query_hash)
        pie_values_ms.append(total_latency_ms)
        custom_data.append([_format_sql_for_hover(item.querystring), item.database, item.user])
        pie_colors.append(color_map[query_hash])

    pie_labels.append("Others")
    pie_values_ms.append(rest)
    pie_colors.append(OTHERS_COLOR)
    custom_data.append(["Aggregated", "Aggregated", "Aggregated"])

    hover_text = [
        f"<b>SQL Hash:</b> {lbl}<br>"
        f"<b>DB:</b> {db}<br>"
        f"<b>User:</b> {usr}<br>"
        f"<b>Total Latency:</b> {val:.2f} ms<br><br>"
        f"<b>Query:</b><br>{_format_sql_for_hover(qry)}<br>"
        for (qry, db, usr), lbl, val in zip(custom_data, pie_labels, pie_values_ms)
    ]

    # --- Make Fig ---
    fig = make_subplots(
        rows=3,
        cols=2,
        specs=[
            [{"type": "domain"}, {"type": "xy"}],  # row 1: two columns
            [{"colspan": 2}, None],  # row 2: one column (span both)
            [{"colspan": 2}, None],  # row 3: one column (span both)
        ],
        row_heights=[0.3, 0.35, 0.35],
        column_widths=[0.40, 0.60],
        horizontal_spacing=0.08,
        vertical_spacing=0.08,
        subplot_titles=[
            "Top Latency SQL", "PR75/AVG",
            "Top Latency SQL by time",
            "Execution Count"
        ]
    )

    # --- Add pie trace ---
    fig.add_trace(
        go.Pie(
            labels=pie_labels,
            values=pie_values_ms,
            text=hover_text,
            customdata=custom_data,
            marker=dict(colors=pie_colors),
            hoverinfo="text+percent",
            hovertemplate=(
                "%{text}"
                "<b>Share:</b><br>%{percent}<br>"
                "<extra></extra>"
            ),
            hole=0.10,
            sort=False,
            textinfo="label",
            textposition="inside",
            insidetextorientation="radial",
            showlegend=False,
        ),
        row=1, col=1
    )

    # --- Data for stacked bar ---
    bar_x_ts = []
    for query_hash, item, total_latency_ms in kept:
        for ts in item.perquery_count.timestamps():
            if ts not in bar_x_ts:
                bar_x_ts.append(ts)
            else:
                continue

    for query_hash, item, total_latency_ms in kept:
        ts_count: dict[datetime, float] = {}
        ts_ms: dict[datetime, float] = {}
        ts_pr75_ms: dict[datetime, float] = {}
        ts_avg_ms: dict[datetime, float] = {}


        # cd = []
        # for ts, count, mean in zip(item.perquery_count.timestamps(), item.perquery_count.data(), item.perquery_latency_mean.data()):
        #
        #     cd.append([query_hash, item.database, item.user, item.querystring, mean/1000, count])

        cd_map = {}
        for ts, count, mean, pr75 in zip(item.perquery_count.timestamps(),
                                         item.perquery_count.data(),
                                         item.perquery_latency_mean.data(),
                                         item.perquery_latency_pr75.data()):
            ms_point = count * mean / 1000
            ts_count[ts] = count
            ts_ms[ts] = ms_point
            ts_pr75_ms[ts] = pr75 / 1000
            ts_avg_ms[ts] = mean / 1000
            cd_map[ts] = [_format_sql_for_hover(query_hash), item.database, item.user, item.querystring, mean / 1000, count]

        customdata = [cd_map.get(ts, [_format_sql_for_hover(query_hash), item.database, item.user, item.querystring, None, None])
                      for ts in bar_x_ts]

        legend_name = query_hash + f"({item.database})"

        y_ms = [ts_ms.get(ts, None) for ts in bar_x_ts]
        y_pr75_ms = [ts_pr75_ms.get(ts, None) for ts in bar_x_ts]
        y_count_ms = [ts_count.get(ts, None) for ts in bar_x_ts]
        y_avg_ms = [ts_avg_ms.get(ts, None) for ts in bar_x_ts]



        fig.add_trace(
            go.Bar(
                x=bar_x_ts,
                y=y_ms,
                name=legend_name,
                marker_color=color_map[query_hash],
                customdata=customdata,
                legendgroup=query_hash,
                hovertemplate=(
                    "<b>Time:</b> %{x|%H:%M} - "
                    "%{x|%Y/%m/%d} - "
                    "%{x|%a}<br><br>"
                    "<b>SQL Hash:</b> %{customdata[0]}<br>"
                    "<b>DB:</b> %{customdata[1]}<br>"
                    "<b>User:</b> %{customdata[2]}<br>"
                    "<b>Latency:</b> %{y:.2f} ms<br>"
                    "<b>AVG. Latency:</b> %{customdata[4]:.2f} ms<br>"
                    "<b>Execution Count:</b> %{customdata[5]}<br><br>"
                    "<b>Query:</b><br>%{customdata[3]}<br>"
                    "<extra></extra>"
                ),
                showlegend=True,
            ),
            row=2, col=1
        )

        # --- Fig: Top75 / AVG ---
        fig.add_trace(
            go.Scatter(
                x=bar_x_ts,
                y=y_pr75_ms,
                name=legend_name,
                mode="markers",
                line=dict(
                    # color="royalblue",
                    # width=2,
                    dash="solid"  # solid | dash | dot | dashdot | longdash
                ),
                marker_color=color_map[query_hash],
                customdata=customdata,
                legendgroup=query_hash,
                hoverinfo="skip",
                hovertemplate=(
                    "<b>Time:</b> %{x|%H:%M} - "
                    "%{x|%Y/%m/%d} - "
                    "%{x|%a}<br><br>"
                    "<b>SQL Hash:</b> %{customdata[0]}<br>"
                    "<b>DB:</b> %{customdata[1]}<br>"
                    "<b>User:</b> %{customdata[2]}<br>"
                    "<b>Latency:</b> %{y:.2f} ms<br>"
                    "<b>AVG. Latency:</b> %{customdata[4]:.2f} ms<br>"
                    "<b>Execution Count:</b> %{customdata[5]}<br><br>"
                    "<b>Query:</b><br>%{customdata[3]}<br>"
                    "<extra></extra>"
                ),
                showlegend=False,
                visible=True,
                meta="pr75"
            ),
            row=1, col=2
        )

        fig.add_trace(
            go.Scatter(
                x=bar_x_ts,
                y=y_avg_ms,
                name=legend_name,
                mode="markers",
                line=dict(
                    # color="royalblue",
                    # width=2,
                    dash="solid"  # solid | dash | dot | dashdot | longdash
                ),
                marker_color=color_map[query_hash],
                customdata=customdata,
                legendgroup=query_hash,
                hoverinfo="skip",
                hovertemplate=(
                    "<b>Time:</b> %{x|%H:%M} - "
                    "%{x|%Y/%m/%d} - "
                    "%{x|%a}<br><br>"
                    "<b>SQL Hash:</b> %{customdata[0]}<br>"
                    "<b>DB:</b> %{customdata[1]}<br>"
                    "<b>User:</b> %{customdata[2]}<br>"
                    "<b>Latency:</b> %{y:.2f} ms<br>"
                    "<b>AVG. Latency:</b> %{customdata[4]:.2f} ms<br>"
                    "<b>Execution Count:</b> %{customdata[5]}<br><br>"
                    "<b>Query:</b><br>%{customdata[3]}<br>"
                    "<extra></extra>"
                ),
                showlegend=False,
                visible=False,
                meta="avg"
            ),
            row=1, col=2
        )

        # --- Fig: Count ---
        fig.add_trace(
            go.Bar(
                x=bar_x_ts,
                y=y_count_ms,
                name=legend_name,
                marker_color=color_map[query_hash],
                customdata=customdata,
                legendgroup=query_hash,
                hovertemplate=(
                    "<b>Time:</b> %{x|%H:%M} - "
                    "%{x|%Y/%m/%d} - "
                    "%{x|%a}<br><br>"
                    "<b>SQL Hash:</b> %{customdata[0]}<br>"
                    "<b>DB:</b> %{customdata[1]}<br>"
                    "<b>User:</b> %{customdata[2]}<br>"
                    "<b>Latency:</b> %{y:.2f} ms<br>"
                    "<b>AVG. Latency:</b> %{customdata[4]:.2f} ms<br>"
                    "<b>Execution Count:</b> %{customdata[5]}<br><br>"
                    "<b>Query:</b><br>%{customdata[3]}<br>"
                    "<extra></extra>"
                ),
                showlegend=False,

            ),
            row=3, col=1
        )

    vis_pr75 = []
    vis_avg = []
    for tr in fig.data:
        if getattr(tr, "meta", None) == "pr75":
            vis_pr75.append(True)
            vis_avg.append(False)
        elif getattr(tr, "meta", None) == "avg":
            vis_pr75.append(False)
            vis_avg.append(True)
        else:
            # all other traces always visible
            vis_pr75.append(True)
            vis_avg.append(True)

    fig.update_layout(
        updatemenus=[
            dict(
                type="buttons",
                direction="right",
                x=0.45,  # left edge of col=2 (tweak slightly)
                y=1.08,  # above the subplot title
                xanchor="left",
                yanchor="top",
                buttons=[
                    dict(
                        label="PR75",
                        method="update",
                        args=[{"visible": vis_pr75}],
                    ),
                    dict(
                        label="AVG",
                        method="update",
                        args=[{"visible": vis_avg}],
                    ),
                ],
            )
        ]
    )

    # --- Make bars stacked ---
    fig.update_layout(barmode="stack")

    # --- Axis formatting (multi-line ticks) ---
    fig.update_xaxes(
        tickformat="%H:%M<br>%Y/%m/%d<br>%a",
        row=1, col=2
    )
    fig.update_xaxes(
        showticklabels=False,
        tickformat="",
        row=2, col=1
    )
    fig.update_xaxes(
        tickformat="%H:%M<br>%Y/%m/%d<br>%a",
        row=3, col=1
    )

    fig.update_yaxes(
        title_text="Latency (ms)",
        row=1, col=2
    )
    fig.update_yaxes(
        title_text="Latency (ms)",
        row=2, col=1
    )
    fig.update_yaxes(
        title_text="Count",
        row=3, col=1
    )

    fig.update_layout(
        height=1100,
        margin=dict(l=20, r=20, t=60, b=120),
        legend=dict(
            orientation="h",
            xanchor="left",
            x=0.0,
            yanchor="top",
            y=-0.10
        ))

    return fig

def sql_perquery_io_time_metrics(metrics: CloudSQLMetrics) -> go.Figure:
    items_with_totals: list[tuple[PerqueryIOTimeMetric, float, list[datetime]]] = []
    for item in metrics.perquery_IO_time_metrics:
        copied_item = item.perquery_IO_time.copy()
        copied_item.group_by_minutes(config.GROUP_BY_MINUTES)
        total_io_wait_time = float(sum(copied_item.data()))
        items_with_totals.append((item, total_io_wait_time, copied_item.timestamps()))

    items_with_totals.sort(key=lambda x: x[1], reverse=True)
    total_io_wait_time_all = sum(total for _, total, _ in items_with_totals)
    if total_io_wait_time_all <= 0:
        fig = go.Figure()
        fig.update_layout(title="No IO wait time data")
        return fig

    kept: list[tuple[PerqueryIOTimeMetric, float]] = []
    cumulative = 0.0
    threshold = 0.9 * total_io_wait_time_all
    for item, total, _ in items_with_totals:
        kept.append((item, total))
        cumulative += total
        if cumulative >= threshold:
            break
    rest = total_io_wait_time_all - cumulative

    pie_labels: list[str] = []
    pie_values_ms: list[float] = []
    custom_data: list[list[str]] = []
    pie_colors = []
    color_map: dict[str, str] = {}

    for i, (item, _total) in enumerate(kept):
        qh = item.query_hash or "(no hash)"
        color_map[qh] = PALETTE_20[i % len(PALETTE_20)]

    for item, total in kept:
        qh = item.query_hash or "(no hash)"
        pie_labels.append(qh)
        pie_values_ms.append(total / 1000)
        custom_data.append(
            [
                item.querystring,
                item.database,
                item.user,
                item.io_type,
            ]
        )
        pie_colors.append(color_map[qh])

    pie_labels.append("Others")
    pie_values_ms.append(rest / 1000)
    pie_colors.append(OTHERS_COLOR)
    custom_data.append(["Aggregated", "Aggregated", "Aggregated", "Aggregated"])

    hover_text = [
        f"<b>SQL Hash:</b> {lbl}<br>"
        f"<b>DB:</b> {db}<br>"
        f"<b>User:</b> {usr}<br>"
        f"<b>IO Type:</b> {io_type}<br>"
        f"<b>Total IO Wait:</b> {val:.2f} ms<br><br>"
        f"<b>Query:</b><br>{_format_sql_for_hover(qry)}<br>"
        for (qry, db, usr, io_type), lbl, val in zip(custom_data, pie_labels, pie_values_ms)
    ]

    fig = make_subplots(
        rows=1,
        cols=2,
        specs=[[{"type": "domain"}, {"type": "xy"}]],
        column_widths=[0.40, 0.60],
        horizontal_spacing=0.08,
        subplot_titles=["Top IO Wait SQL", "IO Wait Time by time"],
    )

    fig.add_trace(
        go.Pie(
            labels=pie_labels,
            values=pie_values_ms,
            text=hover_text,
            customdata=custom_data,
            marker=dict(colors=pie_colors),
            hoverinfo="text+percent",
            hovertemplate=(
                "%{text}"
                "<b>Share:</b><br>%{percent}<br>"
                "<extra></extra>"
            ),
            hole=0.10,
            sort=False,
            textinfo="label",
            textposition="inside",
            insidetextorientation="radial",
            showlegend=False,
        ),
        row=1, col=1
    )

    bar_x_ts: list[datetime] = []
    for item, _total in kept:
        copied_item = item.perquery_IO_time.copy()
        copied_item.group_by_minutes(config.GROUP_BY_MINUTES)
        for ts in copied_item.timestamps():
            if ts not in bar_x_ts:
                bar_x_ts.append(ts)
    bar_x_ts.sort()

    for item, _total in kept:
        qh = item.query_hash or "(no hash)"
        copied_item = item.perquery_IO_time.copy()
        copied_item.group_by_minutes(config.GROUP_BY_MINUTES)
        ts_map = {ts: val / 1000 for ts, val in copied_item.values}
        customdata = [
            [
                qh,
                item.database,
                item.user,
                item.querystring,
                item.io_type,
            ]
            for _ in bar_x_ts
        ]

        fig.add_trace(
            go.Bar(
                x=bar_x_ts,
                y=[ts_map.get(ts, None) for ts in bar_x_ts],
                name=f"{qh}({item.database})",
                marker_color=color_map[qh],
                customdata=customdata,
                legendgroup=qh,
                hovertemplate=(
                    "<b>Time:</b> %{x|%H:%M} - "
                    "%{x|%Y/%m/%d} - "
                    "%{x|%a}<br><br>"
                    "<b>SQL Hash:</b> %{customdata[0]}<br>"
                    "<b>DB:</b> %{customdata[1]}<br>"
                    "<b>User:</b> %{customdata[2]}<br>"
                    "<b>IO Type:</b> %{customdata[4]}<br>"
                    "<b>IO Wait:</b> %{y:.2f} ms<br><br>"
                    "<b>Query:</b><br>%{customdata[3]}<br>"
                    "<extra></extra>"
                ),
                showlegend=True,
            ),
            row=1, col=2
        )

    fig.update_layout(barmode="stack")

    fig.update_xaxes(
        tickformat="%H:%M<br>%Y/%m/%d<br>%a",
        row=1, col=2
    )
    fig.update_yaxes(
        title_text="IO wait (ms)",
        row=1, col=2
    )

    fig.update_layout(
        height=650,
        margin=dict(l=20, r=20, t=60, b=120),
        legend=dict(
            orientation="h",
            xanchor="left",
            x=0.0,
            yanchor="top",
            y=-0.10
        ))

    return fig


def sql_perquery_lock_time_metrics(metrics: CloudSQLMetrics) -> go.Figure:
    # --- Group by (query_hash, database) ---
    group_query_hash_database: dict[tuple[str, str], PerqueryLockTimeMetric] = {}

    for q in metrics.perquery_lock_time_metrics:
        key = (q.query_hash, q.database)
        if key not in group_query_hash_database:
            group_query_hash_database[key] = q
        else:
            group_query_hash_database[key].perquery_lock_time.extend(q.perquery_lock_time)

    # --- Compute totals (microseconds) ---
    items_with_totals: list[tuple[tuple[str, str], PerqueryLockTimeMetric, int]] = []
    for key, item in group_query_hash_database.items():
        total_wait_us = sum(item.perquery_lock_time.data())
        items_with_totals.append((key, item, total_wait_us))

    items_with_totals.sort(key=lambda x: x[2], reverse=True)
    grand_total_us = sum(total for _, _, total in items_with_totals)

    if grand_total_us <= 0:
        fig = go.Figure()
        fig.update_layout(title="No lock wait time data")
        return fig

    def us_to_ms(us: int) -> float:
        return us / 1000.0

    # --- Keep top 90% cumulative (Pareto) ---
    cutoff = 0.9 * grand_total_us
    running = 0
    kept: list[tuple[tuple[str, str], PerqueryLockTimeMetric, int]] = []

    for key, item, total_us in items_with_totals:
        if running >= cutoff:
            break
        kept.append((key, item, total_us))
        running += total_us

    rest_us = grand_total_us - running

    color_map: dict[tuple[str, str], str] = {}

    for i, (key, _item, _total_us) in enumerate(kept):
        color_map[key] = PALETTE_20[i % len(PALETTE_20)]

    # --- Pie inputs (list-of-lists customdata, so hover works reliably) ---
    # --- Build pie inputs (SAFE: hovertext) ---
    pie_labels: list[str] = []
    pie_values_ms: list[float] = []
    pie_hovertext: list[str] = []
    pie_colors = []

    for (key, item, total_us) in kept:
        qh = item.query_hash or "(no hash)"
        db = item.database or ""
        user = item.user or ""
        total_ms = us_to_ms(total_us)
        sql_html = _format_sql_for_hover(item.querystring) or "(no sql)"

        pie_labels.append(qh)  # or f"{qh} ({db})" if you prefer
        pie_values_ms.append(total_ms)

        pie_hovertext.append(
            f"<b>SQL Hash:</b> {qh}<br>"
            f"<b>DB:</b> {db}<br>"
            f"<b>User:</b> {user}<br><br>"
            f"<b>Total wait:</b> {total_ms:.2f} ms<br><br>"
            f"<b>Query:</b><br>{sql_html}<br>"
        )

        pie_colors.append(color_map[key])

    if rest_us > 0:
        rest_ms = us_to_ms(rest_us)
        pie_labels.append("Others")
        pie_values_ms.append(rest_ms)
        pie_hovertext.append(
            f"<b>SQL Hash:</b> Others<br>"
            f"<b>Total wait:</b> {rest_ms:.2f} ms<br><br>"
            f"<b>Query:</b><br>Aggregated remainder<br>"
        )
        pie_colors.append(OTHERS_COLOR)

    # --- Build STACKED time-series bars per query ---
    # 1) collect all normalized timestamps across kept queries
    all_ts_set: set[datetime] = set()
    per_query_map: list[tuple[tuple[str, str], PerqueryLockTimeMetric]] = []

    for key, item, _ in kept:
        per_query_map.append((key, item))
        item.perquery_lock_time.group_by_minutes(config.GROUP_BY_MINUTES)
        for ts, _v in item.perquery_lock_time.values:
            all_ts_set.add(ts)

    bar_x = sorted(all_ts_set)

    # 2) precompute per-query: {ts -> value_us}
    #    then align to bar_x (fill missing ts with 0)
    #    also attach per-point customdata for hover
    fig = make_subplots(
        rows=1,
        cols=2,
        specs=[[{"type": "domain"}, {"type": "xy"}]],
        column_widths=[0.40, 0.60],
        horizontal_spacing=0.08,
        subplot_titles=("Top 90% Lock Wait Share", "Lock Wait Over Time (Stacked by SQL)"),
    )

    # --- Add pie trace ---
    fig.add_trace(
        go.Pie(
            labels=pie_labels,
            values=pie_values_ms,
            marker=dict(colors=pie_colors),
            hovertext=pie_hovertext,
            hoverinfo="text+percent",  # percent still computed by Plotly
            hovertemplate="%{hovertext}<b>Share:</b> %{percent:.2%}<extra></extra>",
            hole=0.35,
            sort=False,
            textinfo="label",
            textposition="inside",
            insidetextorientation="radial",
            showlegend=True,
        ),
        row=1, col=1
    )

    # --- Add stacked bar traces (one trace per query/hash+db) ---
    # 2) inside each trace: aggregate values into the same normalized buckets
    for (key, item) in per_query_map:
        legend_name = f"{item.query_hash}({item.database})"

        ts_to_us = defaultdict(int)
        for ts, v in item.perquery_lock_time.values:
            ts_to_us[ts] += int(v)  # sum duplicates in same bucket

        y_ms = [us_to_ms(ts_to_us.get(ts, 0)) for ts in bar_x]

        formatted_sql = _format_sql_for_hover(item.querystring)
        qh = item.query_hash or ""
        db = item.database or ""
        user = item.user or ""
        cd = [[qh, db, user, formatted_sql] for _ in bar_x]
        fig.add_trace(
            go.Bar(
                x=bar_x,
                y=y_ms,
                name=legend_name,
                marker_color=color_map[key],
                customdata=cd,
                hovertemplate=(
                    "<b>Time:</b> %{x|%H:%M}<br>"
                    "%{x|%Y/%m/%d}<br>"
                    "%{x|%a}<br><br>"
                    "<b>SQL Hash:</b> %{customdata[0]}<br>"
                    "<b>DB:</b> %{customdata[1]}<br>"
                    "<b>User:</b> %{customdata[2]}<br>"
                    "<b>Wait:</b> %{y:.2f} ms<br><br>"
                    "<b>Query:</b><br>%{customdata[3]}<br>"
                    "<extra></extra>"
                ),
            ),
            row=1, col=2
        )
    # --- Make bars stacked ---
    fig.update_layout(barmode="stack")

    # --- Axis formatting (multi-line ticks) ---
    fig.update_xaxes(
        tickformat="%H:%M<br>%Y/%m/%d<br>%a",
        row=1, col=2
    )
    fig.update_yaxes(
        title_text="Lock wait (ms)",
        row=1, col=2
    )

    fig.update_layout(
        height=650,
        margin=dict(l=20, r=20, t=60, b=120),
        legend=dict(
            orientation="h",
            xanchor="left",
            x=0.0,
            yanchor="top",
            y=-0.10
        ))

    fig.update_traces(showlegend=False, selector=dict(type="pie"))

    return fig


def sql_consumption_overview(metrics: CloudSQLMetrics) -> go.Figure:
    """
    Pie: Exec time share for top ~90% queries + Others
    Table: Query, total exec time (mins), calls, avg exec time (ms), plan issue
    """
    df = pd.DataFrame(metrics.pg_stat_statements_top_queries).copy()
    if df.empty:
        fig = go.Figure()
        fig.update_layout(title_text="pg_stat_statements: Total Exec Time Pareto (90%)", height=650)
        return fig

    # ---- helpers ----
    def _num(s, default=0.0):
        return pd.to_numeric(s, errors="coerce").fillna(default)

    # ---- normalize / compute pareto ----
    df["total_exec_time"] = _num(df.get("total_exec_time"))
    df["calls"] = _num(df.get("calls"), default=0).astype(int)

    df = df.sort_values("total_exec_time", ascending=False)
    total_exec = float(df["total_exec_time"].sum())

    if total_exec <= 0:
        fig = go.Figure()
        fig.update_layout(title_text="pg_stat_statements: Total Exec Time Pareto (90%)", height=650)
        return fig

    df["cum_exec_time_pct"] = df["total_exec_time"].cumsum() / total_exec

    # Keep rows until reaching 90% (include first row that crosses it)
    cut_idx = int(df["cum_exec_time_pct"].searchsorted(0.9, side="left"))
    df_top = df.iloc[: cut_idx + 1].copy()

    top_sum = float(df_top["total_exec_time"].sum())
    others_sum = max(total_exec - top_sum, 0.0)

    # ---- pie data ----
    pie_labels = df_top["queryid"].astype(str).tolist() + ["Others"]
    pie_values = df_top["total_exec_time"].tolist() + [others_sum]
    pie_customdata = (
            df_top["query"].fillna("").apply(_format_sql_for_hover).tolist()
            + ["Aggregated remaining queries"]
    )

    # ---- build subplot ----
    fig = make_subplots(
        rows=1,
        cols=2,
        specs=[[{"type": "domain"}, {"type": "table"}]],
        column_widths=[0.35, 0.65],
        horizontal_spacing=0.05,  # optional, keeps separation clean
        subplot_titles=("Exec time share (Top ~90% + Others)", "Top queries (sorted)"),
    )

    fig.add_trace(
        go.Pie(
            labels=pie_labels,
            values=pie_values,
            customdata=pie_customdata,
            hole=0.35,
            sort=False,
            textinfo="label",
            textposition="inside",
            insidetextorientation="radial",
            showlegend=True,
            hovertemplate=(
                "<b>Query:</b><br>%{customdata}<br><br>"
                "<b>Share:</b> %{percent:.2%}<br>"
                "<extra></extra>"
            ),
        ),
        row=1,
        col=1,
    )

    # ---- table data ----
    df_show = df_top[
        ["queryid", "query", "calls", "total_exec_time", "avg_exec_ms", "total_plan_time"]
    ].copy()

    df_show["total_exec_time_mins"] = (_num(df_show["total_exec_time"]) / 1000.0 / 60.0).round(2)

    df_show["avg_exec_ms"] = _num(df_show["avg_exec_ms"]).round(1)

    df_show["planning_issue"] = _num(df_show["total_plan_time"]) > _num(df_show["total_exec_time"])

    # Rename columns EXACTLY as requested (table title names)
    df_show = df_show.rename(
        columns={
            "queryid": "queryid",
            "query": "query",
            "total_exec_time_mins": "total exec time(mins)",
            "calls": "calls",
            "avg_exec_ms": "avg exec time(ms)",
            "planning_issue": "plan issue",
        }
    )

    # Keep only requested columns + order
    df_show = df_show[
        ["queryid", "query", "total exec time(mins)", "calls", "avg exec time(ms)", "plan issue"]
    ]

    fig.add_trace(
        go.Table(
            header=dict(
                values=list(df_show.columns),
                align="left",
                fill_color="rgba(230,230,230,0.8)",
                font=dict(size=12, color="black"),
            ),
            cells=dict(
                values=[df_show[c].tolist() for c in df_show.columns],
                align="left",
                font=dict(size=11),
                height=24,
            ),
            columnwidth=[1.4, 3.8, 0.5, 0.5, 0.5, 0.4],
        ),
        row=1,
        col=2,
    )

    fig.update_layout(
        title_text="pg_stat_statements: Total Exec Time Pareto (90%)",
        height=650,
        margin=dict(l=20, r=20, t=70, b=20),
    )

    return fig


def sql_wal_heavy_job(metrics: "CloudSQLMetrics") -> go.Figure:
    """
    WAL-heavy queries (Top 90% by wal_bytes) + automatic suspicious tagging.

    Visualization: single scatter (clean + diagnostic)
      - X: rows
      - Y: wal_per_row
      - Bubble size: wal_bytes
      - Bubble color: suspicious_main (primary tag)
      - Hover: queryid, query, rows, wal_bytes, calls, total_exec_time, tags, score

    Requires:
      - metrics.pg_stat_statements_heavy_wal: list[dict] or similar
      - _format_sql_for_hover(sql: str) -> str  (your existing helper)
    """
    df = pd.DataFrame(getattr(metrics, "pg_stat_statements_heavy_wal", [])).copy()
    if df.empty:
        fig = go.Figure()
        fig.update_layout(title="pg_stat_statements: WAL-heavy queries (Pareto 90%)", height=650)
        return fig

    # ---------- helpers ----------
    def _num(s, default=0.0):
        return pd.to_numeric(s, errors="coerce").fillna(default)

    def _q(series: pd.Series, q: float, fallback: float = 0.0) -> float:
        series = series.replace([np.inf, -np.inf], np.nan).dropna()
        if series.empty:
            return fallback
        return float(series.quantile(q))

    # ---------- normalize ----------
    df["queryid"] = df.get("queryid")
    df["query"] = df.get("query", "").fillna("")
    df["calls"] = _num(df.get("calls"), default=0).astype(int)
    df["rows"] = _num(df.get("rows"), default=0.0)
    df["total_exec_time"] = _num(df.get("total_exec_time"), default=0.0)  # ms
    df["wal_bytes"] = _num(df.get("wal_bytes"), default=0.0)

    # Derived
    df["wal_per_row"] = np.where(df["rows"] > 0, df["wal_bytes"] / df["rows"], np.nan)
    df["wal_per_call"] = np.where(df["calls"] > 0, df["wal_bytes"] / df["calls"], np.nan)
    df["rows_per_call"] = np.where(df["calls"] > 0, df["rows"] / df["calls"], 0.0)

    # ---------- Pareto filter (Top 90% WAL consumers) ----------
    df = df.sort_values("wal_bytes", ascending=False)
    total_wal = float(df["wal_bytes"].sum())
    if total_wal <= 0:
        fig = go.Figure()
        fig.update_layout(title="pg_stat_statements: WAL-heavy queries (Pareto 90%)", height=650)
        return fig

    df["cum_wal_pct"] = df["wal_bytes"].cumsum() / total_wal
    cut_idx = int(df["cum_wal_pct"].searchsorted(0.9, side="left"))
    df = df.iloc[: cut_idx + 1].copy()

    # ---------- Suspicious tagging (rule-based, explainable) ----------
    # Tunables (reasonable defaults)
    q_high_wal = 0.90
    q_low_rows = 0.25
    q_high_calls = 0.90
    q_high_wal_per_row = 0.95
    q_high_wal_per_call = 0.95

    min_calls_for_chatty = 50
    max_rows_per_call_for_chatty = 5.0
    min_wal_bytes_for_any_tag = 1024 * 1024  # 1MB

    wal = df["wal_bytes"].astype(float)
    rows = df["rows"].astype(float)
    calls = df["calls"].astype(float)

    wal_high = _q(wal, q_high_wal, fallback=0.0)
    rows_low = _q(rows, q_low_rows, fallback=0.0)
    calls_high = _q(calls, q_high_calls, fallback=0.0)
    wpr_high = _q(df["wal_per_row"], q_high_wal_per_row, fallback=np.inf)
    wpc_high = _q(df["wal_per_call"], q_high_wal_per_call, fallback=np.inf)

    cond_big_enough = wal >= float(min_wal_bytes_for_any_tag)

    cond_wal_with_zero_rows = cond_big_enough & (rows <= 0) & (wal > 0)

    cond_high_wal_low_rows = (
            cond_big_enough
            & (wal >= wal_high)
            & (rows <= rows_low)
            & (rows > 0)
    )

    cond_high_wal_per_row = (
            cond_big_enough
            & (rows > 0)
            & (df["wal_per_row"] >= wpr_high)
    )

    cond_high_wal_per_call = (
            cond_big_enough
            & (calls > 0)
            & (df["wal_per_call"] >= wpc_high)
    )

    cond_chatty_small_updates = (
            (df["calls"] >= max(min_calls_for_chatty, int(calls_high)))
            & (df["rows_per_call"] <= float(max_rows_per_call_for_chatty))
            & cond_big_enough
    )

    tag_cols = {
        "WAL_WITH_ZERO_ROWS": cond_wal_with_zero_rows,
        "HIGH_WAL_LOW_ROWS": cond_high_wal_low_rows,
        "HIGH_WAL_PER_ROW": cond_high_wal_per_row,
        "HIGH_WAL_PER_CALL": cond_high_wal_per_call,
        "CHATTY_SMALL_UPDATES": cond_chatty_small_updates,
    }
    tags_matrix = pd.DataFrame(tag_cols, index=df.index)

    df["suspicious_tags"] = tags_matrix.apply(
        lambda r: ", ".join([k for k, v in r.items() if bool(v)]) if r.any() else "OK",
        axis=1,
    )
    df["suspicious"] = df["suspicious_tags"] != "OK"

    priority = [
        "WAL_WITH_ZERO_ROWS",
        "HIGH_WAL_PER_ROW",
        "HIGH_WAL_LOW_ROWS",
        "HIGH_WAL_PER_CALL",
        "CHATTY_SMALL_UPDATES",
    ]

    def _primary(tags: str) -> str:
        if tags == "OK":
            return "OK"
        parts = [t.strip() for t in tags.split(",")]
        for p in priority:
            if p in parts:
                return p
        return parts[0] if parts else "OK"

    df["suspicious_main"] = df["suspicious_tags"].apply(_primary)

    # simple 0..100-ish score
    score = np.zeros(len(df), dtype=float)

    def _ratio_over(value: pd.Series, thr: float) -> np.ndarray:
        if thr <= 0 or np.isinf(thr) or np.isnan(thr):
            return np.zeros(len(value), dtype=float)
        v = value.replace([np.inf, -np.inf], np.nan).fillna(0.0).to_numpy(dtype=float)
        return np.maximum(0.0, (v / thr) - 1.0)

    score += 30.0 * _ratio_over(wal, max(wal_high, min_wal_bytes_for_any_tag))
    score += 35.0 * _ratio_over(df["wal_per_row"], wpr_high if np.isfinite(wpr_high) else 0.0)
    score += 25.0 * _ratio_over(df["wal_per_call"], wpc_high if np.isfinite(wpc_high) else 0.0)
    score += np.where(cond_chatty_small_updates.to_numpy(), 15.0, 0.0)
    score += np.where(cond_wal_with_zero_rows.to_numpy(), 20.0, 0.0)
    df["suspicion_score"] = np.clip(score, 0.0, 100.0).round(1)

    # ---------- presentation prep ----------
    df["qid"] = df["queryid"].astype(str)
    df["query_hover"] = df["query"].apply(_format_sql_for_hover)
    df["total_exec_mins"] = df["total_exec_time"] / 1000.0 / 60.0

    # Stable marker sizing (sqrt scale, clipped)
    # (avoid specifying colors for the chart; only categorical colors below)
    size = np.sqrt(df["wal_bytes"].clip(lower=0.0).to_numpy()) / 50.0
    df["marker_size"] = np.clip(size, 10.0, 45.0)

    # Categorical coloring for tags (explicit + explainable)
    # (If you *really* want zero explicit colors, remove this and use a single color + symbol changes instead.)
    color_map = {
        "OK": "rgba(160,160,160,0.9)",
        "WAL_WITH_ZERO_ROWS": "rgba(220,20,60,0.95)",
        "HIGH_WAL_PER_ROW": "rgba(255,140,0,0.95)",
        "HIGH_WAL_LOW_ROWS": "rgba(255,69,0,0.95)",
        "HIGH_WAL_PER_CALL": "rgba(138,43,226,0.95)",
        "CHATTY_SMALL_UPDATES": "rgba(30,144,255,0.95)",
    }
    df["marker_color"] = df["suspicious_main"].map(color_map).fillna(color_map["OK"])

    # ---------- build figure ----------
    # One trace per tag so legend is meaningful + clickable.
    fig = go.Figure()

    # Keep a consistent order in legend (OK last so problems pop)
    legend_order = [
        "WAL_WITH_ZERO_ROWS",
        "HIGH_WAL_PER_ROW",
        "HIGH_WAL_LOW_ROWS",
        "HIGH_WAL_PER_CALL",
        "CHATTY_SMALL_UPDATES",
        "OK",
    ]

    for tag in legend_order:
        d = df[df["suspicious_main"] == tag]
        if d.empty:
            continue

        custom = np.stack(
            [
                d["qid"].to_numpy(),
                d["query_hover"].to_numpy(),
                d["rows"].to_numpy(),
                d["wal_bytes"].to_numpy(),
                d["calls"].to_numpy(),
                d["total_exec_time"].to_numpy(),
                d["total_exec_mins"].to_numpy(),
                d["wal_per_row"].to_numpy(),
                d["wal_per_call"].to_numpy(),
                d["suspicious_tags"].to_numpy(),
                d["suspicion_score"].to_numpy(),
            ],
            axis=1,
        )

        fig.add_trace(
            go.Scatter(
                x=d["rows"],
                y=d["wal_per_row"],
                mode="markers",
                name=tag,
                marker=dict(size=d["marker_size"], color=d["marker_color"], opacity=0.85),
                customdata=custom,
                hovertemplate=(
                    "<b>queryid:</b> %{customdata[0]}<br>"
                    "<b>query:</b><br>%{customdata[1]}<br><br>"
                    "<b>rows:</b> %{customdata[2]:,.0f}<br>"
                    "<b>wal_bytes:</b> %{customdata[3]:,.0f}<br>"
                    "<b>calls:</b> %{customdata[4]:,}<br>"
                    "<b>total_exec_time:</b> %{customdata[5]:,.0f} ms (%{customdata[6]:.2f} mins)<br>"
                    "<b>wal_per_row:</b> %{customdata[7]:,.1f}<br>"
                    "<b>wal_per_call:</b> %{customdata[8]:,.1f}<br>"
                    "<b>tags:</b> %{customdata[9]}<br>"
                    "<b>score:</b> %{customdata[10]}<br>"
                    "<extra></extra>"
                ),
            )
        )

    fig.update_layout(
        title="pg_stat_statements: WAL-heavy queries (Top 90%) — suspicious tagging",
        height=650,
        margin=dict(l=20, r=20, t=70, b=20),
        xaxis_title="Rows affected",
        yaxis_title="WAL bytes per row",
        # Legend on the right (as requested)
        legend=dict(
            orientation="v",
            yanchor="top",
            y=1,
            xanchor="left",
            x=1.02,
        ),
    )

    # Nice defaults for wide ranges
    fig.update_xaxes(type="log", rangemode="tozero")
    fig.update_yaxes(type="log", rangemode="tozero")

    return fig
