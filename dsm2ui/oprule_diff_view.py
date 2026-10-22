# -*- coding: utf-8 -*-
"""Chart overlaying the base and scenario values of one operating-rule time series."""
import html

import pandas as pd
import panel as pn
from bokeh.models import (
    BoxZoomTool, ColumnDataSource, HoverTool, NumeralTickFormatter, PanTool, Range1d, ResetTool, WheelZoomTool,
)
from bokeh.plotting import figure

from dsm2ui.oprule_view import _CHART_W, _RANGE_JS, _ms, _series_data

_BASE_COLOR = "#2f5fb3"
_SCENARIO_COLOR = "#e02020"
_WIDTH = 584


def _union(views):
    xs = [pd.Timestamp(t) for v in views for t in v["x"]]
    return {"x": (min(xs), max(xs)), "y": (min(v["y"][0] for v in views), max(v["y"][1] for v in views))}


def _caption(name, units, base_row, scenario_row):
    def source(row):
        return "constant " + row["path"] if row["file"].strip().lower() == "constant" else f"{row['file']}  {row['path']}"

    return (
        "<div style='font-size:11px;color:#444;word-break:break-all'>"
        f"<div style='font-size:11.5px'>Time series: <b>{html.escape(name)}</b>"
        f"{' (' + html.escape(units) + ')' if units else ''}</div>"
        f"<div><b style='color:{_BASE_COLOR}'>Base:</b> {html.escape(source(base_row))}</div>"
        f"<div><b style='color:{_SCENARIO_COLOR}'>Scenario:</b> {html.escape(source(scenario_row))}</div></div>"
    )


def _message(text):
    return pn.pane.HTML(f"<div style='color:#b3261e;font-size:11.5px'>{html.escape(text)}</div>", margin=0, width=_WIDTH)


def series_chart(entry, load_series):
    """Panel object plotting the base and scenario series of a ``gate_rule_diff`` series entry.

    ``load_series(side, row)`` returns ``(DataFrame, units, model run period or None)`` for ``"base"`` or ``"scenario"``.
    """
    name, b_row, s_row = entry["name"], entry["base"], entry["scenario"]
    if entry.get("note"):
        return _message(f"Time series {name}: {entry['note']}")
    try:
        data = [_series_data(row, *load_series(side, row)) for side, row in (("base", b_row), ("scenario", s_row))]
    except Exception as exc:  # file or path problems are reported in place of the chart
        return _message(f"Time series {name}: {exc}")
    if any(d["empty"] for d in data):
        return _message(f"Time series {name}: a series has no data.")
    base, scenario = data
    runs = [d["run"] for d in data if d["run"]]
    full = _union([d["full"] for d in data])
    run = _union(runs) if runs else None
    view = run or full
    pan = PanTool(dimensions="width")
    fig = figure(
        x_axis_type="datetime", x_range=Range1d(*[pd.Timestamp(t).to_pydatetime() for t in view["x"]]),
        y_range=Range1d(*view["y"]), width=_CHART_W - 4, height=190, toolbar_location="above", min_border=4,
        tools=[pan, BoxZoomTool(dimensions="width"), WheelZoomTool(dimensions="width"), ResetTool()],
        active_drag=pan, active_scroll=None, outline_line_color=None,
    )
    lines = []
    for label, d, color, width in (("Base", base, _BASE_COLOR, 2.6), ("Scenario", scenario, _SCENARIO_COLOR, 1.6)):
        lines.append(fig.step(
            "t", "v", source=ColumnDataSource({"t": d["t"], "v": d["v"]}), mode="after", line_width=width, color=color,
            legend_label=label, name=label,
        ))
    fig.add_tools(HoverTool(
        renderers=lines, mode="vline", formatters={"@t": "datetime"},
        tooltips=[("", "$name"), ("Time", "@t{%F %H:%M}"), ("Value", "@v{0,0.000}")],
    ))
    fig.legend.location = "top_left"
    fig.legend.orientation = "horizontal"
    fig.legend.background_fill_alpha = 0.7
    fig.legend.click_policy = "hide"
    fig.legend.label_text_font_size = "10px"
    fig.toolbar.logo = None
    fig.yaxis.formatter = NumeralTickFormatter(format="0,0.[000]")
    fig.axis.axis_label_text_font_size = "10px"
    fig.axis.major_label_text_font_size = "10px"
    fig.grid.grid_line_color = "#eeeeee"
    if base["units"]:
        fig.yaxis.axis_label = base["units"]

    buttons = []
    for label, icon, target in (("Model run period", "calendar", run), ("Full record", "arrows-maximize", full)):
        if target is None:
            continue
        btn = pn.widgets.Button(name=label, icon=icon, button_type="light", margin=(0, 6, 0, 0), height=28)
        lim = [_ms(target["x"][0]), _ms(target["x"][1]), target["y"][0], target["y"][1]]
        btn.js_on_click(args=dict(x=fig.x_range, y=fig.y_range, lim=lim), code=_RANGE_JS)
        buttons.append(btn)
    return pn.Column(
        pn.pane.HTML(_caption(name, base["units"], b_row, s_row), margin=0, width=_WIDTH),
        pn.pane.Bokeh(fig, margin=0), pn.Row(*buttons, margin=0), margin=(0, 0, 8, 0), width=_WIDTH,
    )
