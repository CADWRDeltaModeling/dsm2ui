# -*- coding: utf-8 -*-
"""Rendering of operating-rule summaries (see ``oprule_parse``) as Panel objects, shared by the viewer's windows."""
import html
import re

import numpy as np
import pandas as pd
import panel as pn
from bokeh.models import (
    BoxZoomTool, ColumnDataSource, HoverTool, NumeralTickFormatter, PanTool, Range1d, ResetTool, WheelZoomTool,
)
from bokeh.plotting import figure

from dsm2ui.oprule_parse import DIRECTION_TEXT, value_text

_GATE_BLUE = "#2f5fb3"
_BADGES = {
    "whole": ("Trigger: True", "#5f6368"), "condition": ("Trigger: Condition", "#c26a00"),
    "schedule": ("Trigger: Schedule", "#1e8e3e"), "raw": ("Not summarised", "#b3261e"),
}
_BADGE_W = 112  # px
_CONTENT_W = 578  # px inside a card
_CHART_W = _CONTENT_W - _BADGE_W - 8
_CARD_STYLES = {
    "border": "1px solid #e5e7eb", "border-radius": "10px", "padding": "6px 10px", "box-sizing": "border-box",
}
_SUMMARY = "cursor:pointer;color:#777;font-size:11px"
_RANGE_JS = "x.start = lim[0]; x.end = lim[1]; y.start = lim[2]; y.end = lim[3];"


def _rich(text):
    """Escape plain text and emphasise the series names wrapped by ``oprule_parse.mark``."""
    return re.sub("\u2039(.*?)\u203a", r"<b style='color:#2f5fb3'>\1</b>", html.escape(text))


def _badge(kind):
    label, color = _BADGES[kind]
    return (
        f"<span style='flex:none;width:{_BADGE_W}px;text-align:center;font-size:10.5px;font-weight:600;color:white;"
        f"background:{color};border-radius:10px;padding:2px 0;margin-top:1px'>{label}</span>"
    )


def _dirs(entry):
    return " + ".join(DIRECTION_TEXT.get(d, d) for d in entry["directions"])


def _pre(text):
    return (
        "<pre style='white-space:pre-wrap;word-break:break-all;font-size:10.5px;background:#f7f8fa;"
        f"padding:6px;border-radius:6px;margin:4px 0 0'>{html.escape(text)}</pre>"
    )


def _entry_series(entries):
    """Series names an entry (or a schedule's rows) reads, in order."""
    names = []
    for e in entries:
        names += e["when"]["series"] + (e["value"]["series"] if e["value"] else [])
    return list(dict.fromkeys(names))


def _raw_block(entries, series_rows):
    """Collapsible exact rule text, with the definitions of the series it uses."""
    lines = [f"{', '.join(e['rules'])}\n  ACTION:  {e['action']}\n  TRIGGER: {e['trigger']}" for e in entries]
    for name in _entry_series(entries):
        row = series_rows.get(name.lower())
        if row:
            const = row["file"].strip().lower() == "constant"
            lines.append(f"series {name}: " + (f"constant {row['path']}" if const else f"{row['file']}  {row['path']}"))
    return (
        f"<details style='margin-top:3px'><summary style='{_SUMMARY}'>Rule text"
        f"</summary>{_pre(chr(10).join(lines))}</details>"
    )


def _when_text(when):
    op = when["op"]
    word = "Before" if op in ("<", "<=") else "From" if op in (">", ">=") else "At"
    return f"{word} {when['label']}"


def _row(badge, body):
    return (
        "<div style='display:flex;gap:8px;align-items:flex-start;padding:6px 0;border-top:1px solid #f0f0f0'>"
        f"{badge}<div style='flex:1;min-width:0'>{body}</div></div>"
    )


def _item_html(item, series_rows):
    """HTML of one rule block (badge, sentence, rule text); the time series charts are added below it."""
    if item.get("kind") == "schedule":
        rows = item["rows"]
        show_dir = len({tuple(r["directions"]) for r in rows}) > 1
        trs = "".join(
            "<tr style='border-top:1px solid #f0f0f0'>"
            f"<td style='padding:3px 8px 3px 0;white-space:nowrap'>{html.escape(_when_text(r['when']))}</td>"
            f"<td style='padding:3px 0'>{'<i>' + html.escape(_dirs(r)) + '</i>: ' if show_dir and r['directions'] else ''}"
            f"{_rich(value_text(r['var'], r['value'], r['ramp']))}</td></tr>"
            for r in rows
        )
        body = f"<table style='border-collapse:collapse;font-size:12px'>{trs}</table>"
        return _row(_badge("schedule"), body + _raw_block(rows, series_rows))
    if not item["summarised"]:
        text = f"<b>{html.escape(', '.join(item['rules']))}</b>: this rule could not be summarised."
        raw = "ACTION:  " + item["action"] + "\nTRIGGER: " + item["trigger"]
        return _row(_badge("raw"), f"<div style='font-size:12px'>{text}</div>{_pre(raw)}")
    prefix = f"<i>{html.escape(_dirs(item))}</i>: " if item["directions"] else ""
    vt = value_text(item["var"], item["value"], item["ramp"])
    when, extra = item["when"], ""
    if when["kind"] == "startup":
        badge = _badge("whole")
        sentence = (
            f"Follows {_rich(vt)} for the whole run." if item["value"]["kind"] == "series"
            else f"Set to {_rich(vt)} from the start of the run."
        )
    else:
        badge = _badge("condition")
        sentence = f"When {_rich(when['text'])}: set to {_rich(vt)}."
        if when["kind"] == "condition" and when["defs"]:
            extra = "<div style='font-size:11px;color:#555;margin-top:3px'>" + "".join(
                f"<div><b>{html.escape(d['name'])}</b> = {_rich(d['text'])}</div>" for d in when["defs"]
            ) + "</div>"
    return _row(badge, f"<div style='font-size:12px'>{prefix}{sentence}</div>{extra}{_raw_block([item], series_rows)}")


def _html_pane(text, width=_CONTENT_W):
    return pn.pane.HTML(text, margin=0, width=width)


def _rules_layout(view, series_rows, chart_for):
    """Panel objects for the Operating rules tab: one card per device, each rule followed by its series charts."""
    if not view["devices"]:
        return [_html_pane(
            "<div style='font-size:12px;color:#555;padding:10px 0'>No operating rules act on this structure. "
            "Its devices keep their default operation (see the Devices tab).</div>", 600,
        )]
    n = view["n_rules"]
    out = [_html_pane(
        f"<div style='font-size:12px;color:#555;padding:6px 0'><b>{n}</b> operating rule{'' if n == 1 else 's'} "
        f"for this {view.get('scope', 'structure')}.</div>", 600,
    )]
    for dev in view["devices"]:
        heading = dev.get("heading", f"DEVICE: {html.escape(dev['device'])}" if dev["device"] else "WHOLE STRUCTURE")
        card = [] if heading is None else [
            _html_pane(f"<div style='font-size:12px;font-weight:700;color:#2f5fb3;padding:2px 0'>{heading}</div>")
        ]
        for ctl in dev["controls"]:
            card.append(_html_pane(
                f"<div style='font-size:11px;font-weight:700;color:#444;margin:8px 0 2px'>{html.escape(ctl['label'])}</div>"
            ))
            for item in ctl["items"]:
                entries = item["rows"] if item.get("kind") == "schedule" else [item]
                names = _entry_series(entries) if all(e["summarised"] for e in entries) else []
                charts = [c for c in (chart_for(nm) for nm in names) if c is not None]
                card.append(pn.Column(
                    _html_pane(_item_html(item, series_rows)), *charts, margin=0, width=_CONTENT_W,
                ))
        out.append(pn.Column(*card, margin=(8, 0), width=600, styles=_CARD_STYLES))
    return out


def _thin(t, v, limit):
    """Keep the first, lowest and highest point of each slice so that at most about ``limit`` points remain."""
    n = len(v)
    if n <= limit:
        return t, v
    edges = np.linspace(0, n, max(limit // 3, 1) + 1).astype(int)
    keep = []
    for a, b in zip(edges[:-1], edges[1:]):
        if b > a:
            seg = v[a:b]
            keep += [a, a + int(seg.argmin()), a + int(seg.argmax())]
    keep = np.unique(keep)
    return t[keep], v[keep]


def _ms(t):
    return int(pd.Timestamp(t).value // 10**6)


def _pad_range(lo, hi):
    pad = (hi - lo) * 0.08 or 0.5
    return lo - pad, hi + pad


def _series_data(row, df, units, window):
    """Chart data of an operating-rule series: thinned step points, default and full view ranges, source text."""
    constant = row["file"].strip().lower() == "constant"
    s = df.iloc[:, 0].astype(float)
    s = s.mask(s.isin([-901.0, -902.0])).dropna().sort_index()  # -901/-902 are HEC-DSS missing-value flags
    units = "" if constant or units.strip().upper() in ("UNSPECIF", "UND") else units
    if s.empty:
        return {"empty": True}
    s = s[s.ne(s.shift())]  # a step chart only changes where the value does
    t0 = t1 = None
    if window is not None:
        t0, t1 = pd.Timestamp(window[0]), pd.Timestamp(window[1])
        before, inside, after = s[s.index < t0], s[(s.index >= t0) & (s.index <= t1)], s[s.index > t1]
        hold = before.iloc[-1:]  # the value in force at the start of the run
        parts = []
        for seg, limit in ((before.iloc[:-1], 6000), (hold, 1), (inside, 30000), (after, 4000)):
            t, v = _thin(seg.index.values, seg.values, limit)
            parts.append((t, v))
        t_all = np.concatenate([p[0] for p in parts])
        v_all = np.concatenate([p[1] for p in parts])
        held = float((inside if len(inside) else hold).iloc[-1]) if (len(inside) or len(hold)) else None
        if held is not None:  # carry the value to the end of the run
            t_all = np.append(t_all, np.datetime64(t1))
            v_all = np.append(v_all, held)
        order = np.argsort(t_all, kind="stable")
        t_all, v_all = t_all[order], v_all[order]
        run_vals = pd.concat([hold, inside])
        run_ok = len(run_vals) > 0
    else:
        t_all, v_all = _thin(s.index.values, s.values, 30000)
        run_ok = False
    x_full = (s.index.min(), max(s.index.max(), t1) if t1 is not None else s.index.max())
    if x_full[1] <= x_full[0]:
        x_full = (x_full[0], x_full[0] + pd.Timedelta(days=1))
    full = {"x": x_full, "y": _pad_range(float(s.min()), float(s.max()))}
    run = {"x": (t0, t1), "y": _pad_range(float(run_vals.min()), float(run_vals.max()))} if run_ok else None
    if constant:
        source, stats = f"<div><b>Constant value:</b> {html.escape(row['path'])}</div>", ""
    else:
        source = (
            f"<div><b>DSS file:</b> {html.escape(row['file'])}</div>"
            f"<div><b>DSS path:</b> {html.escape(row['path'])}</div>"
        )
        stats = (
            f" | Min {s.min():,.3f} | Mean {s.mean():,.3f} | Max {s.max():,.3f} | Record "
            f"{s.index.min():%d%b%Y} \u2013 {s.index.max():%d%b%Y}"
        )
    return {
        "empty": False, "t": t_all, "v": v_all, "units": units, "run": run, "full": full,
        "info": (
            "<div style='font-size:11px;color:#444;word-break:break-all'>"
            f"<div style='font-size:11.5px'>Time series: <b style='color:{_GATE_BLUE}'>{html.escape(row['name'])}</b>"
            f"{' (' + html.escape(units) + ')' if units else ''}</div>{source}"
            f"<div style='color:#666'>Fill-in: {html.escape(row['fillin'])}{html.escape(stats)}</div></div>"
        ),
    }


def _chart_panel(data):
    """Zoomable step chart of a series, with buttons that jump to the model run period or the full record."""
    view = data["run"] or data["full"]
    pan = PanTool(dimensions="width")
    fig = figure(
        x_axis_type="datetime", x_range=Range1d(*[pd.Timestamp(t).to_pydatetime() for t in view["x"]]),
        y_range=Range1d(*view["y"]), width=_CHART_W - 4, height=170, toolbar_location="above", min_border=4,
        tools=[pan, BoxZoomTool(dimensions="width"), WheelZoomTool(dimensions="width"), ResetTool()],
        active_drag=pan, active_scroll=None, outline_line_color=None,
    )
    src = ColumnDataSource({"t": data["t"], "v": data["v"]})
    line = fig.step("t", "v", source=src, mode="after", line_width=1.8, color=_GATE_BLUE)
    fig.add_tools(HoverTool(
        renderers=[line], mode="vline", formatters={"@t": "datetime"},
        tooltips=[("Time", "@t{%F %H:%M}"), ("Value", "@v{0,0.000}")],
    ))
    fig.toolbar.logo = None
    fig.yaxis.formatter = NumeralTickFormatter(format="0,0.[000]")
    fig.axis.axis_label_text_font_size = "10px"
    fig.axis.major_label_text_font_size = "10px"
    fig.grid.grid_line_color = "#eeeeee"
    if data["units"]:
        fig.yaxis.axis_label = data["units"]

    buttons = []
    for label, icon, target in (("Model run period", "calendar", data["run"]), ("Full record", "arrows-maximize", data["full"])):
        if target is None:
            continue
        btn = pn.widgets.Button(name=label, icon=icon, button_type="light", margin=(0, 6, 0, 0), height=28)
        lim = [_ms(target["x"][0]), _ms(target["x"][1]), target["y"][0], target["y"][1]]
        btn.js_on_click(args=dict(x=fig.x_range, y=fig.y_range, lim=lim), code=_RANGE_JS)
        buttons.append(btn)
    return pn.Column(
        pn.pane.HTML(data["info"], margin=0, width=_CHART_W), pn.pane.Bokeh(fig, margin=0), pn.Row(*buttons, margin=0),
        margin=(0, 0, 6, _BADGE_W + 8), width=_CHART_W,
    )


class RuleSection:
    """Builds the rule blocks and series charts of an operating-rule summary; series are read once and cached."""

    def __init__(self, series_rows, load_series):
        """``series_rows`` maps lower-case series name to its OPRULE_TIME_SERIES row; ``load_series(row)`` returns
        ``(DataFrame with one column, units, model run period or None)``."""
        self._series_rows = series_rows
        self._load_series = load_series
        self._cache = {}

    def layout(self, view, skip=None):
        """Panel objects for a summary from ``oprule_parse``; a series whose DSS ``(file, path)`` is ``skip`` gets no chart."""
        return _rules_layout(view, self._series_rows, lambda name: self._chart_for(name, skip))

    def _chart_for(self, name, skip):
        key = name.lower()
        row = self._series_rows.get(key)
        if row is None:
            return _html_pane(f"<div style='color:#b3261e;font-size:11px'>Series {html.escape(name)} is not defined.</div>")
        if skip and (row["file"], row["path"]) == tuple(skip):
            return None
        if key not in self._cache:
            try:
                self._cache[key] = _series_data(row, *self._load_series(row))
            except Exception as exc:  # file or path problems are reported in place of the chart
                self._cache[key] = {"error": str(exc)}
        data = self._cache[key]
        if "error" in data or data["empty"]:
            msg = data.get("error", "The series has no data.")
            return pn.Column(
                _html_pane(
                    f"<div style='color:#b3261e;font-size:11.5px'>Time series <b>{html.escape(name)}</b>: "
                    f"{html.escape(msg)}</div>", _CHART_W,
                ),
                margin=(0, 0, 6, _BADGE_W + 8), width=_CHART_W,
            )
        return _chart_panel(data)
