# -*- coding: utf-8 -*-
"""Boundary-condition pop-up for the DSM2 input viewer (time series read from HEC-DSS)."""
import html
import io

import pandas as pd
import panel as pn
from bokeh.models import (
    BoxZoomTool, ColumnDataSource, HoverTool, NumeralTickFormatter, PanTool, Range1d, WheelZoomTool,
)
from bokeh.plotting import figure

from dsm2ui.xsection_popup import HEADER_STYLES, POPUP_STYLES, POPUP_WIDTH

_PURPLE = "#7b2cbf"
_MODES = ["Original", "Daily mean", "Tidally filtered", "Monthly mean"]
_BAR_WIDTH_MS = 25 * 24 * 3600 * 1000
_HALF_MONTH = pd.Timedelta(days=15)


def _cell(label, value):
    return (
        "<div style='display:flex;justify-content:space-between;padding:3px 0;font-size:11.5px'>"
        f"<span>{label}</span><span style='margin-right:8px'>{value}</span></div>"
    )


def _num(v):
    return "n/a" if pd.isna(v) else f"{v:,.3f}"


def _as_timestamp(v):
    """Range1d start/end come back as datetimes or epoch milliseconds."""
    if v is None:
        return pd.NaT
    return pd.to_datetime(v, unit="ms") if isinstance(v, (int, float)) else pd.Timestamp(v)


class BCPopup:
    """Pop-up showing the DSS time series behind a boundary node; call :meth:`show` to open it."""

    def __init__(self, on_close, load_series, title="BOUNDARY CONDITION", input_label="Boundary input", rules_for=None):
        """``load_series(row)`` must return ``(DataFrame with one column, units, model run window)`` for a row;
        ``rules_for(row)`` may return Panel objects describing the operating rules that act on the row (or ``[]``)."""
        self._heading = title
        self._on_close = on_close
        self._load = load_series
        self._rules_for = rules_for
        self._rows = []
        self._series = None  # full original record
        self._shown = None  # what the chart currently displays
        self._window = None
        self._row = {}
        self._units = ""
        self._loading = False

        self._title = pn.pane.HTML("", margin=0, width=556)
        close = pn.widgets.Button(icon="x", button_type="light", width=36, height=36, margin=0)
        close.on_click(lambda _: self.hide())
        self._subtitle = pn.pane.HTML("", margin=0)
        header = pn.Column(
            pn.Row(self._title, close, margin=0, width=600), self._subtitle,
            margin=0, width=POPUP_WIDTH - 32, styles=HEADER_STYLES,
        )

        self._select = pn.widgets.Select(name=input_label, options={}, visible=False, margin=(8, 0, 0, 0), width=600)
        self._select.param.watch(self._on_pick, "value")
        self._message = pn.pane.HTML("", margin=(8, 0), visible=False)
        self._info = pn.pane.HTML("", margin=(8, 0, 0, 0), width=600)

        self._line_src = ColumnDataSource({"t": [], "v": []})
        self._bar_src = ColumnDataSource({"t": [], "v": []})
        pan = PanTool(dimensions="width")
        # Explicit ranges (not auto-fit) so the buttons below can set the visible window from Python.
        self._fig = figure(
            x_axis_type="datetime", x_range=Range1d(), y_range=Range1d(),
            width=596, height=260, toolbar_location="above", min_border=4,
            tools=[pan, BoxZoomTool(dimensions="width"), WheelZoomTool(dimensions="width")],
            active_drag=pan, active_scroll=None, outline_line_color=None,
            output_backend="webgl",  # the original 15-minute record can be over a million points
        )
        self._line_r = self._fig.line("t", "v", source=self._line_src, line_width=1.5, color=_PURPLE)
        self._bar_r = self._fig.vbar(
            x="t", top="v", bottom=0, width=_BAR_WIDTH_MS, source=self._bar_src,
            fill_color=_PURPLE, fill_alpha=0.75, line_color=None, visible=False,
        )
        self._fig.add_tools(
            HoverTool(
                renderers=[self._line_r], mode="vline", formatters={"@t": "datetime"},
                tooltips=[("Time", "@t{%F %H:%M}"), ("Value", "@v{0,0.000}")],
            ),
            HoverTool(
                renderers=[self._bar_r], formatters={"@t": "datetime"},
                tooltips=[("Month", "@t{%b %Y}"), ("Mean", "@v{0,0.000}")],
            ),
        )
        self._fig.toolbar.logo = None
        self._fig.yaxis.formatter = NumeralTickFormatter(format="0,0.[000]")
        self._fig.axis.axis_label_text_font_size = "10px"
        self._fig.axis.major_label_text_font_size = "10px"
        self._fig.grid.grid_line_color = "#eeeeee"
        self._plot_note = pn.pane.HTML("", margin=(0, 0, 4, 0), width=600)

        self._mode = pn.widgets.RadioButtonGroup(
            options=_MODES, value=_MODES[0], button_type="light", margin=(0, 0, 6, 0),
        )
        self._mode.param.watch(self._on_mode, "value")
        self._btn_full = pn.widgets.Button(name="Full record", icon="arrows-maximize", button_type="light", margin=0)
        self._btn_run = pn.widgets.Button(name="Model run period", icon="calendar", button_type="light", margin=(0, 0, 0, 6))
        self._btn_full.on_click(lambda _: self._view_full())
        self._btn_run.on_click(lambda _: self._view_run())

        self._export = pn.widgets.FileDownload(
            callback=self._csv, filename="bc_data.csv", label="Export CSV",
            icon="download", button_type="light", margin=0, auto=True,
        )
        self._rules_title = pn.pane.HTML(
            "<div style='font-size:11px;font-weight:700;letter-spacing:.4px;color:#222;margin:10px 0 2px'>"
            "OPERATING RULES</div>", margin=0, visible=False,
        )
        self._rules = pn.Column(margin=0, width=600)
        self._body = pn.Column(
            self._select, self._message, self._info, self._rules_title, self._rules,
            pn.Row(
                pn.pane.HTML(
                    "<div style='font-size:11px;font-weight:700;letter-spacing:.4px;color:#222'>TIME SERIES</div>",
                    margin=(8, 0, 0, 0),
                ),
                pn.HSpacer(), self._btn_full, self._btn_run, margin=(10, 0, 4, 0), width=600,
            ),
            self._mode,
            self._plot_note, pn.pane.Bokeh(self._fig, margin=0),
            pn.Row(pn.HSpacer(), self._export, margin=(8, 0, 0, 0), width=600),
            margin=0,
        )
        self.panel = pn.Column(
            header, self._body, width=POPUP_WIDTH, visible=False, css_classes=["xs-popup"], styles=POPUP_STYLES,
        )

    # --- public API --------------------------------------------------------------
    def show(self, node, rows, place=None):
        """Open for boundary ``node`` (or the free-text ``place``); ``rows`` are dicts with name/kind/sign/fillin/file/path."""
        self._rows = rows
        self._title.object = (
            "<div class='xs-drag' title='Drag to move' style='cursor:move;user-select:none;padding:6px 0;"
            f"font-size:15px;font-weight:700;color:#222'>{self._heading} \u2013 {place or f'NODE {node}'}</div>"
        )
        self._loading = True
        self._select.options = {r.get("label") or f"{r['name']} ({r['kind']})": i for i, r in enumerate(rows)}
        self._select.value = 0
        self._select.visible = len(rows) > 1
        self._loading = False
        self.panel.visible = True
        self._show_row(0)

    def hide(self):
        self.panel.visible = False
        self._on_close()

    # --- internals ---------------------------------------------------------------
    def _on_pick(self, event):
        if not self._loading:
            self._show_row(event.new)

    def _on_mode(self, _):
        if self._series is not None:
            self._apply_mode(keep_view=True)

    def _show_row(self, i):
        row = self._rows[i]
        parts = [
            f"<span>Name: <b style='font-weight:500'>{html.escape(row['name'])}</b></span>",
            f"<span>Type: <b style='font-weight:500'>{html.escape(row['kind'].title())}</b></span>",
        ]
        if row.get("node") is not None:
            parts.append(f"<span>Node: <b style='color:#1a73e8;font-weight:500'>{row['node']}</b></span>")
        if row.get("sign") is not None:
            parts.append(f"<span>Sign: <b style='font-weight:500'>{row['sign']:+d}</b></span>")
        parts.append(f"<span>Fill-in: <b style='font-weight:500'>{html.escape(str(row['fillin']))}</b></span>")
        for label, value in row.get("extra", []):
            parts.append(f"<span>{html.escape(label)}: <b style='font-weight:500'>{html.escape(value)}</b></span>")
        sep = "<span style='color:#bbb'>|</span>"
        self._subtitle.object = (
            "<div style='font-size:12px;color:#333;display:flex;gap:10px;align-items:center;flex-wrap:wrap'>"
            + sep.join(parts) + "</div>"
        )
        self._row = row
        objs = self._rules_for(row) if self._rules_for else []
        self._rules[:] = objs
        self._rules_title.visible = bool(objs)
        self._export.filename = f"bc_{row['node'] if row.get('node') is not None else 'reservoir'}_{row['name']}.csv"
        try:
            df, units, window = self._load(row)
        except Exception as exc:  # file or path problems are reported in the window
            self._series = self._shown = None
            for src in (self._line_src, self._bar_src):
                src.data = {"t": [], "v": []}
            self._info.object = ""
            self._plot_note.object = ""
            self._message.object = (
                f"<div style='color:#b3261e;font-size:12px'>Could not read the time series: "
                f"{html.escape(str(exc))}</div>"
            )
            self._message.visible = True
            return
        self._message.visible = False
        s = df.iloc[:, 0].astype(float)
        self._units = units
        self._series = s
        self._window = window
        self._btn_run.disabled = window is None or s.loc[window[0]:window[1]].dropna().empty
        self._apply_mode(keep_view=False)

        run_txt = f"{window[0]:%d%b%Y} \u2013 {window[1]:%d%b%Y}" if window else "n/a"
        e_part = row["path"].split("/")[5] if row["path"].count("/") >= 6 else ""
        n_missing = int(s.isna().sum())
        self._info.object = (
            "<div style='display:grid;grid-template-columns:1fr 1fr;column-gap:16px;"
            "border-top:1px solid #eee;border-bottom:1px solid #eee;padding:4px 0'>"
            f"<div>{_cell('Units:', html.escape(units))}{_cell('Interval:', html.escape(e_part))}"
            f"{_cell('Period:', f'{s.index.min():%d%b%Y} \u2013 {s.index.max():%d%b%Y}')}"
            f"{_cell('Model run:', run_txt)}</div>"
            f"<div>{_cell('Min:', _num(s.min()))}{_cell('Mean:', _num(s.mean()))}"
            f"{_cell('Max:', _num(s.max()))}"
            f"{_cell('Missing:', f'{n_missing:,} ({n_missing / max(len(s), 1):.1%})')}</div></div>"
            "<div style='font-size:11.5px;padding:6px 0 0;word-break:break-all;color:#333'>"
            f"<div><b>DSS file:</b> {html.escape(row['file'])}</div>"
            f"<div><b>DSS path:</b> {html.escape(row['path'])}</div></div>"
        )

    def _transform(self, mode):
        """Return (series to display, note) for a display mode."""
        s = self._series
        if mode == "Daily mean":
            return s.resample("1D").mean(), "Daily means"
        if mode == "Monthly mean":
            return s.resample("MS").mean(), "Monthly means"
        if mode == "Tidally filtered":
            from vtools.functions.filter import cosine_lanczos

            try:
                return cosine_lanczos(s.to_frame(), "40h").iloc[:, 0], "Tidally filtered (cosine-Lanczos, 40h cutoff)"
            except Exception:
                return s, "Tidal filtering needs regular sub-daily data; showing the original series."
        return s, ""

    def _apply_mode(self, keep_view):
        mode = self._mode.value
        x = self._fig.x_range
        span = (_as_timestamp(x.start), _as_timestamp(x.end)) if keep_view else (pd.NaT, pd.NaT)
        shown, note = self._transform(mode)
        self._shown = shown
        bars = mode == "Monthly mean"
        if bars:
            # Center each bar on its month rather than on the 1st.
            self._bar_src.data = {"t": (shown.index + _HALF_MONTH).values, "v": shown.values}
        else:
            self._line_src.data = {"t": shown.index.values, "v": shown.values}
        self._line_r.visible = not bars
        self._bar_r.visible = bars
        self._fig.yaxis.axis_label = f"{self._row['kind'].upper()} ({self._units})"
        self._plot_note.object = f"<div style='font-size:11px;color:#666'>{html.escape(note)}</div>" if note else ""
        if pd.isna(span[0]) or pd.isna(span[1]):
            self._view_full()
        else:
            self._set_view(*span)

    def _set_view(self, start, end):
        sub = self._shown.loc[start:end].dropna()
        if sub.empty:
            sub = self._shown.dropna()
        lo, hi = float(sub.min()), float(sub.max())
        if self._mode.value == "Monthly mean":
            lo, hi = min(lo, 0.0), max(hi, 0.0)  # bars start at zero
        pad = (hi - lo) * 0.05 or 1.0
        x, y = self._fig.x_range, self._fig.y_range
        x.update(start=start.to_pydatetime(), end=end.to_pydatetime())
        y.update(start=lo - pad, end=hi + pad)

    def _view_full(self):
        if self._shown is not None and len(self._shown):
            self._set_view(self._series.index.min(), self._series.index.max())

    def _view_run(self):
        if self._shown is not None and self._window is not None:
            self._set_view(*self._window)

    def _csv(self):
        buf = io.StringIO()
        if self._shown is not None:
            label = f"{self._row['name']} ({self._units}) - {self._mode.value}"
            self._shown.rename(label).to_csv(buf, index_label="datetime")
        buf.seek(0)
        return buf
