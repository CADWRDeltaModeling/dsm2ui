# -*- coding: utf-8 -*-
"""Reservoir pop-up for the DSM2 input viewer: properties and the elevation-area curve of a reservoir."""
import html
import io

import pandas as pd
import panel as pn
from bokeh.models import (
    BoxZoomTool, ColumnDataSource, HoverTool, NumeralTickFormatter, PanTool, Range1d, ResetTool, Span, WheelZoomTool,
)
from bokeh.plotting import figure

from dsm2ui.xsection_popup import HEADER_STYLES, POPUP_STYLES, POPUP_WIDTH

_RES_BLUE = "#1f78b4"
_BOT_COLOR = "#b3261e"
_IC_COLOR = "#1e8e3e"
_SQFT_PER_ACRE = 43560.0


def _title(text):
    return f"<div style='font-size:11px;font-weight:700;letter-spacing:.4px;color:#222;margin:12px 0 4px'>{text}</div>"


def _cell(label, value):
    return (
        "<div style='display:flex;justify-content:space-between;padding:3px 0;font-size:11.5px'>"
        f"<span>{label}</span><span style='margin-right:8px'>{value}</span></div>"
    )


def _num(v, digits=3):
    return "n/a" if v is None else f"{v:,.{digits}f}"


def _curve_frame(curve):
    """DataFrame of the elevation-area curve with the storage volume DSM2 derives from it (trapezoidal rule)."""
    df = pd.DataFrame(sorted(curve), columns=["elev", "area"])
    if df.empty:
        df["vol"] = []
        return df
    dz = df["elev"].diff().fillna(0.0)
    mean_area = (df["area"] + df["area"].shift().fillna(df["area"])) / 2.0
    df["vol"] = (mean_area * dz).cumsum()  # acre-ft
    return df


class ReservoirPopup:
    """Pop-up window for one reservoir; call :meth:`show` to open it."""

    def __init__(self, on_close):
        self._on_close = on_close
        self._df = pd.DataFrame(columns=["elev", "area", "vol"])
        self._name = ""

        self._title = pn.pane.HTML("", margin=0, width=556)
        close = pn.widgets.Button(icon="x", button_type="light", width=36, height=36, margin=0)
        close.on_click(lambda _: self.hide())
        self._subtitle = pn.pane.HTML("", margin=0)
        header = pn.Column(
            pn.Row(self._title, close, margin=0, width=600), self._subtitle,
            margin=0, width=POPUP_WIDTH - 32, styles=HEADER_STYLES,
        )
        self._props = pn.pane.HTML("", margin=0, width=600)

        self._src = ColumnDataSource({"elev": [], "area": [], "vol": []})
        pan = PanTool()
        self._fig = figure(
            x_range=Range1d(0, 1), y_range=Range1d(0, 1), width=596, height=300, toolbar_location="above",
            min_border=4, outline_line_color=None, active_drag=pan, active_scroll=None,
            tools=[pan, BoxZoomTool(), WheelZoomTool(), ResetTool()],
        )
        line = self._fig.line("area", "elev", source=self._src, line_width=2, color=_RES_BLUE)
        dots = self._fig.scatter("area", "elev", source=self._src, size=6, color=_RES_BLUE)
        self._bot = Span(location=0, dimension="width", line_color=_BOT_COLOR, line_dash="dashed", line_width=1.5)
        self._ic = Span(location=0, dimension="width", line_color=_IC_COLOR, line_dash="dashed", line_width=1.5)
        self._fig.add_layout(self._bot)
        self._fig.add_layout(self._ic)
        self._fig.add_tools(HoverTool(
            renderers=[line, dots], mode="mouse",
            tooltips=[("Elevation", "@elev{0.000} ft"), ("Area", "@area{0,0.0} acres"), ("Volume", "@vol{0,0} acre-ft")],
        ))
        self._fig.toolbar.logo = None
        self._fig.xaxis.axis_label = "AREA (acres)"
        self._fig.yaxis.axis_label = "ELEVATION (ft)"
        self._fig.xaxis.formatter = NumeralTickFormatter(format="0,0")
        self._fig.axis.axis_label_text_font_size = "10px"
        self._fig.axis.major_label_text_font_size = "10px"
        self._fig.grid.grid_line_color = "#eeeeee"
        self._legend = pn.pane.HTML("", margin=(0, 0, 4, 0), width=600)
        self._note = pn.pane.HTML("", margin=0, width=600, visible=False)

        self._table = pn.pane.HTML("", margin=0, width=600)
        self._table_box = pn.Column(self._table, margin=0, visible=False)
        self._toggle = pn.widgets.Button(name="Show Table", icon="table", button_type="light", margin=(0, 6, 0, 0))
        self._toggle.on_click(self._toggle_table)
        self._export = pn.widgets.FileDownload(
            callback=self._csv, filename="reservoir.csv", label="Export CSV", icon="download",
            button_type="light", margin=0, auto=True,
        )
        self.panel = pn.Column(
            header, self._props, pn.pane.HTML(_title("ELEVATION vs AREA"), margin=0, width=600), self._note,
            self._legend, pn.pane.Bokeh(self._fig, margin=0),
            pn.Row(self._toggle, self._export, margin=(6, 0, 4, 0), width=600), self._table_box,
            width=POPUP_WIDTH, visible=False, css_classes=["xs-popup"], styles=POPUP_STYLES,
        )

    def show(self, props):
        """Open for ``props`` (see ``input_viewer._load_reservoir_props``)."""
        self._name = props["name"]
        self._df = _curve_frame(props["curve"])
        self._title.object = (
            "<div class='xs-drag' title='Drag to move' style='cursor:move;user-select:none;padding:6px 0;"
            f"font-size:15px;font-weight:700;color:{_RES_BLUE}'>RESERVOIR \u2013 {html.escape(self._name)}</div>"
        )
        nodes = sorted({c[0] for c in props["connections"]} | {g[1] for g in props["gates"]})
        self._subtitle.object = (
            "<div style='font-size:12px;color:#333'>Connected nodes: <b style='font-weight:500;color:#1a73e8'>"
            f"{', '.join(map(str, nodes)) or 'none'}</b></div>"
        )
        area_acres = props["area"] * 1e6 / _SQFT_PER_ACRE if props["area"] is not None else None
        df = self._df
        left = (
            _cell("Surface area (AREA):", f"{_num(props['area'], 3)} million sq ft ({_num(area_acres, 0)} acres)")
            + _cell("Bottom elevation (BOT_ELEV):", f"{_num(props['bot_elev'])} ft")
            + _cell("Initial stage:", f"{_num(props['ic_stage'])} ft")
        )
        right = (
            _cell("Table points:", f"{len(df)}")
            + _cell("Table elevation range:", f"{_num(df['elev'].min())} to {_num(df['elev'].max())} ft" if len(df) else "n/a")
            + _cell("Table area range:", f"{_num(df['area'].min(), 1)} to {_num(df['area'].max(), 1)} acres" if len(df) else "n/a")
            + _cell("Volume at top of table:", f"{_num(df['vol'].iloc[-1], 0)} acre-ft" if len(df) else "n/a")
        )
        self._props.object = (
            "<div style='display:grid;grid-template-columns:1fr 1fr;column-gap:16px;border-top:1px solid #eee;"
            f"border-bottom:1px solid #eee;padding:4px 0;margin-top:8px'><div>{left}</div><div>{right}</div></div>"
            + self._connections_html(props)
        )
        self._legend.object = (
            "<div style='font-size:11px;color:#444;display:flex;gap:16px;flex-wrap:wrap'>"
            f"<span><span style='color:{_BOT_COLOR}'>- - -</span> Bottom elevation</span>"
            f"<span><span style='color:{_IC_COLOR}'>- - -</span> Initial stage</span></div>"
        )
        self._set_chart(props)
        self._table.object = self._table_html()
        self._export.filename = f"reservoir_{self._name}.csv"
        self._table_box.visible = False
        self._toggle.name = "Show Table"
        self.panel.visible = True

    def hide(self):
        self.panel.visible = False
        self._on_close()

    # --- internals ---------------------------------------------------------------
    @staticmethod
    def _connections_html(props):
        rows = [(n, "connection", f"{cin:g}", f"{cout:g}") for n, cin, cout in props["connections"]]
        rows += [(n, f"gate {html.escape(g)}", "", "") for g, n in props["gates"]]
        if not rows:
            return ""
        body = "".join(
            "<tr style='border-top:1px solid #f0f0f0'>"
            f"<td style='padding:4px 6px'><b style='color:#1a73e8;font-weight:500'>{n}</b></td>"
            f"<td style='padding:4px 6px'>{kind}</td>"
            f"<td style='padding:4px 6px;text-align:right'>{cin}</td><td style='padding:4px 6px;text-align:right'>{cout}</td></tr>"
            for n, kind, cin, cout in sorted(rows)
        )
        return (
            _title("CONNECTIONS")
            + "<table style='width:100%;border-collapse:collapse;font-size:11.5px'>"
            "<tr style='background:#f5f7fb'><th style='text-align:left;padding:5px 6px'>Node</th>"
            "<th style='text-align:left;padding:5px 6px'>Through</th>"
            "<th style='text-align:right;padding:5px 6px'>Coeff. in</th>"
            f"<th style='text-align:right;padding:5px 6px'>Coeff. out</th></tr>{body}</table>"
        )

    def _set_chart(self, props):
        df = self._df
        self._src.data = {"elev": df["elev"].tolist(), "area": df["area"].tolist(), "vol": df["vol"].tolist()}
        bot, ic = props["bot_elev"], props["ic_stage"]
        self._bot.visible, self._ic.visible = bot is not None, ic is not None
        self._bot.location = bot if bot is not None else 0
        self._ic.location = ic if ic is not None else 0
        self._note.visible = df.empty
        self._note.object = (
            "<div style='font-size:12px;color:#777'>No elevation-area table (RESERVOIR_VOL) is defined.</div>"
        )
        elevs = list(df["elev"]) + [v for v in (bot, ic) if v is not None]
        if not elevs:
            return
        lo, hi = min(elevs), max(elevs)
        ypad = (hi - lo) * 0.06 or 1.0
        amax = float(df["area"].max()) if len(df) else 1.0
        xr, yr = self._fig.x_range, self._fig.y_range
        xr.update(start=0, end=amax * 1.05 or 1.0, reset_start=0, reset_end=amax * 1.05 or 1.0)
        yr.update(start=lo - ypad, end=hi + ypad, reset_start=lo - ypad, reset_end=hi + ypad)

    def _table_html(self):
        df = self._df
        head = "".join(
            f"<th style='text-align:right;padding:6px 8px;position:sticky;top:0;background:#f5f7fb'>{h}</th>"
            for h in ("ELEV (ft)", "AREA (acres)", "VOLUME (acre-ft)")
        )
        body = "".join(
            "<tr style='border-top:1px solid #eee'>"
            f"<td style='padding:5px 8px;text-align:right'>{_num(r.elev)}</td>"
            f"<td style='padding:5px 8px;text-align:right'>{_num(r.area)}</td>"
            f"<td style='padding:5px 8px;text-align:right'>{_num(r.vol, 1)}</td></tr>"
            for r in df.itertuples()
        )
        return (
            "<div style='max-height:260px;overflow-y:auto;border:1px solid #e5e7eb;border-radius:8px'>"
            f"<table style='width:100%;border-collapse:collapse;font-size:11.5px'><tr>{head}</tr>{body}</table></div>"
            "<div style='font-size:10.5px;color:#777;padding-top:4px'>Volume is derived from the area curve "
            "(trapezoidal rule from the lowest elevation), as DSM2 does.</div>"
        )

    def _toggle_table(self, _):
        self._table_box.visible = not self._table_box.visible
        self._toggle.name = "Hide Table" if self._table_box.visible else "Show Table"

    def _csv(self):
        buf = io.StringIO()
        out = self._df.rename(columns={"elev": "ELEV_ft", "area": "AREA_acres", "vol": "VOLUME_acre_ft"})
        out.to_csv(buf, index=False)
        buf.seek(0)
        return buf
