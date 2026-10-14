# -*- coding: utf-8 -*-
"""Cross-section pop-up window for the DSM2 input viewer (driven by the XSECT_LAYER table)."""
import io

import pandas as pd
import panel as pn
from bokeh.models import ColumnDataSource
from bokeh.plotting import figure

_BLUE = "#1a73e8"
_GREEN = "#2e9e2e"
_PURPLE = "#8e44ad"
_SOURCE = "XSECT_LAYER"
POPUP_WIDTH = 640  # px; the map is re-framed to the left of this
POPUP_STYLES = {
    "background": "white", "border-radius": "14px", "padding": "0 16px 12px",
    "box-shadow": "0 6px 28px rgba(0,0,0,0.28)", "overflow-y": "auto", "overflow-x": "hidden",
    "max-height": "calc(100vh - 130px)", "max-width": "calc(100vw - 100px)",
}
HEADER_STYLES = {
    "position": "sticky", "top": "0", "z-index": "5", "background": "white",
    "padding": "12px 0 6px", "border-bottom": "1px solid #e5e7eb",
}

_ITEM_CSS = """
.bk-btn { text-align: left; white-space: pre-line; height: auto; line-height: 1.5; font-size: 12px;
  padding: 4px 8px; border: 1px solid transparent; border-radius: 8px; background: transparent; color: #333; }
.bk-btn:hover { background: #f5f7fb; }
:host(.xs-sel) .bk-btn { background: #eaf2ff; border-color: #b6d1fb; }
"""

_TABLE_COLUMNS = [("ELEV", "ELEV (ft)"), ("AREA", "AREA (ft²)"), ("WIDTH", "WIDTH (ft)"), ("WET_PERIM", "WET PERIM (ft)")]


def _fmt(v, nd=3):
    return "0" if v == 0 else f"{v:,.{nd}f}"


def _dist(d):
    """DIST with trailing zeros trimmed but at least two decimals (0.610 -> 0.61)."""
    s = f"{d:.3f}"
    return s[:-1] if s.endswith("0") else s


def _section_title(text):
    return pn.pane.HTML(
        f"<div style='font-size:11px;font-weight:700;letter-spacing:.4px;color:#222'>{text}</div>",
        margin=(6, 0, 2, 0),
    )


def _chart(x_label, y_label, width, height):
    p = figure(
        width=width, height=height, toolbar_location=None, min_border=4,
        outline_line_color=None, background_fill_color="white",
    )
    p.xaxis.axis_label = x_label
    p.yaxis.axis_label = y_label
    p.axis.axis_label_text_font_size = "10px"
    p.axis.major_label_text_font_size = "10px"
    p.grid.grid_line_color = "#eeeeee"
    return p


class XSectionPopup:
    """Builds and updates the pop-up panel; call :meth:`show` to open it for a channel."""

    def __init__(self, on_select, on_zoom_location, on_zoom_all, on_close, max_items=12):
        self._on_select = on_select
        self._on_zoom_location = on_zoom_location
        self._on_zoom_all = on_zoom_all
        self._on_close = on_close
        self._chan = {}
        self._groups = []
        self._i = 0

        self._title = pn.pane.HTML("", margin=0, width=556)
        close = pn.widgets.Button(icon="x", button_type="light", width=36, height=36, margin=0)
        close.on_click(lambda _: self.hide())
        self._subtitle = pn.pane.HTML("", margin=0)

        self._list_title = pn.pane.HTML("", margin=(2, 0, 4, 0))
        # Items are created up front: Panel components built inside a Bokeh callback are not synced to the browser.
        self._pool = []
        for i in range(max_items):
            item = pn.widgets.Button(
                name="", button_type="light", stylesheets=[_ITEM_CSS], margin=(0, 0, 4, 0), width=130, visible=False
            )
            item.on_click(lambda _, i=i: self._select(i))
            self._pool.append(item)
        self._items = pn.Column(*self._pool, margin=0, width=130)
        zoom_all = pn.widgets.Button(
            name="All XS Locations", icon="map-pin", button_type="light", width=130, margin=(8, 0, 0, 0)
        )
        zoom_all.on_click(lambda _: self._on_zoom_all())
        left = pn.Column(
            self._list_title, self._items, zoom_all,
            width=150, margin=(0, 10, 0, 0),
            styles={"border": "1px solid #e5e7eb", "border-radius": "10px", "padding": "8px 9px"},
        )

        self._xs_title = pn.pane.HTML("", margin=0)
        zoom_loc = pn.widgets.Button(
            name="Zoom to Location", icon="map-pin", button_type="light", width=150, margin=0
        )
        zoom_loc.on_click(lambda _: self._on_zoom_location(self._i))
        self._info = pn.pane.HTML("", margin=0, width=418)
        self._props = pn.pane.HTML("", margin=0, width=418)

        self._src = ColumnDataSource({k: [] for k, _ in _TABLE_COLUMNS})
        p_ew = _chart("WIDTH (ft)", "ELEVATION (ft)", 196, 175)
        p_ew.line("WIDTH", "ELEV", source=self._src, line_width=2, color=_BLUE)
        p_ew.scatter("WIDTH", "ELEV", source=self._src, size=5, color=_BLUE)
        p_ae = _chart("ELEVATION (ft)", "AREA (ft²)", 196, 175)
        p_ae.line("ELEV", "AREA", source=self._src, line_width=2, color=_GREEN)
        p_ae.scatter("ELEV", "AREA", source=self._src, size=5, color=_GREEN)
        p_wp = _chart("ELEVATION (ft)", "WETTED PERIM. (ft)", 196, 175)
        p_wp.line("ELEV", "WET_PERIM", source=self._src, line_width=2, color=_PURPLE)
        p_wp.scatter("ELEV", "WET_PERIM", source=self._src, size=5, color=_PURPLE)

        self._table_title = _section_title("")
        self._table = pn.pane.HTML("", margin=0, width=598)
        self._table_box = pn.Column(self._table_title, self._table, margin=0, visible=False)

        right = pn.Column(
            pn.Row(self._xs_title, pn.HSpacer(), zoom_loc, margin=(0, 0, 6, 0), width=418),
            self._info,
            _section_title("CROSS SECTION PROPERTIES"),
            self._props,
            margin=0, width=440,
            styles={"border": "1px solid #e5e7eb", "border-radius": "10px", "padding": "8px 10px"},
        )
        charts = pn.Row(
            pn.Column(_section_title("ELEVATION vs WIDTH"), pn.pane.Bokeh(p_ew, margin=0), margin=0, width=200),
            pn.Column(_section_title("AREA vs ELEVATION"), pn.pane.Bokeh(p_ae, margin=0), margin=0, width=200),
            pn.Column(_section_title("WETTED PERIMETER vs ELEV."), pn.pane.Bokeh(p_wp, margin=0), margin=0, width=200),
            margin=(8, 0, 0, 0), width=600,
        )

        self._prev = pn.widgets.Button(name="Previous", icon="arrow-left", button_type="light", margin=0)
        self._next = pn.widgets.Button(name="Next", icon="arrow-right", button_type="light", margin=0)
        self._prev.on_click(lambda _: self._select(self._i - 1))
        self._next.on_click(lambda _: self._select(self._i + 1))
        self._export = pn.widgets.FileDownload(
            callback=self._csv, filename="cross_sections.csv", label="Export CSV",
            icon="download", button_type="light", margin=0, auto=True,
        )
        self._toggle = pn.widgets.Button(name="Show Table", icon="table", button_type="light", margin=0)
        self._toggle.on_click(self._toggle_table)

        self._body = pn.Column(
            pn.Row(left, right, margin=0),
            charts,
            self._table_box,
            pn.Row(self._prev, self._next, pn.HSpacer(), self._export, self._toggle, margin=(8, 0, 0, 0), width=600),
            margin=0,
        )
        self._message = pn.pane.HTML("", margin=(8, 0), visible=False)

        header = pn.Column(
            pn.Row(self._title, close, margin=0, width=600),
            self._subtitle,
            margin=0, width=POPUP_WIDTH - 32, styles=HEADER_STYLES,
        )
        self.panel = pn.Column(
            header, self._message, self._body,
            width=POPUP_WIDTH, visible=False, css_classes=["xs-popup"], styles=POPUP_STYLES,
        )

    # --- public API --------------------------------------------------------------
    def show(self, chan, xs):
        """Open for ``chan`` (dict with CHAN_NO/LENGTH/MANNING/UPNODE/DOWNNODE) and its XSECT_LAYER rows."""
        self._chan = chan
        self._groups = [(d, g.reset_index(drop=True)) for d, g in xs.groupby("DIST", sort=True)]
        n = int(chan["CHAN_NO"])
        self._title.object = (
            "<div class='xs-drag' title='Drag to move' style='cursor:move;user-select:none;padding:6px 0;"
            f"font-size:15px;font-weight:700;color:#222'>CROSS SECTIONS – CHANNEL {n}</div>"
        )
        self._subtitle.object = (
            "<div style='font-size:12px;color:#333;display:flex;gap:10px;align-items:center;flex-wrap:wrap'>"
            f"<span>Channel Length: {chan['LENGTH']:,.0f} ft</span><span style='color:#bbb'>|</span>"
            f"<span>Manning n: {chan['MANNING']:g}</span><span style='color:#bbb'>|</span>"
            f"<span>Upnode: <b style='color:{_BLUE};font-weight:500'>{int(chan['UPNODE'])}</b></span>"
            f"<span style='color:#bbb'>|</span>"
            f"<span>Downnode: <b style='color:{_BLUE};font-weight:500'>{int(chan['DOWNNODE'])}</b></span></div>"
        )
        self._export.filename = f"channel_{n}_cross_sections.csv"
        has_data = bool(self._groups)
        self._body.visible = has_data
        self._message.visible = not has_data
        self._message.object = (
            f"<i>No {_SOURCE} rows found for channel {n}.</i>" if not has_data else ""
        )
        self.panel.visible = True
        if has_data:
            self._rebuild_items()
            self._select(0)

    def hide(self):
        self.panel.visible = False
        self._on_close()

    # --- internals ---------------------------------------------------------------
    def _rebuild_items(self):
        self._list_title.object = (
            f"<div style='font-size:12px;font-weight:700;letter-spacing:.4px;color:#222;text-align:center'>"
            f"CROSS SECTIONS ({len(self._groups)})</div>"
        )
        for k, item in enumerate(self._pool):
            if k < len(self._groups):
                d, g = self._groups[k]
                item.param.update(
                    name=f"●  XS @ DIST = {_dist(d)}\n{len(g)} points", visible=True,
                )
            else:
                item.visible = False

    def _select(self, i):
        i = max(0, min(i, len(self._groups) - 1))
        self._i = i
        d, g = self._groups[i]
        length = float(self._chan["LENGTH"])
        for k, item in enumerate(self._pool):
            item.css_classes = ["xs-sel"] if k == i else []
        self._xs_title.object = (
            f"<div style='font-size:14px;font-weight:700;color:#222'>XS @ DIST = {_dist(d)}</div>"
        )

        def cell(label, value):
            return (
                f"<div style='display:flex;justify-content:space-between;padding:3px 0;font-size:11.5px'>"
                f"<span>{label}</span><span style='margin-right:8px'>{value}</span></div>"
            )

        self._info.object = (
            "<div style='display:grid;grid-template-columns:1fr 1fr;column-gap:16px;"
            "border-top:1px solid #eee;border-bottom:1px solid #eee;padding:4px 0'>"
            f"<div>{cell('Location:', f'{d * 100:.1f}% along channel')}"
            f"{cell('Distance to Upnode:', f'{d * length:,.0f} ft')}"
            f"{cell('Distance to Downnode:', f'{(1 - d) * length:,.0f} ft')}</div>"
            f"<div>{cell('Reach Length:', f'{length:,.0f} ft')}{cell('Source:', _SOURCE)}"
            f"{cell('# Points:', len(g))}</div></div>"
        )

        def rng(col, unit):
            return f"{_fmt(g[col].min())} to {_fmt(g[col].max())} {unit}"

        self._props.object = (
            cell("Elevation Range:", rng("ELEV", "ft")) + cell("Area Range:", rng("AREA", "ft²"))
            + cell("Wetted Perimeter Range:", rng("WET_PERIM", "ft")) + cell("Width Range:", rng("WIDTH", "ft"))
            + cell("Source File:", _SOURCE)
        )
        self._src.data = {k: g[k].tolist() for k, _ in _TABLE_COLUMNS}
        self._table_title.object = (
            f"<div style='font-size:12px;font-weight:700;letter-spacing:.4px;color:#222'>"
            f"CROSS SECTION DATA ({_SOURCE})</div>"
        )
        head = "".join(
            f"<th style='text-align:left;padding:5px 8px;font-weight:600'>{label}</th>" for _, label in _TABLE_COLUMNS
        )
        rows = "".join(
            "<tr>" + "".join(f"<td style='padding:4px 8px;border-top:1px solid #eee'>{r[k]:,.3f}</td>"
                             for k, _ in _TABLE_COLUMNS) + "</tr>"
            for _, r in g.iterrows()
        )
        self._table.object = (
            "<div style='border:1px solid #e5e7eb;border-radius:8px;overflow:hidden;font-size:11.5px'>"
            f"<table style='width:100%;border-collapse:collapse'><thead style='background:#f3f4f6'><tr>{head}</tr>"
            f"</thead><tbody>{rows}</tbody></table></div>"
        )
        self._prev.disabled = i == 0
        self._next.disabled = i == len(self._groups) - 1
        self._on_select(i)

    def _toggle_table(self, _):
        self._table_box.visible = not self._table_box.visible
        self._toggle.name = "Hide Table" if self._table_box.visible else "Show Table"

    def _csv(self):
        buf = io.StringIO()
        cols = ["CHAN_NO", "DIST", "ELEV", "AREA", "WIDTH", "WET_PERIM"]
        out = []
        for d, g in self._groups:
            part = g.copy()
            part["CHAN_NO"] = int(self._chan["CHAN_NO"])
            part["DIST"] = d
            out.append(part[cols])
        pd.concat(out).to_csv(buf, index=False)
        buf.seek(0)
        return buf
