# -*- coding: utf-8 -*-
"""Interactive dashboard of DSM2 channels and nodes driven by a hydro echo file."""
import base64
import functools
import html
import json
import logging
import math
import os
import pathlib
import re
import urllib.parse

import click
import geopandas as gpd
import pandas as pd
import panel as pn
from bokeh.events import MouseMove
from bokeh.models import (
    BoxZoomTool, ColumnDataSource, CustomJS, Div, HoverTool, Menu,
    PanTool, Range1d, TapTool, WheelZoomTool,
)
from bokeh.plotting import figure
from pydsm.analysis.dsm2study import load_echo_file
from shapely.geometry import LineString

from dsm2ui.bc_popup import BCPopup
from dsm2ui.gate_popup import GatePopup
from dsm2ui.oprule_parse import parse_rules, summarise_ext_flow, summarise_gate
from dsm2ui.oprule_view import RuleSection
from dsm2ui.xsection_popup import POPUP_WIDTH, XSectionPopup

_GIS_DIR = pathlib.Path(__file__).parent / "dsm2gis"
_DEFAULT_CHANNEL_GEO = str(_GIS_DIR / "dsm2_channels_centerlines_9_3.geojson")
_DEFAULT_NODE_GEO = str(_GIS_DIR / "dsm2_nodes_9_3.geojson")
_TILES = "Esri.WorldTopoMap"

_BLUE = "#1a73e8"
_CHANNEL_COLOR = "#f5c518"
_NODE_COLOR = "#5fa83a"
_SELECT_COLOR = "#e02020"
_BOUNDARY_COLOR = "#7b2cbf"
_XS_GREEN = "#2e9e2e"
_SOURCE_DICU = "#b4b8bd"
_SOURCE_OTHER = "#4a4f55"
_PIN_SVG = (
    "<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 36 52'>"
    "<path d='M18 1.5C9 1.5 2 8.4 2 17c0 12 16 33.5 16 33.5S34 29 34 17C34 8.4 27 1.5 18 1.5z' "
    "fill='#e53935' stroke='#b71c1c' stroke-width='2'/><circle cx='18' cy='17' r='7.5' fill='white'/></svg>"
)
_PIN_URL = "data:image/svg+xml;base64," + base64.b64encode(_PIN_SVG.encode()).decode()

_GATE_SVG = (
    "<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 170 106'><g fill='#2f5fb3'>"
    "<rect x='8' y='2' width='17' height='76' rx='2'/><rect x='145' y='2' width='17' height='76' rx='2'/>"
    "<rect x='25' y='8' width='120' height='10'/></g>"
    "<rect x='34' y='22' width='102' height='48' fill='white' stroke='#2f5fb3' stroke-width='5'/>"
    "<path d='M36 36L50 24M54 36L68 24M72 36L86 24M90 36L104 24M108 36L122 24M126 36L136 28' "
    "stroke='#2f5fb3' stroke-width='4' fill='none'/>"
    "<path d='M34 38H136M68 38V70M102 38V70M34 54H136' stroke='#2f5fb3' stroke-width='3.5' fill='none'/>"
    "<path d='M2 78q21-14 42 0t42 0t42 0t42 0M2 94q21-14 42 0t42 0t42 0t42 0' "
    "stroke='#2f5fb3' stroke-width='9' fill='none' stroke-linecap='round'/></svg>"
)
_GATE_URL = "data:image/svg+xml;base64," + base64.b64encode(_GATE_SVG.encode()).decode()

_CONTEXT_MENU_JS = """
const reg = (window.__dsm2ctx = window.__dsm2ctx || {});
if (reg[p.id]) return;
const view = Bokeh.index.find_one(p);
if (!view) return;
reg[p.id] = true;
const menu = document.createElement('div');
Object.assign(menu.style, {
  position: 'fixed', display: 'none', zIndex: 10000, background: 'white', padding: '4px 0',
  borderRadius: '8px', boxShadow: '0 2px 12px rgba(0,0,0,0.3)', font: '13px sans-serif', minWidth: '150px',
});
document.body.appendChild(menu);
const hide = () => { menu.style.display = 'none'; };
const addItem = (text, kind, idx) => {
  const it = document.createElement('div');
  it.textContent = text;
  Object.assign(it.style, {padding: '8px 16px', cursor: 'pointer'});
  it.addEventListener('mouseenter', () => { it.style.background = '#eaf2ff'; });
  it.addEventListener('mouseleave', () => { it.style.background = 'white'; });
  it.addEventListener('click', () => { hide(); req.data = {kind: [kind], idx: [idx], n: [Date.now()]}; });
  menu.appendChild(it);
};
view.el.addEventListener('contextmenu', (e) => {
  const bi = bsrc.inspected.indices, ni = nsrc.inspected.indices;
  const gci = gcs.inspected.indices, gri = grs.inspected.indices;
  // On a node dot only node options are offered, never the channels meeting there; a channel gate offers its channel.
  const ci = (bi.length || ni.length || gri.length) ? []
    : (gci.length ? [gcs.data.ci[gci[0]]] : src.inspected.indices);
  // BC nodes offered: the one under the cursor plus any at either end of the hovered channel.
  const bcs = new Map();
  if (bi.length) bcs.set(bsrc.data.node[bi[0]], bi[0]);
  if (ci.length) {
    for (const n of [src.data.UPNODE[ci[0]], src.data.DOWNNODE[ci[0]]]) {
      const k = Array.from(bsrc.data.node).indexOf(n);
      if (k >= 0 && !bcs.has(n)) bcs.set(n, k);
    }
  }
  // Source flow is offered only for the node dot directly under the cursor.
  const under = bi.length ? bsrc.data.node[bi[0]]
    : (ni.length ? nsrc.data.node[ni[0]] : (gri.length ? grs.data.node[gri[0]] : null));
  const sfNode = (under != null && srcnodes.includes(under)) ? under : null;
  if (!ci.length && !bcs.size && sfNode == null && !gci.length && !gri.length) { hide(); return; }
  e.preventDefault();
  menu.replaceChildren();
  if (ci.length) addItem('View channel info', 'chan', ci[0]);
  if (gci.length) addItem('View hydraulic structure', 'gatec', gci[0]);
  if (gri.length) addItem('View hydraulic structure', 'gater', gri[0]);
  for (const [n, k] of bcs) addItem(bcs.size > 1 ? `View BC data (node ${n})` : 'View BC data', 'bc', k);
  if (sfNode != null) addItem('View source flow data', 'src', sfNode);
  menu.style.left = e.clientX + 'px';
  menu.style.top = e.clientY + 'px';
  menu.style.display = 'block';
});
document.addEventListener('mousedown', (e) => { if (!menu.contains(e.target)) hide(); });
document.addEventListener('keydown', (e) => { if (e.key === 'Escape') hide(); });
window.addEventListener('wheel', hide, {passive: true});

// Channel tooltip drawn here (not by HoverTool) so it can be skipped when a BC node is under the cursor.
const tip = document.createElement('div');
Object.assign(tip.style, {
  position: 'fixed', display: 'none', zIndex: 9999, pointerEvents: 'none', background: 'white',
  padding: '6px 10px', border: '1px solid #ccc', borderRadius: '4px', font: '12px sans-serif',
  boxShadow: '0 1px 6px rgba(0,0,0,0.2)',
});
document.body.appendChild(tip);
const otip = tip.cloneNode(false);
otip.style.display = 'none';
document.body.appendChild(otip);
const ntip = tip.cloneNode(false);
ntip.style.display = 'none';
document.body.appendChild(ntip);
const esc = (s) => String(s).replace(/[&<>]/g, (c) => ({'&': '&amp;', '<': '&lt;', '>': '&gt;'}[c]));
let lit = -1;
const setHover = (i) => {
  if (i === lit) return;
  lit = i;
  hsrc.data = i < 0 ? {xs: [], ys: []} : {xs: [src.data.xs[i]], ys: [src.data.ys[i]]};
};
view.el.addEventListener('mousemove', (e) => {
  // Output-location tooltip yields to the node, BC node and hydraulic-structure tooltips.
  const oi = osrc.inspected.indices;
  const blocked = bsrc.inspected.indices.length > 0 || nsrc.inspected.indices.length > 0
                  || gcs.inspected.indices.length > 0 || grs.inspected.indices.length > 0;
  // Node and BC-node tooltips yield to hydraulic structures (whose tooltip is drawn by Bokeh).
  const bi0 = bsrc.inspected.indices, ni0 = nsrc.inspected.indices;
  const overGate = gcs.inspected.indices.length > 0 || grs.inspected.indices.length > 0;
  if (!overGate && (bi0.length || ni0.length)) {
    ntip.innerHTML = bi0.length
      ? `<b>Boundary node:</b> ${bsrc.data.node[bi0[0]]}<br><b>Inputs:</b> ${esc(bsrc.data.label[bi0[0]])}${bsrc.data.info[bi0[0]]}`
      : `<b>Node:</b> ${nsrc.data.node[ni0[0]]}${nsrc.data.info[ni0[0]]}`;
    ntip.style.left = (e.clientX + 14) + 'px';
    ntip.style.top = (e.clientY + 14) + 'px';
    ntip.style.display = 'block';
  } else {
    ntip.style.display = 'none';
  }
  if (oi.length && !blocked) {
    otip.replaceChildren();
    const b = document.createElement('b');
    b.textContent = osrc.data.label[oi[0]];
    otip.appendChild(b);
    otip.style.left = (e.clientX + 14) + 'px';
    otip.style.top = (e.clientY + 14) + 'px';
    otip.style.display = 'block';
  } else {
    otip.style.display = 'none';
  }
  const ci = src.inspected.indices;
  const overNode = bsrc.inspected.indices.length > 0 || nsrc.inspected.indices.length > 0
                   || osrc.inspected.indices.length > 0 || gcs.inspected.indices.length > 0
                   || grs.inspected.indices.length > 0;
  if (!ci.length || overNode) { tip.style.display = 'none'; setHover(-1); return; }
  const i = ci[0];
  setHover(i);
  tip.innerHTML = `<b>Channel:</b> ${src.data.CHAN_NO[i]}<br><b>Up \u2192 Down:</b> ${src.data.UPNODE[i]} \u2192 ${src.data.DOWNNODE[i]}`;
  tip.style.left = (e.clientX + 14) + 'px';
  tip.style.top = (e.clientY + 14) + 'px';
  tip.style.display = 'block';
});
view.el.addEventListener('mouseleave', () => {
  tip.style.display = 'none'; otip.style.display = 'none'; ntip.style.display = 'none'; setHover(-1);
});

// Drag the cross-section pop-up by its title (.xs-drag); the popup host carries .xs-popup.
let drag = null;
document.addEventListener('pointerdown', (e) => {
  const path = e.composedPath();
  if (!path.some((n) => n.classList && n.classList.contains('xs-drag'))) return;
  const host = path.find((n) => n.classList && n.classList.contains('xs-popup'));
  if (!host) return;
  const area = host.getRootNode().host || host.parentElement;
  const r = host.getBoundingClientRect();
  drag = {host, area, dx: e.clientX - r.left, dy: e.clientY - r.top};
  e.preventDefault();
});
document.addEventListener('pointermove', (e) => {
  if (!drag) return;
  const pr = drag.area.getBoundingClientRect();
  const left = Math.min(Math.max(e.clientX - pr.left - drag.dx, 0), pr.width - 120);
  const top = Math.min(Math.max(e.clientY - pr.top - drag.dy, 0), pr.height - 48);
  Object.assign(drag.host.style, {left: left + 'px', top: top + 'px', right: 'auto'});
});
document.addEventListener('pointerup', () => { drag = null; });
"""

# (column, label, format, unit)
_INFO_FIELDS = [
    ("CHAN_NO", "Channel No", "{:d}", ""),
    ("LENGTH", "Length", "{:,.0f}", " ft"),
    ("MANNING", "Manning n", "{:.4f}", ""),
    ("DISPERSION", "Dispersion", "{:,.2f}", " ft²/s"),
    ("UPNODE", "Upnode", "{:d}", ""),
    ("DOWNNODE", "Downnode", "{:d}", ""),
    ("DX", "DX", "{:,.2f}", " ft"),
]

_LOGO_SVG = (
    "<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32' fill='none' stroke='#1a73e8' "
    "stroke-width='2'><circle cx='6' cy='7' r='3'/><circle cx='26' cy='7' r='3'/>"
    "<circle cx='16' cy='16' r='3'/><circle cx='7' cy='26' r='3'/><circle cx='25' cy='26' r='3'/>"
    "<path d='M8.5 9l5.5 5M23.5 9l-5.5 5M14 18l-5 6M18 18l5 6'/></svg>"
)
_MAP_ICON = (
    "<svg width='20' height='20' viewBox='0 0 24 24' fill='none' stroke='currentColor' stroke-width='2' "
    "stroke-linejoin='round'><path d='M3 6l6-2 6 2 6-2v14l-6 2-6-2-6 2z'/><path d='M9 4v14M15 6v14'/></svg>"
)

_ARROW_SVG = (
    "<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 56'><polygon "
    "points='8,20 54,20 54,7 92,28 54,49 54,36 8,36' fill='#ffe27a' stroke='#7a5c00' "
    "stroke-width='7' stroke-linejoin='round'/></svg>"
)
_ARROW_URL = "data:image/svg+xml;base64," + base64.b64encode(_ARROW_SVG.encode()).decode()
_ARROW_SIZE = (20, 11)  # screen px

_MOUSE_COORD_JS = """
const R = 6378137;
const lon = cb_obj.x / R * 180 / Math.PI;
const lat = (2 * Math.atan(Math.exp(cb_obj.y / R)) - Math.PI / 2) * 180 / Math.PI;
div.text = `Lat: ${Math.abs(lat).toFixed(5)}° ${lat >= 0 ? 'N' : 'S'} &nbsp;|&nbsp; ` +
           `Lon: ${Math.abs(lon).toFixed(5)}° ${lon >= 0 ? 'E' : 'W'} &nbsp;|&nbsp; NAD 83`;
"""

# Moves the selection highlight onto the selected gate icon (k: 0 = channel gate, 1 = reservoir gate).
_GATE_SEL_JS = """
const gd = gsel.data;
if (gd.i.length) {
  const gs = gd.k[0] === 0 ? sc : sr;
  const gx = gs.data.x[gd.i[0]], gy = gs.data.y[gd.i[0]];
  if (gd.x[0] !== gx || gd.y[0] !== gy) gsel.data = {i: gd.i, k: gd.k, x: [gx], y: [gy]};
}
"""

# Gates scale with zoom (16 px wide at the home view, 12-40 px overall). Channel gates sit a fixed
# screen distance from their node, along the channel, so they stay beside the node at any zoom.
_GATE_SIZE_JS = """
let w, h;
try { w = p.inner_width; h = p.inner_height; } catch (e) { return; }
const x = p.x_range;
if (!w || !h || x.start == null || x.end == null) return;
const home = Math.max(home_w, home_h * w / h);
const s = Math.min(40, Math.max(12, 16 * Math.pow(home / (x.end - x.start), 0.18)));
for (const r of [rc, rr]) {
  r.glyph.w = {value: s, units: 'screen'};
  r.glyph.h = {value: s * 0.62, units: 'screen'};
}
hc.glyph.size = s * 0.8;
hr.glyph.size = s * 0.8;
const off = (s * 0.5 + 8) * (x.end - x.start) / w;
const d = sc.data;
sc.data = Object.assign({}, d, {
  x: Array.from(d.nx, (v, i) => v + d.dx[i] * off),
  y: Array.from(d.ny, (v, i) => v + d.dy[i] * off),
});
for (const [r, pad] of [[gouter, 20], [ginner, 12], [gline, 5]]) {
  r.glyph.width = {value: s + pad, units: 'screen'};
  r.glyph.height = {value: s * 0.62 + pad, units: 'screen'};
}
""" + _GATE_SEL_JS

# Pins scale with zoom (9 px tall at the home view, 6-30 px overall). The hover target
# is shifted up to the pin head, since the pin's tip sits on the location.
_OUTPUT_SIZE_JS = """
let w, h;
try { w = p.inner_width; h = p.inner_height; } catch (e) { return; }
const x = p.x_range;
if (!w || !h || x.start == null || x.end == null) return;
const home = Math.max(home_w, home_h * w / h);
const ph = Math.min(30, Math.max(6, 9 * Math.pow(home / (x.end - x.start), 0.45)));
rend.glyph.h = {value: ph, units: 'screen'};
rend.glyph.w = {value: ph * 0.69, units: 'screen'};
const mpp = (x.end - x.start) / w;
const d = hit_src.data;
hit_src.data = {x: d.x, y0: d.y0, y: Array.from(d.y0, (v) => v + ph * 0.6 * mpp), label: d.label};
hit.glyph.size = ph * 0.75;
"""

_ZOOM_JS = """
for (const r of [p.x_range, p.y_range]) {
  const c = (r.start + r.end) / 2, h = (r.end - r.start) / 2 * factor;
  r.start = c - h; r.end = c + h;
}
"""

_FIT_LEFT_JS = """
let w, h;
try { w = p.inner_width; h = p.inner_height; } catch (e) { return; }
if (!cb_obj.text || !w || !h) return;
const [x0, x1, y0, y1, , res, lft] = JSON.parse(cb_obj.text);
const left = lft || 0;  // pixels covered by the Layers card on the left
const avail = Math.max(w - left - (res == null ? reserve : res), 240);  // map pixels between the Layers card and any pop-up
const mpp = Math.max((x1 - x0) / avail, (y1 - y0) / h);
const cx = (x0 + x1) / 2, cy = (y0 + y1) / 2;
p.x_range.start = cx - mpp * (left + avail / 2); p.x_range.end = p.x_range.start + mpp * w;
p.y_range.start = cy - mpp * h / 2; p.y_range.end = cy + mpp * h / 2;
"""

_ZOOM_SELECTED_JS = """
const idx = src.selected.indices;
if (!idx.length) { p.reset.emit(); return; }
const xs = src.data.xs[idx[0]], ys = src.data.ys[idx[0]];
const x0 = Math.min(...xs), x1 = Math.max(...xs), y0 = Math.min(...ys), y1 = Math.max(...ys);
const pad = Math.max(x1 - x0, y1 - y0, 1000) * 1.5;
p.x_range.start = x0 - pad; p.x_range.end = x1 + pad;
p.y_range.start = y0 - pad; p.y_range.end = y1 + pad;
"""


@functools.lru_cache(maxsize=4)
def _load_output_locations(echo_file):
    """Return [(names, chan_no, distance_ft or None for 'length')] from OUTPUT_CHANNEL, one per location."""
    table = _load_echo(echo_file).get("OUTPUT_CHANNEL")
    if table is None:
        return []
    locs = {}
    for rec in table.to_dict("records"):
        dist = str(rec["DISTANCE"]).strip()
        key = (int(rec["CHAN_NO"]), None if dist.lower() == "length" else float(dist))
        names = locs.setdefault(key, [])
        if str(rec["NAME"]) not in names:
            names.append(str(rec["NAME"]))
    return [(", ".join(names), chan, dist) for (chan, dist), names in locs.items()]


@functools.lru_cache(maxsize=4)
def _load_gates(echo_file):
    """Return [(name, from_obj, from_identifier, to_node)] from the GATE table."""
    table = _load_echo(echo_file).get("GATE")
    if table is None:
        return []
    return [
        (str(r["NAME"]), str(r["FROM_OBJ"]).lower(), str(r["FROM_IDENTIFIER"]), int(r["TO_NODE"]))
        for r in table.to_dict("records")
    ]


@functools.lru_cache(maxsize=4)
def _load_gate_devices(echo_file):
    """Return {gate name: (weir device rows, pipe device rows)} from GATE_WEIR_DEVICE / GATE_PIPE_DEVICE."""
    echo = _load_echo(echo_file)
    devices = {}
    for k, table_name in enumerate(("GATE_WEIR_DEVICE", "GATE_PIPE_DEVICE")):
        table = echo.get(table_name)
        if table is None:
            continue
        for rec in table.to_dict("records"):
            devices.setdefault(str(rec["GATE_NAME"]), ([], []))[k].append(rec)
    return devices


@functools.lru_cache(maxsize=4)
def _load_oprules(echo_file):
    """Return (parsed operating rules, {lower-case series name: OPRULE_TIME_SERIES row})."""
    echo = _load_echo(echo_file)

    def records(name):
        table = echo.get(name)
        return [] if table is None else table.to_dict("records")

    expressions = {str(r["NAME"]): str(r["DEFINITION"]) for r in records("OPRULE_EXPRESSION")}
    series = {
        str(r["NAME"]).lower(): {
            "name": str(r["NAME"]), "fillin": str(r["FILLIN"]), "file": str(r["FILE"]), "path": str(r["PATH"]),
        }
        for r in records("OPRULE_TIME_SERIES")
    }
    return parse_rules(records("OPERATING_RULE"), expressions), series


@functools.lru_cache(maxsize=4)
def _load_echo(echo_file):
    return load_echo_file(echo_file)


@functools.lru_cache(maxsize=4)
def _load_xsections(echo_file):
    """Return {chan_no: XSECT_LAYER rows} (DIST is the fraction of channel length from the upnode)."""
    table = _load_echo(echo_file).get("XSECT_LAYER")
    if table is None:
        return {}
    table = table.copy()
    for col in ("DIST", "ELEV", "AREA", "WIDTH", "WET_PERIM"):
        table[col] = table[col].astype(float)
    table["CHAN_NO"] = table["CHAN_NO"].astype(int)
    return {n: g.reset_index(drop=True) for n, g in table.groupby("CHAN_NO")}


@functools.lru_cache(maxsize=4)
def _load_boundary_rows(echo_file):
    """Return {node: [BC row dicts]} from BOUNDARY_FLOW / BOUNDARY_STAGE."""
    rows = {}
    for table_name, kind in (("BOUNDARY_FLOW", "flow"), ("BOUNDARY_STAGE", "stage")):
        table = _load_echo(echo_file).get(table_name)
        if table is None:
            continue
        for rec in table.to_dict("records"):
            node = int(rec["NODE"])
            rows.setdefault(node, []).append({
                "name": str(rec["NAME"]), "kind": kind, "node": node,
                "sign": int(rec["SIGN"]) if rec.get("SIGN") is not None else None,
                "fillin": str(rec["FILLIN"]), "file": str(rec["FILE"]), "path": str(rec["PATH"]),
            })
    return rows


@functools.lru_cache(maxsize=4)
def _reservoir_nodes(echo_file):
    """Return {lower-case reservoir name: sorted nodes it touches} from RESERVOIR_CONNECTION and reservoir gates."""
    nodes = {}
    conn = _load_echo(echo_file).get("RESERVOIR_CONNECTION")
    for rec in ([] if conn is None else conn.to_dict("records")):
        nodes.setdefault(str(rec["RES_NAME"]).lower(), set()).add(int(rec["NODE"]))
    for _, obj, ident, node in _load_gates(echo_file):
        if obj == "reservoir":
            nodes.setdefault(ident.lower(), set()).add(node)
    return {k: sorted(v) for k, v in nodes.items()}


@functools.lru_cache(maxsize=4)
def _load_source_rows(echo_file):
    """Return {node: [source-flow row dicts]} from SOURCE_FLOW and SOURCE_FLOW_RESERVOIR; DICU sources are named ``dicu_*``.

    A reservoir source flow is listed at every node its reservoir touches.
    """
    echo = _load_echo(echo_file)
    rows = {}

    def add(node, rec, reservoir=None):
        name = str(rec["NAME"])
        group = "DICU" if name.lower().startswith("dicu") else "Other"
        extra = [("Group", group)] + ([("Reservoir", reservoir)] if reservoir else [])
        rows.setdefault(node, []).append({
            "name": name, "kind": "flow", "node": node, "sign": int(rec["SIGN"]),
            "fillin": str(rec["FILLIN"]), "file": str(rec["FILE"]), "path": str(rec["PATH"]), "group": group,
            "label": f"{name} ({group}{', reservoir ' + reservoir if reservoir else ''})", "extra": extra,
        })

    table = echo.get("SOURCE_FLOW")
    for rec in ([] if table is None else table.to_dict("records")):
        add(int(rec["NODE"]), rec)
    res_table = echo.get("SOURCE_FLOW_RESERVOIR")
    res_nodes = _reservoir_nodes(echo_file)
    for rec in ([] if res_table is None else res_table.to_dict("records")):
        reservoir = str(rec["RES_NAME"])
        for node in res_nodes.get(reservoir.lower(), []):
            add(node, rec, reservoir)
    return rows


def _echo_env(echo_file):
    envvar = _load_echo(echo_file).get("ENVVAR")
    return dict(zip(envvar["NAME"], envvar["VALUE"])) if envvar is not None else {}


def _resolve_dss(echo_file, name):
    """Locate a BC DSS file; relative names are relative to the study dir (parent of the echo's folder)."""
    env = _echo_env(echo_file)
    name = re.sub(
        r"\$\{(\w+)\}", lambda m: str(env.get(m.group(1), os.environ.get(m.group(1), m.group(0)))), name
    )
    echo_dir = pathlib.Path(echo_file).resolve().parent
    for base in (echo_dir.parent, echo_dir, pathlib.Path.cwd()):
        cand = (base / name).resolve()
        if cand.is_file():
            return str(cand)
    raise FileNotFoundError(f"DSS file not found: {name}")


def _run_window(echo_file):
    """Model run period (start, end) as Timestamps from ENVVAR, or None."""
    env = _echo_env(echo_file)
    if not (env.get("START_DATE") and env.get("END_DATE")):
        return None

    def stamp(date, hhmm):
        return pd.to_datetime(date, format="%d%b%Y") + pd.Timedelta(hours=int(hhmm[:2]), minutes=int(hhmm[2:]))

    return stamp(env["START_DATE"], env.get("START_TIME", "0000")), stamp(env["END_DATE"], env.get("END_TIME", "0000"))


@functools.lru_cache(maxsize=8)
def _dss_catalog(dss_path):
    import pyhecdss

    logging.getLogger("pyhecdss").setLevel(logging.WARNING)  # pyhecdss mis-formats some debug messages
    with pyhecdss.DSSFile(dss_path) as dss:
        return dss.read_catalog()


@functools.lru_cache(maxsize=32)
def _read_bc_series(echo_file, dss_name, path):
    """Return (full-record DataFrame, units, model run period or None) for a BC / source-flow DSS path."""
    import pyhecdss

    if dss_name.strip().lower() == "constant":
        window = _run_window(echo_file)
        if window is None:
            raise ValueError("constant input, but the echo file has no run period")
        df = pd.DataFrame({"constant": [float(path)] * 2}, index=pd.DatetimeIndex(list(window)))
        return df, "cfs", window

    # The path's D part is usually blank, so look the record(s) up in the catalog instead.
    a, b, c, _, e, f = path.split("/")[1:7]
    dss_path = _resolve_dss(echo_file, dss_name)
    cat = _dss_catalog(dss_path)
    hits = cat[(cat.A == a) & (cat.B == b) & (cat.C == c) & (cat.E == e) & (cat.F == f)]
    if hits.empty:
        raise KeyError(f"path not found in DSS file: {path}")
    with pyhecdss.DSSFile(dss_path) as dss:
        parts = []
        for pathname in dss.get_pathnames(hits):
            # Gate schedules are irregular (E part IR-*), which read_rts cannot read.
            reader = dss.read_its if pathname.split("/")[5].upper().startswith("IR-") else dss.read_rts
            df, units, _ = reader(pathname)
            parts.append(df)
    df = pd.concat(parts) if len(parts) > 1 else parts[0]
    if isinstance(df.index, pd.PeriodIndex):
        df.index = df.index.to_timestamp()
    df = df[~df.index.duplicated()].sort_index()
    return df, units, _run_window(echo_file)


@functools.lru_cache(maxsize=4)
def _load_network(echo_file, channel_geo, node_geo):
    """Return (channels, nodes) GeoDataFrames in web-mercator, channels joined to the CHANNEL table."""
    chan_table = _load_echo(echo_file)["CHANNEL"].copy()
    for col, _, _, _ in _INFO_FIELDS:
        chan_table[col] = chan_table[col].astype(float)
    for col in ("CHAN_NO", "UPNODE", "DOWNNODE"):
        chan_table[col] = chan_table[col].astype(int)

    # Plain read: the pydsm loader buffers geometry into polygons, we need lines.
    chans = gpd.read_file(channel_geo).to_crs(epsg=3857)
    chans = chans.merge(chan_table, left_on="id", right_on="CHAN_NO")

    nodes = gpd.read_file(node_geo).to_crs(epsg=3857)
    used = set(chans["UPNODE"]) | set(chans["DOWNNODE"])
    nodes = nodes[nodes["id"].isin(used)].reset_index(drop=True)
    return chans, nodes


def _orient_lines(lines, chans, nodes):
    """Reverse any centerline whose vertex order runs DOWNNODE -> UPNODE."""
    pos = dict(zip(nodes["id"], zip(nodes.geometry.x, nodes.geometry.y)))
    out = []
    for coords, up, down in zip(lines, chans["UPNODE"], chans["DOWNNODE"]):
        if up in pos and down in pos:
            sx, sy = coords[0]
            d_up = math.hypot(sx - pos[up][0], sy - pos[up][1])
            d_down = math.hypot(sx - pos[down][0], sy - pos[down][1])
            if d_down < d_up:
                coords = coords[::-1]
        out.append(coords)
    return out


def _arrow_data(lines):
    """Midpoint and heading (radians, math convention) of each upnode-to-downnode line."""
    xs, ys, angles = [], [], []
    for coords in lines:
        line = LineString(coords)
        mid = line.interpolate(0.5, normalized=True)
        a = line.interpolate(0.49, normalized=True)
        b = line.interpolate(0.51, normalized=True)
        xs.append(mid.x)
        ys.append(mid.y)
        angles.append(math.atan2(b.y - a.y, b.x - a.x))
    return xs, ys, angles


def _overlay(obj, **css):
    """Pin a Panel object over the map at the given CSS offsets (top/left/right/bottom in px)."""
    obj.margin = 0
    obj.styles = {
        **(obj.styles or {}), "position": "absolute", "z-index": "10",
        **{k: f"{v}px" for k, v in css.items()},
    }
    return obj


def _card_styles(**extra):
    return {
        "background": "white", "border-radius": "12px", "padding": "12px 16px",
        "box-shadow": "0 2px 10px rgba(0,0,0,0.18)", **extra,
    }


def _swatch(kind):
    if kind == "arrow":
        return pn.pane.HTML(
            f"<img src='{_ARROW_URL}' width='24' style='margin-top:2px'>", margin=(12, 4, 0, 0), width=30
        )
    if kind == "line":
        css = f"width:26px;height:4px;border-radius:2px;background:{_CHANNEL_COLOR}"
    elif kind == "triangle":
        css = (
            "width:0;height:0;border-left:6px solid transparent;border-right:6px solid transparent;"
            f"border-bottom:10px solid {_BOUNDARY_COLOR};margin-top:1px"
        )
    elif kind in ("dicu", "other"):
        color = _SOURCE_DICU if kind == "dicu" else _SOURCE_OTHER
        css = f"width:8px;height:8px;border-radius:50%;background:{color};margin:1px 0 0 3px"
    elif kind == "pin":
        return pn.pane.HTML(f"<img src='{_PIN_URL}' height='18' style='margin-top:1px'>", margin=(11, 4, 0, 0), width=30)
    elif kind == "gate":
        return pn.pane.HTML(f"<img src='{_GATE_URL}' height='16' style='margin-top:2px'>", margin=(11, 4, 0, 0), width=30)
    else:
        css = f"width:9px;height:9px;border-radius:50%;border:2px solid {_NODE_COLOR}"
    return pn.pane.HTML(f"<div style='{css}'></div>", margin=(12, 4, 0, 0), width=30)


def _icon_button(icon):
    return pn.widgets.Button(icon=icon, button_type="light", width=44, height=44, margin=0)


def build_input_viewer(echo_file, channel_geo=None, node_geo=None):
    """Build the dashboard template for the channel/node viewer."""
    chans, nodes = _load_network(
        echo_file, channel_geo or _DEFAULT_CHANNEL_GEO, node_geo or _DEFAULT_NODE_GEO
    )

    # Channel geometries are single-part MultiLineStrings.
    lines = [[c for part in g.geoms for c in part.coords] for g in chans.geometry]
    lines = _orient_lines(lines, chans, nodes)
    chan_data = {col: chans[col].tolist() for col, _, _, _ in _INFO_FIELDS}
    chan_data["xs"] = [[c[0] for c in ln] for ln in lines]
    chan_data["ys"] = [[c[1] for c in ln] for ln in lines]
    chan_src = ColumnDataSource(chan_data)
    node_src = ColumnDataSource(
        {"x": nodes.geometry.x.tolist(), "y": nodes.geometry.y.tolist(), "node": nodes["id"].tolist()}
    )
    sel_line_src = ColumnDataSource({"xs": [], "ys": []})

    # Explicit ranges (not DataRange1d): scripted start/end changes are ignored by auto-ranged axes.
    minx, miny, maxx, maxy = chans.total_bounds
    padx, pady = (maxx - minx) * 0.12, (maxy - miny) * 0.12
    home_x, home_y = (minx - padx, maxx + padx), (miny - pady, maxy + pady)

    # QGIS-like navigation: left-drag pans, wheel zooms about the cursor in coarse steps.
    pan = PanTool()
    wheel = WheelZoomTool(speed=1 / 250)
    p = figure(
        x_axis_type="mercator", y_axis_type="mercator",
        x_range=Range1d(*home_x), y_range=Range1d(*home_y),
        tools=[pan, wheel, BoxZoomTool()],
        active_drag=pan, active_scroll=wheel,
        match_aspect=True, sizing_mode="stretch_both",
        toolbar_location=None, min_border=0,
        outline_line_color=None,
    )
    p.axis.visible = False
    p.grid.visible = False
    p.add_tile(_TILES)

    chan_r = p.multi_line(
        "xs", "ys", source=chan_src, line_width=3, line_color=_CHANNEL_COLOR,
        selection_line_color=_CHANNEL_COLOR, nonselection_line_alpha=1.0,
    )
    # Hover highlight is drawn by the page script so a node under the cursor can suppress it.
    hover_src = ColumnDataSource({"xs": [], "ys": []})
    hover_r = p.multi_line("xs", "ys", source=hover_src, line_width=5, line_color=_BLUE)
    # Soft glow underneath the bold selected-channel line.
    glow_outer = p.multi_line("xs", "ys", source=sel_line_src, line_width=22, line_color=_SELECT_COLOR, line_alpha=0.15)
    glow_inner = p.multi_line("xs", "ys", source=sel_line_src, line_width=13, line_color=_SELECT_COLOR, line_alpha=0.3)
    sel_r = p.multi_line("xs", "ys", source=sel_line_src, line_width=6, line_color=_SELECT_COLOR)
    ax, ay, aangle = _arrow_data(lines)
    arrow_src = ColumnDataSource({"x": ax, "y": ay, "angle": aangle, "url": [_ARROW_URL] * len(ax)})
    arrow_r = p.image_url(
        url="url", x="x", y="y", w=_ARROW_SIZE[0], h=_ARROW_SIZE[1], angle="angle",
        anchor="center", w_units="screen", h_units="screen", source=arrow_src,
    )
    node_r = p.scatter(
        "x", "y", source=node_src, size=10, fill_color="white", fill_alpha=0.6,
        line_color=_NODE_COLOR, line_width=1.5,
    )
    # Smaller than the node circle so the channel node stays visible around the triangle.
    node_pos = {n: (x, y) for n, x, y in zip(nodes["id"], nodes.geometry.x, nodes.geometry.y)}
    src_rows = {n: r for n, r in _load_source_rows(echo_file).items() if n in node_pos}

    def _source_info(node):
        rows = src_rows.get(node)
        if not rows:
            return ""
        n_dicu = sum(r["group"] == "DICU" for r in rows)
        return f"<br><b>Source flow:</b> {n_dicu} DICU, {len(rows) - n_dicu} other"

    def _source_dots(group):
        ns = sorted(n for n, rows in src_rows.items() if any(r["group"] == group for r in rows))
        return ColumnDataSource(
            {"x": [node_pos[n][0] for n in ns], "y": [node_pos[n][1] for n in ns], "node": ns}
        )

    # Small grey dots inside the node circles; the dark "other" dot sits inside the larger DICU one.
    dicu_r = p.scatter("x", "y", source=_source_dots("DICU"), size=6, fill_color=_SOURCE_DICU, line_alpha=0)
    other_r = p.scatter("x", "y", source=_source_dots("Other"), size=4, fill_color=_SOURCE_OTHER, line_alpha=0)
    src_sel_src = ColumnDataSource({"x": [], "y": []})
    src_ring = p.scatter(
        "x", "y", source=src_sel_src, size=24, fill_color=_SOURCE_OTHER, fill_alpha=0.12,
        line_color=_SOURCE_OTHER, line_width=3,
    )

    # Output locations: one pin per (channel, distance), placed along the channel from the upnode.
    chan_geom = {int(n): LineString(ln) for n, ln in zip(chans["CHAN_NO"], lines)}
    chan_length = dict(zip(chans["CHAN_NO"].astype(int), chans["LENGTH"]))
    out_x, out_y, out_label = [], [], []
    for names, chan, dist in _load_output_locations(echo_file):
        if chan not in chan_geom:
            continue
        length = float(chan_length[chan])
        dist_ft = length if dist is None else dist
        pt = chan_geom[chan].interpolate(min(max(dist_ft / length, 0.0), 1.0), normalized=True)
        out_x.append(pt.x)
        out_y.append(pt.y)
        out_label.append(f"{names} \u2014 channel {chan} @ {dist_ft:,.0f} ft")
    out_src = ColumnDataSource({"x": out_x, "y": out_y, "url": [_PIN_URL] * len(out_x)})
    out_r = p.image_url(
        url="url", x="x", y="y", w=6, h=9, anchor="bottom_center",
        w_units="screen", h_units="screen", source=out_src,
    )
    out_hit_src = ColumnDataSource({"x": out_x, "y0": out_y, "y": out_y, "label": out_label})
    out_hit = p.scatter("x", "y", source=out_hit_src, size=13, fill_alpha=0, line_alpha=0)
    out_size_cb = CustomJS(
        args=dict(
            p=p, rend=out_r, hit=out_hit, hit_src=out_hit_src,
            home_w=home_x[1] - home_x[0], home_h=home_y[1] - home_y[0],
        ),
        code=_OUTPUT_SIZE_JS,
    )
    for rng in (p.x_range, p.y_range):
        rng.js_on_change("start", out_size_cb)
        rng.js_on_change("end", out_size_cb)
    p.js_on_change("inner_width", out_size_cb)

    # Gates: channel gates sit beside their node along the channel, reservoir gates sit on the node.
    chan_pos = {int(n): i for i, n in enumerate(chans["CHAN_NO"])}
    gate_c, gate_r = {}, {}
    for name, obj, ident, node in _load_gates(echo_file):
        if obj == "channel" and ident.isdigit() and int(ident) in chan_pos:
            ci = chan_pos[int(ident)]
            ln = lines[ci]
            up, down = int(chans["UPNODE"].iloc[ci]), int(chans["DOWNNODE"].iloc[ci])
            if node == up:
                (x0, y0), (x1, y1) = ln[0][:2], ln[1][:2]
            elif node == down:
                (x0, y0), (x1, y1) = ln[-1][:2], ln[-2][:2]
            else:
                continue
            norm = math.hypot(x1 - x0, y1 - y0) or 1.0
            rec = gate_c.setdefault((ci, node), {
                "nx": x0, "ny": y0, "dx": (x1 - x0) / norm, "dy": (y1 - y0) / norm,
                "chan": int(ident), "up": up, "down": down, "ci": ci, "node": node, "names": [],
            })
            rec["names"].append(name)
        elif obj == "reservoir" and node in node_pos:
            gate_r.setdefault(node, []).append((name, ident))
    gate_c_recs = list(gate_c.values())
    gate_c_src = ColumnDataSource({
        "x": [r["nx"] for r in gate_c_recs], "y": [r["ny"] for r in gate_c_recs],
        "nx": [r["nx"] for r in gate_c_recs], "ny": [r["ny"] for r in gate_c_recs],
        "dx": [r["dx"] for r in gate_c_recs], "dy": [r["dy"] for r in gate_c_recs],
        "chan": [r["chan"] for r in gate_c_recs], "up": [r["up"] for r in gate_c_recs],
        "down": [r["down"] for r in gate_c_recs], "ci": [r["ci"] for r in gate_c_recs],
        "name": [", ".join(r["names"]) for r in gate_c_recs],
        "url": [_GATE_URL] * len(gate_c_recs),
    })
    res_nodes = sorted(gate_r)
    gate_r_src = ColumnDataSource({
        "x": [node_pos[n][0] for n in res_nodes], "y": [node_pos[n][1] for n in res_nodes],
        "node": res_nodes, "name": [", ".join(n for n, _ in gate_r[k]) for k in res_nodes],
        "info": [_source_info(n) for n in res_nodes], "url": [_GATE_URL] * len(res_nodes),
    })
    # Red glow behind (and outline around) the gate whose window is open, like the selected channel.
    gate_sel_src = ColumnDataSource({"i": [], "k": [], "x": [], "y": []})
    gate_glow = [
        p.rect(
            "x", "y", width=40, height=30, width_units="screen", height_units="screen", source=gate_sel_src,
            fill_color=_SELECT_COLOR, fill_alpha=alpha, line_alpha=0,
        )
        for alpha in (0.15, 0.3)
    ]
    gate_images = []
    for gsrc in (gate_c_src, gate_r_src):
        gate_images.append(p.image_url(
            url="url", x="x", y="y", w=16, h=10, anchor="center",
            w_units="screen", h_units="screen", source=gsrc,
        ))
    gate_c_r, gate_r_r = gate_images
    gate_line = p.rect(
        "x", "y", width=20, height=14, width_units="screen", height_units="screen", source=gate_sel_src,
        fill_alpha=0, line_color=_SELECT_COLOR, line_width=3,
    )
    gate_c_hit = p.scatter("x", "y", source=gate_c_src, marker="square", size=18, fill_alpha=0, line_alpha=0)
    gate_r_hit = p.scatter("x", "y", source=gate_r_src, marker="square", size=18, fill_alpha=0, line_alpha=0)
    gate_size_cb = CustomJS(
        args=dict(
            p=p, rc=gate_c_r, rr=gate_r_r, hc=gate_c_hit, hr=gate_r_hit, sc=gate_c_src, sr=gate_r_src,
            gouter=gate_glow[0], ginner=gate_glow[1], gline=gate_line, gsel=gate_sel_src,
            home_w=home_x[1] - home_x[0], home_h=home_y[1] - home_y[0],
        ),
        code=_GATE_SIZE_JS,
    )
    for rng in (p.x_range, p.y_range):
        rng.js_on_change("start", gate_size_cb)
        rng.js_on_change("end", gate_size_cb)
    p.js_on_change("inner_width", gate_size_cb)
    gate_sel_src.js_on_change(
        "data", CustomJS(args=dict(gsel=gate_sel_src, sc=gate_c_src, sr=gate_r_src), code=_GATE_SEL_JS)
    )
    bc_rows = {n: r for n, r in _load_boundary_rows(echo_file).items() if n in node_pos}
    bnd_nodes = sorted(bc_rows)
    bnd_src = ColumnDataSource({
        "x": [node_pos[n][0] for n in bnd_nodes], "y": [node_pos[n][1] for n in bnd_nodes],
        "node": bnd_nodes,
        "label": [", ".join(f"{r['name']} ({r['kind']})" for r in bc_rows[n]) for n in bnd_nodes],
        "info": [_source_info(n) for n in bnd_nodes],
    })
    # Ring marking the BC node whose data window is open.
    bc_sel_src = ColumnDataSource({"x": [], "y": []})
    bc_ring = p.scatter(
        "x", "y", source=bc_sel_src, size=26, fill_color=_BOUNDARY_COLOR, fill_alpha=0.15,
        line_color=_BOUNDARY_COLOR, line_width=3,
    )
    bnd_r = p.scatter(
        "x", "y", source=bnd_src, marker="triangle", size=6, fill_color=_BOUNDARY_COLOR,
        line_color=_BOUNDARY_COLOR, line_width=1,
    )
    # Transparent, larger hit target so the tiny triangle is easy to hover and right-click.
    bnd_hit = p.scatter("x", "y", source=bnd_src, size=16, fill_alpha=0, line_alpha=0)
    # BC nodes get only the boundary tooltip, so the plain node tooltip hits non-BC nodes only;
    # reservoir-gate nodes are also skipped because the gate tooltip already shows the node.
    plain = [(n, x, y) for n, (x, y) in node_pos.items() if n not in bc_rows and n not in gate_r]
    node_hit_src = ColumnDataSource({
        "x": [x for _, x, _ in plain], "y": [y for _, _, y in plain], "node": [n for n, _, _ in plain],
        "info": [_source_info(n) for n, _, _ in plain],
    })
    node_hit = p.scatter("x", "y", source=node_hit_src, size=10, fill_alpha=0, line_alpha=0)
    xs_src = ColumnDataSource({"x": [], "y": [], "angle": [], "color": []})
    p.scatter(
        "x", "y", source=xs_src, marker="dash", size=26, angle="angle",
        line_color="color", line_width=4,
    )

    # Right-click menu: Bokeh has no right-click glyph event, so a small DOM menu is installed
    # (lazily, on first mouse move) and reads the channel currently under the cursor.
    req_src = ColumnDataSource({"kind": [""], "idx": [-1], "n": [0]})
    fit_div = Div(text="", visible=False)  # carries the target extent to the browser as JSON
    fit_div.js_on_change("text", CustomJS(args=dict(p=p, reserve=POPUP_WIDTH + 100), code=_FIT_LEFT_JS))
    p.context_menu = Menu(items=[])  # suppress Bokeh's default tool menu
    # fit_div is only reachable by the browser if some model references it.
    p.js_on_event(
        MouseMove,
        CustomJS(
            args=dict(
                p=p, src=chan_src, bsrc=bnd_src, nsrc=node_hit_src, hsrc=hover_src, osrc=out_hit_src,
                gcs=gate_c_src, grs=gate_r_src,
                srcnodes=sorted(src_rows),
                req=req_src, fit=fit_div,
            ),
            code=_CONTEXT_MENU_JS,
        ),
    )
    p.add_tools(
        TapTool(renderers=[chan_r]),
        HoverTool(renderers=[chan_r], tooltips=None),  # inspection only; the tooltip is drawn in JS
        HoverTool(renderers=[out_hit], tooltips=None),  # inspection only; the tooltip is drawn in JS
        HoverTool(
            renderers=[gate_c_hit],
            tooltips="<b>Channel:</b> @chan<br><b>Up \u2192 Down:</b> @up \u2192 @down<br><b>Gate:</b> @name",
        ),
        HoverTool(renderers=[gate_r_hit], tooltips="<b>Node:</b> @node<br><b>Gate:</b> @name@info{safe}"),
        HoverTool(renderers=[node_hit], tooltips=None),  # tooltip drawn in JS
        HoverTool(renderers=[bnd_hit], tooltips=None),  # tooltip drawn in JS
    )

    # --- Channel information card -------------------------------------------------
    info_body = pn.pane.HTML("", margin=0, width=288)
    close_btn = pn.widgets.Button(name="✕", button_type="light", width=32, height=32, margin=0)
    title = pn.pane.HTML(
        f"<span style='color:{_BLUE};font-weight:600;letter-spacing:.5px;font-size:15px'>"
        "CHANNEL INFORMATION</span>",
        margin=(6, 0, 0, 0),
    )
    info_card = pn.Column(
        pn.Row(title, pn.HSpacer(), close_btn, margin=0), info_body,
        width=320, visible=False, styles=_card_styles(),
    )
    _overlay(info_card, top=16, right=76)

    def _on_select(attr, old, new):
        if not new:
            sel_line_src.data = {"xs": [], "ys": []}
            info_card.visible = False
            return
        i = new[0]
        row = {col: chan_src.data[col][i] for col, _, _, _ in _INFO_FIELDS}
        rows = "".join(
            "<tr style='border-bottom:1px solid #e5e7eb'>"
            f"<td style='padding:9px 0;color:#222'>{html.escape(label)}</td>"
            f"<td style='padding:9px 0;text-align:right;color:{_BLUE};font-weight:500'>"
            f"{fmt.format(int(row[col]) if fmt.endswith('d}') else row[col])}{unit}</td></tr>"
            for col, label, fmt, unit in _INFO_FIELDS
        )
        info_body.object = f"<table style='width:100%;border-collapse:collapse'>{rows}</table>"
        sel_line_src.data = {"xs": [chan_src.data["xs"][i]], "ys": [chan_src.data["ys"][i]]}
        info_card.visible = True

    chan_src.selected.on_change("indices", _on_select)
    close_btn.on_click(lambda _: setattr(chan_src.selected, "indices", []))

    # --- Cross-section pop-up -----------------------------------------------------
    xs_tables = _load_xsections(echo_file)
    line_geoms = [LineString(c) for c in lines]
    state = {"ci": 0, "dists": [], "n": 0}

    def _fit(x0, x1, y0, y1, reserve=None):
        """Frame an extent; ``reserve`` is the pixel width to keep clear on the right (default: pop-up width); the Layers card keeps the left clear."""
        state["n"] += 1  # nonce so an identical extent still fires a change event
        left = 260 if layers.visible and layers_body.visible else 0
        fit_div.text = json.dumps([x0, x1, y0, y1, state["n"], reserve, left])

    def _set_markers(selected):
        line = line_geoms[state["ci"]]
        pts, angles = [], []
        for d in state["dists"]:
            pts.append(line.interpolate(d, normalized=True))
            a = line.interpolate(max(d - 0.01, 0), normalized=True)
            b = line.interpolate(min(d + 0.01, 1), normalized=True)
            # A dash marker is horizontal at angle 0; turn it perpendicular to the channel.
            angles.append(math.atan2(b.y - a.y, b.x - a.x) + math.pi / 2)
        xs_src.data = {
            "x": [pt.x for pt in pts], "y": [pt.y for pt in pts], "angle": angles,
            "color": [_BLUE if k == selected else _XS_GREEN for k in range(len(pts))],
        }

    def _highlight(ci):
        if ci is None:
            sel_line_src.data = {"xs": [], "ys": []}
        else:
            sel_line_src.data = {"xs": [chan_src.data["xs"][ci]], "ys": [chan_src.data["ys"][ci]]}

    def _close_popup():
        xs_src.data = {"x": [], "y": [], "angle": [], "color": []}
        picked = chan_src.selected.indices
        _highlight(picked[0] if picked else None)

    def _zoom_location(k):
        line = line_geoms[state["ci"]]
        pt = line.interpolate(state["dists"][k], normalized=True)
        half = max(line.length * 0.3, 1200)
        _fit(pt.x - half, pt.x + half, pt.y - half, pt.y + half)

    def _zoom_all():
        x0, y0, x1, y1 = line_geoms[state["ci"]].bounds
        pad = max(x1 - x0, y1 - y0, 1500) * 0.5
        _fit(x0 - pad, x1 + pad, y0 - pad, y1 + pad)

    popup = XSectionPopup(
        on_select=_set_markers, on_zoom_location=_zoom_location, on_zoom_all=_zoom_all,
        on_close=_close_popup,
        max_items=max((t["DIST"].nunique() for t in xs_tables.values()), default=1),
    )
    popup_panel = _overlay(popup.panel, top=16, right=76)
    popup_panel.styles = {**popup_panel.styles, "z-index": "20"}

    oprules, oprule_series = _load_oprules(echo_file)
    rule_section = RuleSection(oprule_series, lambda row: _read_bc_series(echo_file, row["file"], row["path"]))

    def _flow_rules(row):
        """Operating rules that set this boundary / source flow; their charts skip the series already plotted."""
        view = summarise_ext_flow(oprules, row["name"])
        return rule_section.layout(view, skip=(row["file"], row["path"])) if view["devices"] else []

    bc_popup = BCPopup(
        on_close=lambda: setattr(bc_sel_src, "data", {"x": [], "y": []}),
        load_series=lambda row: _read_bc_series(echo_file, row["file"], row["path"]),
        rules_for=_flow_rules,
    )
    bc_panel = _overlay(bc_popup.panel, top=16, right=76)
    bc_panel.styles = {**bc_panel.styles, "z-index": "20"}

    src_popup = BCPopup(
        on_close=lambda: setattr(src_sel_src, "data", {"x": [], "y": []}),
        load_series=lambda row: _read_bc_series(echo_file, row["file"], row["path"]),
        title="SOURCE FLOW", input_label="Source flow input", rules_for=_flow_rules,
    )
    src_panel = _overlay(src_popup.panel, top=16, right=76)
    src_panel.styles = {**src_panel.styles, "z-index": "20"}

    gate_devices = _load_gate_devices(echo_file)
    gate_popup = GatePopup(
        on_close=lambda: setattr(gate_sel_src, "data", {"i": [], "k": [], "x": [], "y": []}),
        load_devices=lambda name: gate_devices.get(name, ([], [])),
        load_rules=lambda name: summarise_gate(oprules, name),
        rule_section=rule_section,
    )
    gate_panel = _overlay(gate_popup.panel, top=16, right=76)
    gate_panel.styles = {**gate_panel.styles, "z-index": "20"}

    popups = [popup, bc_popup, src_popup, gate_popup]

    def _close_others(keep):
        for other in popups:
            if other is not keep and other.panel.visible:
                other.hide()

    def _open_request(attr, old, new):
        idx = int(new["idx"][0])
        kind = new["kind"][0]
        if idx < 0:
            return
        if kind == "bc":
            node = int(bnd_src.data["node"][idx])
            _close_others(bc_popup)
            x, y = node_pos[node]
            bc_sel_src.data = {"x": [x], "y": [y]}
            bc_popup.show(node, bc_rows[node])
            half = 6000
            _fit(x - half, x + half, y - half, y + half)
            return
        if kind == "src":
            node = idx  # for source flows the request carries the node number itself
            _close_others(src_popup)
            x, y = node_pos[node]
            src_sel_src.data = {"x": [x], "y": [y]}
            src_popup.show(node, src_rows[node])
            half = 3000
            _fit(x - half, x + half, y - half, y + half)
            return
        if kind in ("gatec", "gater"):
            _close_others(gate_popup)
            if kind == "gatec":
                rec = gate_c_recs[idx]
                x, y = rec["nx"], rec["ny"]
                rows = [
                    {"name": n, "kind": "Channel", "where": rec["chan"], "node": rec["node"]}
                    for n in rec["names"]
                ]
            else:
                node = res_nodes[idx]
                x, y = node_pos[node]
                rows = [{"name": n, "kind": "Reservoir", "where": r, "node": node} for n, r in gate_r[node]]
            gate_popup.show(rows)
            gate_sel_src.data = {"i": [idx], "k": [0 if kind == "gatec" else 1], "x": [x], "y": [y]}
            half = 3000
            _fit(x - half, x + half, y - half, y + half)
            return
        _close_others(popup)
        ci = idx
        chan_no = int(chan_src.data["CHAN_NO"][ci])
        xs = xs_tables.get(chan_no, pd.DataFrame(columns=["CHAN_NO", "DIST", "ELEV", "AREA", "WIDTH", "WET_PERIM"]))
        state["ci"] = ci
        state["dists"] = sorted(xs["DIST"].unique())
        popup.show({col: chan_src.data[col][ci] for col, _, _, _ in _INFO_FIELDS}, xs)
        _highlight(ci)
        _zoom_all()

    req_src.on_change("data", _open_request)

    # --- Layers card ----------------------------------------------------------------
    chan_cb = pn.widgets.Checkbox(name="Channels", value=True, margin=(8, 0))
    node_cb = pn.widgets.Checkbox(name="Channel Nodes", value=True, margin=(8, 0))
    dir_cb = pn.widgets.Checkbox(name="Channel Direction", value=True, margin=(8, 0))
    bnd_cb = pn.widgets.Checkbox(name="Boundary Nodes", value=True, margin=(8, 0))
    dicu_cb = pn.widgets.Checkbox(name="Source Flow (DICU)", value=True, margin=(8, 0))
    other_cb = pn.widgets.Checkbox(name="Source Flow (other)", value=True, margin=(8, 0))
    out_cb = pn.widgets.Checkbox(name="Output Locations", value=True, margin=(8, 0))
    gate_cb = pn.widgets.Checkbox(name="Hydraulic Structures", value=True, margin=(8, 0))
    chan_cb.jslink(chan_r, value="visible")
    chan_cb.jslink(hover_r, value="visible")
    chan_cb.jslink(sel_r, value="visible")
    chan_cb.jslink(glow_inner, value="visible")
    chan_cb.jslink(glow_outer, value="visible")
    node_cb.jslink(node_r, value="visible")
    node_cb.jslink(node_hit, value="visible")
    dir_cb.jslink(arrow_r, value="visible")
    bnd_cb.jslink(bnd_r, value="visible")
    bnd_cb.jslink(bnd_hit, value="visible")
    bnd_cb.jslink(bc_ring, value="visible")
    dicu_cb.jslink(dicu_r, value="visible")
    other_cb.jslink(other_r, value="visible")
    out_cb.jslink(out_r, value="visible")
    out_cb.jslink(out_hit, value="visible")
    for gate_glyph in (gate_c_r, gate_r_r, gate_c_hit, gate_r_hit, *gate_glow, gate_line):
        gate_cb.jslink(gate_glyph, value="visible")

    collapse_btn = pn.widgets.Button(icon="chevron-up", button_type="light", width=36, height=36, margin=0)
    layers_body = pn.Column(
        pn.Row(chan_cb, pn.HSpacer(), _swatch("line"), margin=0),
        pn.Row(node_cb, pn.HSpacer(), _swatch("circle"), margin=0),
        pn.Row(dir_cb, pn.HSpacer(), _swatch("arrow"), margin=0),
        pn.Row(gate_cb, pn.HSpacer(), _swatch("gate"), margin=0),
        pn.Row(bnd_cb, pn.HSpacer(), _swatch("triangle"), margin=0),
        pn.Row(dicu_cb, pn.HSpacer(), _swatch("dicu"), margin=0),
        pn.Row(other_cb, pn.HSpacer(), _swatch("other"), margin=0),
        pn.Row(out_cb, pn.HSpacer(), _swatch("pin"), margin=0),
        margin=0,
    )

    def _toggle_layers(_):
        layers_body.visible = not layers_body.visible
        collapse_btn.icon = "chevron-up" if layers_body.visible else "chevron-down"

    collapse_btn.on_click(_toggle_layers)
    layers = pn.Column(
        pn.Row(
            pn.pane.HTML("<b style='letter-spacing:.5px;font-size:15px'>LAYERS</b>", margin=(8, 0, 0, 0)),
            pn.HSpacer(), collapse_btn, margin=0,
        ),
        layers_body, width=230, styles=_card_styles(padding="8px 14px"),
    )
    _overlay(layers, top=16, left=16)

    # --- Zoom / navigation buttons (client-side so they respond instantly) ----------
    zoom_in, zoom_out, home, locate, layers_btn = (
        _icon_button(i) for i in ("plus", "minus", "home", "current-location", "stack-2")
    )
    zoom_in.js_on_click(args=dict(p=p, factor=0.5), code=_ZOOM_JS)
    zoom_out.js_on_click(args=dict(p=p, factor=2.0), code=_ZOOM_JS)
    home.js_on_click(args=dict(p=p), code="p.reset.emit();")
    locate.js_on_click(args=dict(p=p, src=chan_src), code=_ZOOM_SELECTED_JS)
    layers_btn.on_click(lambda _: setattr(layers, "visible", not layers.visible))
    controls = pn.Column(
        zoom_in, zoom_out, home, locate, layers_btn, styles=_card_styles(padding="4px"),
    )
    _overlay(controls, top=16, right=16)

    # --- Coordinate read-out --------------------------------------------------------
    coord_div = Div(text="Lat: --- &nbsp;|&nbsp; Lon: --- &nbsp;|&nbsp; NAD 83")
    p.js_on_event(MouseMove, CustomJS(args=dict(div=coord_div), code=_MOUSE_COORD_JS))

    # --- Search -----------------------------------------------------------------------
    chan_index = {int(n): i for i, n in enumerate(chan_src.data["CHAN_NO"])}
    find_src = ColumnDataSource({"x": [], "y": []})
    p.scatter("x", "y", source=find_src, size=46, fill_color=_SELECT_COLOR, fill_alpha=0.15, line_alpha=0)
    p.scatter("x", "y", source=find_src, size=24, fill_alpha=0, line_color=_SELECT_COLOR, line_width=4)

    compact = [
        ".bk-btn { padding: 2px 8px; font-size: 12px; box-sizing: border-box; }"
        " input.bk-input { height: 28px !important; min-height: 0 !important; padding: 0 6px !important;"
        " font-size: 12px; box-sizing: border-box; }"
    ]
    search_kind = pn.widgets.RadioButtonGroup(
        options=["Channel", "Node"], value="Channel", button_type="light", height=28, margin=0,
        stylesheets=compact,
    )
    search_in = pn.widgets.TextInput(
        name="", placeholder="No.", width=52, height=28, margin=(0, 5), stylesheets=compact,
    )
    search_btn = pn.widgets.Button(
        icon="search", button_type="light", width=30, height=28, margin=0, stylesheets=compact,
    )
    search_msg = pn.pane.HTML("", margin=0, width=180)

    doc = pn.state.curdoc
    searched = {"text": "", "kind": None}

    def _run_search(text, kind):
        find_src.data = {"x": [], "y": []}
        search_msg.object = ""
        chan_src.selected.indices = []  # drop any earlier selection (and its info card)
        if not text:
            return
        num = int(text) if text.isdigit() else None
        reserve = None if any(pp.panel.visible for pp in popups) else 0
        if kind == "Channel" and num in chan_index:
            ci = chan_index[num]
            chan_src.selected.indices = [ci]
            x0, y0, x1, y1 = line_geoms[ci].bounds
            pad = max(x1 - x0, y1 - y0, 1500) * 0.6
            _fit(x0 - pad, x1 + pad, y0 - pad, y1 + pad, reserve)
        elif kind == "Node" and num in node_pos:
            x, y = node_pos[num]
            find_src.data = {"x": [x], "y": [y]}
            _fit(x - 3000, x + 3000, y - 3000, y + 3000, reserve)
        else:
            what = f"{kind} {text}" if num is not None else "a number"
            search_msg.object = (
                f"<div style='color:#b3261e;font-size:12px;padding:6px 2px 0'>"
                f"{'Enter ' + what if num is None else what + ' not found'}</div>"
            )

    def _search(_=None):
        # Editing Bokeh models straight from a widget callback fails without the document lock.
        text, kind = (search_in.value_input or "").strip(), search_kind.value
        searched.update(text=text, kind=kind)
        doc.add_next_tick_callback(lambda: _run_search(text, kind))

    def _clear_search():
        searched.update(text="", kind=None)
        find_src.data = {"x": [], "y": []}
        search_msg.object = ""
        chan_src.selected.indices = []

    def _on_edit(_):
        # Typing or deleting anything (or switching Channel/Node) drops the previous result.
        if searched["text"] and (
            (search_in.value_input or "").strip() != searched["text"] or search_kind.value != searched["kind"]
        ):
            doc.add_next_tick_callback(_clear_search)

    search_in.param.watch(lambda e: _search(), "value")
    search_in.param.watch(_on_edit, "value_input")
    search_kind.param.watch(_on_edit, "value")
    search_btn.on_click(_search)
    search_card = pn.Column(
        pn.Row(search_kind, search_in, search_btn, margin=0), search_msg,
        width=224, styles=_card_styles(padding="4px 8px"),
    )
    _overlay(search_card, top=16)
    search_card.styles = {**search_card.styles, "left": "50%", "transform": "translateX(-50%)"}

    map_area = pn.Column(
        pn.pane.Bokeh(p, sizing_mode="stretch_both", margin=0),
        layers, info_card, controls, popup_panel, bc_panel, src_panel, gate_panel, search_card,
        _overlay(
            pn.pane.Bokeh(coord_div, width=360, styles=_card_styles(padding="10px 14px", color="#555")),
            bottom=16, left=16,
        ),
        sizing_mode="stretch_width", margin=0,
        styles={"position": "relative", "height": "calc(100vh - 100px)"},
    )

    # --- Header ---------------------------------------------------------------------
    map_tab = pn.pane.HTML(
        f"<div style='display:flex;align-items:center;gap:8px;color:{_BLUE};font-weight:600;"
        f"padding:0 18px;height:56px;border-bottom:3px solid {_BLUE};box-sizing:border-box'>"
        f"{_MAP_ICON}<span>Map</span></div>",
        margin=(0, 0, 0, 24),
    )
    header_items = [
        map_tab, pn.HSpacer(),
        pn.widgets.Button(icon="help", button_type="light", width=40, margin=(8, 0)),
        pn.widgets.Button(icon="settings", button_type="light", width=40, margin=(8, 8)),
    ]

    return pn.template.FastListTemplate(
        title="DSM2 INPUT VIEWER",
        logo="data:image/svg+xml," + urllib.parse.quote(_LOGO_SVG),
        header=header_items,
        main=[map_area],
        main_layout=None,
        header_background="#ffffff",
        header_color="#1f2937",
        accent_base_color=_BLUE,
        theme_toggle=False,
        collapsed_sidebar=True,
        raw_css=[
            ".main-content, #main, #content, .content { padding: 0 !important; }",
            "#sidebar, #sidebar-button { display: none !important; }",
        ],
    )


@click.command(name="input_viewer")
@click.option(
    "--input", "echo_file", required=True,
    type=click.Path(dir_okay=False, exists=True, readable=True),
    help="DSM2 hydro echo .inp file containing the CHANNEL table.",
)
@click.option(
    "--channel", "channel_geo", default=None,
    type=click.Path(dir_okay=False, exists=True, readable=True),
    help="Channel centerline GeoJSON. Defaults to the bundled file.",
)
@click.option(
    "--node", "node_geo", default=None,
    type=click.Path(dir_okay=False, exists=True, readable=True),
    help="Node GeoJSON. Defaults to the bundled file.",
)
@click.option("--port", default=0, show_default=True, type=int, help="Server port (0 = random).")
def input_viewer(echo_file, channel_geo, node_geo, port):
    """Interactive map of DSM2 channels and nodes; click a channel for its properties."""
    pn.extension(
        notifications=True,
        disconnect_notification="Connection to the server was lost (timed out or server stopped). "
        "Relaunch <b>dsm2ui input_viewer</b> if needed, then reload this page.",
    )
    # Parse once up front; each browser session then builds its own widgets from the cache.
    _load_network(echo_file, channel_geo or _DEFAULT_CHANNEL_GEO, node_geo or _DEFAULT_NODE_GEO)
    pn.serve(
        lambda: build_input_viewer(echo_file, channel_geo, node_geo),
        port=port, title="DSM2 Input Viewer", show=True,
        websocket_max_message_size=200 * 1024 * 1024,  # full-resolution BC series can be tens of MB
    )
