# -*- coding: utf-8 -*-
"""Compare tab of the input viewer: channels, nodes, reservoirs and gates that differ between two echo files."""
import html
import math

import numpy as np
import panel as pn
from bokeh.events import MouseMove
from bokeh.models import (
    BoxZoomTool, ColumnDataSource, CustomJS, Div, HoverTool, Menu, PanTool, Range1d, WheelZoomTool,
)
from bokeh.plotting import figure
from shapely.geometry import LineString

from dsm2ui.channel_diff_popup import ChannelDiffPopup
from dsm2ui.diff_summary_popup import DiffSummaryPopup
from dsm2ui.flow_diff import diff_flows, is_drawn
from dsm2ui.flow_diff_popup import FlowDiffPopup
from dsm2ui.gate_diff import diff_gates
from dsm2ui.gate_diff_popup import GateDiffPopup
from dsm2ui.input_diff import diff_channels
from dsm2ui.input_viewer import (
    _ARROW_SIZE, _ARROW_URL, _BLUE, _CHANNEL_COLOR, _DIFF_COLOR, _DRAG_JS, _GATE_RED_URL,
    _MOUSE_COORD_JS, _NODE_COLOR, _OUTPUT_SIZE_JS, _PIN_RED_URL, _SIDEBAR_W, _TILES, _ZOOM_JS,
    _add_geojson_only_layers, _arrow_data,
    _build_reservoirs, _card_styles, _geojson_only, _icon_button, _load_boundary_rows, _load_echo, _load_geo,
    _load_gates, _load_network, _load_source_rows, _orient_lines, _overlay, _panel_styles, _read_bc_series,
    _resolve_dss, _swatch,
)
from dsm2ui.oprule_diff import default_series_differs
from dsm2ui.output_diff import diff_outputs
from dsm2ui.reservoir_diff import diff_reservoirs
from dsm2ui.reservoir_diff_popup import ReservoirDiffPopup
from dsm2ui.scalar_diff import diff_scalars

# Right-click menu on a highlighted channel, reservoir or gate; reads what is under the cursor from the hover inspection.
_DIFF_MENU_JS = """
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
view.el.addEventListener('contextmenu', (e) => {
  const gi = gsrc.inspected.indices, ri = rsrc.inspected.indices, ci = dsrc.inspected.indices;
  const fi = fsrc.inspected.indices, si = ssrc.inspected.indices;
  // A gate under the cursor wins over a boundary or source-flow icon, which wins over a reservoir, which wins over
  // a channel line; overlapping gates get one item each.
  let items = [];
  if (gi.length) {
    items = Array.from(gi, (i) => [
      gi.length > 1 ? `View gate diff (${gsrc.data.name[i]})` : 'View gate diff', 'gate', String(gsrc.data.name[i]),
    ]);
  } else if (fi.length || si.length) {
    if (fi.length) items.push(['View boundary diff', 'bc', String(fsrc.data.node[fi[0]])]);
    if (si.length) items.push(['View source flow diff', ssrc.data.layer[si[0]], String(ssrc.data.key[si[0]])]);
  } else if (ri.length) {
    items = [['View reservoir diff', 'res', String(rsrc.data.name[ri[0]])]];
  } else if (ci.length) {
    items = [['View channel diff', 'chan', String(dsrc.data.chan[ci[0]])]];
  }
  if (!items.length) { hide(); return; }
  e.preventDefault();
  menu.replaceChildren(...items.map(([text, kind, key]) => {
    const it = document.createElement('div');
    it.textContent = text;
    Object.assign(it.style, {padding: '8px 16px', cursor: 'pointer'});
    it.addEventListener('mouseenter', () => { it.style.background = '#eaf2ff'; });
    it.addEventListener('mouseleave', () => { it.style.background = 'white'; });
    it.addEventListener('click', () => { hide(); req.data = {kind: [kind], key: [key], n: [Date.now()]}; });
    return it;
  }));
  menu.style.left = e.clientX + 'px';
  menu.style.top = e.clientY + 'px';
  menu.style.display = 'block';
});
document.addEventListener('mousedown', (e) => { if (!menu.contains(e.target)) hide(); });
document.addEventListener('keydown', (e) => { if (e.key === 'Escape') hide(); });
window.addEventListener('wheel', hide, {passive: true});

// Hover highlight and tooltips for the base channels and nodes, drawn here like the Map tab.
const tip = document.createElement('div');
Object.assign(tip.style, {
  position: 'fixed', display: 'none', zIndex: 9999, pointerEvents: 'none', background: 'white',
  padding: '6px 10px', border: '1px solid #ccc', borderRadius: '4px', font: '12px sans-serif',
  boxShadow: '0 1px 6px rgba(0,0,0,0.2)',
});
document.body.appendChild(tip);
let lit = -1;
const setHover = (i) => {
  if (i === lit) return;
  lit = i;
  hsrc.data = i < 0 ? {xs: [], ys: []} : {xs: [bsrc.data.xs[i]], ys: [bsrc.data.ys[i]]};
};
view.el.addEventListener('mousemove', (e) => {
  // Gate and reservoir tooltips (drawn by Bokeh) take precedence, as in the Map tab.
  if (gsrc.inspected.indices.length || rsrc.inspected.indices.length || fsrc.inspected.indices.length ||
      ssrc.inspected.indices.length || osrc.inspected.indices.length) {
    tip.style.display = 'none';
    setHover(-1);
    return;
  }
  const ni = nsrc.inspected.indices, ci = bsrc.inspected.indices;
  if (ni.length) {
    tip.innerHTML = `<b>Node:</b> ${nsrc.data.node[ni[0]]}`;
    setHover(-1);
  } else if (ci.length) {
    const i = ci[0];
    tip.innerHTML = `<b>Channel:</b> ${bsrc.data.chan[i]}<br><b>Up \u2192 Down:</b> ${bsrc.data.up[i]} \u2192 ${bsrc.data.down[i]}`;
    setHover(i);
  } else {
    tip.style.display = 'none';
    setHover(-1);
    return;
  }
  tip.style.left = (e.clientX + 14) + 'px';
  tip.style.top = (e.clientY + 14) + 'px';
  tip.style.display = 'block';
});
view.el.addEventListener('mouseleave', () => { tip.style.display = 'none'; setHover(-1); });
"""


# Red reservoir squares grow with the zoom level, like the reservoir icons of the Map tab (10-56 px).
_DIFF_RES_SIZE_JS = """
let w;
try { w = p.inner_width; } catch (e) { return; }
const x = p.x_range;
if (!w || x.start == null || x.end == null) return;
rend.glyph.size = Math.min(56, Math.max(10, 14 * Math.pow(home_w / (x.end - x.start), 0.3)));
"""

# Red gate icons scale with zoom like the Map tab's (12-40 px wide); channel gates stay a fixed screen distance
# from their node along the channel.
_DIFF_GATE_SIZE_JS = """
let w, h;
try { w = p.inner_width; h = p.inner_height; } catch (e) { return; }
const x = p.x_range;
if (!w || !h || x.start == null || x.end == null) return;
const home = Math.max(home_w, home_h * w / h);
const s = Math.min(40, Math.max(12, 16 * Math.pow(home / (x.end - x.start), 0.18)));
rend.glyph.w = {value: s, units: 'screen'};
rend.glyph.h = {value: s * 0.62, units: 'screen'};
hit.glyph.size = s * 0.8;
const off = (s * 0.5 + 8) * (x.end - x.start) / w;
const d = src.data;
src.data = Object.assign({}, d, {
  x: Array.from(d.nx, (v, i) => v + d.dx[i] * off),
  y: Array.from(d.ny, (v, i) => v + d.dy[i] * off),
});
"""


def _gate_geometry(gate_diff, echo_file, chans, lines, node_pos):
    """Columns for the changed gates, placed where the base file has them; new gates are not drawn.

    Channel gates sit beside their node along the channel, reservoir gates on the node, as in the Map tab.
    """
    chan_pos = {int(n): i for i, n in enumerate(chans["CHAN_NO"])}
    cols = {key: [] for key in ("x", "y", "nx", "ny", "dx", "dy", "name", "info", "url")}

    def add(name, x, y, dx, dy, info):
        for key, value in zip(cols, (x, y, x, y, dx, dy, name, info, _GATE_RED_URL)):
            cols[key].append(value)

    for name, obj, ident, node in _load_gates(echo_file):
        if name not in gate_diff.gates:
            continue
        if obj == "channel" and ident.isdigit() and int(ident) in chan_pos:
            ci = chan_pos[int(ident)]
            up, down = int(chans["UPNODE"].iloc[ci]), int(chans["DOWNNODE"].iloc[ci])
            ln = lines[ci]
            if node == up:
                (x0, y0), (x1, y1) = ln[0][:2], ln[1][:2]
            elif node == down:
                (x0, y0), (x1, y1) = ln[-1][:2], ln[-2][:2]
            else:
                continue
            norm = math.hypot(x1 - x0, y1 - y0) or 1.0
            info = (
                f"<b>Channel:</b> {ident}<br><b>Up \u2192 Down:</b> {up} \u2192 {down}<br><b>Gate:</b> {html.escape(name)}"
            )
            add(name, x0, y0, (x1 - x0) / norm, (y1 - y0) / norm, info)
        elif obj == "reservoir" and node in node_pos:
            add(name, *node_pos[node], 0.0, 0.0, f"<b>Node:</b> {node}<br><b>Gate:</b> {html.escape(name)}")
    return cols


def _flow_geometry(flow_diff, base_file, node_pos, node_geo):
    """Return (boundary columns, source-flow columns) for the changed flow inputs, at their base locations.

    New rows are not drawn. Boundary rows sit on their node, source flows on their node, or on the reservoir for
    SOURCE_FLOW_RESERVOIR. Tooltips are the Map tab's, describing the base file.
    """
    base_res = _build_reservoirs(base_file, _load_geo(node_geo))[0]
    res_pos = dict(zip(base_res["name"], zip(base_res["x"], base_res["y"])))
    res_info = dict(zip(base_res["name"], base_res["info"]))
    bc_rows, src_rows = _load_boundary_rows(base_file), _load_source_rows(base_file)
    locations = {
        (e["layer"], e["base"]["where"].lower() if e["layer"] == "ressrc" else int(e["base"]["where"]))
        for e in flow_diff.entries if is_drawn(e)
    }

    def source_info(node):
        rows = src_rows.get(node)
        if not rows:
            return ""
        n_dicu = sum(r["group"] == "DICU" for r in rows)
        return f"<br><b>Source flow:</b> {n_dicu} DICU, {len(rows) - n_dicu} other"

    def node_info(node):
        if node in bc_rows:
            inputs = html.escape(", ".join(f"{r['name']} ({r['kind']})" for r in bc_rows[node]))
            return f"<b>Boundary node:</b> {node}<br><b>Inputs:</b> {inputs}{source_info(node)}"
        return f"<b>Node:</b> {node}{source_info(node)}"

    bc = {"x": [], "y": [], "node": [], "info": []}
    src = {"x": [], "y": [], "layer": [], "key": [], "info": []}
    for layer, where in sorted(locations, key=str):
        if layer == "bc" and where in node_pos:
            x, y = node_pos[where]
            for key, value in zip(bc, (x, y, where, node_info(where))):
                bc[key].append(value)
        elif layer == "src" and where in node_pos:
            x, y = node_pos[where]
            for key, value in zip(src, (x, y, layer, str(where), node_info(where))):
                src[key].append(value)
        elif layer == "ressrc" and where in res_pos:
            x, y = res_pos[where]
            for key, value in zip(src, (x, y, layer, where, res_info[where])):
                src[key].append(value)
    return bc, src


def _output_geometry(output_diff, chans, lines):
    """Return (pin columns, hit-target columns) for the changed and removed output locations, at their base positions.

    Pins are placed along the channel from the upnode as in the Map tab; new locations are not drawn. The tooltip
    lists, for each name at the pin, its base and scenario locations.
    """
    chan_geom = {int(n): LineString(ln) for n, ln in zip(chans["CHAN_NO"], lines)}
    chan_length = dict(zip(chans["CHAN_NO"].astype(int), chans["LENGTH"]))
    entries_at = {}
    for e in output_diff.drawn():
        for loc in e["base"]:
            entries_at.setdefault(loc, []).append(e)

    def where(locs):
        if not locs:
            return "removed"
        return "; ".join(f"channel {c} @ {'length' if d is None else f'{d:,.0f} ft'}" for c, d in locs)

    pins = {"x": [], "y": [], "url": []}
    hits = {"x": [], "y0": [], "y": [], "label": []}
    for (chan, dist), entries in entries_at.items():
        if chan not in chan_geom:
            continue
        length = float(chan_length[chan])
        dist_ft = length if dist is None else dist
        pt = chan_geom[chan].interpolate(min(max(dist_ft / length, 0.0), 1.0), normalized=True)
        label = "<br>".join(
            f"<b>{html.escape(e['name'])}</b><br>Base: {where(e['base'])}<br>Scenario: {where(e['scenario'])}"
            for e in entries
        )
        for key, value in zip(pins, (pt.x, pt.y, _PIN_RED_URL)):
            pins[key].append(value)
        for key, value in zip(hits, (pt.x, pt.y, pt.y, label)):
            hits[key].append(value)
    return pins, hits


def _diff_geometry(diff, channel_geo, node_geo):
    """Return (channel columns, node columns) for the differences; those with no geometry are skipped."""
    chan_geo, node_gdf = _load_geo(channel_geo), _load_geo(node_geo)
    chan_cols = {"xs": [], "ys": [], "chan": [], "up": [], "down": []}
    node_cols = {"x": [], "y": [], "node": []}
    for chan in diff.channels:
        if chan not in chan_geo.index:
            continue
        coords = [c for part in chan_geo.geometry[chan].geoms for c in part.coords]
        rec = diff.scenario.rows.get(chan) or diff.base.rows[chan]
        chan_cols["xs"].append([c[0] for c in coords])
        chan_cols["ys"].append([c[1] for c in coords])
        chan_cols["chan"].append(chan)
        chan_cols["up"].append(int(rec["UPNODE"]))
        chan_cols["down"].append(int(rec["DOWNNODE"]))
    for node in diff.nodes:
        if node not in node_gdf.index:
            continue
        pt = node_gdf.geometry[node]
        node_cols["x"].append(pt.x)
        node_cols["y"].append(pt.y)
        node_cols["node"].append(node)
    return chan_cols, node_cols


def _reservoir_geometry(res_diff, base_file, node_geo):
    """Return (reservoir columns, connection-line columns) of the differing reservoirs, to be drawn in red.

    Only the base network is drawn: new reservoirs and connections added in the scenario have no base line.
    A line is red when that connection changed or was removed, or the reservoir was removed.
    """
    base_res, base_conn = _build_reservoirs(base_file, _load_geo(node_geo))
    res_cols = {key: [] for key in base_res}
    for i, name in enumerate(base_res["name"]):
        if name in res_diff.reservoirs:
            for key in res_cols:
                res_cols[key].append(base_res[key][i])
    red_nodes = {}
    for name in res_cols["name"]:
        detail = res_diff.detail(name)
        red_nodes[name] = None if detail["status"] == "removed" else {c["node"] for c in detail["connections"] if c["changed"]}
    conn_cols = {key: [] for key in base_conn}
    for i, (name, node) in enumerate(zip(base_conn["name"], base_conn["node"])):
        if name in red_nodes and (red_nodes[name] is None or node in red_nodes[name]):
            for key in conn_cols:
                conn_cols[key].append(base_conn[key][i])
    return res_cols, conn_cols


def _section(title, rows):
    """A collapsible sidebar block: heading with a chevron, one row per (checkbox, swatch)."""
    collapse = pn.widgets.Button(icon="chevron-up", button_type="light", width=36, height=36, margin=0)
    body = pn.Column(
        *(pn.Row(cb, pn.HSpacer(), swatch, margin=0) for cb, swatch in rows),
        margin=0,
    )

    def _toggle(_):
        body.visible = not body.visible
        collapse.icon = "chevron-up" if body.visible else "chevron-down"

    collapse.on_click(_toggle)
    return pn.Column(
        pn.Row(
            pn.pane.HTML(f"<b style='letter-spacing:.5px;font-size:15px'>{title}</b>", margin=(8, 0, 0, 0)),
            pn.HSpacer(), collapse, margin=0,
        ),
        body, width=_SIDEBAR_W, margin=0, styles=_panel_styles(),
    )


def _clean_series(df):
    """First column of ``df`` as a sorted float series without DSS missing-value flags (-901, -902) and NaN."""
    s = df.iloc[:, 0].astype(float)
    return s.mask(s.isin([-901.0, -902.0])).dropna().sort_index()


def series_values_differ(base, scenario):
    """True when two cleaned series differ in timestamps or values."""
    return not (base.index.equals(scenario.index) and np.allclose(base.to_numpy(), scenario.to_numpy(), rtol=1e-9, atol=1e-9))


def make_series_differs(base_file, scenario_file):
    """Return ``series_differs(base_row, scenario_row)`` for ``diff_gates``: compares the data, not the file names.

    Identical definitions that resolve to the same file are the same series without reading any data. When a series
    cannot be read, it differs only if its definition does.
    """

    def is_constant(row):
        return row["file"].strip().lower() == "constant"

    def differs(b, s):
        by_definition = default_series_differs(b, s)[0]
        if is_constant(b) and is_constant(s):
            try:
                return not math.isclose(float(b["path"]), float(s["path"]), rel_tol=1e-9, abs_tol=1e-9), None
            except ValueError:
                return by_definition, None
        if not by_definition and not is_constant(b):
            try:
                if _resolve_dss(base_file, b["file"]) == _resolve_dss(scenario_file, s["file"]):
                    return False, None
            except FileNotFoundError:
                return False, None
        try:
            return series_values_differ(
                _clean_series(_read_bc_series(base_file, b["file"], b["path"])[0]),
                _clean_series(_read_bc_series(scenario_file, s["file"], s["path"])[0]),
            ), None
        except Exception as exc:  # unreadable data: fall back to the definitions
            return by_definition, (f"values could not be compared ({exc})" if by_definition else None)

    return differs


def build_compare_view(base_file, scenario_file, channel_geo, node_geo):
    """Return the Compare tab content."""
    chans, nodes = _load_network(base_file, channel_geo, node_geo)
    diff = diff_channels(_load_echo(base_file), _load_echo(scenario_file))
    diff_chan_cols, diff_node_cols = _diff_geometry(diff, channel_geo, node_geo)
    res_diff = diff_reservoirs(_load_echo(base_file), _load_echo(scenario_file))
    diff_res_cols, diff_conn_cols = _reservoir_geometry(res_diff, base_file, node_geo)
    series_differs = make_series_differs(base_file, scenario_file)
    gate_diff = diff_gates(_load_echo(base_file), _load_echo(scenario_file), series_differs)
    flow_diff = diff_flows(_load_echo(base_file), _load_echo(scenario_file), series_differs)
    output_diff = diff_outputs(_load_echo(base_file), _load_echo(scenario_file))
    scalar_diff = diff_scalars(_load_echo(base_file), _load_echo(scenario_file))

    lines = [[c for part in g.geoms for c in part.coords] for g in chans.geometry]
    lines = _orient_lines(lines, chans, nodes)
    chan_src = ColumnDataSource({
        "xs": [[c[0] for c in ln] for ln in lines], "ys": [[c[1] for c in ln] for ln in lines],
        "chan": chans["CHAN_NO"].tolist(), "up": chans["UPNODE"].tolist(), "down": chans["DOWNNODE"].tolist(),
    })
    node_src = ColumnDataSource(
        {"x": nodes.geometry.x.tolist(), "y": nodes.geometry.y.tolist(), "node": nodes["id"].tolist()}
    )
    ax, ay, aangle = _arrow_data(lines)
    arrow_src = ColumnDataSource({"x": ax, "y": ay, "angle": aangle, "url": [_ARROW_URL] * len(ax)})

    minx, miny, maxx, maxy = chans.total_bounds
    padx, pady = (maxx - minx) * 0.12, (maxy - miny) * 0.12
    pan = PanTool()
    wheel = WheelZoomTool(speed=1 / 250)
    p = figure(
        x_axis_type="mercator", y_axis_type="mercator",
        x_range=Range1d(minx - padx, maxx + padx), y_range=Range1d(miny - pady, maxy + pady),
        tools=[pan, wheel, BoxZoomTool()], active_drag=pan, active_scroll=wheel,
        match_aspect=True, sizing_mode="stretch_both", toolbar_location=None, min_border=0,
        outline_line_color=None,
    )
    p.axis.visible = False
    p.grid.visible = False
    p.add_tile(_TILES)
    _add_geojson_only_layers(p, *_geojson_only(base_file, channel_geo, node_geo))

    chan_r = p.multi_line("xs", "ys", source=chan_src, line_width=3, line_color=_CHANNEL_COLOR)
    diff_chan_src = ColumnDataSource(diff_chan_cols)
    diff_node_src = ColumnDataSource(diff_node_cols)
    diff_chan_r = p.multi_line("xs", "ys", source=diff_chan_src, line_width=5, line_color=_DIFF_COLOR)
    # Drawn by the page script so a node under the cursor can suppress it, as in the Map tab.
    hover_src = ColumnDataSource({"xs": [], "ys": []})
    p.multi_line("xs", "ys", source=hover_src, line_width=5, line_color=_BLUE)
    arrow_r = p.image_url(
        url="url", x="x", y="y", w=_ARROW_SIZE[0], h=_ARROW_SIZE[1], angle="angle",
        anchor="center", w_units="screen", h_units="screen", source=arrow_src,
    )
    node_r = p.scatter(
        "x", "y", source=node_src, size=10, fill_color="white", fill_alpha=0.6,
        line_color=_NODE_COLOR, line_width=1.5,
    )
    diff_node_r = p.scatter(
        "x", "y", source=diff_node_src, size=12, fill_alpha=0, line_color=_DIFF_COLOR, line_width=2,
    )
    diff_res_src = ColumnDataSource(diff_res_cols)
    diff_conn_r = p.multi_line(
        "xs", "ys", source=ColumnDataSource(diff_conn_cols), line_width=3, line_color=_DIFF_COLOR, line_dash="dashed",
    )
    diff_res_r = p.scatter(
        "x", "y", source=diff_res_src, marker="square", size=14, fill_color=_DIFF_COLOR, line_color="white",
        line_width=1,
    )
    res_size_cb = CustomJS(args=dict(p=p, rend=diff_res_r, home_w=maxx - minx + 2 * padx), code=_DIFF_RES_SIZE_JS)
    for rng in (p.x_range, p.y_range):
        rng.js_on_change("start", res_size_cb)
        rng.js_on_change("end", res_size_cb)
    p.js_on_change("inner_width", res_size_cb)
    node_pos = {int(n): (x, y) for n, x, y in zip(nodes["id"], nodes.geometry.x, nodes.geometry.y)}
    diff_gate_src = ColumnDataSource(_gate_geometry(gate_diff, base_file, chans, lines, node_pos))
    diff_gate_r = p.image_url(
        url="url", x="x", y="y", w=16, h=10, anchor="center", w_units="screen", h_units="screen",
        source=diff_gate_src,
    )
    diff_gate_hit = p.scatter("x", "y", source=diff_gate_src, marker="square", size=18, fill_alpha=0, line_alpha=0)
    gate_size_cb = CustomJS(
        args=dict(
            p=p, rend=diff_gate_r, hit=diff_gate_hit, src=diff_gate_src,
            home_w=maxx - minx + 2 * padx, home_h=maxy - miny + 2 * pady,
        ),
        code=_DIFF_GATE_SIZE_JS,
    )
    for rng in (p.x_range, p.y_range):
        rng.js_on_change("start", gate_size_cb)
        rng.js_on_change("end", gate_size_cb)
    p.js_on_change("inner_width", gate_size_cb)
    # Boundary triangles and source-flow dots keep the Map tab's icons and sizes, in red; the larger transparent
    # hit targets make them easy to hover and right-click.
    bc_cols, src_cols = _flow_geometry(flow_diff, base_file, node_pos, node_geo)
    diff_bc_src, diff_flow_src = ColumnDataSource(bc_cols), ColumnDataSource(src_cols)
    diff_bc_r = p.scatter(
        "x", "y", source=diff_bc_src, marker="triangle", size=6, fill_color=_DIFF_COLOR, line_color=_DIFF_COLOR,
        line_width=1,
    )
    diff_bc_hit = p.scatter("x", "y", source=diff_bc_src, size=16, fill_alpha=0, line_alpha=0)
    diff_flow_r = p.scatter("x", "y", source=diff_flow_src, size=6, fill_color=_DIFF_COLOR, line_alpha=0)
    diff_flow_hit = p.scatter("x", "y", source=diff_flow_src, size=10, fill_alpha=0, line_alpha=0)
    # Pins keep the Map tab's icon and zoom behaviour; the hover target sits on the pin head.
    pin_cols, pin_hit_cols = _output_geometry(output_diff, chans, lines)
    diff_out_hit_src = ColumnDataSource(pin_hit_cols)
    diff_out_r = p.image_url(
        url="url", x="x", y="y", w=6, h=9, anchor="bottom_center", w_units="screen", h_units="screen",
        source=ColumnDataSource(pin_cols),
    )
    diff_out_hit = p.scatter("x", "y", source=diff_out_hit_src, size=13, fill_alpha=0, line_alpha=0)
    out_size_cb = CustomJS(
        args=dict(
            p=p, rend=diff_out_r, hit=diff_out_hit, hit_src=diff_out_hit_src,
            home_w=maxx - minx + 2 * padx, home_h=maxy - miny + 2 * pady,
        ),
        code=_OUTPUT_SIZE_JS,
    )
    for rng in (p.x_range, p.y_range):
        rng.js_on_change("start", out_size_cb)
        rng.js_on_change("end", out_size_cb)
    p.js_on_change("inner_width", out_size_cb)
    # Tooltips and hover highlight cover the base echo file only; channels and nodes new in the scenario have none.
    p.add_tools(
        HoverTool(renderers=[chan_r], tooltips=None),  # inspection only; the tooltip is drawn in JS
        HoverTool(renderers=[node_r], tooltips=None),  # inspection only; the tooltip is drawn in JS
        HoverTool(renderers=[diff_chan_r], tooltips=None, line_policy="nearest"),  # inspection for the right-click menu
        HoverTool(renderers=[diff_res_r], tooltips="@info{safe}"),  # also the right-click menu's target
        HoverTool(renderers=[diff_gate_hit], tooltips="@info{safe}"),  # also the right-click menu's target
        HoverTool(renderers=[diff_bc_hit], tooltips="@info{safe}"),  # also the right-click menu's target
        HoverTool(renderers=[diff_flow_hit], tooltips="@info{safe}"),  # also the right-click menu's target
        HoverTool(renderers=[diff_out_hit], tooltips="@label{safe}"),
    )

    popup = ChannelDiffPopup()
    popup_panel = _overlay(popup.panel, top=16, right=76)
    popup_panel.styles = {**popup_panel.styles, "z-index": "20"}
    summary_popup = DiffSummaryPopup()
    summary_panel = _overlay(summary_popup.panel, top=16, right=76)
    summary_panel.styles = {**summary_panel.styles, "z-index": "20"}
    res_popup = ReservoirDiffPopup()
    res_panel = _overlay(res_popup.panel, top=16, right=76)
    res_panel.styles = {**res_panel.styles, "z-index": "20"}
    gate_popup = GateDiffPopup(
        lambda side, row: _read_bc_series(base_file if side == "base" else scenario_file, row["file"], row["path"])
    )
    gate_panel = _overlay(gate_popup.panel, top=16, right=76)
    gate_panel.styles = {**gate_panel.styles, "z-index": "20"}
    flow_popup = FlowDiffPopup(
        lambda side, row: _read_bc_series(base_file if side == "base" else scenario_file, row["file"], row["path"])
    )
    flow_panel = _overlay(flow_popup.panel, top=16, right=76)
    flow_panel.styles = {**flow_panel.styles, "z-index": "20"}
    popups = [popup, res_popup, gate_popup, flow_popup, summary_popup]
    req_src = ColumnDataSource({"kind": [""], "key": [""], "n": [0]})

    def _open(window, *args):
        for other in popups:
            if other is not window:
                other.hide()
        window.show(*args)

    def _open_request(attr, old, new):
        kind, key = req_src.data["kind"][0], str(req_src.data["key"][0])
        if kind == "chan":
            _open(popup, diff.detail(int(key)))
        elif kind == "res":
            _open(res_popup, res_diff.detail(key))
        elif kind == "gate":
            _open(gate_popup, gate_diff, key)
        elif kind in ("bc", "src", "ressrc"):
            _open(flow_popup, kind, key, flow_diff.drawn_at(kind, key))

    req_src.on_change("data", _open_request)
    p.context_menu = Menu(items=[])  # suppress Bokeh's default tool menu
    p.js_on_event(MouseMove, CustomJS(
        args=dict(p=p, dsrc=diff_chan_src, rsrc=diff_res_src, gsrc=diff_gate_src, fsrc=diff_bc_src, ssrc=diff_flow_src,
                  osrc=diff_out_hit_src,
                  bsrc=chan_src, nsrc=node_src, hsrc=hover_src, req=req_src),
        code=_DIFF_MENU_JS + _DRAG_JS,
    ))

    def _checkbox(name, *renderers):
        cb = pn.widgets.Checkbox(name=name, value=True, margin=(8, 0))
        for r in renderers:
            cb.jslink(r, value="visible")
        return cb

    summary_btn = pn.widgets.Button(
        name="Diff Summary", icon="list-details", button_type="primary", button_style="outline",
        width=_SIDEBAR_W - 34, margin=(8, 0),
    )

    def _show_summary(_):
        _open(summary_popup, {**diff.summary(), **res_diff.summary(), **gate_diff.summary(), **flow_diff.summary(), **output_diff.summary(),
                              **scalar_diff.summary()})

    summary_btn.on_click(_show_summary)
    summary_panel_btn = pn.Column(summary_btn, width=_SIDEBAR_W, margin=0, styles=_panel_styles())

    base_layers = _section("BASE LAYERS", [
        (_checkbox("Channels", chan_r), _swatch("line")),
        (_checkbox("Channel Nodes", node_r), _swatch("circle")),
        (_checkbox("Channel Direction", arrow_r), _swatch("arrow")),
    ])
    diff_layers = _section("DIFF LAYERS", [
        (_checkbox("Changed Channels", diff_chan_r), _swatch("diff_line")),
        (_checkbox("Changed Nodes", diff_node_r), _swatch("diff_circle")),
        (_checkbox("Changed Reservoirs", diff_res_r), _swatch("diff_square")),
        (_checkbox("Changed Res. Connections", diff_conn_r), _swatch("diff_dashed")),
        (_checkbox("Changed Gates", diff_gate_r, diff_gate_hit), _swatch("diff_gate")),
        (_checkbox("Changed Boundary Nodes", diff_bc_r, diff_bc_hit), _swatch("diff_triangle")),
        (_checkbox("Changed Source Flow", diff_flow_r, diff_flow_hit), _swatch("diff_dot")),
        (_checkbox("Changed Output Locations", diff_out_r, diff_out_hit), _swatch("diff_pin")),
    ])
    sidebar = pn.Column(
        summary_panel_btn, base_layers, diff_layers, width=_SIDEBAR_W, margin=0,
        styles={
            "background": "white", "height": "100%", "overflow-y": "auto", "flex": "none",
            "border-right": "1px solid #d9dde3", "box-shadow": "2px 0 6px rgba(0,0,0,0.08)", "z-index": "30",
        },
    )

    zoom_in, zoom_out, home, layers_btn = (_icon_button(i) for i in ("plus", "minus", "home", "stack-2"))
    zoom_in.js_on_click(args=dict(p=p, factor=0.5), code=_ZOOM_JS)
    zoom_out.js_on_click(args=dict(p=p, factor=2.0), code=_ZOOM_JS)
    home.js_on_click(args=dict(p=p), code="p.reset.emit();")
    layers_btn.on_click(lambda _: setattr(sidebar, "visible", not sidebar.visible))
    controls = pn.Column(zoom_in, zoom_out, home, layers_btn, styles=_card_styles(padding="4px"))
    _overlay(controls, top=16, right=16)

    coord_div = Div(text="Lat: --- &nbsp;|&nbsp; Lon: --- &nbsp;|&nbsp; NAD 83")
    p.js_on_event(MouseMove, CustomJS(args=dict(div=coord_div), code=_MOUSE_COORD_JS))
    map_area = pn.Column(
        pn.pane.Bokeh(p, sizing_mode="stretch_both", margin=0),
        controls, popup_panel, res_panel, gate_panel, flow_panel, summary_panel,
        _overlay(
            pn.pane.Bokeh(
                coord_div, width=270,
                styles=_card_styles(padding="3px 8px", color="#555", **{"font-size": "11px", "border-radius": "8px"}),
            ),
            bottom=10, left=10,
        ),
        sizing_mode="stretch_width", margin=0,
        styles={"position": "relative", "height": "100%"},
    )
    return pn.Row(sidebar, map_area, sizing_mode="stretch_both", margin=0)
