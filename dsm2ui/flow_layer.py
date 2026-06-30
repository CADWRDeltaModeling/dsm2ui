"""DSM2 flow visualization layer — arrows and junction bars.

:class:`FlowLayerSpec` describes which channels to show as flow arrows and
which nodes to show as two-sided flow-split bars.  :class:`FlowLayer` renders
these elements as Bokeh Patch glyphs on an existing Bokeh figure and updates
them every animation frame.  :class:`FlowAnimatorManager` is a standalone
Panel Viewer that wraps :class:`FlowLayer` with a CARTO Light tile background
and player controls.

Geometry is computed in EPSG:3857 (metres) so arrow lengths and bar heights
are physically meaningful.

Typical usage — combined with qual animation::

    from dsm2ui.animate import animate_qual, load_dsm2_nodes_gdf
    from dsm2ui.flow_layer import FlowLayerSpec, FlowLayer

    spec = FlowLayerSpec.from_yaml("flow_config.yaml")
    mgr  = animate_qual("ec.h5", constituent="ec")
    nodes_gdf = load_dsm2_nodes_gdf()

    from dsm2ui.animate import load_dsm2_channel_gdf
    ch_gdf = load_dsm2_channel_gdf()
    flow   = FlowLayer("hydro.h5", spec, ch_gdf, nodes_gdf)
    flow.setup_on_figure(mgr._bk_figure)
    mgr.add_frame_callback(flow.update_frame)

Standalone flow-only animation::

    from dsm2ui.animate import animate_flow
    mgr = animate_flow("hydro.h5", spec)
    mgr.servable()
"""

from __future__ import annotations

import dataclasses
import logging
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd
import panel as pn
from bokeh.models import (
    ColumnDataSource,
    Div,
    HoverTool,
    Range1d,
    WMTSTileSource,
)
from bokeh.plotting import figure as bk_figure

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: Bokeh Category20 colour palette used for junction bar segments.
_CATEGORY20: List[str] = [
    "#1f77b4", "#aec7e8", "#ff7f0e", "#ffbb78",
    "#2ca02c", "#98df8a", "#d62728", "#ff9896",
    "#9467bd", "#c5b0d5", "#8c564b", "#c49c94",
    "#e377c2", "#f7b6d2", "#7f7f7f", "#c7c7c7",
    "#bcbd22", "#dbdb8d", "#17becf", "#9edae5",
]

_CARTO_LIGHT_URL = "https://basemaps.cartocdn.com/light_all/{Z}/{X}/{Y}.png"
_CARTO_LIGHT_ATTR = "© CARTO / © OpenStreetMap contributors"


# ===========================================================================
# Configuration dataclasses
# ===========================================================================

@dataclasses.dataclass
class ChannelArrowSpec:
    """Configuration for one flow arrow on a DSM2 channel.

    Parameters
    ----------
    channel : int
        DSM2 channel number (CHAN_NO).
    position : float, optional
        Fractional position along the channel centreline where the arrow is
        centred.  ``0.0`` = upstream end, ``1.0`` = downstream end,
        ``0.5`` (default) = midpoint.
    label : str, optional
        Optional label shown in hover tooltip.
    point : list of float [lat, lon] or None, optional
        Optional geographic override for the arrow centroid in WGS84 decimal
        degrees ``[latitude, longitude]``.  When provided the computed
        centreline position is ignored; the tangent direction is still derived
        from the channel geometry.
    """

    channel: int
    position: float = 0.5
    label: str = ""
    point: Optional[List[float]] = None


@dataclasses.dataclass
class NodeBarSpec:
    """Configuration for a two-sided flow-split bar at a DSM2 node.

    Parameters
    ----------
    node : int
        DSM2 node number.
    channels : list of int or None, optional
        Channel numbers to include in the split display.  When ``None``
        (default) all channels connected to the node are used.
    label : str, optional
        Optional label shown in hover tooltip.
    """

    node: int
    channels: Optional[List[int]] = None
    label: str = ""


@dataclasses.dataclass
class ReservoirArrowSpec:
    """Configuration for a flow arrow at a reservoir-to-node connection.

    Reads ``/hydro/data/reservoir flow`` using the HDF5 geometry table
    ``/hydro/geometry/reservoir_node_connect``.

    Parameters
    ----------
    reservoir : str
        Reservoir name exactly as stored in the HDF5 (lowercase).
        Examples: ``"clifton_court"``, ``"franks_tract"``.
    node : int
        DSM2 node number the reservoir connects to.  Identifies the correct
        flow column AND the arrow map position (from *nodes_gdf*).
    direction_deg : float, optional
        Arrow pointing direction in degrees — standard math convention:
        0\u202f=\u202fEast, 90\u202f=\u202fNorth, 180\u202f=\u202fWest, 270\u202f=\u202fSouth.
        Default 270 (southward, typical for export-side reservoirs).
    label : str, optional
        Display label shown in hover tooltip.
    """

    reservoir: str
    node: int
    direction_deg: float = 270.0
    label: str = ""
    point: Optional[List[float]] = None
    """Optional ``[lat, lon]`` override for arrow position on the map.

    When supplied the geographic position of *node* is ignored and the arrow
    is placed at this WGS84 coordinate instead.  ``node`` is still required
    to identify the correct flow column.
    """


@dataclasses.dataclass
class QextArrowSpec:
    """Configuration for a flow arrow for an external (qext) source/sink.

    Reads ``/hydro/data/qext flow``.  Use this for special boundary sinks
    such as **swp** (SWP Banks Pumping) and **cvp** (CVP Jones Pumping)
    that are modelled as source flows applied to a reservoir node.

    Parameters
    ----------
    name : str
        Qext name exactly as stored in the HDF5 (lowercase).
        Examples: ``"swp"``, ``"cvp"``.
    node : int
        DSM2 node number used only for map position lookup.
    direction_deg : float, optional
        Arrow pointing direction in degrees.  Default 270 (southward).
    label : str, optional
        Display label shown in hover tooltip.
    """

    name: str
    node: Optional[int] = None
    direction_deg: float = 270.0
    label: str = ""
    point: Optional[List[float]] = None
    """Optional ``[lat, lon]`` override for arrow position on the map.

    Either *node* **or** *point* must be supplied.  When both are given
    *point* takes precedence and *node* is used only as a display hint.
    """

    def __post_init__(self) -> None:
        if self.node is None and self.point is None:
            raise ValueError(
                f"QextArrowSpec '{self.name}': either 'node' or 'point' "
                "must be specified to provide a map position."
            )


@dataclasses.dataclass
class FlowLayerSpec:
    """Complete specification for the flow visualization layer.

    Parameters
    ----------
    arrows : list of ChannelArrowSpec
        Flow arrows to display.
    bars : list of NodeBarSpec
        Junction flow-split bars to display.
    variable : {"flow", "velocity"}, optional
        Data source for arrows and bars.  ``"flow"`` (default) reads channel
        flow (cfs) from the HYDRO HDF5; ``"velocity"`` reads velocity (ft/s)
        computed as flow / cross-sectional area.  Use ``scale_mode: linear``
        with velocity for physically meaningful arrow lengths.
    scale_mode : {"linear", "log"}, optional
        Arrow length scaling mode.  ``"linear"`` (default) scales length
        linearly with ``|value| / reference``; ``"log"`` uses
        log10(1 + |value| / reference).  ``"linear"`` is recommended when
        *variable* is ``"velocity"``.
    reference_flow : float, optional
        Flow value in cfs that maps to *reference_arrow_length_m* (and
        *bar_max_height_m*) when *variable* is ``"flow"``.  Default 10 000 cfs.
    reference_velocity : float, optional
        Velocity in ft/s that maps to *reference_arrow_length_m* when
        *variable* is ``"velocity"``.  Default 2.5 ft/s.
    reference_arrow_length_m : float, optional
        Arrow length in EPSG:3857 metres for the reference value.  Default 500 m.
    arrow_width_m : float, optional
        Arrow body half-width in metres.  Default 150 m.
    bar_width_m : float, optional
        Width of each bar side (inflow and outflow are separate sides) in
        metres.  Default 200 m.
    bar_max_height_m : float, optional
        Bar height in metres for the reference value.  Default 600 m.
    min_flow_cfs : float, optional
        Value magnitude below which a stub symbol is rendered instead of a
        full arrow.  Default 10 (cfs for flow mode, ft/s for velocity mode).
    """

    arrows: List[ChannelArrowSpec] = dataclasses.field(default_factory=list)
    bars: List[NodeBarSpec] = dataclasses.field(default_factory=list)
    reservoir_arrows: List[ReservoirArrowSpec] = dataclasses.field(default_factory=list)
    qext_arrows: List[QextArrowSpec] = dataclasses.field(default_factory=list)
    scale_mode: str = "linear"
    reference_flow: float = 10_000.0
    reference_arrow_length_m: float = 500.0
    arrow_width_m: float = 150.0
    bar_width_m: float = 200.0
    bar_max_height_m: float = 600.0
    min_flow_cfs: float = 10.0
    min_velocity_fps: float = 0.0    # ft/s — stub threshold in velocity mode (default 0 = no stubs)
    colormap: str = "coolwarm"  # diverging colormap: positive=warm, negative=cool
    flow_vmin: Optional[float] = None  # colour lower bound; None = -reference value
    flow_vmax: Optional[float] = None  # colour upper bound; None = +reference value
    variable: str = "flow"             # "flow" | "velocity"
    reference_velocity: float = 2.5   # ft/s → reference_arrow_length_m (velocity mode)
    alpha: float = 1.0                 # overall opacity of all arrow/bar renderers (0–1)

    @classmethod
    def from_yaml(cls, path: "str | Path") -> "FlowLayerSpec":
        """Load a :class:`FlowLayerSpec` from a YAML file.

        YAML schema::

            variable: flow              # "flow" (default) or "velocity"
            reference_velocity: 2.5    # ft/s → reference_arrow_length_m (velocity mode)
            scale_mode: linear          # "linear" or "log" ("linear" recommended for velocity)
            reference_flow: 10000       # cfs (flow mode)
            reference_arrow_length_m: 500
            arrow_width_m: 150
            bar_width_m: 200
            bar_max_height_m: 600
            min_flow_cfs: 10
            arrows:
              - channel: 10
                position: 0.5
                label: "Sacramento R"
              - channel: 35
                position: 0.75
            bars:
              - node: 329
                label: "Confluence"
                channels: [10, 11, 12]

        Parameters
        ----------
        path : str or Path
            YAML file path.
        """
        import yaml

        with open(path) as fh:
            d = yaml.safe_load(fh) or {}

        arrows = [ChannelArrowSpec(**a) for a in d.pop("arrows", [])]
        bars = [NodeBarSpec(**b) for b in d.pop("bars", [])]
        reservoir_arrows = [ReservoirArrowSpec(**r) for r in d.pop("reservoir_arrows", [])]
        qext_arrows = [QextArrowSpec(**q) for q in d.pop("qext_arrows", [])]
        valid_keys = (
            {f.name for f in dataclasses.fields(cls)}
            - {"arrows", "bars", "reservoir_arrows", "qext_arrows"}
        )
        return cls(
            arrows=arrows,
            bars=bars,
            reservoir_arrows=reservoir_arrows,
            qext_arrows=qext_arrows,
            **{k: v for k, v in d.items() if k in valid_keys},
        )


# ===========================================================================
# Internal geometry dataclasses (all coordinates in EPSG:3857, metres)
# ===========================================================================

@dataclasses.dataclass
class _ArrowGeom:
    """Pre-computed geometry for one channel arrow."""

    channel: int
    label: str
    cx: float   # centroid x (m, EPSG:3857)
    cy: float   # centroid y (m, EPSG:3857)
    tx: float   # unit tangent x (upstream→downstream)
    ty: float   # unit tangent y
    # Note: normal = (-ty, tx) — 90° CCW from tangent, computed on demand


@dataclasses.dataclass
class _ChannelNodeConn:
    """One channel connected to a junction bar node."""

    channel: int
    is_upnode: bool  # True if the bar-node is the UPNODE of this channel
    color: str       # hex colour from _CATEGORY20


@dataclasses.dataclass
class _BarGeom:
    """Pre-computed geometry for one junction bar."""

    node: int
    label: str
    bx: float               # node x (m, EPSG:3857) — centre of the bar pair
    by: float               # node y (m, EPSG:3857) — bottom of the bar columns
    connections: List[_ChannelNodeConn]


@dataclasses.dataclass
class _ExtArrowGeom:
    """Pre-computed geometry for a reservoir-connection or qext arrow.

    Unlike channel arrows, the direction is user-specified via
    ``direction_deg`` rather than derived from a channel centreline.
    """

    source_type: str   # ``"reservoir"`` or ``"qext"``
    lookup_key: object # ``(res_name, node)`` tuple  or  qext name str
    label: str
    cx: float          # node x (m, EPSG:3857)
    cy: float          # node y (m, EPSG:3857)
    tx: float          # unit direction x = cos(direction_deg)
    ty: float          # unit direction y = sin(direction_deg)


# ===========================================================================
# Geometry computation helpers
# ===========================================================================


def _latlon_to_3857(lat: float, lon: float) -> "tuple[float, float]":
    """Convert WGS84 decimal degrees to EPSG:3857 (x, y) in metres."""
    from pyproj import Transformer

    tr = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
    x, y = tr.transform(lon, lat)   # always_xy: (lon, lat) → (x, y)
    return float(x), float(y)


def _compute_ext_arrow_geom(
    node: Optional[int],
    direction_deg: float,
    label: str,
    source_type: str,
    lookup_key: object,
    nodes_gdf_3857,
    point_latlon: Optional[List[float]] = None,
) -> "Optional[_ExtArrowGeom]":
    """Compute position and direction for a reservoir/qext arrow (EPSG:3857).

    Position resolution order:
    1. *point_latlon* ``[lat, lon]`` — explicit geographic coordinates
    2. *node* lookup in *nodes_gdf_3857*
    """
    import math

    if point_latlon is not None:
        cx, cy = _latlon_to_3857(point_latlon[0], point_latlon[1])
    elif node is not None:
        mask = nodes_gdf_3857["id"] == node
        if not mask.any():
            log.warning(
                "Node %d not found in nodes GDF \u2014 %s arrow skipped.", node, source_type
            )
            return None
        pt = nodes_gdf_3857.loc[mask, "geometry"].iloc[0]
        cx, cy = float(pt.x), float(pt.y)
    else:
        log.warning(
            "No position info for %s arrow '%s' \u2014 skipped.", source_type, label
        )
        return None

    tx = math.cos(math.radians(direction_deg))
    ty = math.sin(math.radians(direction_deg))
    return _ExtArrowGeom(
        source_type=source_type,
        lookup_key=lookup_key,
        label=label,
        cx=cx, cy=cy, tx=tx, ty=ty,
    )


def _compute_arrow_geom(
    spec: ChannelArrowSpec,
    channel_gdf_3857: "geopandas.GeoDataFrame",
) -> Optional[_ArrowGeom]:
    """Compute centroid and tangent direction for a channel arrow.

    Parameters
    ----------
    spec : ChannelArrowSpec
    channel_gdf_3857 : GeoDataFrame
        Channel centreline geometry reproduced in EPSG:3857.
        Must have a ``geo_id`` column (int channel number).

    Returns
    -------
    _ArrowGeom or None
        ``None`` when the channel is not found in *channel_gdf_3857*.
    """
    mask = channel_gdf_3857["geo_id"] == spec.channel
    if not mask.any():
        log.warning("Channel %d not found in channel GDF — arrow skipped.", spec.channel)
        return None

    geom = channel_gdf_3857.loc[mask, "geometry"].iloc[0]

    # Flatten MultiLineString → single LineString (longest component wins)
    if geom.geom_type == "MultiLineString":
        try:
            import shapely.ops

            merged = shapely.ops.linemerge(geom)
            geom = merged if merged.geom_type == "LineString" else max(
                merged.geoms if merged.geom_type == "MultiLineString" else [merged],
                key=lambda g: g.length,
            )
        except Exception:
            geom = max(geom.geoms, key=lambda g: g.length)

    pos = max(0.0, min(1.0, spec.position))
    point = geom.interpolate(pos, normalized=True)
    cx, cy = float(point.x), float(point.y)

    # Override centroid if the user supplied an explicit geographic point
    if spec.point is not None:
        cx, cy = _latlon_to_3857(spec.point[0], spec.point[1])

    # Tangent via finite difference (avoid the exact endpoints)
    delta = 0.02
    p0 = geom.interpolate(max(0.0, pos - delta), normalized=True)
    p1 = geom.interpolate(min(1.0, pos + delta), normalized=True)
    dx, dy = p1.x - p0.x, p1.y - p0.y
    length = float(np.hypot(dx, dy))
    if length < 1.0:
        log.debug("Degenerate tangent for channel %d — defaulting East.", spec.channel)
        tx, ty = 1.0, 0.0
    else:
        tx, ty = dx / length, dy / length

    return _ArrowGeom(
        channel=spec.channel, label=spec.label,
        cx=cx, cy=cy, tx=tx, ty=ty,
    )


def _compute_bar_geom(
    spec: NodeBarSpec,
    nodes_gdf_3857: "geopandas.GeoDataFrame",
    chan_no_arr: np.ndarray,
    upnode_arr: np.ndarray,
    downnode_arr: np.ndarray,
) -> Optional[_BarGeom]:
    """Compute node position and connected channel topology for a bar.

    Parameters
    ----------
    spec : NodeBarSpec
    nodes_gdf_3857 : GeoDataFrame
        Node point geometry in EPSG:3857.  Must have an ``id`` column.
    chan_no_arr : ndarray of int
        Channel numbers array (length = n_channels).
    upnode_arr : ndarray of int
        Upstream-node numbers per channel.
    downnode_arr : ndarray of int
        Downstream-node numbers per channel.

    Returns
    -------
    _BarGeom or None
        ``None`` when the node is not found or no channels are connected.
    """
    mask = nodes_gdf_3857["id"] == spec.node
    if not mask.any():
        log.warning("Node %d not found in nodes GDF — bar skipped.", spec.node)
        return None

    point = nodes_gdf_3857.loc[mask, "geometry"].iloc[0]
    bx, by = float(point.x), float(point.y)

    connections: List[_ChannelNodeConn] = []
    color_idx = 0
    for ch_no, upn, downn in zip(chan_no_arr, upnode_arr, downnode_arr):
        ch_int = int(ch_no)
        if spec.channels is not None and ch_int not in spec.channels:
            continue
        if int(upn) == spec.node:
            is_upnode = True
        elif int(downn) == spec.node:
            is_upnode = False
        else:
            continue
        connections.append(_ChannelNodeConn(
            channel=ch_int,
            is_upnode=is_upnode,
            color=_CATEGORY20[color_idx % len(_CATEGORY20)],
        ))
        color_idx += 1

    if not connections:
        log.warning(
            "Node %d: no channels found in connection (spec.channels=%s) — bar skipped.",
            spec.node, spec.channels,
        )
        return None

    return _BarGeom(
        node=spec.node, label=spec.label,
        bx=bx, by=by,
        connections=connections,
    )


# ===========================================================================
# Per-frame polygon construction
# ===========================================================================


def _scaled_arrow_length(
    abs_flow: float,
    reference_flow: float,
    reference_length_m: float,
    scale_mode: str,
) -> float:
    """Return the arrow length in EPSG:3857 metres for the given absolute flow.

    Shared by :func:`_arrow_polygon` (polygon construction) and the text-label
    positioning in :meth:`FlowLayer.update_frame`.
    """
    ratio = abs_flow / reference_flow
    if scale_mode == "log":
        L = np.log10(1.0 + ratio) / np.log10(2.0) * reference_length_m
    elif scale_mode == "sqrt":
        L = np.sqrt(ratio) * reference_length_m
    elif scale_mode == "cbrt":
        L = np.cbrt(ratio) * reference_length_m
    else:  # "linear"
        L = ratio * reference_length_m
    return min(float(L), 3.0 * reference_length_m)


def _arrow_polygon(
    cx: float, cy: float,
    tx: float, ty: float,
    flow: float,
    reference_flow: float,
    reference_length_m: float,
    arrow_width_m: float,
    scale_mode: str,
    min_flow_cfs: float,
) -> tuple:
    """Build the arrow polygon for one channel at the current flow value.

    The arrow is a 7-point polygon: rectangular body + triangular head.
    The arrow direction reflects the instantaneous tidal sign of *flow*
    (positive = upstream→downstream in DSM2 convention, arrow points in
    the downstream direction of the centreline; negative = reversed).

    For ``|flow| < min_flow_cfs`` a small diamond stub is returned to keep
    the arrow location visible.

    Parameters
    ----------
    cx, cy : float
        Arrow centroid in EPSG:3857 metres.
    tx, ty : float
        Unit tangent vector of the channel (upstream→downstream).
    flow : float
        Signed flow value in cfs.
    reference_flow, reference_length_m : float
        Scaling: *reference_flow* cfs maps to *reference_length_m* metres.
    arrow_width_m : float
        Arrow body half-width in metres.
    scale_mode : str
        ``"linear"``, ``"log"``, ``"sqrt"``, or ``"cbrt"``.
    min_flow_cfs : float
        Threshold below which a stub is shown.

    Returns
    -------
    (xs, ys, display_value) : (list[float], list[float], float)
    """
    abs_flow = abs(flow)
    sign_d = 1.0 if flow >= 0.0 else -1.0

    if abs_flow < min_flow_cfs:
        # Tiny diamond stub so the location stays visible
        r = arrow_width_m * 0.35
        xs = [cx + r, cx, cx - r, cx, cx + r]
        ys = [cy, cy + r, cy, cy - r, cy]
        return xs, ys, flow

    L = _scaled_arrow_length(abs_flow, reference_flow, reference_length_m, scale_mode)

    W = float(arrow_width_m)           # body half-width
    HW = W * 1.25                      # arrowhead half-width (flare)
    BF = 0.65                          # fraction of L that is the body

    body_L = L * BF

    # Direction vector aligned with (possibly reversed) flow
    dx, dy = sign_d * tx, sign_d * ty

    def pt(along: float, perp: float):
        """Point at *along* metres ahead, *perp* metres left of arrow dir."""
        # Normal = 90° CCW of (dx, dy) = (-dy, dx)
        # new = (cx + along*dx + perp*(-dy), cy + along*dy + perp*dx)
        return (cx + along * dx - perp * dy,
                cy + along * dy + perp * dx)

    # 7-point polygon: tail-left → shoulder-left → flare-left → tip →
    #                  flare-right → shoulder-right → tail-right
    pts = [
        pt(-0.15 * L,  W),    # tail-left  (small notch behind centroid)
        pt(body_L,     W),    # shoulder-left
        pt(body_L,     HW),   # flare-left (arrowhead base)
        pt(L,          0.0),  # tip
        pt(body_L,    -HW),   # flare-right
        pt(body_L,    -W),    # shoulder-right
        pt(-0.15 * L, -W),    # tail-right
    ]
    return [p[0] for p in pts], [p[1] for p in pts], flow


def _bar_segments(
    bx: float, by: float,
    connections: List[_ChannelNodeConn],
    flow_values: dict,
    reference_flow: float,
    bar_width_m: float,
    bar_max_height_m: float,
) -> tuple:
    """Build bar segment polygons for one junction node.

    The bar is two-sided:

    * **Left side** (x < *bx*): inflow channels — segments stacked from
      bottom to top, each proportional to the channel's contribution
      flowing INTO the node.
    * **Right side** (x ≥ *bx*): outflow channels — same convention for
      flow leaving the node.

    Inflow/outflow sign convention:

    * ``is_upnode=True``: positive flow goes downstream (away from this node)
      → outflow.  Negative flow enters the node → inflow.
    * ``is_upnode=False``: positive flow arrives at this node → inflow.
      Negative flow leaves the node → outflow.

    Parameters
    ----------
    bx, by : float
        Node position in EPSG:3857.  Bars grow upward from *by*.
    connections : list of _ChannelNodeConn
        Connected channels with pre-assigned colours.
    flow_values : dict mapping channel_no (int) → flow (float, cfs)
    reference_flow, bar_width_m, bar_max_height_m : float
        Scaling parameters from :class:`FlowLayerSpec`.

    Returns
    -------
    (xs_list, ys_list, colors, channel_ids, node_id_list, values, sides)
        Each element is a list; one entry per bar segment.
    """
    inflows: List[tuple] = []   # (channel, color, |contrib|)
    outflows: List[tuple] = []

    for conn in connections:
        f = float(flow_values.get(conn.channel, 0.0))
        # Signed contribution INTO this node
        contrib = (-f) if conn.is_upnode else f
        if abs(contrib) < 1.0:
            continue
        if contrib > 0:
            inflows.append((conn.channel, conn.color, contrib))
        else:
            outflows.append((conn.channel, conn.color, abs(contrib)))

    xs_list, ys_list, colors = [], [], []
    ch_ids, vals, sides = [], [], []

    GAP_M = 4.0  # visual gap between stacked segments (metres)

    for side_name, segments, x_left in (
        ("inflow",  inflows,  bx - bar_width_m),
        ("outflow", outflows, bx),
    ):
        y_cursor = by
        for ch, color, magnitude in segments:
            seg_h = (magnitude / reference_flow) * bar_max_height_m
            # Cap at 5× reference height to avoid extreme overflow
            seg_h = min(float(seg_h), 5.0 * bar_max_height_m)
            x0, y0 = x_left, y_cursor
            x1, y1 = x_left + bar_width_m, y_cursor + max(seg_h - GAP_M, 1.0)
            xs_list.append([x0, x1, x1, x0, x0])
            ys_list.append([y0, y0, y1, y1, y0])
            colors.append(color)
            ch_ids.append(ch)
            vals.append(round(magnitude, 1))
            sides.append(side_name)
            y_cursor += seg_h

    return xs_list, ys_list, colors, ch_ids, vals, sides


# ===========================================================================
# FlowLayer — the main data + rendering class
# ===========================================================================

class FlowLayer:
    """Animated flow arrows and junction bars for DSM2 HYDRO data.

    This class manages its own Bokeh ``ColumnDataSource`` objects and can be
    attached to any existing Bokeh figure via :meth:`setup_on_figure`.  It
    is designed to plug into :class:`~dvue.animator.GeoAnimatorManager`::

        flow = FlowLayer("hydro.h5", spec, channel_gdf, nodes_gdf)
        flow.setup_on_figure(mgr._bk_figure)
        mgr.add_frame_callback(flow.update_frame)

    Parameters
    ----------
    hydro_h5_path : str or Path
        HYDRO HDF5 tidefile.
    spec : FlowLayerSpec
        Arrow and bar configuration.
    channel_gdf : geopandas.GeoDataFrame
        DSM2 channel centreline geometry returned by
        :func:`~dsm2ui.animate.load_dsm2_channel_gdf`.
    nodes_gdf : geopandas.GeoDataFrame
        DSM2 node point geometry returned by
        :func:`~dsm2ui.animate.load_dsm2_nodes_gdf`.
    """

    def __init__(
        self,
        hydro_h5_path: "str | Path",
        spec: FlowLayerSpec,
        channel_gdf: "geopandas.GeoDataFrame",
        nodes_gdf: "geopandas.GeoDataFrame",
    ) -> None:
        import h5py

        self._spec = spec
        self._hydro_h5_path = Path(hydro_h5_path)

        # ----------------------------------------------------------------
        # Project GDFs to EPSG:3857 (metres)
        # ----------------------------------------------------------------
        ch_gdf_3857 = channel_gdf.to_crs("EPSG:3857").copy()
        nd_gdf_3857 = nodes_gdf.to_crs("EPSG:3857").copy()

        # ----------------------------------------------------------------
        # Read channel topology (upnode/downnode) from the HDF5 file
        # ----------------------------------------------------------------
        with h5py.File(self._hydro_h5_path, "r") as hf:
            ch_table = hf["/hydro/input/channel"]
            dtype_names = list(ch_table.dtype.names or [])
            raw = ch_table[:]

        def _int_col(*name_candidates):
            for nm in name_candidates:
                if nm in dtype_names:
                    arr = raw[nm]
                    if arr.dtype.kind in ("S", "U", "O"):
                        return np.array([
                            int(v.decode("utf-8").strip())
                            if isinstance(v, (bytes, np.bytes_))
                            else int(str(v).strip())
                            for v in arr
                        ], dtype=np.int64)
                    return arr.astype(np.int64)
            return np.array([], dtype=np.int64)

        chan_no_arr  = _int_col("chan_no", "CHAN_NO", "channel_number")
        upnode_arr   = _int_col("upnode", "UPNODE")
        downnode_arr = _int_col("downnode", "DOWNNODE")

        # ----------------------------------------------------------------
        # Pre-compute arrow and bar geometries
        # ----------------------------------------------------------------
        self._arrow_geoms: List[_ArrowGeom] = []
        for asp in spec.arrows:
            ag = _compute_arrow_geom(asp, ch_gdf_3857)
            if ag is not None:
                self._arrow_geoms.append(ag)

        self._bar_geoms: List[_BarGeom] = []
        for bsp in spec.bars:
            bg = _compute_bar_geom(bsp, nd_gdf_3857, chan_no_arr, upnode_arr, downnode_arr)
            if bg is not None:
                self._bar_geoms.append(bg)

        # ----------------------------------------------------------------
        # Pre-compute ext arrow geometries (reservoir connections + qext)
        # ----------------------------------------------------------------
        self._ext_arrow_geoms: List[_ExtArrowGeom] = []
        for rspec in spec.reservoir_arrows:
            eg = _compute_ext_arrow_geom(
                rspec.node, rspec.direction_deg,
                rspec.label or f"{rspec.reservoir}@{rspec.node}",
                "reservoir",
                (rspec.reservoir.lower().strip(), rspec.node),
                nd_gdf_3857,
                point_latlon=rspec.point,
            )
            if eg is not None:
                self._ext_arrow_geoms.append(eg)
        for qspec in spec.qext_arrows:
            eg = _compute_ext_arrow_geom(
                qspec.node, qspec.direction_deg,
                qspec.label or qspec.name,
                "qext",
                qspec.name.lower().strip(),
                nd_gdf_3857,
                point_latlon=qspec.point,
            )
            if eg is not None:
                self._ext_arrow_geoms.append(eg)

        # ----------------------------------------------------------------
        # Collect all channel numbers that will need flow data
        # ----------------------------------------------------------------
        arrow_channels = [ag.channel for ag in self._arrow_geoms]
        bar_channels = [
            conn.channel
            for bg in self._bar_geoms
            for conn in bg.connections
        ]
        self._all_channels: List[int] = sorted(set(arrow_channels + bar_channels))

        # Readers — lazily initialised on first update_frame() call
        self._reader = None
        self._res_reader = None
        self._qext_reader = None
        self._last_ts: Optional[pd.Timestamp] = None   # for live control re-render

        # Bokeh sources — set in setup_on_figure()
        self._arrow_source: Optional[ColumnDataSource] = None
        self._arrow_text_source: Optional[ColumnDataSource] = None
        self._bar_source: Optional[ColumnDataSource] = None
        self._ext_arrow_source: Optional[ColumnDataSource] = None
        self._ext_arrow_text_source: Optional[ColumnDataSource] = None
        # Extra renderers added to additional figures (e.g. diff map).
        # Each entry is a dict with keys "arrow", "bar", "ext" (may be None).
        self._extra_renderers: list = []

    # ----------------------------------------------------------------
    # Reader (lazy, transform-aware)
    # ----------------------------------------------------------------

    @property
    def _ref_scale(self) -> float:
        """Scaling reference value: ``reference_velocity`` (ft/s) for velocity
        mode or ``reference_flow`` (cfs) for flow mode."""
        if getattr(self._spec, "variable", "flow") == "velocity":
            return max(getattr(self._spec, "reference_velocity", 2.5), 1e-6)
        return max(self._spec.reference_flow, 1.0)

    @property
    def _min_threshold(self) -> float:
        """Stub threshold: ``min_velocity_fps`` (ft/s) in velocity mode, else ``min_flow_cfs`` (cfs)."""
        if getattr(self._spec, "variable", "flow") == "velocity":
            return getattr(self._spec, "min_velocity_fps", 0.0)
        return self._spec.min_flow_cfs

    def _get_base_reader(self):
        """Return the raw (untransformed) base reader, built once.

        Creates :class:`~dsm2ui.animate.HydroH5VelocityReader` when
        ``spec.variable`` is ``'velocity'``, otherwise
        :class:`~dsm2ui.animate.HydroH5FlowReader`.
        """
        if self._reader is not None and not hasattr(self, "_base_reader_raw"):
            # Legacy: reader was already built without base/transform split
            return None
        if not hasattr(self, "_base_reader_raw") or self._base_reader_raw is None:
            if getattr(self._spec, "variable", "flow") == "velocity":
                from dsm2ui.animate import HydroH5VelocityReader
                self._base_reader_raw = HydroH5VelocityReader(
                    self._hydro_h5_path, location="both"
                )
            else:
                from dsm2ui.animate import HydroH5FlowReader
                self._base_reader_raw = HydroH5FlowReader(
                    self._hydro_h5_path, location="both"
                )
        return self._base_reader_raw

    def _get_reader(self):
        """Return the current active channel flow reader (raw or transformed+buffered)."""
        if self._reader is None:
            from dvue.animator import BufferedSlicingReader
            self._base_reader_raw = None  # trigger creation in _get_base_reader
            base = self._get_base_reader()
            self._reader = BufferedSlicingReader(
                base, chunk_size=200, prefetch=True,
                adaptive=True, min_chunk_size=50, max_chunk_size=2000,
            )
        return self._reader

    def _get_res_reader(self):
        """Return the reservoir-connection flow reader, building it on first call."""
        if self._res_reader is None:
            from dsm2ui.animate import HydroH5ReservoirConnectionReader
            from dvue.animator import BufferedSlicingReader
            self._base_res_reader_raw = HydroH5ReservoirConnectionReader(
                self._hydro_h5_path
            )
            self._res_reader = BufferedSlicingReader(
                self._base_res_reader_raw, chunk_size=200, prefetch=True,
                adaptive=True, min_chunk_size=50, max_chunk_size=2000,
            )
        return self._res_reader

    def _get_qext_reader(self):
        """Return the qext source/sink flow reader, building it on first call."""
        if self._qext_reader is None:
            from dsm2ui.animate import HydroH5QextReader
            from dvue.animator import BufferedSlicingReader
            self._base_qext_reader_raw = HydroH5QextReader(self._hydro_h5_path)
            self._qext_reader = BufferedSlicingReader(
                self._base_qext_reader_raw, chunk_size=200, prefetch=True,
                adaptive=True, min_chunk_size=50, max_chunk_size=2000,
            )
        return self._qext_reader

    def _apply_transform_to_base(self, base_reader, transform_spec_or_none):
        """Wrap *base_reader* with an optional transform and buffer it.

        When the transform has a non-zero overlap (e.g. Godin filter, rolling
        average) a :class:`~dvue.animator.RawSequentialBuffer` is inserted
        between the raw reader and the transform so that HDF5 I/O and the
        transform computation overlap in time (pipelined).
        """
        from dvue.animator import BufferedSlicingReader, RawSequentialBuffer
        if transform_spec_or_none is not None:
            from dvue.animator.reader import StreamingTransformedSlicingReader, TransformSpec
            if isinstance(transform_spec_or_none, TransformSpec):
                try:
                    import pandas as _pd
                    freq_nanos = int(
                        _pd.tseries.frequencies.to_offset(
                            base_reader.time_index.freq
                        ).nanos
                    )
                    raw_overlap = transform_spec_or_none.get_overlap(freq_nanos)
                except (AttributeError, TypeError):
                    raw_overlap = 0
                if raw_overlap > 0:
                    base_reader = RawSequentialBuffer(base_reader)
                base_reader = StreamingTransformedSlicingReader(
                    base_reader, transform_spec_or_none
                )
        return BufferedSlicingReader(
            base_reader, chunk_size=200, prefetch=True,
            adaptive=True, min_chunk_size=50, max_chunk_size=2000,
        )

    def set_transform(self, transform_spec_or_none) -> None:
        """Rebuild all flow readers to apply (or remove) a time-domain transform.

        Called automatically when the :class:`~dvue.animator.GeoAnimatorManager`
        transform is changed via :meth:`~dvue.animator.GeoAnimatorManager.add_transform_callback`.
        Rebuilds the channel, reservoir-connection, and qext readers so they
        all stay in sync with the background animation.

        Parameters
        ----------
        transform_spec_or_none : TransformSpec or None
            When ``None`` the raw readers are used.  Otherwise a
            :class:`~dvue.animator.StreamingTransformedSlicingReader` is wrapped
            before buffering.
        """
        # Channel reader
        self._reader = self._apply_transform_to_base(
            self._get_base_reader(), transform_spec_or_none
        )
        # Reservoir reader (only if previously built)
        if hasattr(self, "_base_res_reader_raw") and self._base_res_reader_raw is not None:
            self._res_reader = self._apply_transform_to_base(
                self._base_res_reader_raw, transform_spec_or_none
            )
        # Qext reader (only if previously built)
        if hasattr(self, "_base_qext_reader_raw") and self._base_qext_reader_raw is not None:
            self._qext_reader = self._apply_transform_to_base(
                self._base_qext_reader_raw, transform_spec_or_none
            )

    @property
    def time_index(self) -> pd.DatetimeIndex:
        """Time index from the current (possibly transformed) flow reader."""
        return self._get_reader().time_index

    # ----------------------------------------------------------------
    # Color computation
    # ----------------------------------------------------------------

    def _flow_to_color(self, flow: float) -> str:
        """Map a signed flow value to a hex colour via the current diverging colormap.

        Uses :class:`matplotlib.colors.TwoSlopeNorm` so that zero is always at
        the colour-map centre (0.5) even when *flow_vmin* and *flow_vmax* are
        asymmetric.  Falls back to symmetric \u00b1reference_flow when no explicit
        range is set.
        """
        import matplotlib.cm
        import matplotlib.colors as mcolors

        try:
            cmap = matplotlib.colormaps[self._spec.colormap]
        except (AttributeError, KeyError):
            cmap = matplotlib.cm.get_cmap(self._spec.colormap)

        ref  = self._ref_scale
        vmin = self._spec.flow_vmin if self._spec.flow_vmin is not None else -ref
        vmax = self._spec.flow_vmax if self._spec.flow_vmax is not None else  ref

        # Reversed range (e.g. "1000, -1000") inverts the colourmap so that
        # positive flow maps to the cool end and negative to the warm end.
        inverted = (vmin > vmax)
        if inverted:
            vmin, vmax = vmax, vmin   # normalise so TwoSlopeNorm gets valid args

        if abs(flow) < self._min_threshold:
            mapped = 0.5  # neutral mid-point for sub-threshold stubs
        else:
            try:
                norm   = mcolors.TwoSlopeNorm(vcenter=0.0, vmin=vmin, vmax=vmax)
                mapped = float(np.clip(norm(flow), 0.0, 1.0))
            except (ValueError, ZeroDivisionError):
                # Fallback: symmetric linear normalisation
                norm_val = float(np.clip(flow / ref, -3.0, 3.0) / 3.0)
                mapped   = (norm_val + 1.0) / 2.0

        if inverted:
            mapped = 1.0 - mapped  # flip colour direction

        return mcolors.to_hex(cmap(mapped))

    # ----------------------------------------------------------------
    # Bokeh setup
    # ----------------------------------------------------------------

    def setup_on_figure(self, figure) -> None:
        """Add Bokeh renderers and HoverTools to an existing Bokeh figure.

        Must be called before :meth:`update_frame`.  Renderers are added at
        the ``"overlay"`` level so they appear above the channel colour patches.
        """
        # ---- Arrow patches ----
        _is_vel = getattr(self._spec, "variable", "flow") == "velocity"
        _val_label = "Velocity" if _is_vel else "Flow"
        _val_fmt   = "@values{0,0.000} ft/s" if _is_vel else "@values{0,0.} cfs"

        self._arrow_source = ColumnDataSource({
            "xs": [], "ys": [],
            "values": [], "channel_ids": [], "labels": [], "directions": [],
            "color": [],
        })
        arrow_renderer = figure.patches(
            xs="xs", ys="ys",
            source=self._arrow_source,
            fill_color="color",
            fill_alpha=0.88,
            line_color="white",
            line_width=0.8,
            level="overlay",
        )
        self._arrow_renderer = arrow_renderer
        figure.add_tools(HoverTool(
            renderers=[arrow_renderer],
            tooltips=[
                ("Arrow",     "@labels"),
                ("Ch #",      "@channel_ids"),
                (_val_label,  _val_fmt),
                ("Direction", "@directions"),
            ],
        ))

        # ---- Arrow text labels (flow value inside body) ----
        self._arrow_text_source = ColumnDataSource({
            "x": [], "y": [], "text": [], "angle": [],
        })
        figure.text(
            x="x", y="y",
            text="text",
            angle="angle",
            source=self._arrow_text_source,
            text_color="white",
            text_align="center",
            text_baseline="middle",
            text_font_size="10px",
            text_font_style="bold",
            level="overlay",
        )

        # ---- Bar patches ----
        self._bar_source = ColumnDataSource({
            "xs": [], "ys": [],
            "colors": [], "channel_ids": [], "node_ids": [], "node_labels": [],
            "values": [], "sides": [],
        })
        bar_renderer = figure.patches(
            xs="xs", ys="ys",
            fill_color="colors",
            source=self._bar_source,
            fill_alpha=0.80,
            line_color="black",
            line_width=0.7,
            level="overlay",
        )
        self._bar_renderer = bar_renderer
        figure.add_tools(HoverTool(
            renderers=[bar_renderer],
            tooltips=[
                ("Node",      "@node_labels"),
                ("Channel",   "ch @channel_ids"),
                ("Direction", "@sides"),
                (_val_label,  _val_fmt),
            ],
        ))

        # ---- Ext arrow patches (reservoir connections + qext) ----
        if self._ext_arrow_geoms:
            self._ext_arrow_source = ColumnDataSource({
                "xs": [], "ys": [],
                "values": [], "labels": [], "directions": [], "color": [],
            })
            ext_renderer = figure.patches(
                xs="xs", ys="ys",
                source=self._ext_arrow_source,
                fill_color="color",
                fill_alpha=0.88,
                line_color="white",
                line_width=0.8,
                level="overlay",
            )
            self._ext_arrow_renderer = ext_renderer
            figure.add_tools(HoverTool(
                renderers=[ext_renderer],
                tooltips=[
                    ("Source",    "@labels"),
                    (_val_label,  _val_fmt),
                    ("Direction", "@directions"),
                ],
            ))
            self._ext_arrow_text_source = ColumnDataSource({
                "x": [], "y": [], "text": [], "angle": [],
            })
            figure.text(
                x="x", y="y",
                text="text",
                angle="angle",
                source=self._ext_arrow_text_source,
                text_color="white",
                text_align="center",
                text_baseline="middle",
                text_font_size="10px",
                text_font_style="bold",
                level="overlay",
            )

        # Apply initial alpha from spec (allows YAML to control default opacity)
        if self._spec.alpha < 1.0:
            self._apply_alpha(self._spec.alpha)

    def setup_on_additional_figure(self, figure) -> None:
        """Mirror this layer's renderers onto *figure* using the same data sources.

        Call this **after** :meth:`setup_on_figure` to add the flow overlay to
        an extra Bokeh figure (e.g. the diff map in
        :class:`~dvue.animator.MultiGeoAnimatorManager`).  Because the same
        ``ColumnDataSource`` objects are shared, every :meth:`update_frame`
        call automatically propagates to all figures.

        Parameters
        ----------
        figure : bokeh.plotting.figure
            An additional Bokeh figure to receive the overlay renderers.
        """
        if self._arrow_source is None:
            raise RuntimeError(
                "Call setup_on_figure() before setup_on_additional_figure()."
            )
        _is_vel = getattr(self._spec, "variable", "flow") == "velocity"
        _val_label = "Velocity" if _is_vel else "Flow"
        _val_fmt   = "@values{0,0.000} ft/s" if _is_vel else "@values{0,0.} cfs"

        # Arrow patches — shared source
        r_arrow = figure.patches(
            xs="xs", ys="ys",
            source=self._arrow_source,
            fill_color="color",
            fill_alpha=self._spec.alpha * 0.88,
            line_color="white",
            line_width=0.8,
            level="overlay",
        )
        figure.add_tools(HoverTool(
            renderers=[r_arrow],
            tooltips=[
                ("Arrow",     "@labels"),
                ("Ch #",      "@channel_ids"),
                (_val_label,  _val_fmt),
                ("Direction", "@directions"),
            ],
        ))

        # Arrow text labels — shared source
        figure.text(
            x="x", y="y",
            text="text",
            angle="angle",
            source=self._arrow_text_source,
            text_color="white",
            text_align="center",
            text_baseline="middle",
            text_font_size="10px",
            text_font_style="bold",
            level="overlay",
        )

        # Bar patches — shared source
        r_bar = figure.patches(
            xs="xs", ys="ys",
            fill_color="colors",
            source=self._bar_source,
            fill_alpha=self._spec.alpha * 0.80,
            line_color="black",
            line_width=0.7,
            level="overlay",
        )
        figure.add_tools(HoverTool(
            renderers=[r_bar],
            tooltips=[
                ("Node",      "@node_labels"),
                ("Channel",   "ch @channel_ids"),
                ("Direction", "@sides"),
                (_val_label,  _val_fmt),
            ],
        ))

        # Ext arrow patches — shared source (only if ext arrows were configured)
        r_ext = None
        if self._ext_arrow_source is not None:
            r_ext = figure.patches(
                xs="xs", ys="ys",
                source=self._ext_arrow_source,
                fill_color="color",
                fill_alpha=self._spec.alpha * 0.88,
                line_color="white",
                line_width=0.8,
                level="overlay",
            )
            figure.add_tools(HoverTool(
                renderers=[r_ext],
                tooltips=[
                    ("Source",    "@labels"),
                    (_val_label,  _val_fmt),
                    ("Direction", "@directions"),
                ],
            ))
            figure.text(
                x="x", y="y",
                text="text",
                angle="angle",
                source=self._ext_arrow_text_source,
                text_color="white",
                text_align="center",
                text_baseline="middle",
                text_font_size="10px",
                text_font_style="bold",
                level="overlay",
            )

        self._extra_renderers.append({"arrow": r_arrow, "bar": r_bar, "ext": r_ext})

    def _apply_alpha(self, alpha: float) -> None:
        """Set fill alpha on all flow renderers without touching line outlines."""
        for r in (
            getattr(self, "_arrow_renderer", None),
            getattr(self, "_bar_renderer", None),
            getattr(self, "_ext_arrow_renderer", None),
        ):
            if r is not None:
                r.glyph.fill_alpha = alpha
        for extra in self._extra_renderers:
            for r in extra.values():
                if r is not None:
                    r.glyph.fill_alpha = alpha

    def get_state_dict(self) -> dict:
        """Return current widget/spec settings as a serialisable dict."""
        state = {
            "colormap":               self._spec.colormap,
            "scale_mode":             self._spec.scale_mode,
            "reference_arrow_length_m": self._spec.reference_arrow_length_m,
            "arrow_width_m":          self._spec.arrow_width_m,
            "bar_max_height_m":       self._spec.bar_max_height_m,
            "alpha":                  self._spec.alpha,
            "visible":                self._w_visible.value if hasattr(self, "_w_visible") else True,
        }
        if self._spec.flow_vmin is not None:
            state["flow_vmin"] = self._spec.flow_vmin
        if self._spec.flow_vmax is not None:
            state["flow_vmax"] = self._spec.flow_vmax
        if getattr(self._spec, "variable", "flow") == "velocity":
            state["reference_velocity"] = self._spec.reference_velocity
            state["min_velocity_fps"]    = getattr(self._spec, "min_velocity_fps", 0.0)
        else:
            state["reference_flow"]  = self._spec.reference_flow
            state["min_flow_cfs"]    = self._spec.min_flow_cfs
        return state

    # ----------------------------------------------------------------
    # Per-frame update
    # ----------------------------------------------------------------

    def update_frame(self, ts: pd.Timestamp) -> None:
        """Compute and patch Bokeh sources for the given animation timestamp.

        Designed to be registered with
        :meth:`~dvue.animator.GeoAnimatorManager.add_frame_callback` — the
        manager passes the resolved :class:`pandas.Timestamp` for the current
        frame, so the flow reader can snap to its nearest available step
        independently of the main animation reader's time index (important when
        a transform changes the step count).

        Parameters
        ----------
        ts : pd.Timestamp
            Current animation timestamp from the main reader.
        """
        if self._arrow_source is None:
            raise RuntimeError(
                "Call setup_on_figure() before update_frame()."
            )

        series = self._get_reader().get_slice_nearest(ts)
        self._last_ts = ts  # remember for control-panel live re-render

        # Fast lookup dict
        flow_dict: dict = {
            int(ch): float(v)
            for ch, v in series.items()
            if int(ch) in set(self._all_channels)
        }

        # ---- Arrow update ----
        arr_xs, arr_ys, arr_vals, arr_chs, arr_labels, arr_dirs = [], [], [], [], [], []
        spec = self._spec
        _ref = self._ref_scale
        _min = self._min_threshold
        for ag in self._arrow_geoms:
            flow = flow_dict.get(ag.channel, 0.0)
            xs, ys, fval = _arrow_polygon(
                ag.cx, ag.cy, ag.tx, ag.ty,
                flow,
                _ref,
                spec.reference_arrow_length_m,
                spec.arrow_width_m,
                spec.scale_mode,
                _min,
            )
            arr_xs.append(xs)
            arr_ys.append(ys)
            arr_vals.append(round(fval, 1))
            arr_chs.append(ag.channel)
            arr_labels.append(ag.label or f"Ch {ag.channel}")
            if abs(fval) < _min:
                arr_dirs.append("~ 0")
            elif fval > 0:
                arr_dirs.append("\u2192 downstream")
            else:
                arr_dirs.append("\u2190 upstream")

        self._arrow_source.data = {
            "xs":          arr_xs,
            "ys":          arr_ys,
            "values":      arr_vals,
            "channel_ids": arr_chs,
            "labels":      arr_labels,
            "directions":  arr_dirs,
            "color":       [self._flow_to_color(v) for v in arr_vals],
        }

        # ---- Arrow text labels ----
        if self._arrow_text_source is not None:
            import math
            txt_xs, txt_ys, txt_texts, txt_angles = [], [], [], []
            for ag, fval in zip(self._arrow_geoms, arr_vals):
                abs_f = abs(fval)
                # Compact label: sub-1k shows integer, ≥1k shows "N.Nk"
                if abs_f < _min:
                    txt_texts.append("")
                    txt_xs.append(ag.cx)
                    txt_ys.append(ag.cy)
                    txt_angles.append(0.0)
                    continue
                if abs_f < 1_000:
                    txt_texts.append(f"{abs_f:.0f}")
                else:
                    txt_texts.append(f"{abs_f / 1_000:.1f}k")
                sign_d = 1.0 if fval >= 0.0 else -1.0
                # Compute arrow length using the same scaling as the polygon,
                # then place text at 0.75L — inside the arrowhead where there
                # is clear space (head starts at 0.65L, tip at L).
                L = _scaled_arrow_length(
                    abs_f,
                    _ref,
                    spec.reference_arrow_length_m,
                    spec.scale_mode,
                )
                txt_xs.append(ag.cx + 0.15 * L * sign_d * ag.tx)
                txt_ys.append(ag.cy + 0.15 * L * sign_d * ag.ty)
                # Angle: follow arrow direction, normalised to avoid upside-down text
                raw = math.atan2(sign_d * ag.ty, sign_d * ag.tx)
                if raw > math.pi / 2:
                    raw -= math.pi
                elif raw < -math.pi / 2:
                    raw += math.pi
                txt_angles.append(raw)
            self._arrow_text_source.data = {
                "x": txt_xs, "y": txt_ys,
                "text": txt_texts, "angle": txt_angles,
            }

        # ---- Ext arrow update (reservoir connections + qext) ----
        if self._ext_arrow_geoms and self._ext_arrow_source is not None:
            need_res  = any(eg.source_type == "reservoir" for eg in self._ext_arrow_geoms)
            need_qext = any(eg.source_type == "qext"      for eg in self._ext_arrow_geoms)
            res_series  = self._get_res_reader().get_slice_nearest(ts)  if need_res  else None
            qext_series = self._get_qext_reader().get_slice_nearest(ts) if need_qext else None

            ext_xs, ext_ys, ext_vals, ext_labels, ext_dirs = [], [], [], [], []
            ext_txt_xs, ext_txt_ys, ext_txt_texts, ext_txt_angles = [], [], [], []
            for eg in self._ext_arrow_geoms:
                if eg.source_type == "reservoir":
                    flow = float(res_series.get(eg.lookup_key, 0.0)) if res_series is not None else 0.0
                else:
                    flow = float(qext_series.get(eg.lookup_key, 0.0)) if qext_series is not None else 0.0

                xs, ys, fval = _arrow_polygon(
                    eg.cx, eg.cy, eg.tx, eg.ty,
                    flow,
                    _ref,
                    spec.reference_arrow_length_m,
                    spec.arrow_width_m,
                    spec.scale_mode,
                    _min,
                )
                ext_xs.append(xs)
                ext_ys.append(ys)
                ext_vals.append(round(fval, 1))
                ext_labels.append(eg.label)
                abs_f = abs(fval)
                if abs_f < _min:
                    ext_dirs.append("~ 0")
                elif fval > 0:
                    ext_dirs.append("\u2192 outflow" if eg.source_type == "qext" else "\u2192 into reservoir")
                else:
                    ext_dirs.append("\u2190 return" if eg.source_type == "qext" else "\u2190 from reservoir")

                # Text label inside arrowhead
                sign_d = 1.0 if fval >= 0.0 else -1.0
                if abs_f < _min:
                    ext_txt_texts.append("")
                    ext_txt_xs.append(eg.cx)
                    ext_txt_ys.append(eg.cy)
                    ext_txt_angles.append(0.0)
                else:
                    ext_txt_texts.append(
                        f"{abs_f:.0f}" if abs_f < 1_000 else f"{abs_f / 1_000:.1f}k"
                    )
                    L = _scaled_arrow_length(
                        abs_f, _ref,
                        spec.reference_arrow_length_m, spec.scale_mode,
                    )
                    ext_txt_xs.append(eg.cx + 0.15 * L * sign_d * eg.tx)
                    ext_txt_ys.append(eg.cy + 0.15 * L * sign_d * eg.ty)
                    raw = math.atan2(sign_d * eg.ty, sign_d * eg.tx)
                    if raw > math.pi / 2:
                        raw -= math.pi
                    elif raw < -math.pi / 2:
                        raw += math.pi
                    ext_txt_angles.append(raw)

            self._ext_arrow_source.data = {
                "xs": ext_xs, "ys": ext_ys,
                "values": ext_vals, "labels": ext_labels, "directions": ext_dirs,
                "color": [self._flow_to_color(v) for v in ext_vals],
            }
            if self._ext_arrow_text_source is not None:
                self._ext_arrow_text_source.data = {
                    "x": ext_txt_xs, "y": ext_txt_ys,
                    "text": ext_txt_texts, "angle": ext_txt_angles,
                }

        # ---- Bar update ----
        bar_xs, bar_ys, bar_colors = [], [], []
        bar_ch_ids, bar_node_ids, bar_node_labels, bar_vals, bar_sides = [], [], [], [], []
        for bg in self._bar_geoms:
            ch_flows = {conn.channel: flow_dict.get(conn.channel, 0.0) for conn in bg.connections}
            xs_l, ys_l, c_l, ch_l, v_l, s_l = _bar_segments(
                bg.bx, bg.by, bg.connections, ch_flows,
                _ref, spec.bar_width_m, spec.bar_max_height_m,
            )
            node_label = bg.label or f"Node {bg.node}"
            bar_xs.extend(xs_l)
            bar_ys.extend(ys_l)
            bar_colors.extend(c_l)
            bar_ch_ids.extend(ch_l)
            bar_node_ids.extend([bg.node] * n)
            bar_node_labels.extend([node_label] * n)
            bar_vals.extend(v_l)
            bar_sides.extend(s_l)

        self._bar_source.data = {
            "xs":          bar_xs,
            "ys":          bar_ys,
            "colors":      bar_colors,
            "channel_ids": bar_ch_ids,
            "node_ids":    bar_node_ids,
            "node_labels": bar_node_labels,
            "values":      bar_vals,
            "sides":       bar_sides,
        }

    # ----------------------------------------------------------------
    # Live control card
    # ----------------------------------------------------------------

    #: Diverging colormaps available in the flow controls panel.
    _FLOW_COLORMAPS = ["coolwarm", "RdBu_r", "RdYlBu_r", "seismic", "bwr", "PiYG"]

    def create_control_card(self) -> "pn.Card":
        """Return a collapsible Panel Card with live flow-layer controls.

        Wire the card into a parent layout::

            card = flow_layer.create_control_card()
            mgr._controls.append(card)      # GeoAnimatorManager sidebar
        """
        self._w_colormap = pn.widgets.Select(
            name="Colormap",
            options=self._FLOW_COLORMAPS,
            value=self._spec.colormap,
            sizing_mode="stretch_width",
        )
        self._w_alpha = pn.widgets.IntSlider(
            name="Opacity %",
            value=int(round(getattr(self._spec, "alpha", 1.0) * 100)),
            start=0, end=100, step=5,
            sizing_mode="stretch_width",
        )
        _is_velocity = getattr(self._spec, "variable", "flow") == "velocity"
        ref = self._ref_scale
        _vmin = self._spec.flow_vmin if self._spec.flow_vmin is not None else -ref
        _vmax = self._spec.flow_vmax if self._spec.flow_vmax is not None else  ref
        self._w_clim = pn.widgets.TextInput(
            name="Color range  (min, max)",
            value=f"{_vmin:.4g}, {_vmax:.4g}",
            sizing_mode="stretch_width",
        )
        self._w_scale = pn.widgets.Select(
            name="Scale mode",
            options=["linear", "log", "sqrt", "cbrt"],
            value=self._spec.scale_mode,
            sizing_mode="stretch_width",
        )
        self._w_ref_flow = pn.widgets.FloatInput(
            name="Reference velocity (ft/s)" if _is_velocity else "Reference flow (cfs)",
            value=self._spec.reference_velocity if _is_velocity else self._spec.reference_flow,
            step=1_000.0,
            start=1.0,
            sizing_mode="stretch_width",
        )
        self._w_arrow_length = pn.widgets.FloatInput(
            name="Arrow length at ref (m)",
            value=self._spec.reference_arrow_length_m,
            step=100.0,
            start=10.0,
            sizing_mode="stretch_width",
        )
        self._w_arrow_width = pn.widgets.FloatInput(
            name="Arrow width (m)",
            value=self._spec.arrow_width_m,
            step=50.0,
            start=10.0,
            sizing_mode="stretch_width",
        )
        self._w_bar_height = pn.widgets.FloatInput(
            name="Bar height at ref (m)",
            value=self._spec.bar_max_height_m,
            step=200.0,
            start=10.0,
            sizing_mode="stretch_width",
        )
        self._w_min_flow = pn.widgets.FloatInput(
            name="Min velocity stub (ft/s)" if _is_velocity else "Min flow stub (cfs)",
            value=getattr(self._spec, "min_velocity_fps", 0.0) if _is_velocity else self._spec.min_flow_cfs,
            step=0.1 if _is_velocity else 10.0,
            start=0.0,
            sizing_mode="stretch_width",
        )
        _show_label = "Show velocity arrows" if _is_velocity else "Show flow arrows"
        _hide_label = "Velocity arrows hidden" if _is_velocity else "Flow arrows hidden"
        self._w_visible = pn.widgets.Toggle(
            name=_show_label, value=True,
            button_type="success", sizing_mode="stretch_width",
        )

        def _on_visible_flow(event):
            visible = bool(event.new)
            for r in (
                getattr(self, "_arrow_renderer", None),
                getattr(self, "_bar_renderer", None),
                getattr(self, "_ext_arrow_renderer", None),
            ):
                if r is not None:
                    r.visible = visible
            for extra in self._extra_renderers:
                for r in extra.values():
                    if r is not None:
                        r.visible = visible
            self._w_visible.name = _show_label if visible else _hide_label

        def _on_alpha_flow(event):
            self._spec.alpha = event.new / 100.0
            self._apply_alpha(self._spec.alpha)

        self._w_visible.param.watch(_on_visible_flow, "value")
        self._w_alpha.param.watch(_on_alpha_flow, "value")
        for w in (
            self._w_colormap, self._w_clim, self._w_scale, self._w_ref_flow,
            self._w_arrow_length, self._w_arrow_width,
            self._w_bar_height, self._w_min_flow,
        ):
            w.param.watch(self._on_spec_change, "value")

        return pn.Card(
            self._w_visible,
            self._w_alpha,
            pn.layout.Divider(margin=(2, 0, 2, 0)),
            self._w_colormap,
            self._w_clim,
            self._w_scale,
            pn.layout.Divider(margin=(2, 0, 2, 0)),
            self._w_ref_flow,
            self._w_arrow_length,
            self._w_arrow_width,
            pn.layout.Divider(margin=(2, 0, 2, 0)),
            self._w_bar_height,
            self._w_min_flow,
            title="\U0001f9ed Velocity Layer" if _is_velocity else "\U0001f9ed Flow Layer",
            collapsed=True,
            sizing_mode="stretch_width",
        )

    def _on_spec_change(self, event=None) -> None:
        """Apply current widget values to spec and re-render the current frame."""
        if not hasattr(self, "_w_colormap"):
            return
        self._spec.colormap             = self._w_colormap.value
        self._spec.scale_mode           = self._w_scale.value
        if getattr(self._spec, "variable", "flow") == "velocity":
            self._spec.reference_velocity   = float(self._w_ref_flow.value or 1)
        else:
            self._spec.reference_flow       = float(self._w_ref_flow.value or 1)
        self._spec.reference_arrow_length_m = float(self._w_arrow_length.value or 10)
        self._spec.arrow_width_m        = float(self._w_arrow_width.value or 10)
        self._spec.bar_max_height_m     = float(self._w_bar_height.value or 10)
        if getattr(self._spec, "variable", "flow") == "velocity":
            self._spec.min_velocity_fps = float(self._w_min_flow.value or 0)
        else:
            self._spec.min_flow_cfs     = float(self._w_min_flow.value or 0)
        # Parse "min, max" color range — reversed order (e.g. "1000, -1000") inverts
        # the colourmap, identical to the channel appearance colour range behaviour.
        try:
            parts = [p.strip() for p in self._w_clim.value.split(",")]
            if len(parts) == 2:
                vmin, vmax = float(parts[0]), float(parts[1])
                if vmin != vmax:   # reject only equal values (would cause div/0)
                    self._spec.flow_vmin = vmin
                    self._spec.flow_vmax = vmax
        except (ValueError, AttributeError):
            pass
        if self._last_ts is None or self._arrow_source is None:
            return
        ts = self._last_ts
        doc = self._arrow_source.document
        if doc is not None:
            doc.add_next_tick_callback(lambda: self.update_frame(ts))
        else:
            self.update_frame(ts)

    def trigger_redraw(self, event=None) -> None:
        """Re-render the current frame using the current spec.

        Useful when an external actor updates ``_spec`` (e.g. a shared
        control card) and needs to force a visual refresh without touching
        widget state on this instance.
        """
        if self._last_ts is None or self._arrow_source is None:
            return
        ts = self._last_ts
        doc = self._arrow_source.document
        if doc is not None:
            doc.add_next_tick_callback(lambda: self.update_frame(ts))
        else:
            self.update_frame(ts)



class FlowAnimatorManager(pn.viewable.Viewer):
    """Standalone Panel Viewer for DSM2 flow arrows and junction bars.

    Displays flow arrows and junction flow-split bars on a CARTO Light map
    tile background with no channel colourmap.  The viewer has the same
    player/DatetimePicker interface as
    :class:`~dvue.animator.GeoAnimatorManager`.

    For overlaying flow on an existing qual animation use
    :func:`~dsm2ui.animate.animate_qual` with the ``flow_spec`` /
    ``hydro_h5_path`` keyword arguments instead.

    Parameters
    ----------
    hydro_h5_path : str or Path
        HYDRO HDF5 tidefile.
    spec : FlowLayerSpec
        Arrow and bar configuration.
    channel_gdf : geopandas.GeoDataFrame, optional
        DSM2 channel centreline geometry.  Defaults to the bundled GeoJSON.
    nodes_gdf : geopandas.GeoDataFrame, optional
        DSM2 node geometry.  Defaults to the bundled GeoJSON.
    title : str, optional
        Figure title.  Default ``"DSM2 Flow"``.
    map_height : int, optional
        Minimum map height in pixels.  Default 500.
    """

    def __init__(
        self,
        hydro_h5_path: "str | Path",
        spec: FlowLayerSpec,
        channel_gdf: "geopandas.GeoDataFrame | None" = None,
        nodes_gdf: "geopandas.GeoDataFrame | None" = None,
        title: str = "DSM2 Flow",
        map_height: int = 500,
        **params,
    ) -> None:
        from dsm2ui.animate import load_dsm2_channel_gdf, load_dsm2_nodes_gdf

        if channel_gdf is None:
            channel_gdf = load_dsm2_channel_gdf()
        if nodes_gdf is None:
            nodes_gdf = load_dsm2_nodes_gdf()

        self._title = title
        self._map_height = map_height

        # Build FlowLayer
        self._flow_layer = FlowLayer(hydro_h5_path, spec, channel_gdf, nodes_gdf)

        # Build Bokeh figure zoomed to arrow/bar locations + add layer
        figure = self._build_figure(channel_gdf)
        self._flow_layer.setup_on_figure(figure)
        self._bk_figure = figure
        self._chart_pane = pn.pane.Bokeh(
            figure, sizing_mode="stretch_both", min_height=map_height
        )

        # Player controls
        ti = self._flow_layer.time_index
        self._time_div = Div(
            text=f"<b>{ti[0].strftime('%Y-%m-%d %H:%M')}</b>",
            styles={"font-size": "13px", "margin": "2px 0 6px 0"},
        )
        self._time_label_pane = pn.pane.Bokeh(self._time_div, sizing_mode="stretch_width")
        self._time_slider = pn.widgets.DiscretePlayer(
            name="",
            options=list(range(len(ti))),
            value=0,
            interval=500,
            loop_policy="once",
            show_value=False,
            sizing_mode="stretch_width",
        )
        self._datetime_picker = pn.widgets.DatetimePicker(
            name="Go to date/time",
            value=ti[0].to_pydatetime(),
            start=ti[0].to_pydatetime(),
            end=ti[-1].to_pydatetime(),
            sizing_mode="stretch_width",
        )
        self._syncing = False

        super().__init__(**params)

        # Wire watchers after super().__init__
        self._time_slider.param.watch(self._on_slider_change, "value")
        self._datetime_picker.param.watch(self._on_datetime_picker_change, "value")

        # Render the first frame immediately
        self._flow_layer.update_frame(self._flow_layer.time_index[0])

    # ----------------------------------------------------------------
    # Figure construction
    # ----------------------------------------------------------------

    def _build_figure(self, channel_gdf: "geopandas.GeoDataFrame"):
        """Build a Bokeh figure with WMTS tile.

        The initial viewport is centred on the pre-computed arrow centroids and
        bar node positions so the elements are immediately visible without the
        user having to zoom in.  Falls back to the full channel-network bounds
        when no arrows or bars are configured.
        """
        spec = self._flow_layer._spec

        # Gather all pre-computed positions in EPSG:3857 (metres)
        xs = [ag.cx for ag in self._flow_layer._arrow_geoms]
        ys = [ag.cy for ag in self._flow_layer._arrow_geoms]
        xs += [bg.bx for bg in self._flow_layer._bar_geoms]
        ys += [bg.by for bg in self._flow_layer._bar_geoms]

        if xs:
            x_min, x_max = min(xs), max(xs)
            y_min, y_max = min(ys), max(ys)
            cx = (x_min + x_max) / 2.0
            cy = (y_min + y_max) / 2.0

            # Minimum span: ensure arrows fit comfortably even when all
            # features are co-located (e.g. a single node with no arrows).
            min_span = spec.reference_arrow_length_m * 8.0
            span = max(x_max - x_min, y_max - y_min, min_span)

            # 60 % padding on each side so features aren't right at the edge.
            half = span * 0.5 + span * 0.6
            x0, x1 = cx - half, cx + half
            y0, y1 = cy - half, cy + half
        else:
            # Fall back to full channel-network bounds.
            gdf_3857 = channel_gdf.to_crs("EPSG:3857")
            b = gdf_3857.total_bounds  # [minx, miny, maxx, maxy]
            pw, ph = (b[2] - b[0]) * 0.05, (b[3] - b[1]) * 0.05
            x0, x1 = b[0] - pw, b[2] + pw
            y0, y1 = b[1] - ph, b[3] + ph

        x_range = Range1d(x0, x1, bounds="auto")
        y_range = Range1d(y0, y1, bounds="auto")

        p = bk_figure(
            x_range=x_range,
            y_range=y_range,
            x_axis_type="mercator",
            y_axis_type="mercator",
            match_aspect=True,
            sizing_mode="stretch_both",
            min_height=self._map_height,
            title=self._title,
            tools="pan,wheel_zoom,box_zoom,reset,save",
            active_scroll="wheel_zoom",
        )
        p.axis.visible = False
        p.add_tile(WMTSTileSource(url=_CARTO_LIGHT_URL, attribution=_CARTO_LIGHT_ATTR))
        return p

    # ----------------------------------------------------------------
    # Callbacks
    # ----------------------------------------------------------------

    def _on_slider_change(self, event) -> None:
        if self._syncing:
            return
        idx = int(event.new)
        ti = self._flow_layer.time_index
        ts = ti[idx]
        self._syncing = True
        try:
            self._datetime_picker.value = ts.to_pydatetime()
        finally:
            self._syncing = False
        doc = self._bk_figure.document
        if doc is not None:
            doc.add_next_tick_callback(
                lambda _i=idx, _ts=ts: self._apply_frame(_i, _ts)
            )
        else:
            self._apply_frame(idx, ts)

    def _apply_frame(self, idx: int, ts: pd.Timestamp) -> None:
        self._time_div.text = f"<b>{ts.strftime('%Y-%m-%d %H:%M')}</b>"
        self._flow_layer.update_frame(ts)

    def _on_datetime_picker_change(self, event) -> None:
        if self._syncing:
            return
        ti = self._flow_layer.time_index
        ts = pd.Timestamp(event.new)
        idx = int(ti.get_indexer([ts], method="nearest")[0])
        idx = max(0, min(idx, len(ti) - 1))
        self._syncing = True
        try:
            self._time_slider.value = idx
        finally:
            self._syncing = False
        doc = self._bk_figure.document
        if doc is not None:
            doc.add_next_tick_callback(
                lambda _i=idx, _ts=ti[_i]: self._apply_frame(_i, _ts)
            )
        else:
            self._apply_frame(idx, ti[idx])

    # ----------------------------------------------------------------
    # Panel layout
    # ----------------------------------------------------------------

    def __panel__(self) -> pn.viewable.Viewable:
        controls = pn.Column(
            pn.pane.Markdown("### Flow Controls", margin=(4, 0, 2, 0)),
            self._time_label_pane,
            self._time_slider,
            self._datetime_picker,
            pn.layout.Divider(margin=(4, 0, 4, 0)),
            self._flow_layer.create_control_card(),
            sizing_mode="stretch_width",
            max_width=300,
            margin=(4, 8, 4, 4),
        )
        return pn.Column(
            pn.Row(controls, self._chart_pane, sizing_mode="stretch_both"),
            sizing_mode="stretch_both",
            min_height=self._map_height,
        )

    def servable(self, title: Optional[str] = None, **kwargs) -> "FlowAnimatorManager":
        """Mark this component as the app entry point."""
        super().servable(title=title or self._title, **kwargs)
        return self
