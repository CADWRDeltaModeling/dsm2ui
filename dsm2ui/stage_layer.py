"""DSM2 stage visualization layer — per-channel water-level deviation bars.

:class:`StageLayerSpec` describes which channels to display as vertical
stage-deviation bars.  :class:`StageLayer` renders them as Bokeh Patch glyphs
on an existing Bokeh figure and updates them every animation frame.

The bar height above or below the reference line shows how much the current
channel stage deviates from the user-supplied mean stage.  Bars above the
mean are drawn in blue; bars below are drawn in red.

The per-channel mean stage is **user-supplied** in the YAML config
(``mean_stage_ft`` per bar).  Use a separate utility to compute the means
from the HDF5 file if needed.

Typical usage with hydro animation::

    from dsm2ui.animate import animate_hydro, load_dsm2_channel_gdf
    from dsm2ui.stage_layer import StageLayerSpec, StageLayer

    stage_spec = StageLayerSpec.from_yaml("stage_config.yaml")
    mgr = animate_hydro("hydro.h5", variable="stage")

    ch_gdf = load_dsm2_channel_gdf()
    stage_layer = StageLayer("hydro.h5", stage_spec, ch_gdf)
    stage_layer.setup_on_figure(mgr._bk_figure)
    mgr.add_frame_callback(stage_layer.update_frame)
    mgr._controls.append(stage_layer.create_control_card())

Typical usage with qual animation::

    from dsm2ui.animate import animate_qual
    mgr = animate_qual(
        "qual_ec.h5",
        stage_spec=stage_spec,
        hydro_h5_path="hydro.h5",
    )
"""

from __future__ import annotations

import dataclasses
import logging
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd
import panel as pn

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Spec dataclasses
# ---------------------------------------------------------------------------

@dataclasses.dataclass
class StageBarSpec:
    """Specification for one channel stage bar.

    Parameters
    ----------
    channel : int
        DSM2 channel number.
    position : float, optional
        Fractional position along the channel (0 = upstream end,
        1 = downstream end, 0.5 = midpoint).  Default 0.5.
    label : str, optional
        Display label shown in the hover tooltip.
    mean_stage_ft : float
        Mean (reference) **water-surface elevation** at this location in feet
        (ft NAVD88 or NGVD29 — same datum as the model).  The bar height is
        proportional to ``current_stage − mean_stage_ft``.

        The stage reader (:class:`~dsm2ui.animate.HydroH5StageReader`) returns
        true elevation (depth + channel bottom from ``/hydro/geometry/channel_bottom``),
        so this value must be a datum-referenced elevation, not a depth.
        Compute it as the temporal mean of the corrected stage over your period
        of interest (e.g. using pydsm's ``HydroH5.get_channel_stage()``).
    """

    channel: int
    position: float = 0.5
    label: str = ""
    mean_stage_ft: float = 0.0


@dataclasses.dataclass
class StageLayerSpec:
    """Complete specification for the stage bar visualization layer.

    Parameters
    ----------
    bars : list of StageBarSpec
        Stage bars to display.
    bar_width_m : float, optional
        Bar width in EPSG:3857 metres.  Default 150 m.
    bar_max_height_m : float, optional
        Height in EPSG:3857 metres corresponding to *reference_stage_range_ft*
        of deviation.  Larger deviations are clamped.  Default 600 m.
    reference_stage_range_ft : float, optional
        Stage deviation in feet that maps to *bar_max_height_m*.  Default 3 ft.
    show_labels : bool, optional
        Render channel labels below the reference tick.  Default ``True``.

    Example YAML (``stage_config.yaml``)::

        bars:
          - channel: 10
            position: 0.5
            label: "Sacramento at Freeport"
            mean_stage_ft: 5.2
          - channel: 25
            position: 0.5
            label: "San Joaquin at Vernalis"
            mean_stage_ft: 3.1
        bar_width_m: 150
        bar_max_height_m: 600
        reference_stage_range_ft: 3.0
        show_labels: true
    """

    bars: List[StageBarSpec] = dataclasses.field(default_factory=list)
    bar_width_m: float = 150.0
    bar_max_height_m: float = 600.0
    reference_stage_range_ft: float = 3.0
    show_labels: bool = False
    show_range_box: bool = True

    @classmethod
    def from_yaml(cls, path: "str | Path") -> "StageLayerSpec":
        """Load a :class:`StageLayerSpec` from a YAML file.

        Parameters
        ----------
        path : str or Path
            Path to the YAML config file.
        """
        import yaml

        with open(path) as fh:
            d = yaml.safe_load(fh) or {}

        bars = [StageBarSpec(**b) for b in d.pop("bars", [])]
        valid_keys = {f.name for f in dataclasses.fields(cls)} - {"bars"}
        return cls(
            bars=bars,
            **{k: v for k, v in d.items() if k in valid_keys},
        )


# ---------------------------------------------------------------------------
# Internal geometry dataclass
# ---------------------------------------------------------------------------

@dataclasses.dataclass
class _StageBarGeom:
    """Pre-computed geometry for one stage bar (EPSG:3857 coordinates)."""

    channel: int
    label: str
    mean_stage_ft: float
    cx: float      # bar centre x (m)
    base_y: float  # mean reference y (m) — bars extend up or down from here
    # hw is computed dynamically from StageLayerSpec.bar_width_m each frame


# ---------------------------------------------------------------------------
# StageLayer
# ---------------------------------------------------------------------------

class StageLayer:
    """Animated stage deviation bars for DSM2 HYDRO data.

    Creates two Bokeh renderers on the target figure:

    * A static ``multi_line`` renderer drawing a dashed horizontal tick at
      ``base_y`` for each bar (the mean reference line).
    * A ``patches`` renderer drawing a filled rectangle per bar whose height
      changes every frame.  Blue = above mean, red = below mean.

    The layer plugs into :class:`~dvue.animator.GeoAnimatorManager` via the
    ``add_frame_callback`` mechanism::

        stage = StageLayer("hydro.h5", spec, channel_gdf)
        stage.setup_on_figure(mgr._bk_figure)
        mgr.add_frame_callback(stage.update_frame)

    Parameters
    ----------
    hydro_h5_path : str or Path
        HYDRO HDF5 tidefile.
    spec : StageLayerSpec
        Bar configuration.
    channel_gdf : geopandas.GeoDataFrame
        DSM2 channel centreline geometry (must have a ``geo_id`` column of
        integer channel numbers) as returned by
        :func:`~dsm2ui.animate.load_dsm2_channel_gdf`.
    """

    def __init__(
        self,
        hydro_h5_path: "str | Path",
        spec: StageLayerSpec,
        channel_gdf: "geopandas.GeoDataFrame",
    ) -> None:
        self._spec = spec
        self._hydro_h5_path = Path(hydro_h5_path)

        ch_gdf_3857 = channel_gdf.to_crs("EPSG:3857").copy()

        self._bar_geoms: List[_StageBarGeom] = []
        for bsp in spec.bars:
            row = ch_gdf_3857[ch_gdf_3857["geo_id"] == bsp.channel]
            if row.empty:
                log.warning(
                    "StageLayer: channel %d not found in channel_gdf — skipping",
                    bsp.channel,
                )
                continue
            geom = row.iloc[0].geometry
            pt = geom.interpolate(bsp.position, normalized=True)
            self._bar_geoms.append(_StageBarGeom(
                channel=bsp.channel,
                label=bsp.label or f"Ch {bsp.channel}",
                mean_stage_ft=bsp.mean_stage_ft,
                cx=pt.x,
                base_y=pt.y,
            ))

        # Reader and Bokeh objects — set lazily / in setup_on_figure()
        self._reader = None
        self._bar_source = None
        self._bar_renderer = None
        self._ref_source = None
        self._range_box_source = None
        self._range_box_renderer = None
        self._label_set = None

        # UI widgets — created lazily in create_control_card()
        self._w_visible = None
        self._w_alpha = None
        self._w_bar_width = None
        self._w_bar_height = None
        self._w_ref_range = None
        self._w_show_labels = None
        self._w_show_range_box = None
        self._last_ts: Optional[pd.Timestamp] = None

    # ----------------------------------------------------------------
    # Reader (lazy)
    # ----------------------------------------------------------------

    def _get_reader(self):
        if self._reader is None:
            from dsm2ui.animate import HydroH5StageReader
            from dvue.animator import BufferedSlicingReader
            base = HydroH5StageReader(self._hydro_h5_path, location="both")
            self._reader = BufferedSlicingReader(base, chunk_size=200, prefetch=True)
        return self._reader

    # ----------------------------------------------------------------
    # Setup
    # ----------------------------------------------------------------

    def setup_on_figure(self, bk_fig) -> None:
        """Add Bokeh renderers to *bk_fig*.

        Must be called once before :meth:`update_frame`.

        Parameters
        ----------
        bk_fig : bokeh.plotting.figure
            The existing Bokeh figure.
        """
        from bokeh.models import ColumnDataSource, HoverTool, LabelSet

        n = len(self._bar_geoms)
        hw = self._spec.bar_width_m / 2.0
        max_h = self._spec.bar_max_height_m

        # ---- Static range box (semi-transparent band showing ±reference_stage_range_ft) ----
        # Rendered first so it appears behind the dynamic bars and reference line.
        box_xs = [
            [g.cx - hw, g.cx + hw, g.cx + hw, g.cx - hw]
            for g in self._bar_geoms
        ]
        box_ys = [
            [g.base_y - max_h, g.base_y - max_h, g.base_y + max_h, g.base_y + max_h]
            for g in self._bar_geoms
        ]
        self._range_box_source = ColumnDataSource({"xs": box_xs, "ys": box_ys})
        self._range_box_renderer = bk_fig.patches(
            xs="xs", ys="ys",
            fill_color="#aaaaaa", fill_alpha=0.18,
            line_color="#888888", line_width=1,
            source=self._range_box_source,
            level="overlay",
            visible=self._spec.show_range_box,
        )

        # ---- Static reference-line (dashed horizontal tick at base_y) ----
        ref_xs = [[g.cx - hw, g.cx + hw] for g in self._bar_geoms]
        ref_ys = [[g.base_y,  g.base_y]  for g in self._bar_geoms]
        self._ref_source = ColumnDataSource({"xs": ref_xs, "ys": ref_ys})
        bk_fig.multi_line(
            xs="xs", ys="ys",
            source=self._ref_source,
            line_color="#444444", line_width=2, line_dash="dashed",
            level="overlay",
        )

        # ---- Dynamic bar rectangles (Patches, patched every frame) ----
        init_xs = [
            [g.cx - hw, g.cx + hw, g.cx + hw, g.cx - hw]
            for g in self._bar_geoms
        ]
        # All bars start at zero height
        init_ys = [
            [g.base_y, g.base_y, g.base_y, g.base_y]
            for g in self._bar_geoms
        ]
        self._bar_source = ColumnDataSource({
            "xs":     init_xs,
            "ys":     init_ys,
            "color":  ["steelblue"] * n,
            "labels": [g.label for g in self._bar_geoms],
            "stages": [0.0] * n,
            "means":  [g.mean_stage_ft for g in self._bar_geoms],
            "devs":   [0.0] * n,
        })
        self._bar_renderer = bk_fig.patches(
            xs="xs", ys="ys",
            fill_color="color", fill_alpha=0.75,
            line_color="#333333", line_width=1,
            source=self._bar_source,
            level="overlay",
        )
        bk_fig.add_tools(HoverTool(
            renderers=[self._bar_renderer],
            tooltips=[
                ("Station",        "@labels"),
                ("Stage (ft)",     "@stages{0.00}"),
                ("Mean (ft)",      "@means{0.00}"),
                ("Deviation (ft)", "@devs{+0.00}"),
            ],
        ))

        # ---- Labels below the reference tick (always created, visibility controlled) ----
        label_src = ColumnDataSource({
            "x":    [g.cx     for g in self._bar_geoms],
            "y":    [g.base_y for g in self._bar_geoms],
            "text": [g.label  for g in self._bar_geoms],
        })
        self._label_set = LabelSet(
            x="x", y="y",
            text="text",
            source=label_src,
            text_font_size="9px",
            x_offset=-25, y_offset=-14,
            level="overlay",
            visible=self._spec.show_labels,
        )
        bk_fig.add_layout(self._label_set)

    # ----------------------------------------------------------------
    # Frame update
    # ----------------------------------------------------------------

    def update_frame(self, ts: pd.Timestamp) -> None:
        """Patch bar heights and colours for the current animation timestamp.

        Parameters
        ----------
        ts : pd.Timestamp
            Current animation timestamp.
        """
        if self._bar_source is None:
            return  # setup_on_figure not yet called

        self._last_ts = ts
        reader = self._get_reader()
        series = reader.get_slice_nearest(ts)  # Series(index=channel_no, values=stage_ft)

        spec = self._spec
        hw = spec.bar_width_m / 2.0
        max_h = spec.bar_max_height_m
        ref_range = max(spec.reference_stage_range_ft, 1e-6)

        new_xs: list = []
        new_ys: list = []
        new_colors: list = []
        stages: list = []
        devs: list = []

        for g in self._bar_geoms:
            stage = float(series.get(g.channel, g.mean_stage_ft))
            dev = stage - g.mean_stage_ft
            h_m = float(np.clip(dev / ref_range * max_h, -max_h, max_h))
            top_y = g.base_y + h_m
            new_xs.append([g.cx - hw, g.cx + hw, g.cx + hw, g.cx - hw])
            # 4-vertex rectangle (counter-clockwise from bottom-left)
            new_ys.append([g.base_y, g.base_y, top_y, top_y])
            new_colors.append("steelblue" if dev >= 0.0 else "crimson")
            stages.append(round(stage, 3))
            devs.append(round(dev, 3))

        self._bar_source.data = dict(
            self._bar_source.data,
            xs=new_xs,
            ys=new_ys,
            color=new_colors,
            stages=stages,
            devs=devs,
        )

    # ----------------------------------------------------------------
    # Control card
    # ----------------------------------------------------------------

    # ----------------------------------------------------------------
    # State dict (for config save/load)
    # ----------------------------------------------------------------

    def get_state_dict(self) -> dict:
        """Return a dict of current spec settings for YAML config persistence."""
        alpha_pct = 75
        if hasattr(self, "_w_alpha") and self._w_alpha is not None:
            alpha_pct = self._w_alpha.value
        return {
            "bar_width_m":              self._spec.bar_width_m,
            "bar_max_height_m":         self._spec.bar_max_height_m,
            "reference_stage_range_ft": self._spec.reference_stage_range_ft,
            "show_labels":              self._spec.show_labels,
            "show_range_box":           self._spec.show_range_box,
            "alpha":                    alpha_pct,
        }

    # ----------------------------------------------------------------
    # Control card
    # ----------------------------------------------------------------

    def create_control_card(self) -> "pn.Card":
        """Return a collapsible Panel Card with visibility, sizing, and label controls."""
        self._w_visible = pn.widgets.Toggle(
            name="Show stage bars", value=True,
            button_type="success", sizing_mode="stretch_width",
        )
        self._w_alpha = pn.widgets.IntSlider(
            name="Opacity %", value=75, start=0, end=100, step=5,
            sizing_mode="stretch_width",
        )
        self._w_bar_width = pn.widgets.FloatInput(
            name="Bar width (m)",
            value=self._spec.bar_width_m,
            step=50.0, start=10.0,
            sizing_mode="stretch_width",
        )
        self._w_bar_height = pn.widgets.FloatInput(
            name="Bar height at ref range (m)",
            value=self._spec.bar_max_height_m,
            step=100.0, start=10.0,
            sizing_mode="stretch_width",
        )
        self._w_ref_range = pn.widgets.FloatInput(
            name="Reference stage range (ft)",
            value=self._spec.reference_stage_range_ft,
            step=0.5, start=0.1,
            sizing_mode="stretch_width",
        )
        self._w_show_labels = pn.widgets.Toggle(
            name="Show labels", value=self._spec.show_labels,
            button_type="default", sizing_mode="stretch_width",
        )
        self._w_show_range_box = pn.widgets.Toggle(
            name="Show tidal range box", value=self._spec.show_range_box,
            button_type="default", sizing_mode="stretch_width",
        )

        def _on_visible(event):
            visible = bool(event.new)
            if self._bar_renderer is not None:
                self._bar_renderer.visible = visible
            if self._range_box_renderer is not None:
                self._range_box_renderer.visible = (
                    visible and self._spec.show_range_box
                )
            self._w_visible.name = (
                "Show stage bars" if visible else "Stage bars hidden"
            )

        def _on_alpha(event):
            if self._bar_renderer is not None:
                self._bar_renderer.glyph.fill_alpha = event.new / 100.0

        def _on_show_labels(event):
            self._spec.show_labels = bool(event.new)
            if self._label_set is not None:
                self._label_set.visible = bool(event.new)
            self._w_show_labels.name = "Show labels" if event.new else "Hide labels"

        def _on_show_range_box(event):
            self._spec.show_range_box = bool(event.new)
            if self._range_box_renderer is not None:
                self._range_box_renderer.visible = bool(event.new)
            self._w_show_range_box.name = (
                "Show tidal range box" if event.new else "Hide tidal range box"
            )

        self._w_visible.param.watch(_on_visible, "value")
        self._w_alpha.param.watch(_on_alpha, "value")
        self._w_show_labels.param.watch(_on_show_labels, "value")
        self._w_show_range_box.param.watch(_on_show_range_box, "value")

        for w in (self._w_bar_width, self._w_bar_height, self._w_ref_range):
            w.param.watch(self._on_spec_change, "value")

        return pn.Card(
            self._w_visible,
            self._w_alpha,
            pn.layout.Divider(margin=(2, 0, 2, 0)),
            self._w_bar_width,
            self._w_bar_height,
            self._w_ref_range,
            pn.layout.Divider(margin=(2, 0, 2, 0)),
            self._w_show_range_box,
            self._w_show_labels,
            title="\U0001f4ca Stage Bars",
            collapsed=True,
            sizing_mode="stretch_width",
        )

    def _sync_geometry_and_redraw(self) -> None:
        """Rebuild static geometry (ref-line, range-box) from the current shared
        spec, then re-render the current frame.

        Called by a primary :class:`StageLayer` in a two-panel comparison after
        its ``_on_spec_change`` has mutated the shared :class:`StageLayerSpec`.
        """
        hw = self._spec.bar_width_m / 2.0
        max_h = self._spec.bar_max_height_m
        if self._ref_source is not None:
            self._ref_source.data = {
                "xs": [[g.cx - hw, g.cx + hw] for g in self._bar_geoms],
                "ys": [[g.base_y,  g.base_y]  for g in self._bar_geoms],
            }
        if self._range_box_source is not None:
            self._range_box_source.data = {
                "xs": [[g.cx - hw, g.cx + hw, g.cx + hw, g.cx - hw]
                        for g in self._bar_geoms],
                "ys": [[g.base_y - max_h, g.base_y - max_h,
                        g.base_y + max_h, g.base_y + max_h]
                        for g in self._bar_geoms],
            }
        self.trigger_redraw()

    def link_to_secondary(self, secondary: "StageLayer") -> None:
        """Register watchers so every widget change on *self* propagates to
        *secondary*'s renderers.

        Both layers must share the same :class:`StageLayerSpec` instance so
        spec mutations from *self*\'s ``_on_spec_change`` are automatically
        visible to *secondary*.

        Parameters
        ----------
        secondary : StageLayer
            The other panel's layer (e.g. panel B in a two-panel comparison).
        """
        def _sync_spec(event):
            # self._on_spec_change has already mutated the shared spec.
            secondary._sync_geometry_and_redraw()

        def _sync_visible(event):
            visible = bool(event.new)
            if secondary._bar_renderer is not None:
                secondary._bar_renderer.visible = visible
            if secondary._range_box_renderer is not None:
                secondary._range_box_renderer.visible = (
                    visible and secondary._spec.show_range_box
                )

        def _sync_alpha(event):
            if secondary._bar_renderer is not None:
                secondary._bar_renderer.glyph.fill_alpha = event.new / 100.0

        def _sync_show_labels(event):
            if secondary._label_set is not None:
                secondary._label_set.visible = bool(event.new)

        def _sync_show_range_box(event):
            if secondary._range_box_renderer is not None:
                secondary._range_box_renderer.visible = bool(event.new)

        for w in (self._w_bar_width, self._w_bar_height, self._w_ref_range):
            w.param.watch(_sync_spec, "value")
        self._w_visible.param.watch(_sync_visible, "value")
        self._w_alpha.param.watch(_sync_alpha, "value")
        self._w_show_labels.param.watch(_sync_show_labels, "value")
        self._w_show_range_box.param.watch(_sync_show_range_box, "value")

    def trigger_redraw(self, event=None) -> None:
        """Re-render the current frame.

        Called by a sibling :class:`StageLayer` in a two-panel comparison to
        propagate shared-spec changes to this panel without requiring its own
        widget callbacks to fire.
        """
        if self._last_ts is None or self._bar_source is None:
            return
        ts = self._last_ts
        doc = self._bar_source.document
        if doc is not None:
            doc.add_next_tick_callback(lambda: self.update_frame(ts))
        else:
            self.update_frame(ts)

    def _on_spec_change(self, event=None) -> None:
        """Apply widget values to spec and re-render the current frame."""
        if self._w_bar_width is None:
            return
        self._spec.bar_width_m            = float(self._w_bar_width.value or 10)
        self._spec.bar_max_height_m       = float(self._w_bar_height.value or 10)
        self._spec.reference_stage_range_ft = float(self._w_ref_range.value or 0.1)

        # Update static layers (ref-line + range box) when dimensions change
        hw = self._spec.bar_width_m / 2.0
        max_h = self._spec.bar_max_height_m
        if self._ref_source is not None:
            self._ref_source.data = {
                "xs": [[g.cx - hw, g.cx + hw] for g in self._bar_geoms],
                "ys": [[g.base_y,  g.base_y]  for g in self._bar_geoms],
            }
        if self._range_box_source is not None:
            self._range_box_source.data = {
                "xs": [[g.cx - hw, g.cx + hw, g.cx + hw, g.cx - hw] for g in self._bar_geoms],
                "ys": [[g.base_y - max_h, g.base_y - max_h,
                        g.base_y + max_h, g.base_y + max_h] for g in self._bar_geoms],
            }

        if self._last_ts is None or self._bar_source is None:
            return
        ts = self._last_ts
        doc = self._bar_source.document
        if doc is not None:
            doc.add_next_tick_callback(lambda: self.update_frame(ts))
        else:
            self.update_frame(ts)


# ---------------------------------------------------------------------------
# Standalone workhorse: compute mean stage from a HYDRO HDF5 file
# ---------------------------------------------------------------------------

def compute_stage_means(
    hydro_h5_path: "str | Path",
    channels: "List[int]",
    location: str = "both",
    start: "Optional[pd.Timestamp]" = None,
    end: "Optional[pd.Timestamp]" = None,
    chunk_size: int = 5000,
) -> "dict[int, float]":
    """Compute per-channel mean water-surface stage from a HYDRO HDF5 tidefile.

    Applies the channel-bottom correction (DSM2 issue #164) so the returned
    values are datum-referenced elevations (ft NAVD88 / NGVD29), matching the
    ``mean_stage_ft`` field expected by :class:`StageLayerSpec`.

    Parameters
    ----------
    hydro_h5_path : str or Path
        HYDRO HDF5 tidefile.
    channels : list of int
        DSM2 channel numbers to compute means for.
    location : {"both", "upstream", "downstream"}, optional
        Which channel end to use.  ``"both"`` (default) averages the two ends.
    start : pd.Timestamp or None, optional
        Start of averaging window (inclusive).  Defaults to start of run.
    end : pd.Timestamp or None, optional
        End of averaging window (inclusive).  Defaults to end of run.
    chunk_size : int, optional
        Number of timesteps read per HDF5 chunk.  Reduce if memory is limited.
        Default 5000.

    Returns
    -------
    dict[int, float]
        Mapping of channel number → mean stage in ft.  Channels with no data
        in the window return ``float("nan")``.
    """
    from dsm2ui.animate import HydroH5StageReader

    reader = HydroH5StageReader(hydro_h5_path, location=location)
    ti = reader.time_index

    # Resolve index bounds
    i0 = 0 if start is None else int(ti.searchsorted(start, side="left"))
    i1 = len(ti) if end is None else int(ti.searchsorted(end, side="right"))
    i0 = max(0, min(i0, len(ti)))
    i1 = max(i0, min(i1, len(ti)))

    if i0 >= i1:
        reader.close()
        raise ValueError(
            f"No timesteps in the requested window "
            f"[{start or ti[0]}, {end or ti[-1]}]."
        )

    channel_set = set(channels)
    sums: dict[int, float] = {ch: 0.0 for ch in channels}
    counts: dict[int, int] = {ch: 0 for ch in channels}

    for chunk_start in range(i0, i1, chunk_size):
        chunk_end = min(i1, chunk_start + chunk_size)
        df = reader.get_slice_range(chunk_start, chunk_end)
        for ch in channel_set:
            if ch in df.columns:
                col = df[ch].dropna()
                sums[ch] += float(col.sum())
                counts[ch] += len(col)

    reader.close()

    return {
        ch: (sums[ch] / counts[ch] if counts[ch] > 0 else float("nan"))
        for ch in channels
    }


def _update_yaml_means(yaml_text: str, means: "dict[int, float]") -> str:
    """Return *yaml_text* with ``mean_stage_ft`` values replaced in-place.

    All other content (comments, whitespace, ordering) is preserved.
    Only lines matching ``mean_stage_ft:`` that follow a ``channel: N``
    declaration are updated.

    Parameters
    ----------
    yaml_text : str
        Raw YAML file contents.
    means : dict[int, float]
        Mapping of channel number → new mean stage value.
    """
    import re

    lines = yaml_text.split("\n")
    result: List[str] = []
    current_channel: Optional[int] = None

    for line in lines:
        # Track which channel block we are in (handles both
        # "  - channel: N" list-item lines and "    channel: N" plain fields)
        m_ch = re.match(r"^[\s\-]*channel:\s*(\d+)", line)
        if m_ch:
            current_channel = int(m_ch.group(1))

        # Replace mean_stage_ft value, preserving indentation and any inline comment
        m_ms = re.match(r"^(\s+mean_stage_ft:\s*)([\d.eE+\-]+)(.*)", line)
        if m_ms and current_channel is not None and current_channel in means:
            val = means[current_channel]
            line = f"{m_ms.group(1)}{val:.4f}{m_ms.group(3)}"

        result.append(line)

    return "\n".join(result)
