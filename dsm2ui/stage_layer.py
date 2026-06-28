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
    show_labels: bool = True

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
    hw: float      # bar half-width (m)


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
        hw = spec.bar_width_m / 2.0

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
                hw=hw,
            ))

        # Reader and Bokeh objects — set lazily / in setup_on_figure()
        self._reader = None
        self._bar_source = None
        self._bar_renderer = None
        self._ref_source = None

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

        # ---- Static reference-line (dashed horizontal tick at base_y) ----
        ref_xs = [[g.cx - g.hw, g.cx + g.hw] for g in self._bar_geoms]
        ref_ys = [[g.base_y,    g.base_y]     for g in self._bar_geoms]
        self._ref_source = ColumnDataSource({"xs": ref_xs, "ys": ref_ys})
        bk_fig.multi_line(
            xs="xs", ys="ys",
            source=self._ref_source,
            line_color="#444444", line_width=2, line_dash="dashed",
            level="overlay",
        )

        # ---- Dynamic bar rectangles (Patches, patched every frame) ----
        init_xs = [
            [g.cx - g.hw, g.cx + g.hw, g.cx + g.hw, g.cx - g.hw]
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

        # ---- Optional labels below the reference tick ----
        if self._spec.show_labels:
            label_src = ColumnDataSource({
                "x":    [g.cx      for g in self._bar_geoms],
                "y":    [g.base_y  for g in self._bar_geoms],
                "text": [g.label   for g in self._bar_geoms],
            })
            bk_fig.add_layout(LabelSet(
                x="x", y="y",
                text="text",
                source=label_src,
                text_font_size="9px",
                x_offset=-25, y_offset=-14,
                level="overlay",
            ))

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

        reader = self._get_reader()
        series = reader.get_slice_nearest(ts)  # Series(index=channel_no, values=stage_ft)

        spec = self._spec
        max_h = spec.bar_max_height_m
        ref_range = max(spec.reference_stage_range_ft, 1e-6)

        new_ys: list = []
        new_colors: list = []
        stages: list = []
        devs: list = []

        for g in self._bar_geoms:
            stage = float(series.get(g.channel, g.mean_stage_ft))
            dev = stage - g.mean_stage_ft
            h_m = float(np.clip(dev / ref_range * max_h, -max_h, max_h))
            top_y = g.base_y + h_m
            # 4-vertex rectangle (counter-clockwise from bottom-left)
            new_ys.append([g.base_y, g.base_y, top_y, top_y])
            new_colors.append("steelblue" if dev >= 0.0 else "crimson")
            stages.append(round(stage, 3))
            devs.append(round(dev, 3))

        self._bar_source.data = dict(
            self._bar_source.data,
            ys=new_ys,
            color=new_colors,
            stages=stages,
            devs=devs,
        )

    # ----------------------------------------------------------------
    # Control card
    # ----------------------------------------------------------------

    def create_control_card(self) -> "pn.Card":
        """Return a collapsible Panel Card with visibility and opacity controls."""
        visible_toggle = pn.widgets.Toggle(
            name="Show stage bars", value=True,
            button_type="success", sizing_mode="stretch_width",
        )
        alpha_slider = pn.widgets.IntSlider(
            name="Opacity %", value=75, start=0, end=100, step=5,
            sizing_mode="stretch_width",
        )

        def _on_visible(event):
            if self._bar_renderer is not None:
                self._bar_renderer.visible = bool(event.new)
            visible_toggle.name = (
                "Show stage bars" if event.new else "Stage bars hidden"
            )

        def _on_alpha(event):
            if self._bar_renderer is not None:
                self._bar_renderer.glyph.fill_alpha = event.new / 100.0

        visible_toggle.param.watch(_on_visible, "value")
        alpha_slider.param.watch(_on_alpha, "value")

        return pn.Card(
            visible_toggle,
            alpha_slider,
            title="\U0001f4ca Stage Bars",
            collapsed=True,
            sizing_mode="stretch_width",
        )
