"""CLI commands for DSM2 HDF5 geo-animation.

Adds the ``dsm2ui animate`` group with two subcommands::

    dsm2ui animate hydro FILE.h5  [--variable flow|stage]
                                   [--location both|upstream|downstream]
                                   [--port N] [--desktop]
                                   [--shapefile PATH]
                                   [--vmin F] [--vmax F]
                                   [--colormap NAME] [--title TEXT]
                                   [--size F]

    dsm2ui animate qual  FILE.h5  [--constituent ec]
                                   [--port N] [--desktop]
                                   [--shapefile PATH]
                                   [--vmin F] [--vmax F]
                                   [--colormap NAME] [--title TEXT]
                                   [--size F]

The ``--size`` option controls line width (channels are rendered as LineStrings
by default).

``GeoAnimatorManager`` is a ``pn.viewable.Viewer`` and serves itself directly
via ``pn.serve``.  It does NOT go through ``serve_session_app`` (which wraps
the result in ``DataUI`` and expects a ``DataUIManager``).
"""

from __future__ import annotations

import re

import click

from dvue.animator import CURATED_COLORMAPS


def _title_to_slug(title: str) -> str:
    """Convert a display title to a URL-safe Bokeh app route key."""
    return re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-") or "animate"


_CLI_TRANSFORM_MAP = {
    "none":               "none",
    "daily":              "Daily mean",
    "daily-min":          "Daily min",
    "daily-max":          "Daily max",
    "rolling-24h":        "Rolling 24 h",
    "rolling-14d":        "Rolling 14 D",
    "rolling-14d-daily":  "Rolling 14 D \u2192 Daily mean",
    "godin":              "Godin filter",
    "godin-daily":        "Godin \u2192 Daily mean",
    "godin-daily-min":    "Godin \u2192 Daily min",
    "godin-daily-max":    "Godin \u2192 Daily max",
}


def _serve_viewer(build_fn, slug: str, title: str, port: int, desktop: bool) -> None:
    """Serve a factory that returns a ``pn.viewable.Viewer`` per-session.

    The factory is called fresh for each browser connection so the
    HoloViews DynamicMap and Panel panes are always created inside a live
    Bokeh document context.  Building them before ``pn.serve`` (outside a
    document) causes a blank page because the DynamicMap first-render
    callback never fires.
    """
    import panel as pn

    if desktop:
        try:
            import webview  # type: ignore
            import threading

            def _run():
                pn.serve({slug: build_fn}, port=port, show=False)

            t = threading.Thread(target=_run, daemon=True)
            t.start()
            import time; time.sleep(1.5)
            url = f"http://localhost:{port}/{slug}"
            webview.create_window(title, url)
            webview.start()
        except ImportError:
            raise SystemExit(
                "pywebview is required for --desktop mode: pip install pywebview"
            )
    else:
        pn.serve({slug: build_fn}, port=port, show=True, title=title)

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])

# Common options shared by both subcommands
_COMMON_OPTIONS = [
    click.option("--port", default=0, show_default=True, type=int,
                 help="Web server port (0 = random)."),
    click.option("--desktop", is_flag=True, default=False,
                 help="Open in a native desktop window (requires pywebview)."),
    click.option("--shapefile", default=None, type=click.Path(exists=True, dir_okay=False),
                 help="Override bundled channel centreline GeoJSON/shapefile."),
    click.option("--vmin", default=None, type=float,
                 help="Colour-scale lower bound (default: data min)."),
    click.option("--vmax", default=None, type=float,
                 help="Colour-scale upper bound (default: data max)."),
    click.option("--colormap", default="turbo", show_default=True,
                 type=click.Choice(CURATED_COLORMAPS, case_sensitive=False),
                 help="Colormap name."),
    click.option("--title", default=None,
                 help="Map title (default: auto-generated from file and variable)."),
    click.option("--size", default=3.0, show_default=True, type=float,
                 help="Line width in pixels (channels are LineStrings)."),
    click.option("--simplify", default=50.0, show_default=True, type=float,
                 help="Geometry simplification tolerance in metres (0 = off)."),
    click.option("--channel-id-column", default=None,
                 help="Column in the shapefile holding integer channel numbers. "
                      "Auto-detected when omitted (tries 'id', 'channel_nu', 'CHAN_NO')."),
    click.option("--log-level", default="warning", show_default=True,
                 type=click.Choice(["debug", "info", "warning", "error"], case_sensitive=False),
                 help="Logging verbosity."),
]


def _add_common_options(fn):
    """Decorator that applies all common options to a click command."""
    for opt in reversed(_COMMON_OPTIONS):
        fn = opt(fn)
    return fn


def _apply_config_to_manager(mgr, cfg: dict) -> None:
    """Apply saved UI state from a config dict to a live manager.

    Called inside ``build()`` after the manager is constructed so that
    visual settings (contours, show-channels, etc.) saved with the UI are
    restored on load.  Data-level settings (transform, colormap, vmin/vmax)
    are already applied before construction via CLI arg overrides.
    """
    contours = cfg.get("contours", {})
    if contours.get("enabled"):
        mgr._contours_check.value = True          # triggers _on_contours_toggle
    mgr._n_contours_slider.value = int(contours.get("n_levels", 8))
    mgr._contour_smooth_slider.value = float(contours.get("smoothing", 3.0))
    mgr._contour_levels_select.value = contours.get("level_mode", "nice")
    mgr._contour_custom_input.value = contours.get("custom_levels", "")
    mgr._contour_color_check.value  = bool(contours.get("color", True))
    mgr._contour_labels_check.value = bool(contours.get("labels", False))
    if hasattr(mgr, "_contour_label_spacing_slider"):
        mgr._contour_label_spacing_slider.value = int(
            contours.get("label_spacing", 30)
        )
    if hasattr(mgr, "_contour_clip_slider") and "clip_radius_km" in contours:
        new_val = float(contours["clip_radius_km"])
        mgr._contour_clip_slider.value = new_val
        # Unconditionally rebuild clip zones — the watcher may not fire if
        # the loaded value equals the current slider value (no param change).
        _buf = new_val * 1000.0
        try:
            from shapely.ops import unary_union
            if hasattr(mgr, "_gdf_proj"):       # single-panel manager
                mgr._contour_clip_zone = unary_union(
                    mgr._gdf_proj.geometry
                ).buffer(_buf)
            elif hasattr(mgr, "_gdf_a_proj"):   # multi-panel manager
                cz_a = unary_union(mgr._gdf_a_proj.geometry).buffer(_buf)
                cz_b = unary_union(mgr._gdf_b_proj.geometry).buffer(_buf)
                mgr._ctour_a.clip_zone    = cz_a
                mgr._ctour_b.clip_zone    = cz_b
                mgr._ctour_diff.clip_zone = cz_a
        except Exception:
            pass
    # Channel/basemap opacity — new format is 0-100 int; old format was bool.
    def _to_alpha(val, default=100):
        if isinstance(val, bool):
            return 100 if val else 0
        try:
            return int(val)
        except (TypeError, ValueError):
            return default

    if hasattr(mgr, "_channels_alpha_slider"):
        mgr._channels_alpha_slider.value = _to_alpha(cfg.get("show_channels", 100))
    if hasattr(mgr, "_basemap_alpha_slider"):
        mgr._basemap_alpha_slider.value  = _to_alpha(cfg.get("show_basemap", 100))
    # Observation station opacity (added after construction by _add_obs_station_overlay)
    obs_cfg = cfg.get("observations", {})
    if hasattr(mgr, "_obs_alpha_slider") and "opacity" in obs_cfg:
        mgr._obs_alpha_slider.value = int(obs_cfg["opacity"])
    # X2 (GeoAnimatorManager only)
    x2 = cfg.get("x2", {})
    if hasattr(mgr, "_x2_check") and x2.get("enabled"):
        mgr._x2_check.value = True
        if x2.get("threshold") is not None:
            mgr._x2_threshold_input.value = float(x2["threshold"])
    # Diff (MultiGeoAnimatorManager only)
    diff = cfg.get("diff", {})
    if hasattr(mgr, "_show_diff_check") and diff.get("show"):
        mgr._show_diff_check.value = True
    # Layout orientation (MultiGeoAnimatorManager only)
    if hasattr(mgr, "_orientation_select") and "layout_orientation" in cfg:
        val = str(cfg["layout_orientation"]).capitalize()
        if val in ("Horizontal", "Vertical"):
            mgr._orientation_select.value = val
    # Sidebar collapsed state (both single and multi managers)
    if hasattr(mgr, "_sidebar_toggle") and "sidebar_collapsed" in cfg:
        collapsed = bool(cfg["sidebar_collapsed"])
        mgr._sidebar_toggle.value = not collapsed
        mgr._controls.visible = not collapsed
        mgr._sidebar_toggle.name = "\u25ba" if collapsed else "\u25c4"
    # Stage bars settings (added after construction by animate_hydro / animate_qual)
    stage_cfg = cfg.get("stage_bars", {})
    if hasattr(mgr, "_stage_layer") and stage_cfg:
        sl = mgr._stage_layer
        if hasattr(sl, "_w_bar_width") and sl._w_bar_width is not None:
            if "bar_width_m" in stage_cfg:
                sl._w_bar_width.value = float(stage_cfg["bar_width_m"])
            if "bar_max_height_m" in stage_cfg:
                sl._w_bar_height.value = float(stage_cfg["bar_max_height_m"])
            if "reference_stage_range_ft" in stage_cfg:
                sl._w_ref_range.value = float(stage_cfg["reference_stage_range_ft"])
            if "show_labels" in stage_cfg:
                sl._w_show_labels.value = bool(stage_cfg["show_labels"])
            if "show_range_box" in stage_cfg:
                sl._w_show_range_box.value = bool(stage_cfg["show_range_box"])
            if "alpha" in stage_cfg:
                sl._w_alpha.value = int(stage_cfg["alpha"])

    # Flow layer settings (opacity etc. changed via UI controls)
    flow_state = cfg.get("flow_state", {})
    if hasattr(mgr, "_flow_layer") and flow_state:
        fl = mgr._flow_layer
        if hasattr(fl, "_w_colormap") and fl._w_colormap is not None:
            if "colormap" in flow_state:
                fl._w_colormap.value = str(flow_state["colormap"])
            if "alpha" in flow_state:
                fl._w_alpha.value = int(round(float(flow_state["alpha"]) * 100))
            if "scale_mode" in flow_state:
                fl._w_scale.value = str(flow_state["scale_mode"])
            if "reference_arrow_length_m" in flow_state:
                fl._w_arrow_length.value = float(flow_state["reference_arrow_length_m"])
            if "arrow_width_m" in flow_state:
                fl._w_arrow_width.value = float(flow_state["arrow_width_m"])
            if "bar_max_height_m" in flow_state:
                fl._w_bar_height.value = float(flow_state["bar_max_height_m"])
            _is_vel = getattr(fl._spec, "variable", "flow") == "velocity"
            if _is_vel and "reference_velocity" in flow_state:
                fl._w_ref_flow.value = float(flow_state["reference_velocity"])
            elif not _is_vel and "reference_flow" in flow_state:
                fl._w_ref_flow.value = float(flow_state["reference_flow"])
            if _is_vel and "min_velocity_fps" in flow_state:
                fl._w_min_flow.value = float(flow_state["min_velocity_fps"])
            elif not _is_vel and "min_flow_cfs" in flow_state:
                fl._w_min_flow.value = float(flow_state["min_flow_cfs"])
            # flow_vmin / flow_vmax as clim widget string
            if "flow_vmin" in flow_state and "flow_vmax" in flow_state:
                fl._w_clim.value = (
                    f"{flow_state['flow_vmin']:.4g}, {flow_state['flow_vmax']:.4g}"
                )

    # Map viewport / zoom extents
    extents = cfg.get("map_extents", {})
    if extents:
        try:
            if hasattr(mgr, "_bk_figure"):          # single-panel manager
                mgr._bk_figure.x_range.start = float(extents["x_start"])
                mgr._bk_figure.x_range.end   = float(extents["x_end"])
                mgr._bk_figure.y_range.start = float(extents["y_start"])
                mgr._bk_figure.y_range.end   = float(extents["y_end"])
            elif hasattr(mgr, "_shared_x_range"):   # multi-panel manager (shared viewport)
                mgr._shared_x_range.start = float(extents["x_start"])
                mgr._shared_x_range.end   = float(extents["x_end"])
                mgr._shared_y_range.start = float(extents["y_start"])
                mgr._shared_y_range.end   = float(extents["y_end"])
        except (KeyError, TypeError, ValueError):
            pass


@click.group(name="animate", context_settings=CONTEXT_SETTINGS)
def animate():
    """Animate DSM2 HDF5 tidefile data on a map.

    Time-step slider colours DSM2 channel centrelines by flow, stage, or
    constituent concentration using the dvue GeoAnimatorManager.

    \b
    Examples:
        dsm2ui animate hydro path/to/tidefile.h5
        dsm2ui animate hydro path/to/tidefile.h5 --variable stage
        dsm2ui animate qual  path/to/qual_ec.h5 --constituent ec
        dsm2ui animate hydro study_a.h5 study_b.h5          # side-by-side
        dsm2ui animate hydro study_a.h5 study_b.h5 --diff   # diff map
    """


@animate.command(name="hydro", context_settings=CONTEXT_SETTINGS)
@click.argument("h5files", nargs=-1, required=False,
                type=click.Path(exists=True, dir_okay=False, readable=True))
@click.option("--variable", default="flow", show_default=True,
              type=click.Choice(["flow", "stage", "depth", "velocity"], case_sensitive=False),
              help="Tidefile variable to animate.  "
                   "'stage' = water-surface elevation (ft NAVD, depth + channel bottom). "
                   "'depth' = raw water depth above channel bottom (ft, from HDF5 directly). "
                   "See DSM2 issue #164.")
@click.option("--location", default="both", show_default=True,
              type=click.Choice(["both", "upstream", "downstream"], case_sensitive=False),
              help="Channel location ('both' averages upstream and downstream).")
@click.option("--diff", "show_diff", is_flag=True, default=False,
              help="Show diff map (A \u2212 B) instead of side-by-side (only with 2 files).")
@click.option("--transform", default="none", show_default=True,
              type=click.Choice(
                  ["none", "daily", "daily-min", "daily-max",
                   "rolling-24h", "rolling-14d", "rolling-14d-daily",
                   "godin", "godin-daily", "godin-daily-min", "godin-daily-max"],
                  case_sensitive=False),
              help="Time-domain transform to apply before animation.\n"
                   "none: raw data (default).\n"
                   "daily / daily-min / daily-max: daily resample (mean / min / max).\n"
                   "rolling-24h: 24 h centred rolling mean.\n"
                   "rolling-14d: 14-day centred rolling mean.\n"
                   "rolling-14d-daily: 14-day rolling mean then daily mean.\n"
                   "godin: Godin tidal filter.\n"
                   "godin-daily / godin-daily-min / godin-daily-max: Godin then daily.")   
@click.option("--config", "config_file", default=None,
              type=click.Path(exists=True, dir_okay=False),
              help="Load all settings from a YAML config file saved by the UI. "
                   "H5FILES and other options are not required when --config is used.")
@click.option("--flow-config", "flow_config_file", default=None,
              type=click.Path(exists=True, dir_okay=False),
              help="YAML file defining flow (cfs) arrows and junction bars to overlay. "
                   "See FlowLayerSpec.from_yaml() for the schema.  "
                   "The flow layer reads flow from the first H5FILE and mirrors "
                   "the active transform.  Mutually exclusive with --velocity-config.")
@click.option("--velocity-config", "velocity_config_file", default=None,
              type=click.Path(exists=True, dir_okay=False),
              help="YAML file defining velocity (ft/s) arrows to overlay. "
                   "Equivalent to --flow-config but automatically sets variable=velocity. "
                   "Mutually exclusive with --flow-config.")
@click.option("--nodes-file", "flow_nodes_file", default=None,
              type=click.Path(exists=True, dir_okay=False),
              help="Override bundled nodes GeoJSON/shapefile for flow/velocity bars.")
@click.option("--stage-config", "stage_config_file", default=None,
              type=click.Path(exists=True, dir_okay=False),
              help="YAML file defining stage deviation bars to overlay. "
                   "See StageLayerSpec.from_yaml() for the schema.")
@_add_common_options
def hydro_cmd(
    h5files, variable, location, show_diff, transform, config_file,
    flow_config_file, velocity_config_file, flow_nodes_file, stage_config_file,
    port, desktop, shapefile, vmin, vmax, colormap, title, size, simplify,
    channel_id_column, log_level,
):
    """Animate 1 or 2 HYDRO tidefiles.  With 2 files: side-by-side or --diff."""
    import logging
    logging.basicConfig(level=getattr(logging, log_level.upper()),
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    log = logging.getLogger("dsm2ui.animate")

    cfg = {}
    if config_file:
        import yaml
        with open(config_file, encoding="utf-8") as _f:
            cfg = yaml.safe_load(_f) or {}
        _files = [entry["path"] for entry in cfg.get("files", [])]
        if not _files:
            raise click.UsageError(f"Config '{config_file}' contains no 'files' entries.")
        h5files   = tuple(_files)
        variable  = cfg.get("variable", variable)
        location  = cfg.get("location", location)
        show_diff = cfg.get("diff", {}).get("show", show_diff)
        shapefile = cfg.get("shapefile") or shapefile
        channel_id_column = cfg.get("channel_id_column") or channel_id_column
        simplify  = cfg.get("simplify", simplify)
        colormap  = cfg.get("colormap", colormap)
        vmin      = cfg.get("vmin")
        vmax      = cfg.get("vmax")
        size      = cfg.get("size", size)
        title     = cfg.get("title") or title
        log.info("Loaded config from %s (%d file(s))", config_file, len(h5files))
    elif not h5files:
        raise click.UsageError(
            "Provide at least one H5FILE argument, or use --config to load a YAML config."
        )
    else:
        pass  # no extra vars needed

    if len(h5files) > 2:
        raise click.UsageError("At most 2 H5FILE arguments are supported.")
    multi = len(h5files) == 2

    # Resolve --flow-config / --velocity-config (mutually exclusive)
    if flow_config_file and velocity_config_file:
        raise click.UsageError(
            "--flow-config and --velocity-config are mutually exclusive."
        )
    _overlay_config_file = velocity_config_file or flow_config_file
    _overlay_is_velocity = velocity_config_file is not None

    # Validate flow overlay options (single-file only)
    if _overlay_config_file and multi:
        raise click.UsageError(
            "Flow/velocity overlay is only supported with a single HYDRO file."
        )

    effective_title = title or (
        f"DSM2 Hydro {variable.title()} (\u0394)" if (multi and show_diff)
        else f"DSM2 Hydro {variable.title()}"
    )
    slug = _title_to_slug(effective_title)
    shapefiles = [shapefile] if shapefile else None

    def build():
        import holoviews as hv
        import panel as pn
        hv.extension("bokeh")
        pn.extension(throttled=True)
        if multi:
            from dsm2ui.animate import animate_hydro_multi
            mgr = animate_hydro_multi(
                h5files[0], h5files[1],
                variable=variable, location=location,
                shapefiles=shapefiles,
                simplify_tolerance=simplify,
                channel_id_column=channel_id_column,
                show_diff=show_diff,
                vmin=vmin, vmax=vmax, colormap=colormap, size=size,
                initial_transform=_CLI_TRANSFORM_MAP.get(transform.lower(), "none"),
            )
        else:
            from dsm2ui.animate import animate_hydro
            _flow_spec = None
            if _overlay_config_file:
                from dsm2ui.flow_layer import FlowLayerSpec
                _flow_spec = FlowLayerSpec.from_yaml(_overlay_config_file)
                if _overlay_is_velocity:
                    _flow_spec.variable = "velocity"
            _stage_spec = None
            if stage_config_file:
                from dsm2ui.stage_layer import StageLayerSpec
                _stage_spec = StageLayerSpec.from_yaml(stage_config_file)
            mgr = animate_hydro(
                h5files[0],
                variable=variable, location=location,
                shapefile=shapefile,
                simplify_tolerance=simplify,
                channel_id_column=channel_id_column,
                flow_spec=_flow_spec,
                nodes_file=flow_nodes_file,
                stage_spec=_stage_spec,
                vmin=vmin, vmax=vmax, colormap=colormap,
                title=effective_title, size=size,
                initial_transform=_CLI_TRANSFORM_MAP.get(transform.lower(), "none"),
            )
            # Persist flow layer paths + current widget state in saved YAML
            if _overlay_config_file:
                import os as _os_h
                _fc_h = _os_h.path.abspath(_overlay_config_file)
                _hh_h = _os_h.path.abspath(flow_hydro_h5[0]) if flow_hydro_h5 else None
                _nf_h = _os_h.path.abspath(flow_nodes_file) if flow_nodes_file else None
                _orig_cs_h = mgr.collect_state
                def _cs_with_flow_hydro(
                    _ocs=_orig_cs_h, _fc=_fc_h, _hh=_hh_h, _nf=_nf_h, _mgr=mgr,
                ):
                    state = _ocs()
                    state["flow"] = {"flow_config": _fc, "hydro_h5": _hh, "nodes_file": _nf}
                    if hasattr(_mgr, "_flow_layer"):
                        state["flow_state"] = _mgr._flow_layer.get_state_dict()
                    return state
                mgr.collect_state = _cs_with_flow_hydro
        if cfg:
            _apply_config_to_manager(mgr, cfg)
        log.info("Reader time_index: %d steps",
                 len(mgr._reader_a.time_index if multi else mgr._reader.time_index))
        return mgr

    _serve_viewer(build, slug=slug, title=effective_title, port=port, desktop=desktop)

@animate.command(name="qual", context_settings=CONTEXT_SETTINGS)
@click.argument("h5files", nargs=-1, required=False,
                type=click.Path(exists=True, dir_okay=False, readable=True))
@click.option("--constituent", default="ec", show_default=True,
              help="Constituent name (case-insensitive, e.g. ec, cl, do).")
@click.option("--x2-threshold", default=None, type=float,
              help="Enable X2 isohaline overlay at this EC threshold (\u00b5S/cm). "
                   "Only used with a single file.")
@click.option("--diff", "show_diff", is_flag=True, default=False,
              help="Show diff map (A \u2212 B) instead of side-by-side (only with 2 files).")
@click.option("--transform", default="none", show_default=True,
              type=click.Choice(
                  ["none", "daily", "daily-min", "daily-max",
                   "rolling-24h", "rolling-14d", "rolling-14d-daily",
                   "godin", "godin-daily", "godin-daily-min", "godin-daily-max"],
                  case_sensitive=False),
              help="Time-domain transform (see hydro --help for details).")
@click.option("--resample-freq",
              type=str, default=None,
              help="Apply an additional resample on top of --transform "
                   "(e.g. 1D, 6h, 12h). Stacks with the primary transform.")
@click.option("--resample-agg",
              type=click.Choice(["mean", "min", "max"], case_sensitive=False),
              default="mean", show_default=True,
              help="Aggregation for --resample-freq.")
@click.option("--observations-csv", default=None,
              type=click.Path(dir_okay=False),
              help="Time-indexed CSV of sparse observations (station IDs as columns). "
                   "When supplied the model output is bias-corrected via network IDW "
                   "before any transform is applied.")
@click.option("--stations-csv", default=None,
              type=click.Path(dir_okay=False),
              help="CSV with station_id and lat/lon or x/y columns. "
                   "Required when --observations-csv is given.")
@click.option("--centerlines-file", default=None,
              type=click.Path(dir_okay=False),
              help="GeoJSON/shapefile of DSM2 channel centrelines for station snapping. "
                   "Defaults to the bundled centrelines when --observations-csv is given.")
@click.option("--echo-inp", default=None,
              type=click.Path(dir_okay=False),
              help="DSM2 echo .inp file as fallback CHANNEL table source "
                   "(only needed when the H5 file has no /input/channel table).")
@click.option("--idw-power", default=2.0, show_default=True, type=float,
              help="IDW distance exponent for the network correction.")
@click.option("--max-obs-age", default="2h", show_default=True,
              help="Maximum age of observations relative to a model timestep "
                   '(e.g. "2h", "30min"). Older matches are treated as missing.')
@click.option("--correction-method", default="idw", show_default=True,
              type=click.Choice(["idw", "oi"], case_sensitive=False),
              help="Correction algorithm: idw (inverse-distance weighting, default) or "
                   "oi (optimal interpolation). Only used when --observations-csv is given.")
@click.option("--oi-sigma-obs", default=10.0, show_default=True, type=float,
              help="OI observation error standard deviation (\u00b5S/cm). "
                   "Controls how much the OI trusts observations vs the background.")
@click.option("--oi-kernel", default="exponential", show_default=True,
              type=click.Choice(["exponential", "channel_direction"], case_sensitive=False),
              help="OI correlation kernel. 'exponential' is symmetric; "
                   "'channel_direction' penalises against-flow path segments.")
@click.option("--oi-resistance", default=3.0, show_default=True, type=float,
              help="Against-flow cost multiplier for the channel_direction kernel (>= 1).")
@click.option("--compare-correction", "compare_correction", is_flag=True, default=False,
              help="Show model-only and model+correction maps side by side.  "
                   "Only used when --observations-csv is given.")
@click.option("--config", "config_file", default=None,
              type=click.Path(exists=True, dir_okay=False),
              help="Load all settings from a YAML config file saved by the UI.")
@click.option("--flow-config", "flow_config_file", default=None,
              type=click.Path(exists=True, dir_okay=False),
              help="YAML file defining flow (cfs) arrows and junction bars to overlay. "
                   "See FlowLayerSpec.from_yaml() for the schema.  "
                   "Requires --hydro-h5.  Mutually exclusive with --velocity-config.")
@click.option("--velocity-config", "velocity_config_file", default=None,
              type=click.Path(exists=True, dir_okay=False),
              help="YAML file defining velocity (ft/s) arrows to overlay. "
                   "Equivalent to --flow-config but automatically sets variable=velocity. "
                   "Requires --hydro-h5.  Mutually exclusive with --flow-config.")
@click.option("--hydro-h5", "flow_hydro_h5", multiple=True,
              type=click.Path(exists=True, dir_okay=False),
              help="HYDRO HDF5 tidefile(s) for the flow/velocity overlay layer. "
                   "Pass once for a single QUAL file, or twice (in the same order "
                   "as the QUAL files) when animating two QUAL files side-by-side. "
                   "Required when --flow-config or --velocity-config is given.")
@click.option("--nodes-file", "flow_nodes_file", default=None,
              type=click.Path(exists=True, dir_okay=False),
              help="Override bundled nodes GeoJSON/shapefile for flow/velocity bars.")
@click.option("--stage-config", "stage_config_file", default=None,
              type=click.Path(exists=True, dir_okay=False),
              help="YAML file defining stage deviation bars to overlay. "
                   "See StageLayerSpec.from_yaml() for the schema. "
                   "Requires --hydro-h5.")
@_add_common_options
def qual_cmd(
    h5files, constituent, x2_threshold, show_diff, transform,
    observations_csv, stations_csv, centerlines_file, echo_inp,
    idw_power, max_obs_age,
    correction_method, oi_sigma_obs, oi_kernel, oi_resistance,
    compare_correction,
    resample_freq, resample_agg,
    config_file,
    flow_config_file, velocity_config_file, flow_hydro_h5, flow_nodes_file, stage_config_file,
    port, desktop, shapefile, vmin, vmax, colormap, title, size, simplify,
    channel_id_column, log_level,
):
    """Animate 1 or 2 QUAL/GTM tidefiles.  With 2 files: side-by-side or --diff."""
    import logging
    logging.basicConfig(level=getattr(logging, log_level.upper()),
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    log = logging.getLogger("dsm2ui.animate")

    cfg = {}
    if config_file:
        import yaml
        with open(config_file, encoding="utf-8") as _f:
            cfg = yaml.safe_load(_f) or {}
        _files = [entry["path"] for entry in cfg.get("files", [])]
        if not _files:
            raise click.UsageError(f"Config '{config_file}' contains no 'files' entries.")
        h5files     = tuple(_files)
        constituent = cfg.get("variable", constituent)
        show_diff   = cfg.get("diff", {}).get("show", show_diff)
        shapefile   = cfg.get("shapefile") or shapefile
        channel_id_column = cfg.get("channel_id_column") or channel_id_column
        simplify    = cfg.get("simplify", simplify)
        colormap    = cfg.get("colormap", colormap)
        vmin        = cfg.get("vmin")
        vmax        = cfg.get("vmax")
        size        = cfg.get("size", size)
        x2cfg       = cfg.get("x2", {})
        if x2cfg.get("enabled") and x2_threshold is None:
            x2_threshold = x2cfg.get("threshold", 2700.0)
        title = cfg.get("title") or title
        # --- correction section ---
        corr_cfg = cfg.get("correction", {})
        if corr_cfg.get("enabled"):
            observations_csv  = corr_cfg.get("observations_csv")  or observations_csv
            stations_csv      = corr_cfg.get("stations_csv")       or stations_csv
            centerlines_file  = corr_cfg.get("centerlines_file")   or centerlines_file
            echo_inp          = corr_cfg.get("echo_inp_file")       or echo_inp
            max_obs_age       = corr_cfg.get("max_obs_age", max_obs_age)
            correction_method = corr_cfg.get("method", correction_method).lower()
            idw_power         = corr_cfg.get("idw", {}).get("power",    idw_power)
            oi_sigma_obs      = corr_cfg.get("oi",  {}).get("sigma_obs", oi_sigma_obs)
            oi_kernel         = corr_cfg.get("oi",  {}).get("kernel",   oi_kernel)
            oi_resistance     = corr_cfg.get("oi",  {}).get("resistance", oi_resistance)
        # Restore compare-correction mode when config was saved from it
        if cfg.get("mode") == "corrected_multi":
            compare_correction = True
        # Restore custom resample from config
        resample_cfg = cfg.get("resample", {})
        if resample_cfg.get("enabled"):
            resample_freq = resample_cfg.get("freq") or resample_freq
            resample_agg  = resample_cfg.get("agg",  resample_agg)
        # Restore flow layer overlay from config (CLI flags take precedence)
        _flow_cfg = cfg.get("flow", {})
        if not flow_config_file and _flow_cfg.get("flow_config") and _flow_cfg.get("hydro_h5"):
            flow_config_file = _flow_cfg["flow_config"]
            flow_hydro_h5    = (_flow_cfg["hydro_h5"],)
            flow_nodes_file  = _flow_cfg.get("nodes_file") or flow_nodes_file
        # Restore hydro_h5_paths for multi-qual flow overlay from config
        if not flow_hydro_h5 and cfg.get("hydro_h5_paths"):
            _paths = [p for p in cfg["hydro_h5_paths"] if p]
            if _paths:
                flow_hydro_h5 = tuple(_paths)
        # Restore flow_config for multi-qual case (saved as top-level key)
        if not flow_config_file and cfg.get("flow_config"):
            flow_config_file = cfg["flow_config"]
            if not flow_nodes_file:
                flow_nodes_file = cfg.get("flow_nodes_file") or flow_nodes_file
        # Restore stage_config for multi-qual case (saved as top-level key)
        if not stage_config_file and cfg.get("stage_config"):
            stage_config_file = cfg["stage_config"]
        log.info("Loaded config from %s (%d file(s))", config_file, len(h5files))
    elif not h5files:
        raise click.UsageError(
            "Provide at least one H5FILE argument, or use --config to load a YAML config."
        )
    else:
        pass  # no extra vars needed

    if len(h5files) > 2:
        raise click.UsageError("At most 2 H5FILE arguments are supported.")
    multi = len(h5files) == 2

    # Resolve --flow-config / --velocity-config (mutually exclusive)
    if flow_config_file and velocity_config_file:
        raise click.UsageError(
            "--flow-config and --velocity-config are mutually exclusive."
        )
    _overlay_config_file = velocity_config_file or flow_config_file
    _overlay_is_velocity = velocity_config_file is not None

    # Validate overlay options
    if _overlay_config_file and not flow_hydro_h5:
        raise click.UsageError(
            "--hydro-h5 is required when --flow-config or --velocity-config is given."
        )
    if flow_hydro_h5 and not _overlay_config_file:
        raise click.UsageError(
            "--flow-config or --velocity-config is required when --hydro-h5 is given."
        )
    if _overlay_config_file and multi and len(flow_hydro_h5) not in (1, 2):
        raise click.UsageError(
            "Pass --hydro-h5 twice (once per QUAL file) when animating two "
            "QUAL files with a flow/velocity overlay."
        )

    # ------------------------------------------------------------------
    # IDW correction validation (eager, before building the reader)
    # ------------------------------------------------------------------
    use_correction = observations_csv is not None
    if use_correction:
        if multi:
            raise click.UsageError(
                "IDW observation correction (--observations-csv) is not supported "
                "with two H5 files.  Use a single file."
            )
        if stations_csv is None:
            raise click.UsageError(
                "--stations-csv is required when --observations-csv is given."
            )

    effective_title = title or (
        f"DSM2 QUAL {constituent.upper()} (\u0394)" if (multi and show_diff)
        else (
            f"DSM2 QUAL/GTM \u2014 {constituent.upper()} (Model vs {correction_method.upper()} Corrected)"
            if (use_correction and compare_correction)
            else (
                f"DSM2 QUAL/GTM \u2014 {constituent.upper()} ({correction_method.upper()} corrected)"
                if use_correction
                else f"DSM2 QUAL {constituent.upper()}"
            )
        )
    )
    slug = _title_to_slug(effective_title)
    shapefiles = [shapefile] if shapefile else None

    def build():
        import holoviews as hv
        import panel as pn
        hv.extension("bokeh")
        pn.extension(throttled=True)
        if use_correction:
            # --- build corrector (shared for both single and comparison modes)
            if correction_method.lower() == "oi":
                from pydsm.analysis.network_correction import (
                    NetworkOICorrector,
                    snap_stations_to_channel_ends,
                    exponential_kernel,
                    channel_direction_kernel,
                )
                from pydsm.viz.dsm2gis import read_stations
                import geopandas as gpd
                from dsm2ui.animate import CorrectedQualH5ConcentrationReader
                _chan_df = CorrectedQualH5ConcentrationReader._load_channels(
                    h5files[0], echo_inp
                )
                _cl = (
                    centerlines_file
                    or str(
                        __import__("pathlib").Path(
                            __import__("dsm2ui.animate", fromlist=["animate"]).__file__
                        ).parent / "dsm2gis" / "dsm2_channels_centerlines_8_2.geojson"
                    )
                )
                import warnings as _w
                with _w.catch_warnings():
                    _w.simplefilter("ignore")
                    _snapped = snap_stations_to_channel_ends(
                        read_stations(stations_csv),
                        gpd.read_file(_cl),
                        _chan_df,
                    )
                _kfn = (
                    exponential_kernel()
                    if oi_kernel.lower() == "exponential"
                    else channel_direction_kernel(resistance=oi_resistance)
                )
                corrector = NetworkOICorrector(
                    _chan_df, _snapped,
                    sigma_obs=oi_sigma_obs, corr_fn=_kfn,
                )
            else:
                corrector = None  # IDW built inside the reader

            _correction_kwargs = dict(
                observations_csv=observations_csv,
                stations_csv=stations_csv,
                centerlines_file=centerlines_file,
                constituent=constituent,
                power=idw_power,
                max_obs_age=max_obs_age,
                echo_inp_file=echo_inp,
                corrector=corrector,
                shapefile=shapefile,
                simplify_tolerance=simplify,
                channel_id_column=channel_id_column,
                vmin=vmin, vmax=vmax, colormap=colormap,
                size=size,
                initial_transform=_CLI_TRANSFORM_MAP.get(transform.lower(), "none"),
            )

            if compare_correction:
                from dsm2ui.animate import animate_qual_corrected_multi
                mgr = animate_qual_corrected_multi(
                    h5files[0],
                    **_correction_kwargs,
                )
            else:
                from dsm2ui.animate import animate_qual_corrected
                mgr = animate_qual_corrected(
                    h5files[0],
                    title=effective_title,
                    **_correction_kwargs,
                )
        elif multi:
            from dsm2ui.animate import animate_qual_multi
            _flow_spec_multi = None
            _hydro_paths_multi = None
            if _overlay_config_file:
                from dsm2ui.flow_layer import FlowLayerSpec
                _flow_spec_multi = FlowLayerSpec.from_yaml(_overlay_config_file)
                if _overlay_is_velocity:
                    _flow_spec_multi.variable = "velocity"
                # Accept 1 shared hydro h5 (applied to both panels) or 2 separate ones.
                if len(flow_hydro_h5) == 1:
                    _hydro_paths_multi = [flow_hydro_h5[0], flow_hydro_h5[0]]
                else:
                    _hydro_paths_multi = list(flow_hydro_h5)
            _stage_spec_multi = None
            if stage_config_file:
                from dsm2ui.stage_layer import StageLayerSpec
                _stage_spec_multi = StageLayerSpec.from_yaml(stage_config_file)
                # Hydro paths needed for stage too; reuse flow paths if already set,
                # otherwise build from flow_hydro_h5 directly.
                if _hydro_paths_multi is None and flow_hydro_h5:
                    if len(flow_hydro_h5) == 1:
                        _hydro_paths_multi = [flow_hydro_h5[0], flow_hydro_h5[0]]
                    else:
                        _hydro_paths_multi = list(flow_hydro_h5)
            mgr = animate_qual_multi(
                h5files[0], h5files[1],
                constituent=constituent,
                shapefiles=shapefiles,
                simplify_tolerance=simplify,
                channel_id_column=channel_id_column,
                show_diff=show_diff,
                hydro_h5_paths=_hydro_paths_multi,
                flow_spec=_flow_spec_multi,
                nodes_file=flow_nodes_file or None,
                stage_spec=_stage_spec_multi,
                vmin=vmin, vmax=vmax, colormap=colormap, size=size,
                initial_transform=_CLI_TRANSFORM_MAP.get(transform.lower(), "none"),
            )
            # Patch collect_state to persist the flow config path in saved YAML
            if _overlay_config_file:
                import os as _os
                _fc_multi = _os.path.abspath(_overlay_config_file)
                _nf_multi = _os.path.abspath(flow_nodes_file) if flow_nodes_file else None
                _orig_cs_multi = mgr.collect_state
                def _cs_with_flow_multi(
                    _ocs=_orig_cs_multi, _fc=_fc_multi, _nf=_nf_multi, _mgr=mgr,
                ):
                    state = _ocs()
                    state["flow_config"] = _fc
                    if _nf:
                        state["flow_nodes_file"] = _nf
                    if hasattr(_mgr, "_flow_layer"):
                        state["flow_state"] = _mgr._flow_layer.get_state_dict()
                    return state
                mgr.collect_state = _cs_with_flow_multi
            # Patch collect_state to persist the stage config path in saved YAML
            if stage_config_file:
                import os as _os_s
                _sc_multi = _os_s.path.abspath(stage_config_file)
                _orig_cs_sc_multi = mgr.collect_state
                def _cs_with_stage_multi(
                    _ocs=_orig_cs_sc_multi, _sc=_sc_multi,
                ):
                    state = _ocs()
                    state["stage_config"] = _sc
                    return state
                mgr.collect_state = _cs_with_stage_multi
        else:
            from dsm2ui.animate import animate_qual
            _flow_spec = None
            if _overlay_config_file:
                from dsm2ui.flow_layer import FlowLayerSpec
                _flow_spec = FlowLayerSpec.from_yaml(_overlay_config_file)
                if _overlay_is_velocity:
                    _flow_spec.variable = "velocity"
            _stage_spec = None
            if stage_config_file:
                from dsm2ui.stage_layer import StageLayerSpec
                _stage_spec = StageLayerSpec.from_yaml(stage_config_file)
            _hydro_h5 = flow_hydro_h5[0] if flow_hydro_h5 else None
            mgr = animate_qual(
                h5files[0],
                constituent=constituent,
                shapefile=shapefile,
                simplify_tolerance=simplify,
                channel_id_column=channel_id_column,
                x2_threshold=x2_threshold,
                flow_spec=_flow_spec,
                hydro_h5_path=_hydro_h5,
                nodes_file=flow_nodes_file,
                stage_spec=_stage_spec,
                vmin=vmin, vmax=vmax, colormap=colormap,
                title=effective_title, size=size,
                initial_transform=_CLI_TRANSFORM_MAP.get(transform.lower(), "none"),
            )
            # Patch collect_state() to persist the flow layer paths in saved YAML
            if _overlay_config_file:
                import os as _os
                _fc = _os.path.abspath(_overlay_config_file)
                _hh = _os.path.abspath(flow_hydro_h5[0])
                _nf = _os.path.abspath(flow_nodes_file) if flow_nodes_file else None
                _orig_cs = mgr.collect_state
                def _cs_with_flow(
                    _ocs=_orig_cs, _fc=_fc, _hh=_hh, _nf=_nf, _mgr=mgr,
                ):
                    state = _ocs()
                    state["flow"] = {
                        "flow_config": _fc,
                        "hydro_h5":    _hh,
                        "nodes_file":  _nf,
                    }
                    if hasattr(_mgr, "_flow_layer"):
                        state["flow_state"] = _mgr._flow_layer.get_state_dict()
                    return state
                mgr.collect_state = _cs_with_flow
        # Apply custom resample on top of the primary transform if requested.
        if resample_freq:
            from dsm2ui.animate import (
                make_resample_transform, make_composed_transform,
                _dsm2_transform_options,
            )
            r_spec = make_resample_transform(resample_freq, resample_agg)
            base_disp = _CLI_TRANSFORM_MAP.get(transform.lower(), "none")
            opts = _dsm2_transform_options()
            if base_disp != "none" and base_disp in opts:
                composed = make_composed_transform(opts[base_disp], r_spec)
                display_name = f"{base_disp} \u2192 {resample_agg}({resample_freq})"
            else:
                composed = r_spec
                display_name = f"{resample_agg}({resample_freq})"
            mgr._transform_options[display_name] = composed
            if display_name not in mgr._transform_select.options:
                mgr._transform_select.options = (
                    mgr._transform_select.options + [display_name]
                )
            mgr._transform_select.value = display_name
            mgr._animate_meta["resample"] = {
                "enabled": True, "freq": resample_freq, "agg": resample_agg,
            }
        if cfg:
            _apply_config_to_manager(mgr, cfg)
        log.info("Ready")
        return mgr

    _serve_viewer(build, slug=slug, title=effective_title, port=port, desktop=desktop)


@animate.command(name="flow", context_settings=CONTEXT_SETTINGS)
@click.argument("hydro_h5", type=click.Path(exists=True, dir_okay=False, readable=True))
@click.option("--flow-config", "flow_config_file", required=True,
              type=click.Path(exists=True, dir_okay=False),
              help="YAML file defining flow arrows and junction bars.  "
                   "See FlowLayerSpec.from_yaml() for the schema.")
@click.option("--nodes-file", "nodes_file", default=None,
              type=click.Path(exists=True, dir_okay=False),
              help="Override bundled nodes GeoJSON/shapefile for junction bars.")
@click.option("--shapefile", default=None,
              type=click.Path(exists=True, dir_okay=False),
              help="Override bundled channel centreline GeoJSON/shapefile "
                   "(used for bounding box and tangent computation).")
@click.option("--title", default="DSM2 Flow",
              help="Map title.")
@click.option("--port", default=0, show_default=True, type=int,
              help="Web server port (0 = random).")
@click.option("--desktop", is_flag=True, default=False,
              help="Open in a native desktop window (requires pywebview).")
@click.option("--map-height", default=500, show_default=True, type=int,
              help="Minimum map height in pixels.")
@click.option("--log-level", default="warning", show_default=True,
              type=click.Choice(["debug", "info", "warning", "error"], case_sensitive=False),
              help="Logging verbosity.")
def flow_cmd(
    hydro_h5, flow_config_file, nodes_file, shapefile,
    title, port, desktop, map_height, log_level,
):
    """Animate DSM2 flow arrows and junction bars on a map tile background.

    Displays user-specified flow arrows and two-sided junction flow-split bars
    animated from a HYDRO HDF5 tidefile.  The YAML config specifies which
    channels and nodes to visualise.

    \b
    Example YAML (flow_config.yaml):
        scale_mode: linear
        reference_flow: 10000
        reference_arrow_length_m: 500
        arrow_width_m: 150
        bar_width_m: 200
        bar_max_height_m: 600
        arrows:
          - channel: 10
            position: 0.5
            label: "Sacramento R"
        bars:
          - node: 329
            label: "Confluence"
            channels: [10, 11, 12]

    \b
    Usage:
        dsm2ui animate flow hydro.h5 --flow-config flow.yaml
        dsm2ui animate flow hydro.h5 --flow-config flow.yaml --port 5008
    """
    import logging
    logging.basicConfig(level=getattr(logging, log_level.upper()),
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")

    slug = _title_to_slug(title)

    def build():
        import panel as pn
        pn.extension(throttled=True)
        from dsm2ui.flow_layer import FlowLayerSpec
        from dsm2ui.animate import animate_flow
        flow_spec = FlowLayerSpec.from_yaml(flow_config_file)
        return animate_flow(
            hydro_h5,
            flow_spec,
            channel_shapefile=shapefile,
            nodes_file=nodes_file,
            title=title,
            map_height=map_height,
        )

    _serve_viewer(build, slug=slug, title=title, port=port, desktop=desktop)


# ---------------------------------------------------------------------------
# Export corrected QUAL HDF5
# ---------------------------------------------------------------------------

@animate.command(name="export-corrected", context_settings=CONTEXT_SETTINGS)
@click.argument("h5file", type=click.Path(exists=True, dir_okay=False, readable=True))
@click.option("--output", "output_h5", required=True,
              type=click.Path(dir_okay=False),
              help="Output HDF5 path for the corrected concentrations.")
@click.option("--constituent", default="ec", show_default=True,
              help="Constituent to correct and write (e.g. ec, cl, do).")
@click.option("--observations-csv", required=True,
              type=click.Path(exists=True, dir_okay=False),
              help="Time-indexed CSV of observations (station IDs as columns).")
@click.option("--stations-csv", required=True,
              type=click.Path(exists=True, dir_okay=False),
              help="CSV with station_id and lat/lon or x/y columns.")
@click.option("--echo-inp", default=None,
              type=click.Path(dir_okay=False),
              help="DSM2 echo .inp fallback for CHANNEL table "
                   "(needed when the H5 has no /input/channel dataset).")
@click.option("--centerlines-file", default=None,
              type=click.Path(exists=True, dir_okay=False),
              help="GeoJSON or shapefile of DSM2 channel centrelines used for "
                   "station snapping.  Defaults to the bundled DSM2 8.2 "
                   "centrelines when omitted.")
@click.option("--start", type=str, default=None,
              help="Start date: ISO (2014-10-01) or DSM2 military (01OCT2014).")
@click.option("--end", type=str, default=None,
              help="End date: ISO or DSM2 military.")
@click.option("--idw-power", default=2.0, show_default=True, type=float,
              help="IDW distance exponent.")
@click.option("--max-obs-age", default="2h", show_default=True,
              help='Max observation age relative to model timestep (e.g. "2h").')
@click.option("--chunk-size", default=1000, show_default=True, type=int,
              help="Timesteps per write chunk (controls memory usage).")
def export_corrected_cmd(
    h5file, output_h5, constituent, observations_csv, stations_csv,
    echo_inp, centerlines_file, start, end, idw_power, max_obs_age, chunk_size,
):
    """Pre-compute IDW-corrected concentrations and write a new QUAL HDF5.

    The output file is a drop-in replacement for the raw QUAL HDF5 \u2014 same
    dataset paths and time attributes \u2014 so it can be compared with the raw
    model using the standard two-file comparison animation with no extra setup:

    \b
        dsm2ui animate qual RAW.h5 CORRECTED.h5 --constituent ec
        dsm2ui animate qual RAW.h5 CORRECTED.h5 --transform godin-daily --diff

    Because corrections are pre-computed, animation is pure HDF5 reads:
    Godin filter, daily resample, and diff mode all work at full speed.
    A /correction group in the output records provenance (obs file, power,
    creation time).
    """
    from pydsm.analysis.dsm2study import parse_military_date
    import pandas as _pd

    def _parse(s):
        if s is None:
            return None
        try:
            return parse_military_date(s)
        except Exception:
            return _pd.Timestamp(s)

    from dsm2ui.animate import export_corrected_qual_h5
    export_corrected_qual_h5(
        input_h5=h5file,
        output_h5=output_h5,
        observations_csv=observations_csv,
        stations_csv=stations_csv,
        constituent=constituent,
        centerlines_file=centerlines_file,
        power=idw_power,
        max_obs_age=max_obs_age,
        echo_inp_file=echo_inp,
        start=_parse(start),
        end=_parse(end),
        chunk_size=chunk_size,
    )


# ---------------------------------------------------------------------------
# compute-stage-means
# ---------------------------------------------------------------------------

@animate.command(name="compute-stage-means", context_settings=CONTEXT_SETTINGS)
@click.argument("hydro_h5", type=click.Path(exists=True, dir_okay=False, readable=True))
@click.option("--stage-config", "stage_config_file", required=True,
              type=click.Path(exists=True, dir_okay=False),
              help="stage_config.yaml to read channels from (and update in-place unless --dry-run).")
@click.option("--start", type=str, default=None,
              help="Start of averaging window: ISO (2014-10-01) or DSM2 military (01OCT2014). "
                   "Default: start of run.")
@click.option("--end", type=str, default=None,
              help="End of averaging window: ISO or DSM2 military. Default: end of run.")
@click.option("--location", default="both", show_default=True,
              type=click.Choice(["both", "upstream", "downstream"], case_sensitive=False),
              help="Which channel end to use for the stage reading.")
@click.option("--dry-run", is_flag=True, default=False,
              help="Print computed means without modifying the YAML file.")
@click.option("--chunk-size", default=5000, show_default=True, type=int,
              help="HDF5 timesteps read per chunk (reduce if memory is limited).")
def compute_stage_means_cmd(
    hydro_h5, stage_config_file, start, end, location, dry_run, chunk_size,
):
    """Compute mean water-surface stage for each bar in a stage_config.yaml.

    Reads the HYDRO HDF5 tidefile over the specified time window, averages the
    datum-corrected stage (ft NAVD88) for every channel listed in the YAML, and
    either prints the results (--dry-run) or writes them back into the YAML,
    preserving all comments and other content.

    \b
    Typical usage:

        # Compute from the full run and update the YAML in-place:
        dsm2ui animate compute-stage-means hydro.h5 --stage-config stage_config.yaml

        # Restrict to a calibration window:
        dsm2ui animate compute-stage-means hydro.h5 \\
            --stage-config stage_config.yaml \\
            --start 01OCT2014 --end 30SEP2017

        # Preview without writing:
        dsm2ui animate compute-stage-means hydro.h5 \\
            --stage-config stage_config.yaml --dry-run

    The mean is the temporal mean of the corrected water-surface elevation
    (depth + channel_bottom from /hydro/geometry/channel_bottom), matching the
    datum convention required by mean_stage_ft in the YAML.
    """
    import pandas as _pd
    from pydsm.analysis.dsm2study import parse_military_date

    def _parse(s):
        if s is None:
            return None
        try:
            return parse_military_date(s)
        except Exception:
            return _pd.Timestamp(s)

    t_start = _parse(start)
    t_end = _parse(end)

    # Load the stage config to get channel list
    from dsm2ui.stage_layer import StageLayerSpec, compute_stage_means, _update_yaml_means
    spec = StageLayerSpec.from_yaml(stage_config_file)

    if not spec.bars:
        raise SystemExit("No bars defined in the stage config — nothing to compute.")

    channels = [b.channel for b in spec.bars]

    click.echo(f"Computing mean stage for {len(channels)} channel(s) ...")
    click.echo(f"  File    : {hydro_h5}")
    click.echo(f"  Window  : {t_start or 'start'} — {t_end or 'end'}")
    click.echo(f"  Location: {location}")

    means = compute_stage_means(
        hydro_h5_path=hydro_h5,
        channels=channels,
        location=location,
        start=t_start,
        end=t_end,
        chunk_size=chunk_size,
    )

    # Report results
    click.echo("")
    click.echo(f"{'Channel':>8}  {'Label':<40}  {'Mean stage (ft NAVD88)':>22}")
    click.echo("-" * 78)
    for bar in spec.bars:
        val = means.get(bar.channel, float("nan"))
        flag = "  (not found in H5)" if val != val else ""  # NaN check
        click.echo(f"{bar.channel:>8}  {bar.label:<40}  {val:>22.4f}{flag}")

    if dry_run:
        click.echo("\n(--dry-run: YAML not modified)")
        return

    # Update the YAML file in-place, preserving comments
    yaml_text = open(stage_config_file, encoding="utf-8").read()
    updated = _update_yaml_means(yaml_text, means)
    with open(stage_config_file, "w", encoding="utf-8") as fh:
        fh.write(updated)
    click.echo(f"\nUpdated {stage_config_file}")
