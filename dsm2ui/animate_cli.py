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
    "none":        "none",
    "daily":       "Daily mean",
    "rolling-24h": "Rolling 24 h",
    "rolling-14d": "Rolling 14 D",
    "godin":       "Godin filter",
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
    click.option("--colormap", default="rainbow", show_default=True,
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
    mgr._contour_color_check.value = bool(contours.get("color", True))
    mgr._contour_labels_check.value = bool(contours.get("labels", False))
    mgr._show_channels_check.value = bool(cfg.get("show_channels", True))
    mgr._show_basemap_check.value = bool(cfg.get("show_basemap", True))
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
              type=click.Choice(["flow", "stage", "velocity"], case_sensitive=False),
              help="Tidefile variable to animate.")
@click.option("--location", default="both", show_default=True,
              type=click.Choice(["both", "upstream", "downstream"], case_sensitive=False),
              help="Channel location ('both' averages upstream and downstream).")
@click.option("--diff", "show_diff", is_flag=True, default=False,
              help="Show diff map (A \u2212 B) instead of side-by-side (only with 2 files).")
@click.option("--transform", default="none", show_default=True,
              type=click.Choice(["none", "daily", "rolling-24h", "rolling-14d", "godin"],
                                case_sensitive=False),
              help="Time-domain transform to apply before animation.\n"
                   "none: raw data (default).\n"
                   "daily: daily mean (resamples to 1-day steps).\n"
                   "rolling-24h: 24 h centred rolling mean (same timestep).\n"
                   "rolling-14d: 14-day centred rolling mean (same timestep).\n"
                   "godin: Godin tidal filter (requires vtools3).")
@click.option("--config", "config_file", default=None,
              type=click.Path(exists=True, dir_okay=False),
              help="Load all settings from a YAML config file saved by the UI. "
                   "H5FILES and other options are not required when --config is used.")
@_add_common_options
def hydro_cmd(
    h5files, variable, location, show_diff, transform, config_file,
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
        transform = cfg.get("transform", transform)
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

    if len(h5files) > 2:
        raise click.UsageError("At most 2 H5FILE arguments are supported.")
    multi = len(h5files) == 2
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
            mgr = animate_hydro(
                h5files[0],
                variable=variable, location=location,
                shapefile=shapefile,
                simplify_tolerance=simplify,
                channel_id_column=channel_id_column,
                vmin=vmin, vmax=vmax, colormap=colormap,
                title=effective_title, size=size,
                initial_transform=_CLI_TRANSFORM_MAP.get(transform.lower(), "none"),
            )
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
              type=click.Choice(["none", "daily", "rolling-24h", "rolling-14d", "godin"],
                                case_sensitive=False),
              help="Time-domain transform (none/daily/rolling-24h/rolling-14d/godin).")
@click.option("--config", "config_file", default=None,
              type=click.Path(exists=True, dir_okay=False),
              help="Load all settings from a YAML config file saved by the UI.")
@_add_common_options
def qual_cmd(
    h5files, constituent, x2_threshold, show_diff, transform, config_file,
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
        transform   = cfg.get("transform", transform)
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
        log.info("Loaded config from %s (%d file(s))", config_file, len(h5files))
    elif not h5files:
        raise click.UsageError(
            "Provide at least one H5FILE argument, or use --config to load a YAML config."
        )

    if len(h5files) > 2:
        raise click.UsageError("At most 2 H5FILE arguments are supported.")
    multi = len(h5files) == 2
    effective_title = title or (
        f"DSM2 QUAL {constituent.upper()} (\u0394)" if (multi and show_diff)
        else f"DSM2 QUAL {constituent.upper()}"
    )
    slug = _title_to_slug(effective_title)
    shapefiles = [shapefile] if shapefile else None

    def build():
        import holoviews as hv
        import panel as pn
        hv.extension("bokeh")
        pn.extension(throttled=True)
        if multi:
            from dsm2ui.animate import animate_qual_multi
            mgr = animate_qual_multi(
                h5files[0], h5files[1],
                constituent=constituent,
                shapefiles=shapefiles,
                simplify_tolerance=simplify,
                channel_id_column=channel_id_column,
                show_diff=show_diff,
                vmin=vmin, vmax=vmax, colormap=colormap, size=size,
                initial_transform=_CLI_TRANSFORM_MAP.get(transform.lower(), "none"),
            )
        else:
            from dsm2ui.animate import animate_qual
            mgr = animate_qual(
                h5files[0],
                constituent=constituent,
                shapefile=shapefile,
                simplify_tolerance=simplify,
                channel_id_column=channel_id_column,
                x2_threshold=x2_threshold,
                vmin=vmin, vmax=vmax, colormap=colormap,
                title=effective_title, size=size,
                initial_transform=_CLI_TRANSFORM_MAP.get(transform.lower(), "none"),
            )
        if cfg:
            _apply_config_to_manager(mgr, cfg)
        log.info("Ready")
        return mgr

    _serve_viewer(build, slug=slug, title=effective_title, port=port, desktop=desktop)
