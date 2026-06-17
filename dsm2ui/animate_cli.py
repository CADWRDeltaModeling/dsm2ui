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
    "rolling-48h": "Rolling 48 h",
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
    """


@animate.command(name="hydro", context_settings=CONTEXT_SETTINGS)
@click.argument("h5file", type=click.Path(exists=True, dir_okay=False, readable=True))
@click.option("--variable", default="flow", show_default=True,
              type=click.Choice(["flow", "stage", "velocity"], case_sensitive=False),
              help="Tidefile variable to animate.")
@click.option("--location", default="both", show_default=True,
              type=click.Choice(["both", "upstream", "downstream"], case_sensitive=False),
              help="Channel location ('both' averages upstream and downstream).")
@click.option("--transform", default="none", show_default=True,
              type=click.Choice(["none", "daily", "rolling-24h", "rolling-48h", "godin"],
                                case_sensitive=False),
              help="Time-domain transform to apply before animation.\n"
                   "none: raw data (default).\n"
                   "daily: daily mean (resamples to 1-day steps).\n"
                   "rolling-24h: 24 h centred rolling mean (same timestep).\n"
                   "rolling-48h: 48 h centred rolling mean (same timestep).\n"
                   "godin: Godin tidal filter (requires vtools3).")
@_add_common_options
def hydro_cmd(
    h5file, variable, location, transform,
    port, desktop, shapefile, vmin, vmax, colormap, title, size, simplify,
    channel_id_column, log_level,
):
    """Animate a HYDRO tidefile (flow or stage) on the channel network map."""
    import logging
    logging.basicConfig(level=getattr(logging, log_level.upper()),
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    log = logging.getLogger("dsm2ui.animate")

    effective_title = title or f"DSM2 Hydro {variable.title()}"
    slug = _title_to_slug(effective_title)
    log.info("Building HYDRO reader from %s (variable=%s, location=%s, transform=%s)",
             h5file, variable, location, transform)

    def build():
        import holoviews as hv
        import panel as pn
        hv.extension("bokeh")
        pn.extension(throttled=True)
        from dsm2ui.animate import animate_hydro
        log.info("Constructing GeoAnimatorManager for new session")
        mgr = animate_hydro(
            h5file,
            variable=variable,
            location=location,
            shapefile=shapefile,
            simplify_tolerance=simplify,
            channel_id_column=channel_id_column,
            vmin=vmin,
            vmax=vmax,
            colormap=colormap,
            title=effective_title,
            size=size,
            initial_transform=_CLI_TRANSFORM_MAP.get(transform.lower(), "none"),
        )
        log.info("Reader time_index: %d steps from %s to %s",
                 len(mgr._reader.time_index),
                 mgr._reader.time_index[0],
                 mgr._reader.time_index[-1])
        log.info("vmin=%.4g  vmax=%.4g", mgr._reader.vmin, mgr._reader.vmax)
        return mgr

    _serve_viewer(build, slug=slug, title=effective_title, port=port, desktop=desktop)


@animate.command(name="qual", context_settings=CONTEXT_SETTINGS)
@click.argument("h5file", type=click.Path(exists=True, dir_okay=False, readable=True))
@click.option("--constituent", default="ec", show_default=True,
              help="Constituent name (case-insensitive, e.g. ec, cl, do).")
@click.option("--x2-threshold", default=None, type=float,
              help="Enable X2 isohaline overlay at this EC threshold (µS/cm). "
                   "Example: --x2-threshold 2700")
@click.option("--transform", default="none", show_default=True,
              type=click.Choice(["none", "daily", "rolling-24h", "rolling-48h", "godin"],
                                case_sensitive=False),
              help="Time-domain transform (none/daily/rolling-24h/rolling-48h/godin).")
@_add_common_options
def qual_cmd(
    h5file, constituent, x2_threshold, transform,
    port, desktop, shapefile, vmin, vmax, colormap, title, size, simplify,
    channel_id_column, log_level,
):
    """Animate a QUAL or GTM tidefile (concentration) on the channel network map."""
    import logging
    logging.basicConfig(level=getattr(logging, log_level.upper()),
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    log = logging.getLogger("dsm2ui.animate")

    effective_title = title or f"DSM2 QUAL {constituent.upper()}"
    slug = _title_to_slug(effective_title)
    log.info("Building QUAL/GTM reader from %s (constituent=%s)", h5file, constituent)

    def build():
        import holoviews as hv
        import panel as pn
        hv.extension("bokeh")
        pn.extension(throttled=True)
        from dsm2ui.animate import animate_qual
        log.info("Constructing GeoAnimatorManager for new session")
        mgr = animate_qual(
            h5file,
            constituent=constituent,
            shapefile=shapefile,
            simplify_tolerance=simplify,
            channel_id_column=channel_id_column,
            x2_threshold=x2_threshold,
            vmin=vmin,
            vmax=vmax,
            colormap=colormap,
            title=effective_title,
            size=size,
            initial_transform=_CLI_TRANSFORM_MAP.get(transform.lower(), "none"),
        )
        log.info("Reader time_index: %d steps from %s to %s",
                 len(mgr._reader.time_index),
                 mgr._reader.time_index[0],
                 mgr._reader.time_index[-1])
        log.info("vmin=%.4g  vmax=%.4g", mgr._reader.vmin, mgr._reader.vmax)
        return mgr

    _serve_viewer(build, slug=slug, title=effective_title, port=port, desktop=desktop)
