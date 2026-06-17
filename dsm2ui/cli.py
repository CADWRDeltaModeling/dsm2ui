# -*- coding: utf-8 -*-
"""Console script for dsm2ui."""
import sys
import click
from dsm2ui._version import __version__
from dsm2ui._logging import setup_logging

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


class _LazyGroup(click.Group):
    """Click Group that defers importing subcommands until they are actually invoked.

    Pass ``lazy_subcommands`` as a dict mapping command name to
    ``(module_path, attribute, help_text)`` tuples (help_text is optional), e.g.::

        {"calib-ui": ("dsm2ui.calib.calibplotui", "calib_plot_ui", "Launch calib UI")}
    """

    def __init__(self, *args, lazy_subcommands=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._lazy_subcommands = lazy_subcommands or {}

    def list_commands(self, ctx):
        return sorted(set(super().list_commands(ctx)) | set(self._lazy_subcommands))

    def get_command(self, ctx, name):
        if name in self._lazy_subcommands:
            import importlib
            entry = self._lazy_subcommands[name]
            mod = importlib.import_module(entry[0])
            return getattr(mod, entry[1])
        return super().get_command(ctx, name)

    def format_commands(self, ctx, formatter):
        """Override to avoid importing lazy subcommands just to get help text."""
        commands = []
        for name in self.list_commands(ctx):
            if name in self._lazy_subcommands:
                entry = self._lazy_subcommands[name]
                hidden = entry[3] if len(entry) > 3 else False
                if hidden:
                    continue
                help_text = entry[2] if len(entry) > 2 else ""
                commands.append((name, help_text))
            else:
                cmd = self.commands.get(name)
                if cmd and not cmd.hidden:
                    commands.append((name, cmd.get_short_help_str(limit=formatter.width)))
        if commands:
            with formatter.section("Commands"):
                formatter.write_dl(commands)


class _SmartUIGroup(_LazyGroup):
    """_LazyGroup that routes DSM2 file-path arguments to _smart_ui.

    When the first remaining argument looks like a DSM2 file path
    (.inp, .h5, .hdf5, .dss), ``resolve_command`` returns a hidden
    dispatch command that forwards all remaining args to ``_smart_ui``.
    Known subcommand names (map, xsect, …) are still resolved normally.
    """

    _FILE_EXTS = frozenset([".inp", ".h5", ".hdf5", ".dss"])

    def _is_file_arg(self, name: str) -> bool:
        import os.path
        _, ext = os.path.splitext(name.replace("\\", "/").split("/")[-1])
        return ext.lower() in self._FILE_EXTS

    def resolve_command(self, ctx, args):
        """Route file-path args to a hidden dispatch command."""
        if args and self._is_file_arg(args[0]):
            return "_files_", self._make_file_dispatch_cmd(ctx), args
        return super().resolve_command(ctx, args)

    def _make_file_dispatch_cmd(self, ctx):
        """Build a hidden Click command that forwards files to _smart_ui."""
        # Capture group-level params now (before sub-context is created).
        _port = ctx.params.get("port", 0)
        _desktop = ctx.params.get("desktop", False)
        _channel_shapefile = ctx.params.get("channel_shapefile", None)

        @click.command("_files_", hidden=True)
        @click.argument("files", nargs=-1, type=click.Path())
        def _dispatch(files):
            _smart_ui(
                list(files),
                port=_port,
                desktop=_desktop,
                channel_shapefile=_channel_shapefile,
            )

        return _dispatch


@click.group(cls=_LazyGroup, context_settings=CONTEXT_SETTINGS)
@click.version_option(
    __version__, "-v", "--version", message="%(prog)s, version %(version)s"
)
def main():
    """dsm2ui - DSM2 User Interface and Analysis Tools."""
    pass


# ---------------------------------------------------------------------------
# calib group (run / optimize / init)
# ---------------------------------------------------------------------------

from dsm2ui.calib.calib_cli import calib  # noqa: E402
main.add_command(calib)


# ---------------------------------------------------------------------------
# animate group — DSM2 HDF5 geo-animation
# ---------------------------------------------------------------------------

from dsm2ui.animate_cli import animate  # noqa: E402
main.add_command(animate)


# ---------------------------------------------------------------------------
# ui group — smart file-type dispatcher
# ---------------------------------------------------------------------------

def _smart_ui(files, port=0, desktop=False, channel_shapefile=None):
    """Launch the right DSM2 viewer based on file extensions.

    * Any ``.inp`` echo file  → :class:`~dsm2ui.echo_plugin.EchoUIManager`
      (shows both input boundary conditions and output channel time series;
      also accepts ``.h5`` / ``.dss`` alongside ``.inp``).
    * ``.h5`` / ``.hdf5`` / ``.dss`` only  → :class:`~dsm2ui.dsm2ui.DSM2CombinedUIManager`
      (HDF5 tidefile + DSS viewer with drag-and-drop support).
    * No files  → empty :class:`~dsm2ui.dsm2ui.DSM2CombinedUIManager`
      (drag-and-drop ``.h5`` or ``.dss`` files into the running UI).
    """
    import panel as pn
    pn.extension()
    from dsm2ui.session import serve_session_app, serve_desktop_app

    inp_files = [f for f in files if f.lower().endswith(".inp")]
    other_files = [f for f in files if not f.lower().endswith(".inp")]

    _serve = serve_desktop_app if desktop else serve_session_app

    if inp_files:
        # Echo files present → EchoUIManager handles everything.
        # EchoUIManager is a RegistryUIManager subclass, so it also accepts
        # .h5 and .dss files via the reader registry.
        from dsm2ui.echo_plugin import (
            EchoUIManager,
            _load_channel_midpoints,
            _DEFAULT_CHANNEL_GEO,
        )
        import os as _os
        channel_geo = channel_shapefile or (
            _DEFAULT_CHANNEL_GEO if _os.path.isfile(_DEFAULT_CHANNEL_GEO) else None
        )

        def build_manager():
            mgr = EchoUIManager()
            all_files = inp_files + other_files
            if all_files:
                mgr.add_source_files(*all_files)
            if channel_geo and not mgr._geo_loaded:
                try:
                    midpoints = _load_channel_midpoints(channel_geo)
                    mgr._geo_source_df = midpoints
                    mgr._geo_id_column = "id"
                    mgr._geo_station_column = "chan_no"
                    mgr._apply_geo_merge()
                    mgr._geo_loaded = True
                except Exception as exc:
                    import logging
                    logging.getLogger(__name__).warning(
                        "_smart_ui: could not load channel geo %s: %s", channel_geo, exc
                    )
            return mgr

        _serve(build_manager, title="DSM2 UI", port=port)
    else:
        # Only .h5 / .dss files (or no files) → DSM2CombinedUIManager.
        from dsm2ui.dsm2ui import DSM2CombinedUIManager

        def build_manager():
            return DSM2CombinedUIManager(files=other_files)

        _serve(build_manager, title="DSM2 UI", port=port)


@click.group(
    name="ui",
    cls=_SmartUIGroup,
    invoke_without_command=True,
    context_settings=CONTEXT_SETTINGS,
    lazy_subcommands={
        # Hidden backward-compat aliases — functionality merged into 'dsm2ui ui [FILES]'.
        "input":    ("dsm2ui.dsm2ui",        "show_dsm2_input_ui",            "Interactive viewer for DSM2 input boundary condition time series", True),
        "output":   ("dsm2ui.dsm2ui",        "show_dsm2_output_ui",           "Interactive map + time-series viewer for DSM2 output files",        True),
        "tide":     ("dsm2ui.dsm2ui",        "show_dsm2_tidefile_ui",         "Interactive map + time-series viewer for DSM2 HDF5 tidefiles",      True),
        "combined": ("dsm2ui.dsm2ui",        "show_dsm2_combined_ui",         "Mixed HDF5 + DSS viewer with drag-and-drop support",                 True),
        "echo":     ("dsm2ui.echo_plugin",   "show_dsm2_echo_ui",            "Input+Output viewer from DSM2 echo .inp file",                      True),
        "dss":      ("dsm2ui.dssui.dss_cli", "show_dss_ui",                  "Generic HEC-DSS file browser",                                      True),
        # Still-visible specialist sub-commands (different viewer type).
        "xsect":    ("dsm2ui.dsm2ui",        "show_dsm2_tidefile_xsect_ui",  "Cross-section viewer for a DSM2 tidefile"),
    },
)
@click.option(
    "--port",
    default=0,
    show_default=True,
    type=int,
    help="Port for the web server (0 = random available port).",
)
@click.option(
    "--desktop",
    is_flag=True,
    default=False,
    help="Open in a native desktop window (requires pywebview).",
)
@click.option(
    "--channel-shapefile",
    default=None,
    help="Override the bundled channel centerline GeoJSON (used with .inp files).",
)
@click.pass_context
def ui_group(ctx, port, desktop, channel_shapefile):
    """Launch a DSM2 viewer for any DSM2 file type.

    Pass one or more FILES and the right viewer is selected automatically:

    \b
      .inp  (echo file)          → input+output time-series viewer with channel map
      .h5 / .hdf5  (tidefile)    → HDF5 tidefile viewer
      .dss  (DSS output)         → DSS time-series browser
      mixed .inp + .h5 / .dss   → unified echo viewer with all references
      (no files)                 → empty HDF5+DSS viewer; drag-and-drop to add files

    Examples::

        dsm2ui ui run_hydro_echo.inp
        dsm2ui ui hist_fc_mss.h5
        dsm2ui ui hist_qual.dss hist_hydro.dss
        dsm2ui ui run_hydro_echo.inp hist_fc_mss.h5
        dsm2ui ui                              # empty, drop files in

    Specialist sub-commands for other viewer types::

        dsm2ui ui map  run_hydro_echo.inp      # Manning/dispersion/length map
        dsm2ui ui xsect  run.h5               # cross-section viewer
    """
    # This callback is reached only when no subcommand / file args are given
    # (invoke_without_command=True).  File-path args are intercepted by
    # _SmartUIGroup.resolve_command and handled by a hidden _files_ command.
    if ctx.invoked_subcommand is None:
        _smart_ui([], port=port, desktop=desktop, channel_shapefile=channel_shapefile)


main.add_command(ui_group)


def _default_channel_shapefile():
    """Return path to the bundled DSM2 channel centerline GeoJSON."""
    import pathlib
    return str(pathlib.Path(__file__).parent / "dsm2gis" / "dsm2_channels_centerlines_8_2.geojson")


def _default_node_shapefile():
    """Return path to the bundled DSM2 node GeoJSON."""
    import pathlib
    return str(pathlib.Path(__file__).parent / "dsm2gis" / "dsm2_nodes_8_2.geojson")


@ui_group.command(name="map")
@click.argument(
    "hydro_echo_files",
    nargs=-1,
    required=True,
    type=click.Path(dir_okay=False, exists=True, readable=True),
)
@click.option(
    "--channel", "flowline_shapefile",
    type=click.Path(dir_okay=False, exists=True, readable=True),
    default=None,
    help="Channel centerline shapefile. Defaults to the bundled GeoJSON.",
)
@click.option(
    "--node", "node_shapefile",
    type=click.Path(dir_okay=False, exists=True, readable=True),
    default=None,
    help="Node shapefile — show node flow-split network map. Defaults to bundled GeoJSON when provided.",
)
@click.option(
    "-c", "--colored-by",
    type=click.Choice(["MANNING", "DISPERSION", "LENGTH", "ALL"], case_sensitive=False),
    default="MANNING",
    show_default=True,
    help="Color channels by this attribute (channel map only).",
)
@click.option(
    "--base-file", "-b",
    type=click.Path(dir_okay=False, exists=True, readable=True),
    default=None,
    help="Base hydro echo file for comparison overlay (channel map only).",
)
def ui_map(hydro_echo_files, flowline_shapefile, node_shapefile, colored_by, base_file):
    """Show an interactive DSM2 network map.

    One or more HYDRO_ECHO_FILES may be supplied; when multiple are given each
    gets its own map panel arranged in a Column.

    --channel defaults to the bundled centerline GeoJSON when omitted.
    Use --node to additionally show a node flow-split map (single file only).
    """
    if not flowline_shapefile:
        flowline_shapefile = _default_channel_shapefile()
    import panel as pn
    pn.extension()
    from dsm2ui.dsm2ui import DSM2FlowlineMap

    if len(hydro_echo_files) == 1:
        mapui = DSM2FlowlineMap(flowline_shapefile, hydro_echo_files[0], base_file)
        if colored_by == "ALL":
            channel_panel = pn.Column(
                *[mapui.show_map_colored_by_column(c.upper()) for c in ["MANNING", "DISPERSION", "LENGTH"]]
            )
        else:
            channel_panel = mapui.show_map_colored_by_column(colored_by.upper())
        if node_shapefile:
            from dsm2ui import dsm2ui as _dsm2ui
            netmap = _dsm2ui.DSM2GraphNetworkMap(node_shapefile, hydro_echo_files[0])
            layout = pn.Column(channel_panel, netmap.get_panel())
        else:
            layout = channel_panel
    else:
        if colored_by == "ALL":
            layout = pn.Column(*[
                DSM2FlowlineMap.multi_file_panel(
                    flowline_shapefile, hydro_echo_files, c.upper(), base_file
                )
                for c in ["MANNING", "DISPERSION", "LENGTH"]
            ])
        else:
            layout = DSM2FlowlineMap.multi_file_panel(
                flowline_shapefile, hydro_echo_files, colored_by.upper(), base_file
            )
    pn.serve(layout, websocket_max_message_size=100 * 1024 * 1024)


# ---------------------------------------------------------------------------
# dcd group (map / nodes / ui)
# ---------------------------------------------------------------------------

@click.group(
    name="dcd",
    cls=_LazyGroup,
    context_settings=CONTEXT_SETTINGS,
    lazy_subcommands={
        "map":   ("dsm2ui.deltacdui.deltacdui",    "dcd_geomap",         "Show Delta CD geographic map"),
        "nodes": ("dsm2ui.deltacdui.deltacdui",    "show_deltacd_nodes_ui", "Show Delta CD nodes UI"),
        "ui":    ("dsm2ui.deltacdui.deltacduimgr", "show_deltacd_ui",    "Full Delta CD netCDF data viewer"),
    },
)
def dcd_group():
    """Delta CD (crop model) viewers and utilities."""
    pass


main.add_command(dcd_group)


# ---------------------------------------------------------------------------
# datastore group (extract)
# ---------------------------------------------------------------------------

_PARAM_CHOICES = click.Choice(
    ["elev", "predictions", "flow", "temp", "do", "ec", "ssc", "turbidity", "ph", "velocity", "cla"],
    case_sensitive=False,
)


@click.group(name="datastore", context_settings=CONTEXT_SETTINGS)
def datastore_group():
    """DMS Datastore export utilities."""
    pass


@datastore_group.command(name="extract")
@click.argument("param", type=_PARAM_CHOICES)
@click.option(
    "--repo",
    "datastore_dir",
    required=True,
    type=click.Path(exists=True, file_okay=False, dir_okay=True, readable=True),
    help="Path to the DMS Datastore directory.",
)
@click.option(
    "--output",
    "dssfile",
    type=click.Path(dir_okay=False),
    default=None,
    help="DSS file to write extracted time series to.",
)
@click.option(
    "--repo-level",
    type=click.Choice(["screened"], case_sensitive=False),
    default="screened",
    show_default=True,
    help="Data repository level to use.",
)
@click.option(
    "--unit-name",
    type=str,
    default=None,
    help="Override the unit name written to the DSS file.",
)
@click.option(
    "--stations",
    type=click.Path(dir_okay=False),
    default=None,
    help="Write a station CSV (station_id, lat, lon) to this file path.",
)
def datastore_extract(datastore_dir, dssfile, param, repo_level, unit_name, stations):
    """Extract a parameter from a DMS Datastore.

    At least one of --output or --stations must be provided.

    Valid PARAM values: elev, predictions, flow, temp, do, ec, ssc, turbidity, ph, velocity, cla
    """
    if not dssfile and not stations:
        raise click.UsageError("Provide --output, --stations, or both.")
    from dsm2ui import datastore2dss
    if dssfile:
        datastore2dss.read_from_datastore_write_to_dss(
            datastore_dir, dssfile, param, repo_level, unit_name=unit_name
        )
    if stations:
        datastore2dss.write_station_lat_lng(datastore_dir, stations, param, repo_level)
        click.echo(f"Station CSV written to: {stations}")


main.add_command(datastore_group)


# ---------------------------------------------------------------------------
# channel-map
# ---------------------------------------------------------------------------

@main.command(name="channel-map", hidden=True)
@click.argument(
    "hydro_echo_file", type=click.Path(dir_okay=False, exists=True, readable=True)
)
@click.option(
    "--channel", "flowline_shapefile",
    type=click.Path(dir_okay=False, exists=True, readable=True),
    default=None,
    help="Channel centerline shapefile. Defaults to the bundled GeoJSON.",
)
@click.option(
    "-c",
    "--colored-by",
    type=click.Choice(["MANNING", "DISPERSION", "LENGTH", "ALL"], case_sensitive=False),
    default="MANNING",
)
@click.option(
    "--base-file", "-b", type=click.Path(dir_okay=False, exists=True, readable=True)
)
def map_channels_colored(hydro_echo_file, flowline_shapefile, colored_by, base_file):
    """Show an interactive map of DSM2 channels colored by Manning's n, dispersion, or length."""
    if not flowline_shapefile:
        flowline_shapefile = _default_channel_shapefile()
    import panel as pn
    from dsm2ui.dsm2ui import DSM2FlowlineMap
    pn.extension()
    mapui = DSM2FlowlineMap(flowline_shapefile, hydro_echo_file, base_file)
    if colored_by == "ALL":
        return pn.panel(
            pn.Column(
                *[
                    mapui.show_map_colored_by_column(c.upper())
                    for c in ["MANNING", "DISPERSION", "LENGTH"]
                ]
            )
        ).show()
    else:
        return pn.panel(mapui.show_map_colored_by_column(colored_by.upper())).show()


# ---------------------------------------------------------------------------
# node-map
# ---------------------------------------------------------------------------

@main.command(name="node-map", hidden=True)
@click.argument(
    "hydro_echo_file", type=click.Path(dir_okay=False, exists=True, readable=True)
)
@click.option(
    "--node", "node_shapefile",
    type=click.Path(dir_okay=False, exists=True, readable=True),
    default=None,
    help="Node GeoJSON. Defaults to the bundled DSM2 v8.2 node GeoJSON.",
)
def node_map_flow_splits(hydro_echo_file, node_shapefile):
    """Show an interactive panel map of DSM2 network nodes and flow splits."""
    if not node_shapefile:
        node_shapefile = _default_node_shapefile()
    import panel as pn
    from dsm2ui import dsm2ui as _dsm2ui
    pn.extension()
    netmap = _dsm2ui.DSM2GraphNetworkMap(node_shapefile, hydro_echo_file)
    pn.serve(
        netmap.get_panel(), kwargs={"websocket-max-message-size": 100 * 1024 * 1024}
    )


# ---------------------------------------------------------------------------
# postpro
# ---------------------------------------------------------------------------

@main.command(name="postpro", hidden=True)
@click.argument(
    "process_name",
    type=click.Choice(
        [
            "observed",
            "model",
            "plots",
            "heatmaps",
            "validation_bar_charts",
            "copy_plot_files",
        ],
        case_sensitive=False,
    ),
    default="",
)
@click.argument("json_config_file")
@click.option("--dask/--no-dask", default=False, hidden=True)
@click.option("--skip-cached", is_flag=True, default=False, help="Use existing post-processing cache instead of clearing and recomputing (applies to model and plots).")
@click.option("--workers", default=1, show_default=True, type=click.INT, help="Number of parallel worker processes for the 'plots' step.")
def exec_postpro_dsm2(process_name, json_config_file, dask, skip_cached, workers):
    """Run a DSM2 post-processing step (observed, model, plots, heatmaps, validation_bar_charts, or copy_plot_files)."""
    setup_logging()
    from dsm2ui.calib import postpro_dsm2
    postpro_dsm2.run_process(process_name, json_config_file, dask, skip_if_cached=skip_cached, n_workers=workers)


# ---------------------------------------------------------------------------
# mann-disp
# ---------------------------------------------------------------------------

@main.command(name="mann-disp")
@click.argument(
    "chan_to_group_filename",
    type=click.Path(dir_okay=False, exists=True, readable=True),
)
@click.argument(
    "chan_group_mann_disp_filename",
    type=click.Path(dir_okay=False, exists=True, readable=True),
)
@click.argument(
    "dsm2_channels_input_filename",
    type=click.Path(dir_okay=False, exists=True, readable=True),
)
@click.argument(
    "dsm2_channels_output_filename",
    type=click.Path(dir_okay=False, exists=False, readable=False),
)
def exec_dsm2_chan_mann_disp(
    chan_to_group_filename,
    chan_group_mann_disp_filename,
    dsm2_channels_input_filename,
    dsm2_channels_output_filename,
):
    """Apply group-based Manning's n and dispersion values to a DSM2 channels input file."""
    setup_logging()
    from dsm2ui import dsm2_chan_mann_disp
    dsm2_chan_mann_disp.prepro(
        chan_to_group_filename,
        chan_group_mann_disp_filename,
        dsm2_channels_input_filename,
        dsm2_channels_output_filename,
    )


# ---------------------------------------------------------------------------
# checklist
# ---------------------------------------------------------------------------

@main.command(name="checklist", hidden=True)
@click.argument(
    "process_name",
    type=click.Choice(["resample", "extract", "plot"], case_sensitive=False),
    default="",
)
@click.argument("json_config_file")
def exec_checklist_dsm2(process_name, json_config_file):
    """Run a DSM2 calibration checklist step (resample, extract, or plot)."""
    setup_logging()
    from dsm2ui.calib import checklist_dsm2
    checklist_dsm2.run_checklist(process_name, json_config_file)


# ---------------------------------------------------------------------------
# ds2dss
# ---------------------------------------------------------------------------

@main.command(name="ds2dss", hidden=True)
@click.argument(
    "datastore_dir",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, readable=True),
)
@click.argument(
    "dssfile", type=click.Path(dir_okay=False, exists=False, readable=False)
)
@click.argument(
    "param",
    type=click.Choice(
        [
            "elev",
            "predictions",
            "flow",
            "temp",
            "do",
            "ec",
            "ssc",
            "turbidity",
            "ph",
            "velocity",
            "cla",
        ],
        case_sensitive=False,
    ),
)
@click.option(
    "--repo-level",
    type=click.Choice(["screened"], case_sensitive=False),
    default="screened",
)
@click.option(
    "--unit-name",
    type=str,
    default=None,
)
def datastore_to_dss(
    datastore_dir, dssfile, param, repo_level="screened", unit_name=None
):
    """Reads datastore timeseries files and writes to a DSS file."""
    setup_logging()
    from dsm2ui import datastore2dss
    datastore2dss.read_from_datastore_write_to_dss(
        datastore_dir, dssfile, param, repo_level, unit_name=unit_name
    )


# ---------------------------------------------------------------------------
# ds2stations
# ---------------------------------------------------------------------------

@main.command(name="ds2stations", hidden=True)
@click.argument(
    "datastore_dir",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, readable=True),
)
@click.argument(
    "stationfile", type=click.Path(dir_okay=False, exists=False, readable=False)
)
@click.argument(
    "param",
    type=click.Choice(
        [
            "elev",
            "predictions",
            "flow",
            "temp",
            "do",
            "ec",
            "ssc",
            "turbidity",
            "ph",
            "velocity",
            "cla",
        ],
        case_sensitive=False,
    ),
)
def datastore_to_stationfile(datastore_dir, stationfile, param):
    """Writes station_id, latitude, longitude to a csv file."""
    setup_logging()
    from dsm2ui import datastore2dss
    datastore2dss.write_station_lat_lng(datastore_dir, stationfile, param)


# ---------------------------------------------------------------------------
# stations-out
# ---------------------------------------------------------------------------

@main.command(name="stations-out", hidden=True)
@click.argument(
    "stations_file",
    type=click.Path(dir_okay=False, exists=True, readable=True),
)
@click.argument(
    "output_file", type=click.Path(dir_okay=False, exists=False, readable=False)
)
@click.option(
    "--centerlines", "centerlines_file",
    type=click.Path(dir_okay=False, exists=True, readable=True),
    default=None,
    help="Channel centerlines GeoJSON. Defaults to the bundled DSM2 v8.2 GeoJSON.",
)
@click.option(
    "--distance-tolerance",
    type=click.INT,
    default=100,
    help="Maximum distance from a line that a station can be to be considered on that line",
)
def stations_output_file(
    stations_file, output_file, centerlines_file, distance_tolerance=100
):
    """[Deprecated] Use 'dsm2ui station-map to-dsm2' instead."""
    if not centerlines_file:
        centerlines_file = _default_channel_shapefile()
    from pydsm.viz import dsm2gis
    dsm2gis.create_stations_output_file(
        stations_file=stations_file,
        centerlines_file=centerlines_file,
        output_file=output_file,
        distance_tolerance=distance_tolerance,
    )


# ---------------------------------------------------------------------------
# station-map group
# ---------------------------------------------------------------------------

@click.group(name="station-map", context_settings=CONTEXT_SETTINGS)
def station_map_group():
    """Map stations between lat/lon and DSM2 channel locations."""
    pass


@station_map_group.command(name="to-dsm2")
@click.argument(
    "stations_csv",
    type=click.Path(dir_okay=False, exists=True, readable=True),
)
@click.argument(
    "output_csv",
    type=click.Path(dir_okay=False),
)
@click.option(
    "--centerlines", "centerlines_geojson",
    type=click.Path(dir_okay=False, exists=True, readable=True),
    default=None,
    help="Channel centerlines GeoJSON. Defaults to the bundled DSM2 v8.2 GeoJSON.",
)
@click.option(
    "--distance-tolerance",
    type=click.INT,
    default=100,
    show_default=True,
    help="Maximum distance (ft) from a channel centerline for a station to be considered matched.",
)
@click.option(
    "--unmatched",
    "unmatched_csv",
    type=click.Path(dir_okay=False),
    default=None,
    help="Write unmatched stations to this CSV (default: <output>_unmatched.csv).",
)
def station_map_to_dsm2(stations_csv, output_csv, centerlines_geojson, distance_tolerance, unmatched_csv):
    """Snap lat/lon stations to DSM2 channels, writing NAME (uppercased), CHAN_NO, DISTANCE.

    Stations that cannot be snapped within the distance tolerance are written to
    a separate unmatched CSV for review and correction.
    """
    import tempfile, os
    import pandas as pd
    from pydsm.viz import dsm2gis
    from pathlib import Path

    out_path = Path(output_csv)
    if unmatched_csv is None:
        unmatched_csv = str(out_path.parent / (out_path.stem + "_unmatched.csv"))

    stations_df = pd.read_csv(stations_csv)

    if not centerlines_geojson:
        centerlines_geojson = _default_channel_shapefile()
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        dsm2gis.create_stations_output_file(
            stations_file=stations_csv,
            centerlines_file=centerlines_geojson,
            output_file=tmp_path,
            distance_tolerance=distance_tolerance,
        )
        snapped = pd.read_csv(tmp_path, sep=" ")
    finally:
        os.unlink(tmp_path)

    snapped["NAME"] = snapped["NAME"].str.upper()

    snapped_ids = set(snapped["NAME"].str.lower())
    input_ids = stations_df["station_id"].str.lower() if "station_id" in stations_df.columns else pd.Series(dtype=str)
    unmatched_mask = ~input_ids.isin(snapped_ids)
    unmatched = stations_df[unmatched_mask]

    snapped.to_csv(output_csv, index=False, sep=" ")
    click.echo(f"Wrote {len(snapped)} matched stations to: {output_csv}")

    if not unmatched.empty:
        unmatched.to_csv(unmatched_csv, index=False)
        click.echo(
            f"WARNING: {len(unmatched)} station(s) unmatched (tolerance={distance_tolerance} ft) "
            f"— written to: {unmatched_csv}"
        )


@station_map_group.command(name="from-dsm2")
@click.argument(
    "echo_file",
    type=click.Path(dir_okay=False, exists=True, readable=True),
)
@click.argument(
    "output_geojson",
    type=click.Path(dir_okay=False),
)
@click.option(
    "--centerlines", "centerlines_geojson",
    type=click.Path(dir_okay=False, exists=True, readable=True),
    default=None,
    help="Channel centerlines GeoJSON. Defaults to the bundled DSM2 v8.2 GeoJSON.",
)
def station_map_from_dsm2(echo_file, output_geojson, centerlines_geojson):
    """Geolocate DSM2 OUTPUT_CHANNEL stations from an echo file to a GeoJSON.

    Reads the CHANNEL and OUTPUT_CHANNEL tables from the DSM2 echo file,
    interpolates each station's position along its channel centerline, and
    writes a GeoJSON with NAME, CHAN_NO, DISTANCE and point geometry.
    """
    if not centerlines_geojson:
        centerlines_geojson = _default_channel_shapefile()
    from pydsm.viz import dsm2gis
    dsm2gis.geolocate_output_locations.callback(echo_file, centerlines_geojson, output_geojson)


main.add_command(station_map_group)


# ---------------------------------------------------------------------------
# build-calib-config
# ---------------------------------------------------------------------------

@main.command(name="build-calib-config", hidden=True)
@click.option(
    "--study",
    "-s",
    "study_folders",
    multiple=True,
    required=True,
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    help="Study folder path (repeat -s for multiple studies).",
)
@click.option(
    "--postprocessing",
    "-p",
    required=True,
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    help="Path to the postprocessing folder (contains location_info/ and observed_data/).",
)
@click.option(
    "--output",
    "-o",
    required=True,
    type=click.Path(dir_okay=False),
    help="Output YAML config file path.",
)
@click.option(
    "--module",
    "-m",
    type=click.Choice(["hydro", "qual", "gtm"], case_sensitive=False),
    default="hydro",
    show_default=True,
    help="DSM2 module whose DSS output to reference.",
)
@click.option(
    "--output-folder",
    default="./plots/",
    show_default=True,
    help="Plot output folder written into the YAML options_dict.",
)
def build_calib_config_cmd(study_folders, postprocessing, output, module, output_folder):
    """Generate a calib-ui YAML config from study folders and postprocessing data."""
    from dsm2ui.calib import calib_config_builder
    result = calib_config_builder.build_calib_config(
        study_folders=list(study_folders),
        postprocessing_folder=postprocessing,
        output_file=output,
        module=module,
        output_folder=output_folder,
    )
    click.echo(f"Config written to: {result}")


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
 
