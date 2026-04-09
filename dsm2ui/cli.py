# -*- coding: utf-8 -*-
"""Console script for dsm2ui."""
import sys
import click
from dsm2ui._version import __version__

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


@click.group(
    cls=_LazyGroup,
    context_settings=CONTEXT_SETTINGS,
    lazy_subcommands={
        "output-ui":   ("dsm2ui.dsm2ui",                 "show_dsm2_output_ui",            "Show DSM2 model output UI",              True),
        "tide-ui":     ("dsm2ui.dsm2ui",                 "show_dsm2_tidefile_ui",           "Show DSM2 tidefile UI",                  True),
        "xsect-ui":    ("dsm2ui.dsm2ui",                 "show_dsm2_tidefile_xsect_ui",     "Show DSM2 tidefile cross-section UI",    True),
        "dss-ui":      ("dsm2ui.dssui.dssui",            "show_dss_ui",                     "Show DSS file browser UI"),
        "geo-heatmap": ("dsm2ui.calib.geoheatmap",       "show_metrics_geo_heatmap",        "Show calibration metrics geo heatmap",        True),
        "geolocate":   ("pydsm.viz.dsm2gis",             "geolocate_output_locations",      "Geolocate DSM2 output locations"),
        "dcd-map":     ("dsm2ui.deltacdui.deltacdui",    "dcd_geomap",                      "Show Delta CD geographic map",           True),
        "dcd-nodes":   ("dsm2ui.deltacdui.deltacdui",    "show_deltacd_nodes_ui",           "Show Delta CD nodes UI",                 True),
        "calib-ui":    ("dsm2ui.calib.calibplotui",      "calib_plot_ui",                   "Launch interactive calibration plot viewer",  True),
        "ptm-animate": ("dsm2ui.ptm.ptm_animator",       "ptm_animate",                     "Animate PTM particle tracks"),
        "dcd-ui":      ("dsm2ui.deltacdui.deltacduimgr", "show_deltacd_ui",                 "Show Delta CD UI",                       True),
    },
)
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
# ui group (output / tide / xsect)
# ---------------------------------------------------------------------------

@click.group(
    name="ui",
    cls=_LazyGroup,
    context_settings=CONTEXT_SETTINGS,
    lazy_subcommands={
        "output": ("dsm2ui.dsm2ui", "show_dsm2_output_ui",         "Interactive map + time-series viewer for DSM2 output files"),
        "tide":   ("dsm2ui.dsm2ui", "show_dsm2_tidefile_ui",        "Interactive map + time-series viewer for DSM2 HDF5 tidefiles"),
        "xsect":  ("dsm2ui.dsm2ui", "show_dsm2_tidefile_xsect_ui",  "Cross-section viewer for a DSM2 tidefile"),
    },
)
def ui_group():
    """Launch DSM2 interactive viewers (output, tidefile, cross-sections)."""
    pass


main.add_command(ui_group)


@ui_group.command(name="map")
@click.argument(
    "hydro_echo_file", type=click.Path(dir_okay=False, exists=True, readable=True)
)
@click.option(
    "--channel", "flowline_shapefile",
    type=click.Path(dir_okay=False, exists=True, readable=True),
    default=None,
    help="Flowline shapefile — show channel map colored by Manning/dispersion/length.",
)
@click.option(
    "--node", "node_shapefile",
    type=click.Path(dir_okay=False, exists=True, readable=True),
    default=None,
    help="Node shapefile — show node flow-split network map.",
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
def ui_map(hydro_echo_file, flowline_shapefile, node_shapefile, colored_by, base_file):
    """Show an interactive DSM2 network map.

    Use --channel FLOWLINE_SHP for a channel map colored by Manning/dispersion/length.
    Use --node NODE_SHP for a node flow-split map.
    Both flags may be combined to show both maps together.
    """
    if not flowline_shapefile and not node_shapefile:
        raise click.UsageError("Provide at least one of --channel or --node.")
    import panel as pn
    pn.extension()
    panels = []
    if flowline_shapefile:
        from dsm2ui.dsm2ui import DSM2FlowlineMap
        mapui = DSM2FlowlineMap(flowline_shapefile, hydro_echo_file, base_file)
        if colored_by == "ALL":
            panels.append(pn.Column(
                *[mapui.show_map_colored_by_column(c.upper()) for c in ["MANNING", "DISPERSION", "LENGTH"]]
            ))
        else:
            panels.append(mapui.show_map_colored_by_column(colored_by.upper()))
    if node_shapefile:
        from dsm2ui import dsm2ui as _dsm2ui
        netmap = _dsm2ui.DSM2GraphNetworkMap(node_shapefile, hydro_echo_file)
        panels.append(netmap.get_panel())
    layout = panels[0] if len(panels) == 1 else pn.Column(*panels)
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
@click.argument(
    "datastore_dir",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, readable=True),
)
@click.argument(
    "dssfile",
    type=click.Path(dir_okay=False, exists=False, readable=False),
)
@click.argument("param", type=_PARAM_CHOICES)
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
    help="Also write a station CSV (station_id, lat, lon) to this file path.",
)
def datastore_extract(datastore_dir, dssfile, param, repo_level, unit_name, stations):
    """Extract a parameter from a DMS Datastore into a DSS file.

    Optionally write a station lat/lon CSV with --stations FILE.

    Valid PARAM values: elev, predictions, flow, temp, do, ec, ssc, turbidity, ph, velocity, cla
    """
    from dsm2ui import datastore2dss
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
    "flowline_shapefile", type=click.Path(dir_okay=False, exists=True, readable=True)
)
@click.argument(
    "hydro_echo_file", type=click.Path(dir_okay=False, exists=True, readable=True)
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
def map_channels_colored(flowline_shapefile, hydro_echo_file, colored_by, base_file):
    """Show an interactive map of DSM2 channels colored by Manning's n, dispersion, or length."""
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
    "node_shapefile", type=click.Path(dir_okay=False, exists=True, readable=True)
)
@click.argument(
    "hydro_echo_file", type=click.Path(dir_okay=False, exists=True, readable=True)
)
def node_map_flow_splits(node_shapefile, hydro_echo_file):
    """Show an interactive panel map of DSM2 network nodes and flow splits."""
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
@click.option("--dask/--no-dask", default=False)
@click.option("--skip-cached", is_flag=True, default=False, help="Skip locations already present in the post-processing cache (model only).")
def exec_postpro_dsm2(process_name, json_config_file, dask, skip_cached):
    """Run a DSM2 post-processing step (observed, model, plots, heatmaps, validation_bar_charts, or copy_plot_files)."""
    from dsm2ui.calib import postpro_dsm2
    postpro_dsm2.run_process(process_name, json_config_file, dask, skip_if_cached=skip_cached)


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
    from dsm2ui import datastore2dss
    datastore2dss.write_station_lat_lng(datastore_dir, stationfile, param)


# ---------------------------------------------------------------------------
# stations-out
# ---------------------------------------------------------------------------

@main.command(name="stations-out")
@click.argument(
    "stations_file",
    type=click.Path(dir_okay=False, exists=True, readable=True),
)
@click.argument(
    "centerlines_file",
    type=click.Path(dir_okay=False, exists=True, readable=True),
)
@click.argument(
    "output_file", type=click.Path(dir_okay=False, exists=False, readable=False)
)
@click.option(
    "--distance-tolerance",
    type=click.INT,
    default=100,
    help="Maximum distance from a line that a station can be to be considered on that line",
)
def stations_output_file(
    stations_file, centerlines_file, output_file, distance_tolerance=100
):
    """Create DSM2 channels output compatible file for given stations and centerlines."""
    from pydsm.viz import dsm2gis
    dsm2gis.create_stations_output_file(
        stations_file=stations_file,
        centerlines_file=centerlines_file,
        output_file=output_file,
        distance_tolerance=distance_tolerance,
    )


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
 
