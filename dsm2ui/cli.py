# -*- coding: utf-8 -*-
"""Console script for dsm2ui."""
from dsm2ui import dsm2ui
from dsm2ui.dsm2ui import DSM2FlowlineMap, build_output_plotter
from dsm2ui.calib import postpro_dsm2
from dsm2ui.calib import checklist_dsm2
from dsm2ui import dsm2_chan_mann_disp
from dsm2ui import create_ann_inputs
from dsm2ui import datastore2dss
from dsm2ui._version import __version__
import sys
import click
import panel as pn

pn.extension()


CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option(
    __version__, "-v", "--version", message="%(prog)s, version %(version)s"
)
def main():
    """dsm2ui - DSM2 User Interface and Analysis Tools."""
    pass


@click.command(name="channel-map")
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


@click.command(name="node-map")
@click.argument(
    "node_shapefile", type=click.Path(dir_okay=False, exists=True, readable=True)
)
@click.argument(
    "hydro_echo_file", type=click.Path(dir_okay=False, exists=True, readable=True)
)
def node_map_flow_splits(node_shapefile, hydro_echo_file):
    """Show an interactive panel map of DSM2 network nodes and flow splits."""
    netmap = dsm2ui.DSM2GraphNetworkMap(node_shapefile, hydro_echo_file)
    pn.serve(
        netmap.get_panel(), kwargs={"websocket-max-message-size": 100 * 1024 * 1024}
    )


@click.command(name="postpro")
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
def exec_postpro_dsm2(process_name, json_config_file, dask):
    """Run a DSM2 post-processing step (observed, model, plots, heatmaps, validation_bar_charts, or copy_plot_files)."""
    postpro_dsm2.run_process(process_name, json_config_file, dask)


@click.command(name="mann-disp")
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
    dsm2_chan_mann_disp.prepro(
        chan_to_group_filename,
        chan_group_mann_disp_filename,
        dsm2_channels_input_filename,
        dsm2_channels_output_filename,
    )


@click.command(name="checklist")
@click.argument(
    "process_name",
    type=click.Choice(["resample", "extract", "plot"], case_sensitive=False),
    default="",
)
@click.argument("json_config_file")
def exec_checklist_dsm2(process_name, json_config_file):
    """Run a DSM2 calibration checklist step (resample, extract, or plot)."""
    checklist_dsm2.run_checklist(process_name, json_config_file)


@click.command(name="ds2dss")
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
    """
    Reads datastore timeseries files and writes to a DSS file

    Parameters
    datastore_dir : str
        Directory where Datastore files are stored
    repo_level : str
        default is screened
    dssfile : str
        Filename to write to
    param : str
        e.g one of "flow","elev", "ec", etc.
    """
    datastore2dss.read_from_datastore_write_to_dss(
        datastore_dir, dssfile, param, repo_level, unit_name=unit_name
    )


@click.command(name="ds2stations")
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
    """
    Writes station_id, latitude, longitude to a csv file

    Parameters
    datastore_dir : str
        Directory where Datastore files are stored
    station_file : str
        Filename to write to
    param : str
        e.g one of "flow","elev", "ec", etc.
    """
    datastore2dss.write_station_lat_lng(datastore_dir, stationfile, param)


@click.command(name="stations-out")
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
    """
    Create DSM2 channels output compatible file for given stations info (station_id, lat lon)
    and centerlines geojson file (DSM2 channels centerlines) and writing out output_file

    stations_file :  The stations file should be a csv file with columns 'station_id', 'lat', 'lon'
        You can generate this file from a shapefile using the `dsm2ui datastore tostationfile` command

    centerlines_file : Path to the centerlines geojson file for dsm2 channel centerlines

    output_file : Path to the output file, the format will be a pandas dataframe with columns 'NAME', 'CHAN_NO', 'DISTANCE' and space separated

    distance_tolerance : default 100
    """
    from pydsm.viz import dsm2gis

    dsm2gis.create_stations_output_file(
        stations_file=stations_file,
        centerlines_file=centerlines_file,
        output_file=output_file,
        distance_tolerance=distance_tolerance,
    )


from pydsm.viz import dsm2gis
from dsm2ui.dssui import dssui
from dsm2ui.calib import geoheatmap
from dsm2ui.deltacdui import deltacdui
from dsm2ui.calib import calibplotui
from dsm2ui import dsm2ui
from dsm2ui.deltacdui import deltacduimgr
from dsm2ui.ptm import ptm_animator

main.add_command(dsm2ui.show_dsm2_output_ui, "output-ui")
main.add_command(dsm2ui.show_dsm2_tidefile_ui, "tide-ui")
main.add_command(dssui.show_dss_ui, "dss-ui")
main.add_command(map_channels_colored)
main.add_command(node_map_flow_splits)
main.add_command(exec_postpro_dsm2)
main.add_command(exec_dsm2_chan_mann_disp)
main.add_command(exec_checklist_dsm2)
main.add_command(geoheatmap.show_metrics_geo_heatmap, "geo-heatmap")
main.add_command(datastore_to_dss)
main.add_command(datastore_to_stationfile)
main.add_command(stations_output_file)
main.add_command(dsm2gis.geolocate_output_locations, "geolocate")
main.add_command(deltacdui.dcd_geomap, "dcd-map")
main.add_command(deltacdui.show_deltacd_nodes_ui, "dcd-nodes")
main.add_command(calibplotui.calib_plot_ui, "calib-ui")
main.add_command(ptm_animator.ptm_animate, "ptm-animate")
main.add_command(deltacduimgr.show_deltacd_ui, "dcd-ui")
main.add_command(dsm2ui.show_dsm2_tidefile_xsect_ui, "xsect-ui")
if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
