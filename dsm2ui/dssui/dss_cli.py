"""CLI command for the generic HEC-DSS file browser."""
from __future__ import annotations

import click


@click.command("dss")
@click.argument("dss_files", nargs=-1, required=False, metavar="DSS_FILE")
@click.option(
    "--geo-file",
    default=None,
    show_default=True,
    help="Path to a GeoJSON/CSV/shapefile with station geometry for map display.",
)
@click.option(
    "--geo-id-column",
    default="station_id",
    show_default=True,
    help="Column in the geo file that matches the DSS B-part station names.",
)
@click.option(
    "--port", default=5006, show_default=True, help="Port to serve the UI on."
)
@click.option(
    "--desktop",
    is_flag=True,
    default=False,
    help="Launch a standalone desktop window instead of a browser tab.",
)
def show_dss_ui(dss_files, geo_file, geo_id_column, port, desktop):
    """Generic HEC-DSS file browser.

    Opens one or more DSS files and presents their full path catalog for
    interactive time-series plotting.  You can also drop additional ``.dss``
    files onto the running UI window.

    Optionally attach station geometry for map display::

        dsm2ui ui dss data.dss --geo-file stations.geojson --geo-id-column STATION_ID
    """
    import panel as pn

    pn.extension()

    from dvue.session_persistence import serve_desktop_app, serve_session_app

    from dsm2ui.dssui.dss_registry import DSSRegistryUIManager

    def build_manager():
        mgr = DSSRegistryUIManager()
        if dss_files:
            mgr.add_source_files(list(dss_files))
        if geo_file:
            try:
                mgr.add_geo_source(geo_file, id_column=geo_id_column, station_column="B")
            except Exception as exc:
                import logging
                logging.getLogger(__name__).warning(
                    "show_dss_ui: could not load geo file %s: %s", geo_file, exc
                )
        return mgr

    _serve = serve_desktop_app if desktop else serve_session_app
    _serve(build_manager, title="DSS Browser", port=port)
