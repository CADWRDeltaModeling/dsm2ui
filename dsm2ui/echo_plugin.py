"""DSM2 echo-file plugin for the dvue RegistryUIManager.

Registers two readers with :class:`~dvue.registry.ReaderRegistry`:

* ``DSM2EchoFileReader`` — scans a DSM2 echo ``.inp`` file and produces
  :class:`~dvue.catalog.DataReference` objects for every input boundary
  condition row and every output channel row.
* ``DSM2BCFlowLoader`` — loads a single input boundary-condition time series
  from a HEC-DSS file given ``FILE``, ``PATH``, and ``SIGN`` attributes.

Output channel refs reuse ``ref_type="dsm2_dss"`` so the existing
:class:`~dsm2ui.dsm2ui.DSM2DSSReader` handles their loading.

Example::

    from dsm2ui.echo_plugin import EchoUIManager
    mgr = EchoUIManager()
    mgr.add_source_files(["path/to/my_run_hydro_echo.inp"])
"""
from __future__ import annotations

import logging
import os
from typing import List

import click
import pandas as pd

from dvue.catalog import DataReference
from dvue.registry import ReaderRegistry
from dvue.registry_ui import RegistryUIManager

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Tables that carry DSS FILE+PATH references in a DSM2 echo file.
# Maps table name → station-name column (None = composite key for INPUT_GATE).
# ---------------------------------------------------------------------------
_INPUT_TS_TABLES: dict = {
    "BOUNDARY_FLOW": "NAME",
    "BOUNDARY_STAGE": "NAME",
    "SOURCE_FLOW": "NAME",
    "SOURCE_FLOW_RESERVOIR": "NAME",
    "INPUT_GATE": None,
    "INPUT_TRANSFER_FLOW": "TRANSFER_NAME",
    "OPRULE_TIME_SERIES": "NAME",
}


def _resolve_envvars(value: str, envvars: dict) -> str:
    """Substitute ``${VAR}`` tokens in *value* using *envvars*."""
    for k, v in envvars.items():
        value = value.replace(f"${{{k}}}", str(v))
    return value


def _resolve_path(raw: str, *search_dirs: str) -> str:
    """Return the first existing resolution of *raw* in *search_dirs*, else *raw*."""
    if os.path.isabs(raw) and os.path.isfile(raw):
        return raw
    for d in search_dirs:
        candidate = os.path.join(d, raw)
        if os.path.isfile(candidate):
            return candidate
    return raw


# ---------------------------------------------------------------------------
# Ref-type subclasses (lightweight — only override ``ref_type``)
# ---------------------------------------------------------------------------

class _BCFlowRef(DataReference):
    """DataReference for DSM2 input boundary condition time series."""
    ref_type: str = "dsm2_bc_flow"


class _OutputChannelRef(DataReference):
    """DataReference for DSM2 output channel time series (loaded by DSM2DSSReader)."""
    ref_type: str = "dsm2_dss"


# ---------------------------------------------------------------------------
# Scanner: parses one echo .inp file → refs for both input and output tables
# ---------------------------------------------------------------------------

class DSM2EchoFileReader:
    """Scan a DSM2 echo ``.inp`` file and produce :class:`DataReference` objects.

    Registered for the ``.inp`` extension.  Returns mixed ref types:

    * ``ref_type="dsm2_bc_flow"`` for every row in input boundary tables
      (``BOUNDARY_FLOW``, ``BOUNDARY_STAGE``, etc.)
    * ``ref_type="dsm2_dss"`` for every row in ``OUTPUT_CHANNEL``

    The loader dispatch is handled by :class:`ReaderRegistry` — input refs use
    :class:`DSM2BCFlowLoader` and output refs use the existing DSM2DSSReader.
    """

    def __init__(self, source: str) -> None:
        self._source = source

    @classmethod
    def scan(cls, path: str) -> List[DataReference]:
        """Parse *path* as a DSM2 echo ``.inp`` file and return all refs."""
        try:
            from pydsm.input.parser import read_input as parse_input
        except ImportError:
            logger.warning(
                "DSM2EchoFileReader.scan: pydsm not available — cannot parse %s", path
            )
            return []

        try:
            tables = parse_input(path)
        except Exception as exc:
            logger.warning("DSM2EchoFileReader.scan: failed to parse %s: %s", path, exc)
            return []

        echo_dir = os.path.dirname(os.path.abspath(path))
        # Echo files typically live inside an output/ or run/ subdirectory.
        # Resolve relative DSS paths against both the echo dir and its parent.
        study_dir = os.path.dirname(echo_dir)

        # Collect ENVVARS for ${VAR} substitution.
        envvars: dict = {}
        if "ENVVARS" in tables:
            ev = tables["ENVVARS"]
            if "NAME" in ev.columns and "VALUE" in ev.columns:
                for _, row in ev.iterrows():
                    envvars[str(row["NAME"])] = str(row["VALUE"])

        refs: List[DataReference] = []

        # --- Input boundary condition refs ---
        for table_name, name_col in _INPUT_TS_TABLES.items():
            if table_name not in tables:
                continue
            tbl = tables[table_name]
            if "FILE" not in tbl.columns or "PATH" not in tbl.columns:
                continue

            for _, row in tbl.iterrows():
                raw_file = str(row.get("FILE", ""))
                raw_dss_path = str(row.get("PATH", ""))
                if not raw_file or raw_file in ("nan", ""):
                    continue
                if not raw_dss_path or raw_dss_path in ("nan", ""):
                    continue

                dss_file = _resolve_path(
                    _resolve_envvars(raw_file, envvars), echo_dir, study_dir
                )
                dss_path = _resolve_envvars(raw_dss_path, envvars)

                # Station name
                if name_col and name_col in tbl.columns:
                    station = str(row[name_col])
                elif "TRANSFER_NAME" in tbl.columns:
                    station = str(row["TRANSFER_NAME"])
                else:
                    # INPUT_GATE composite key
                    station = "__".join(
                        str(row.get(c, ""))
                        for c in ("GATE_NAME", "DEVICE")
                        if c in tbl.columns
                    )

                # Infer variable from DSS C-part
                try:
                    cpart = dss_path.strip("/").split("/")[2]
                except (IndexError, AttributeError):
                    cpart = ""

                sign = float(row["SIGN"]) if "SIGN" in tbl.columns else 1.0
                fillin = str(row["FILLIN"]) if "FILLIN" in tbl.columns else ""

                ref = _BCFlowRef(
                    source=path,  # echo file → registry key for DSM2BCFlowLoader
                    cache=True,
                    TABLE=table_name,
                    category="Input",
                    station=station,
                    variable=cpart.lower() if cpart else "flow",
                    SIGN=sign,
                    FILLIN=fillin,
                    FILE=dss_file,
                    PATH=dss_path,
                )
                refs.append(ref)

        # --- Output channel refs ---
        if "OUTPUT_CHANNEL" in tables:
            oc = tables["OUTPUT_CHANNEL"]
            if "FILE" in oc.columns:
                for _, row in oc.iterrows():
                    raw_file = str(row.get("FILE", ""))
                    if not raw_file or raw_file in ("nan", ""):
                        continue

                    dss_file = _resolve_path(
                        _resolve_envvars(raw_file, envvars), echo_dir, study_dir
                    )
                    name = str(row.get("NAME", ""))
                    variable = str(row.get("VARIABLE", "FLOW"))
                    try:
                        chan_no = int(row.get("CHAN_NO", 0))
                    except (ValueError, TypeError):
                        chan_no = 0

                    ref = _OutputChannelRef(
                        source=dss_file,  # DSS file → DSM2DSSReader cached per file
                        cache=True,
                        TABLE="OUTPUT_CHANNEL",
                        category="Output",
                        station=name.lower(),
                        variable=variable.lower(),
                        chan_no=chan_no,
                        NAME=name,
                        VARIABLE=variable,
                        CHAN_NO=chan_no,
                        FILE=dss_file,
                    )
                    refs.append(ref)

        logger.info(
            "DSM2EchoFileReader.scan: %s → %d input refs, %d output refs",
            os.path.basename(path),
            sum(1 for r in refs if r.ref_type == "dsm2_bc_flow"),
            sum(1 for r in refs if r.ref_type == "dsm2_dss"),
        )
        return refs

    def __repr__(self) -> str:
        return f"DSM2EchoFileReader(source={self._source!r})"


# ---------------------------------------------------------------------------
# Loader: input boundary-condition time series (FILE + PATH + SIGN)
# ---------------------------------------------------------------------------

class DSM2BCFlowLoader:
    """Load a DSM2 input boundary-condition time series from a HEC-DSS file.

    Expects ref attributes: ``FILE``, ``PATH``, ``SIGN`` (default 1.0),
    and optionally ``time_range``.
    """

    def __init__(self, source: str) -> None:
        self._source = source

    def load(self, **attributes) -> pd.DataFrame:
        try:
            import pyhecdss as dss
        except ImportError:
            logger.warning("DSM2BCFlowLoader: pyhecdss not available")
            return pd.DataFrame()

        dssfile = attributes.get("FILE", "")
        dss_path = attributes.get("PATH", "")
        sign = float(attributes.get("SIGN", 1.0))
        time_range = attributes.get("time_range")

        if not dssfile or not dss_path:
            logger.warning(
                "DSM2BCFlowLoader: missing FILE or PATH in attributes: %s", attributes
            )
            return pd.DataFrame()

        try:
            result = next(dss.get_ts(dssfile, dss_path), None)
            if result is None:
                logger.warning(
                    "DSM2BCFlowLoader: no data for path %s in %s", dss_path, dssfile
                )
                return pd.DataFrame()
            ts, unit, ptype = result
            if isinstance(ts, pd.DataFrame):
                ts = ts.iloc[:, 0]
            if isinstance(ts.index, pd.PeriodIndex):
                ts.index = ts.index.to_timestamp()
            ts = sign * ts
            if time_range is not None and len(time_range) == 2:
                start = pd.Timestamp(time_range[0])
                end = pd.Timestamp(time_range[1])
                ts = ts.loc[start:end]
            df = ts.to_frame(name="value")
            df.attrs["unit"] = unit.lower() if isinstance(unit, str) else ""
            df.attrs["ptype"] = ptype if ptype else "inst-val"
            return df
        except Exception as exc:
            logger.warning(
                "DSM2BCFlowLoader: error loading %s from %s: %s", dss_path, dssfile, exc
            )
            return pd.DataFrame()

    def __repr__(self) -> str:
        return f"DSM2BCFlowLoader(source={self._source!r})"


# ---------------------------------------------------------------------------
# Register readers with the global registry
# ---------------------------------------------------------------------------
ReaderRegistry.register("dsm2_echo_inp", DSM2EchoFileReader, extensions=[".inp"])
ReaderRegistry.register("dsm2_bc_flow", DSM2BCFlowLoader)

# Register DSM2DSSReader for ref_type="dsm2_dss" so _OutputChannelRef can load data.
# DSM2DSSReader lives in dssui to avoid circular imports (dssui does not import echo_plugin).
try:
    from dsm2ui.dssui.dss_registry import DSM2DSSReader as _DSM2DSSReader
    ReaderRegistry.register("dsm2_dss", _DSM2DSSReader, extensions=[".dss"])
except Exception as _e:  # pragma: no cover
    logger.warning("echo_plugin: could not register dsm2_dss reader: %s", _e)


# ---------------------------------------------------------------------------
# EchoUIManager
# ---------------------------------------------------------------------------
import cartopy.crs as ccrs  # noqa: E402  (must be after registry setup)

# Bundled GeoJSON files shipped with dsm2ui
_PKG_DIR = os.path.dirname(__file__)
_DEFAULT_CHANNEL_GEO = os.path.join(
    _PKG_DIR, "dsm2gis", "dsm2_channels_centerlines_8_2.geojson"
)
_DEFAULT_NODE_GEO = os.path.join(
    _PKG_DIR, "dsm2gis", "dsm2_nodes_8_2.geojson"
)

# Cached midpoint GeoDataFrame (computed once, reused across sessions)
_CHANNEL_MIDPOINTS_CACHE: dict = {}


def _load_channel_midpoints(geo_path: str):
    """Load a channel centerline GeoJSON and return a midpoint GeoDataFrame.

    The interpolated midpoint (normalised distance=0.5) of each linestring is
    used instead of the full geometry so that:
    - Output channel refs appear as point markers on the map (correct semantics
      — a DSM2 OUTPUT_CHANNEL observation is at a point, not a line).
    - Map rendering is much faster (Points vs Path).

    The result is cached in-process so the file is only read once.
    """
    import geopandas as gpd
    if geo_path in _CHANNEL_MIDPOINTS_CACHE:
        return _CHANNEL_MIDPOINTS_CACHE[geo_path]
    gdf = gpd.read_file(geo_path)
    gdf = gdf[gdf.geometry.notna()].copy()
    gdf["geometry"] = gdf.geometry.interpolate(0.5, normalized=True)
    _CHANNEL_MIDPOINTS_CACHE[geo_path] = gdf
    return gdf


class EchoUIManager(RegistryUIManager):
    """RegistryUIManager for DSM2 echo ``.inp`` files.

    Presents a unified catalog of input boundary conditions and output
    channel references from one or more DSM2 echo files.  Drops any ``.inp``
    file to populate the catalog.

    Auto-loads the bundled DSM2 channel centre-line GeoJSON for map display.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Override default primary key so input and output refs can share
        # the same station/variable without colliding, and so that refs from
        # different echo files (multiple studies) are distinguished by source_num.
        from dvue.catalog import DataCatalog
        self._dvue_catalog = DataCatalog(
            primary_key=["source_num", "category", "TABLE", "station", "variable"]
        )
        self._display_dfcat = pd.DataFrame(
            columns=["name", "source_num", "category", "TABLE", "station", "variable", "source"]
        )
        self.crs = ccrs.epsg("26910")
        self.station_id_column = "station"
        self._geo_loaded = False

    def normalize_ref(self, ref: DataReference) -> None:
        """Normalise ref attributes; call base then set echo-specific defaults."""
        super().normalize_ref(ref)
        attrs = ref._attributes
        if not attrs.get("category"):
            ref.set_attribute("category", "Unknown")
        if not attrs.get("TABLE"):
            ref.set_attribute("TABLE", "")
        if "SIGN" not in attrs:
            ref.set_attribute("SIGN", 1.0)
        if "FILLIN" not in attrs:
            ref.set_attribute("FILLIN", "")

    def on_file_added(self, path: str, refs: List[DataReference]) -> None:
        """Auto-load bundled channel geometry once on the first file add."""
        if self._geo_loaded:
            return
        for geo_path in (_DEFAULT_CHANNEL_GEO, _DEFAULT_NODE_GEO):
            if os.path.isfile(geo_path):
                try:
                    midpoints = _load_channel_midpoints(geo_path)
                    # Bypass add_geo_source (file-path-only API) and inject the
                    # pre-computed midpoint GeoDataFrame directly.
                    self._geo_source_df = midpoints
                    self._geo_id_column = "id"
                    self._geo_station_column = "chan_no"
                    self._apply_geo_merge()
                    self._geo_loaded = True
                except Exception as exc:
                    logger.warning(
                        "EchoUIManager: could not load geo %s: %s", geo_path, exc
                    )
                break

    def _get_table_column_width_map(self) -> dict:
        return {
            "source_num": "6%",
            "category": "8%",
            "TABLE": "18%",
            "station": "16%",
            "variable": "10%",
            "SIGN": "5%",
            "FILLIN": "8%",
        }


# ---------------------------------------------------------------------------
# CLI command
# ---------------------------------------------------------------------------

@click.command("echo")
@click.argument("inp_files", nargs=-1, required=False, metavar="ECHO_INP")
@click.option(
    "--channel-file",
    default=None,
    show_default=True,
    help="Override bundled channel GeoJSON with a custom file.",
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
def show_dsm2_echo_ui(inp_files, channel_file, port, desktop):
    """Interactive input+output viewer from DSM2 echo .inp file(s).

    Accepts one or more DSM2 echo files.  You can also drop additional
    files onto the running UI window without restarting.

    Example::

        dsm2ui ui echo path/to/output/run_hydro_echo.inp --desktop
    """
    import panel as pn

    pn.extension()

    from dvue.session_persistence import serve_desktop_app, serve_session_app

    channel_geo = channel_file or (
        _DEFAULT_CHANNEL_GEO if os.path.isfile(_DEFAULT_CHANNEL_GEO) else None
    )

    def build_manager():
        mgr = EchoUIManager()
        if inp_files:
            mgr.add_source_files(*inp_files)
        # Allow explicit --channel-file override even when no inp_files given.
        if channel_geo and not mgr._geo_loaded:
            try:
                midpoints = _load_channel_midpoints(channel_geo)
                mgr._geo_source_df = midpoints
                mgr._geo_id_column = "id"
                mgr._geo_station_column = "chan_no"
                mgr._apply_geo_merge()
                mgr._geo_loaded = True
            except Exception as exc:
                logger.warning(
                    "show_dsm2_echo_ui: could not load geo %s: %s", channel_geo, exc
                )
        return mgr

    _serve = serve_desktop_app if desktop else serve_session_app
    _serve(build_manager, title="DSM2 Echo UI", port=port)
