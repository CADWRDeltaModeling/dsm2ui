# %%
# organize imports by category
from datetime import datetime, timedelta
from functools import lru_cache
import logging
import warnings

warnings.filterwarnings("ignore")
#
import pandas as pd
import geopandas as gpd
import holoviews as hv
import cartopy.crs as ccrs

hv.extension("bokeh")
# viz and ui
import param
import panel as pn

pn.extension()
#
import pyhecdss as dss
from vtools.functions.filter import cosine_lanczos

from dvue.catalog import DataReferenceReader, DataReference, DataCatalog, build_catalog_from_dataframe
from dvue.dataui import DataUI, full_stack
from dvue.tsdataui import TimeSeriesDataUIManager, TimeSeriesPlotAction

logger = logging.getLogger(__name__)


def _smart_title(s: str) -> str:
    """Title-case *s* only when it is ALL-CAPS and longer than 2 characters.

    Keeps short abbreviations (EC, DO, CFS) unchanged while converting
    long ALL-CAPS DSS variable names (FLOW → Flow, STAGE → Stage).
    """
    if isinstance(s, str) and s.isupper() and len(s) > 2:
        return s.title()
    return s


class DSSTimeSeriesPlotAction(TimeSeriesPlotAction):
    """``TimeSeriesPlotAction`` with DSS-aware axis labels and smart legend.

    Produces clean, readable plots:
    - **X-axis**: always "Time"
    - **Y-axis**: title-cased variable + unit, e.g. "Flow (cfs)"
    - **Legend / title**: only the DSS path-parts that actually vary across
      the selected rows are shown, reducing noise when all selected curves
      are for the same station or the same study.
    """

    def render(self, df, refs_and_data, manager):
        """Pre-compute which DSS path parts vary, then delegate to super."""
        self._varying = {
            part: (df[part].nunique() > 1 if part in df.columns else False)
            for part in ("B", "C", "A", "F")
        }
        self._multi_file = (
            df[manager.filename_column].nunique() > 1
            if hasattr(manager, "filename_column") and manager.filename_column in df.columns
            else False
        )
        return super().render(df, refs_and_data, manager)

    def create_curve(self, data, row, unit, file_index=""):
        """Build a DSS curve with smart legend label, title-cased y-axis."""
        varying = getattr(self, "_varying", {"B": True, "C": False, "A": False, "F": False})
        multi_file = getattr(self, "_multi_file", bool(file_index))

        # Build legend label from only the parts that vary
        parts = []
        if varying.get("B") or not any(varying.values()):
            # Always show station when stations vary, or as fallback for a single curve
            parts.append(str(row.get("B", "")))
        if varying.get("C"):
            parts.append(_smart_title(str(row.get("C", ""))))
        if varying.get("F"):
            parts.append(str(row.get("F", "")))
        if multi_file and file_index:
            parts.append(f"[{file_index}]")

        label = " / ".join(p for p in parts if p) or str(row.get("B", "value"))

        # Y-axis: title-cased variable + unit
        c_part = _smart_title(str(row.get("C", "")))
        ylabel = f"{c_part} ({unit})" if unit else c_part

        # Normalize the data column to the semantic C-part name (e.g. "Stage")
        # so that all curves for the same variable share the same HoloViews vdim.
        # Without this, math refs return columns named after their ref name
        # ("godinx", "czx", …) causing hv.Overlay to receive curves with different
        # vdims and silently drop all but the first.
        vdim_name = c_part if c_part else "value"
        col_name = data.columns[0]
        data_for_curve = data.iloc[:, [0]].rename(columns={col_name: vdim_name})
        crv = hv.Curve(data_for_curve, label=label)
        return crv.opts(
            xlabel="Time",
            ylabel=ylabel,
            responsive=True,
            active_tools=["wheel_zoom"],
            tools=["hover"],
        )

    def append_to_title_map(self, title_map, group_key, row):
        """Accumulate sets of B, C, A, F values per unit group."""
        if group_key not in title_map:
            title_map[group_key] = {"B": set(), "C": set(), "A": set(), "F": set()}
        for part in ("B", "C", "A", "F"):
            val = row.get(part)
            if val is not None and str(val).strip():
                title_map[group_key][part].add(str(val))

    def create_title(self, v) -> str:
        """Format accumulated path-part sets into a readable panel title."""
        if not isinstance(v, dict):
            return str(v)
        b_str = ", ".join(sorted(v["B"]))
        c_str = ", ".join(_smart_title(c) for c in sorted(v["C"]))
        a_str = ", ".join(sorted(v["A"]))
        f_str = ", ".join(sorted(v["F"]))
        parts = []
        if b_str:
            parts.append(b_str)
        if c_str:
            parts.append(f"({c_str})")
        if a_str or f_str:
            parts.append(f"[{a_str}/{f_str}]")
        return " ".join(parts) if parts else str(v)


class DSSReader(DataReferenceReader):
    """Reads a single DSS time series given DSS path-part attributes.

    A single ``DSSReader`` instance is shared across all
    :class:`~dvue.catalog.DataReference` objects that point to the same
    set of open DSS file handles (flyweight pattern).

    When ``time_range`` is present in the attributes passed by
    :meth:`~dvue.catalog.DataReference.getData`, only that window is read
    from disk.  The result is cached per ``(start, end)`` pair inside the
    ``DataReference``, so repeated requests for the same window avoid
    re-reading.

    Parameters
    ----------
    dssfh_map : dict
        ``{filepath: pyhecdss.DSSFile}`` — open DSS file handles.
    dss_catalog_map : dict
        ``{filepath: DataFrame}`` — per-file catalog DataFrames that include
        a ``pathname`` column built from the A–F path parts.
    """

    def __init__(self, dssfh_map: dict, dss_catalog_map: dict) -> None:
        self._dssfh_map = dssfh_map
        self._dss_catalog_map = dss_catalog_map

    def load(self, **attributes) -> pd.DataFrame:
        dssfile = attributes["filename"]
        a = attributes["A"]
        b = attributes["B"]
        c = attributes["C"]
        e = attributes["E"]
        f = attributes["F"]
        pathname = f"/{a}/{b}/{c}//{e}/{f}/"

        dssfh = self._dssfh_map[dssfile]
        dfcatp = self._dss_catalog_map[dssfile]
        dfcatp_match = dfcatp[dfcatp["pathname"] == pathname]
        pathnames = dssfh.get_pathnames(dfcatp_match)
        if not pathnames:
            logger.warning("No DSS pathname found for %s", pathname)
            return pd.DataFrame()
        actual_pathname = pathnames[0]

        is_irregular = e.startswith("IR-")
        time_range = attributes.get("time_range")
        if time_range is not None:
            start_str = pd.Timestamp(time_range[0]).strftime("%Y-%m-%d")
            end_str = pd.Timestamp(time_range[1]).strftime("%Y-%m-%d")
        else:
            start_str = "1753-01-01"
            end_str = "2200-12-31"

        try:
            if is_irregular:
                df, unit, ptype = dssfh.read_its(actual_pathname, start_str, end_str)
            else:
                df, unit, ptype = dssfh.read_rts(actual_pathname, start_str, end_str)
            fvi = df.first_valid_index()
            lvi = df.last_valid_index()
            if fvi is not None and lvi is not None:
                df = df[fvi:lvi]
            df.attrs["unit"] = unit.lower() if unit else unit
            df.attrs["ptype"] = ptype
            return df
        except Exception as exc:
            logger.error("Error reading DSS pathname %s: %s", actual_pathname, exc)
            return pd.DataFrame()

    def __repr__(self) -> str:
        return f"DSSReader(files={list(self._dssfh_map.keys())!r})"


class DSSDataUIManager(TimeSeriesDataUIManager):

    def __init__(self, *dssfiles, **kwargs):
        """
        geolocations is a geodataframe with station_id, and geometry columns
        This is merged with the data catalog to get the station locations.
        """
        self.time_range = kwargs.pop("time_range", None)
        self.geo_locations = kwargs.pop("geo_locations", None)
        self.geo_id_column = kwargs.pop("geo_id_column", "station_id")
        self.station_id_column = kwargs.pop(
            "station_id_column", "B"
        )  # The column in the data catalog that contains the station id
        if len(dssfiles) == 0:
            raise ValueError("At least one DSS file is required")
        self.dssfiles = dssfiles
        dfcats = []
        dssfh = {}
        dsscats = {}
        for dssfile in dssfiles:
            dssfh[dssfile] = dss.DSSFile(dssfile)
            dfcat = dssfh[dssfile].read_catalog()
            dsscats[dssfile] = self._build_map_pathname_to_catalog(dfcat)
            dfcat = dfcat.drop(columns=["T"])
            dfcat["filename"] = dssfile
            dfcats.append(dfcat)
        self.dssfh = dssfh
        self.dsscats = dsscats
        self.dfcat = pd.concat(dfcats)
        self.dfcat = self.dfcat.drop_duplicates().reset_index(drop=True)
        # add in the geo locations
        if self.geo_locations is not None:
            # DSS names are always in upper case
            self.geo_locations[self.geo_id_column] = (
                self.geo_locations[self.geo_id_column].astype(str).str.upper()
            )
            self.dfcat = pd.merge(
                self.geo_locations,
                self.dfcat,
                left_on=self.geo_id_column,
                right_on=self.station_id_column,
            )
        self.dssfiles = dssfiles
        self.dfcatpath = self._build_map_pathname_to_catalog(self.dfcat)

        # Build DataCatalog backed by DSSReader references
        self._reader = DSSReader(self.dssfh, self.dsscats)
        geo_crs = (
            str(self.geo_locations.crs)
            if self.geo_locations is not None and hasattr(self.geo_locations, "crs")
            else None
        )
        self._dvue_catalog = self._build_dvue_catalog(geo_crs)

        super().__init__(**kwargs)
        self.color_cycle_column = "B"
        self.dashed_line_cycle_column = "filename"
        self.marker_cycle_column = "F"

    def build_ref_key(self, row):
        """Unique catalog key: filename + pathname so the same DSS path in
        different files gets its own DataReference."""
        return f'{row["filename"]}::{self.build_pathname(row)}'

    def _build_dvue_catalog(self, crs=None) -> DataCatalog:
        return build_catalog_from_dataframe(self.dfcat, self._reader, self.build_ref_key, crs)

    @property
    def data_catalog(self) -> DataCatalog:
        return self._dvue_catalog

    def get_data_reference(self, row):
        """Look up DataReference by filename + pathname key.

        For math references (which have no DSS filename), the catalog key is
        the reference ``name`` directly rather than the ``filename::pathname``
        composite used for raw DSS entries.
        """
        filename = row.get("filename", None)
        if pd.isna(filename):
            return self._dvue_catalog.get(row["name"])
        return self._dvue_catalog.get(self.build_ref_key(row))

    def __del__(self):
        if hasattr(self, "dssfiles"):
            for dssfile in self.dssfiles:
                self.dssfh[dssfile].close()

    def build_pathname(self, r):
        return f'/{r["A"]}/{r["B"]}/{r["C"]}//{r["E"]}/{r["F"]}/'

    def build_station_name(self, r):
        pathname = self.build_pathname(r)
        if "FILE_NUM" not in r:
            return f"{pathname}"
        else:
            return f'{r["FILE_NUM"]}:{pathname}'

    def _build_map_pathname_to_catalog(self, dfcat):
        dfcatpath = dfcat.copy()
        dfcatpath["pathname"] = dfcatpath.apply(self.build_pathname, axis=1)
        return dfcatpath

    def get_time_range(self, dfcat):
        """
        Calculate time range from the data catalog
        """
        if self.time_range is None:  # guess from catalog of DSS files
            dftw = dfcat.D.str.split("-", expand=True)
            dftw.columns = ["Tmin", "Tmax"]
            dftw["Tmin"] = pd.to_datetime(dftw["Tmin"])
            dftw["Tmax"] = pd.to_datetime(dftw["Tmax"])
            tmin = dftw["Tmin"].min()
            tmax = dftw["Tmax"].max()
            self.time_range = (tmin, tmax)
        return self.time_range

    def _get_table_column_width_map(self):
        """only columns to be displayed in the table should be included in the map"""
        column_width_map = {
            "A": "15%",
            "B": "15%",
            "C": "15%",
            "E": "10%",
            "F": "15%",
            "D": "20%",
        }
        return column_width_map

    def get_table_filters(self):
        table_filters = {
            "A": {"type": "input", "func": "like", "placeholder": "Enter match"},
            "B": {"type": "input", "func": "like", "placeholder": "Enter match"},
            "C": {"type": "input", "func": "like", "placeholder": "Enter match"},
            "E": {"type": "input", "func": "like", "placeholder": "Enter match"},
            "F": {"type": "input", "func": "like", "placeholder": "Enter match"},
        }
        return table_filters

    def _make_plot_action(self):
        """Return a :class:`DSSTimeSeriesPlotAction` for DSS-aware labels."""
        return DSSTimeSeriesPlotAction()

    def _append_value(self, new_value, value):
        if new_value not in value:
            value += f'{", " if value else ""}{new_value}'
        return value

    # NOTE: the three methods below are NOT called by the framework — the
    # framework calls DSSTimeSeriesPlotAction methods instead (wired via
    # _make_plot_action above).  They are kept here only to avoid breaking
    # any existing subclasses that may override them directly on the manager.

    def append_to_title_map(self, title_map, unit, r):  # dead — override DSSTimeSeriesPlotAction
        if unit in title_map:
            value = title_map[unit]
        else:
            value = ["", "", "", ""]
        value[0] = self._append_value(r["C"], value[0])
        value[1] = self._append_value(r["B"], value[1])
        value[2] = self._append_value(r["A"], value[2])
        value[3] = self._append_value(r["F"], value[3])
        title_map[unit] = value

    def create_title(self, v):  # dead — override DSSTimeSeriesPlotAction
        title = f"{v[1]} @ {v[2]} ({v[3]}::{v[0]})"
        return title

    def is_irregular(self, r):
        return r["E"].startswith("IR-")

    def create_curve(self, df, r, unit, file_index=None):  # dead — override DSSTimeSeriesPlotAction
        file_index_label = f"{file_index}:" if file_index is not None else ""
        crvlabel = f'{file_index_label}{r["B"]}/{r["C"]}'
        ylabel = f'{r["C"]} ({unit})'
        title = f'{r["C"]} @ {r["B"]} ({r["A"]}/{r["F"]})'
        crv = hv.Curve(df.iloc[:, [0]], label=crvlabel).redim(value=crvlabel)
        return crv.opts(
            xlabel="Time",
            ylabel=ylabel,
            title=title,
            responsive=True,
            active_tools=["wheel_zoom"],
            tools=["hover"],
        )  # dead — override DSSTimeSeriesPlotAction.create_curve instead  # dead — override DSSTimeSeriesPlotAction.create_curve instead

    def get_data_for_time_range(self, r, time_range):
        pathname = self.build_pathname(r)
        ref = self._dvue_catalog.get(pathname)
        df = ref.getData(time_range=time_range)
        unit = df.attrs.get("unit", "")
        ptype = df.attrs.get("ptype", "inst-val")
        return df, unit, ptype

    # methods below if geolocation data is available
    def get_tooltips(self):
        return [
            ("station_id", "@station_id"),
            ("A", "@A"),
            ("B", "@B"),
            ("C", "@C"),
            ("E", "@E"),
            ("F", "@F"),
        ]

    def get_map_color_columns(self):
        """return the columns that can be used to color the map"""
        return ["C", "A", "F"]

    def get_map_marker_columns(self):
        """return the columns that can be used to color the map"""
        return ["C", "A", "F"]



import glob
import click


@click.command()
@click.argument("dssfiles", nargs=-1)
@click.option(
    "--location-file",
    default=None,
    help="Location file as geojson containing station locations as lat and lon columns",
)
@click.option(
    "--location-id-column",
    default="station_id",
    help="Station ID column in location file",
)
@click.option(
    "--station-id-column",
    default="B",
    help="Station ID column in data catalog, e.g. B part for DSS file pathname",
)
@click.option(
    "--clear-cache",
    is_flag=True,
    default=False,
    help="Invalidate the in-memory data cache before launching the UI.",
)
def show_dss_ui(
    dssfiles, location_file=None, location_id_column="station_id", station_id_column="B",
    clear_cache=False,
):
    """
    Show DSS UI for the given DSS files

    dssfiles : list of DSS files
    location_file : Location file as geojson containing station locations as lat and lon columns
    location_id_column : Station ID column in location file
    station_id_column : Station ID column in data catalog, e.g. B part for DSS file pathname
    """
    geodf = None
    crs_cartopy = None
    # TODO: Add support for other location file formats and move to a utility module
    if location_file is not None:
        if location_file.endswith(".shp") or location_file.endswith(".geojson"):
            geodf = gpd.read_file(location_file)
            # Extract EPSG code
            epsg_code = geodf.crs.to_epsg()
            # Create Cartopy CRS from EPSG
            crs_cartopy = ccrs.epsg(epsg_code)
        elif location_file.endswith(".csv"):
            df = pd.read_csv(location_file)
            if all(column in df.columns for column in ["lat", "lon"]):
                geodf = gpd.GeoDataFrame(
                    df, geometry=gpd.points_from_xy(df.lon, df.lat, crs="EPSG:4326")
                )
                crs_cartopy = ccrs.PlateCarree()
            elif all(
                column in df.columns for column in ["utm_easting", "utm_northing"]
            ) or all(column in df.columns for column in ["utm_x", "utm_y"]):
                geodf = gpd.GeoDataFrame(
                    df,
                    geometry=gpd.points_from_xy(df.utm_easting, df.utm_northing),
                    crs="EPSG:26910",
                )
                crs_cartopy = ccrs.UTM(10)
            else:
                raise ValueError(
                    "Location file should be a geojson file or should have lat and lon or utm_easting and utm_northing columns"
                )
        if not (location_id_column in geodf.columns):
            raise ValueError(
                f"Station ID column {location_id_column} not found in location file"
            )

    dssuimgr = DSSDataUIManager(
        *dssfiles,
        geo_locations=geodf,
        geo_id_column=location_id_column,
        station_id_column=station_id_column,
        filename_column="filename",
    )
    if clear_cache:
        dssuimgr.data_catalog.invalidate_all_caches()
    ui = DataUI(dssuimgr, crs=crs_cartopy)
    ui.create_view(title="DSS Data UI").show()
