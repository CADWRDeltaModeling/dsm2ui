import pathlib
import yaml
import pandas as pd
import geopandas as gpd
import hvplot.pandas
import panel as pn

pn.extension()
import holoviews as hv
from holoviews import opts

import pyhecdss as dss
from pydsm.analysis import postpro
from dsm2ui.calib import postpro_dsm2

from dvue.dataui import DataUI, DataUIManager
from dvue.catalog import DataReferenceReader, DataReference, DataCatalog


# substitue the base_dir in location_files_dict, observed_files_dict, study_files_dict
def substitute_base_dir(base_dir, dict):
    for key in dict:
        dict[key] = str((pathlib.Path(base_dir) / dict[key]).resolve())
    return dict


def load_location_file(location_file):
    df = postpro.load_location_file(location_file)
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df.Longitude, df.Latitude), crs="EPSG:4326"
    )
    return gdf


def _get_cached_bparts(dssfile, vartype):
    """Return the set of B-parts that have valid (non-error) cache entries for *vartype*.

    Opens the diskcache written by ``postpro_dsm2.run_process`` alongside *dssfile*
    and returns the upper-cased B-parts whose ``/{BPART}/{VARTYPE}/15MIN/`` key
    contains actual data (i.e. the stored value is a DataFrame, not an error string).
    """
    import diskcache
    cache_dir = postpro.get_cache_dir(dssfile)
    if not pathlib.Path(cache_dir).exists():
        return set()
    cache = diskcache.Cache(cache_dir)
    bparts = set()
    cpart_upper = vartype.upper()
    try:
        for key in cache:
            # Key format: /{BPART}/{CPART}/{EPART}/
            parts = str(key).strip("/").split("/")
            if len(parts) != 3:
                continue
            key_bpart, key_cpart, key_epart = parts
            if key_cpart != cpart_upper:
                continue
            try:
                value, *_ = cache[key]
            except KeyError:
                continue
            # Filter out error strings stored by store_processed on failure
            if isinstance(value, str):
                continue
            if hasattr(value, "empty") and value.empty:
                continue
            bparts.add(key_bpart.upper())
    finally:
        cache.close()
    return bparts


import param


class CalibNullReader(DataReferenceReader):
    """Placeholder reader for CalibPlotUIManager entries.

    Calibration plots are built lazily inside :meth:`CalibPlotUIManager.create_panel`
    via ``postpro_dsm2.build_plot()``; ``getData()`` is never called on these refs.
    """

    def load(self, **attributes) -> pd.DataFrame:
        raise NotImplementedError(
            "CalibPlotUIManager entries are rendered via create_panel(), not getData()."
        )

    def __repr__(self) -> str:
        return "CalibNullReader()"


class CalibPlotUIManager(DataUIManager):

    def __init__(self, config_file, base_dir=None, polygon_bounds=None, **kwargs):
        """
        config_file: str
            yaml file containing configuration

        base_dir: str
            base directory for config file, if None is assumed to be same as config file directory
        """
        base_dir = kwargs.pop("base_dir", None)
        self.polygon_bounds = polygon_bounds
        super().__init__(**kwargs)
        self.config_file = config_file
        with open(self.config_file, "r", encoding="utf-8") as file:
            config = yaml.safe_load(file)
        # substitue the base_dir in location_files_dict, observed_files_dict, study_files_dict
        if base_dir is None:
            base_dir = pathlib.Path(self.config_file).parent
        config["location_files_dict"] = substitute_base_dir(
            base_dir, config["location_files_dict"]
        )
        config["observed_files_dict"] = substitute_base_dir(
            base_dir, config["observed_files_dict"]
        )
        config["study_files_dict"] = substitute_base_dir(
            base_dir, config["study_files_dict"]
        )
        self.config = config
        # Build catalog once — avoids re-reading location files on every get_data_catalog() call.
        self._dvue_catalog = self._build_dvue_catalog()

    def _build_raw_catalog(self) -> gpd.GeoDataFrame:
        """Build the merged GeoDataFrame from all configured location files.

        Rows are filtered to only those whose B-part has valid cached data in
        **both** the observed DSS cache and at least one model DSS cache.
        This removes duplicate CSV rows and stations that were never in the
        model output or have no observed data.
        """
        gdfs = []
        for tkey, tvalue in self.config["vartype_timewindow_dict"].items():
            if tvalue is None:
                continue
            value = self.config["location_files_dict"][tkey]
            gdf = postpro.load_location_file(value)
            gdf.Latitude = pd.to_numeric(gdf.Latitude, errors="coerce")
            gdf.Longitude = pd.to_numeric(gdf.Longitude, errors="coerce")
            gdf.threshold_value = pd.to_numeric(gdf.threshold_value, errors="coerce")
            gdf = gpd.GeoDataFrame(
                gdf,
                geometry=gpd.points_from_xy(gdf.Longitude, gdf.Latitude),
                crs="EPSG:4326",
            )
            gdf["vartype"] = str(tkey)

            # --- filter to stations with valid cached data ---
            obs_bparts = _get_cached_bparts(
                self.config["observed_files_dict"][tkey], tkey
            )
            model_bparts = set()
            for dssfile in self.config["study_files_dict"].values():
                model_bparts |= _get_cached_bparts(dssfile, tkey)

            if obs_bparts or model_bparts:
                # Keep rows whose BPart is in both observed AND at least one model cache.
                # Fall back to no filtering if caches are empty (e.g. pre-postpro run).
                valid_bparts = obs_bparts & model_bparts
                if valid_bparts:
                    gdf = gdf[gdf["BPart"].str.upper().isin(valid_bparts)]

            gdfs.append(gdf)
        gdf = pd.concat(gdfs, axis=0).reset_index(drop=True)
        gdf = gdf.astype(
            {
                "Name": "str",
                "BPart": "str",
                "Description": "str",
                "subtract": "str",
                "time_window_exclusion_list": "str",
                "vartype": "str",
            },
            errors="raise",
        )
        gdf = gdf.dropna(subset=["Latitude", "Longitude"])
        if self.polygon_bounds:
            gdf = gdf.loc[gdf.within(self.polygon_bounds)]
        return gdf

    def _build_dvue_catalog(self) -> DataCatalog:
        dfcat = self._build_raw_catalog()
        reader = CalibNullReader()
        catalog = DataCatalog(crs="EPSG:4326")
        for _, row in dfcat.iterrows():
            attrs = {k: v for k, v in row.items() if k != "geometry"}
            if row.get("geometry") is not None:
                attrs["geometry"] = row["geometry"]
            catalog.add(DataReference(
                reader,
                name=f'{row["Name"]}_{row["vartype"]}',
                cache=False,
                **attrs,
            ))
        return catalog

    @property
    def data_catalog(self) -> DataCatalog:
        return self._dvue_catalog

    def get_studies(self, varname):
        studies = list(self.config["study_files_dict"].keys())
        obs_study = postpro.Study(
            "Observed", self.config["observed_files_dict"][varname]
        )
        model_studies = [
            postpro.Study(name, self.config["study_files_dict"][name])
            for name in self.config["study_files_dict"]
        ]
        studies = [obs_study] + model_studies
        return studies

    def build_location(self, row):
        return postpro.Location(
            row["Name"],
            row["BPart"],
            row["Description"],
            row["time_window_exclusion_list"],
            row["threshold_value"],
        )

    def get_locations(self, df):
        locations = [self.build_location(r) for i, r in df.iterrows()]
        return locations

    def get_widgets(self):
        return pn.Column(pn.pane.Markdown("UI Controls Placeholder"))

    def get_table_column_width_map(self):
        """only columns to be displayed in the table should be included in the map"""
        column_width_map = {
            "Name": "20%",
            "BPart": "10%",
            "vartype": "5%",
            "Description": "25%",
            "subtract": "5%",
            "time_window_exclusion_list": "10%",
            "threshold_value": "5%",
            "Latitude": "5%",
            "Longitude": "5%",
        }
        return column_width_map

    def get_table_filters(self):
        table_filters = {
            "Name": {"type": "input", "func": "like", "placeholder": "Enter match"},
            "BPart": {"type": "input", "func": "like", "placeholder": "Enter match"},
            "vartype": {"type": "input", "func": "like", "placeholder": "Enter match"},
            "Description": {
                "type": "input",
                "func": "like",
                "placeholder": "Enter match",
            },
        }
        return table_filters

    def create_panel(self, df):
        plots = []
        for _, row in df.iterrows():
            varname = row["vartype"]
            vartype = postpro.VarType(varname, self.config["vartype_dict"][varname])
            studies = self.get_studies(varname)
            location = self.build_location(row)
            try:
                calib_plot_template_dict, metrics_df = postpro_dsm2.build_plot(
                    self.config, studies, location, vartype
                )
                if calib_plot_template_dict and ("with" in calib_plot_template_dict):
                    plots.append(
                        (
                            location.name + "@" + varname,
                            calib_plot_template_dict["with"],
                        )
                    )
                else:
                    raise ValueError("No plot found for location: " + location.name)
            except Exception as e:
                print(e)
                print("No plot found for location: " + location.name)
        return pn.Tabs(*plots, dynamic=True, closable=True)

    # methods below if geolocation data is available
    def get_tooltips(self):
        return [
            ("Name", "@Name"),
            ("BPart", "@BPart"),
            ("Description", "@Description"),
            ("vartype", "@vartype"),
        ]

    def get_map_color_columns(self):
        """return the columns that can be used to color the map"""
        return ["vartype"]

    def get_name_to_color(self):
        return {
            "STAGE": "green",
            "FLOW": "blue",
            "EC": "orange",
            "TEMP": "black",
        }

    def get_map_marker_columns(self):
        """return the columns that can be used to color the map"""
        return ["vartype"]

    def get_name_to_marker(self):
        return {
            "STAGE": "square",
            "FLOW": "circle",
            "EC": "diamond",
            "TEMP": "triangle",
        }

    def get_version(self):
        return "1.0.0-2/3/2025"

    def get_about_text(self):
        return """
        # Calibration Plot UI for DSM2

        This tool allows users to visualize and analyze calibration plots for the DSM2 model. 

        Users can load configuration files, filter data, and generate plots for various locations and variables. 

        The UI provides interactive controls and map-based visualizations to facilitate the calibration process.
        """


import click


@click.command()
@click.argument("config_file", type=click.Path(exists=True, readable=True))
@click.option("--base_dir", required=False, help="Base directory for config file")
def calib_plot_ui(config_file, base_dir=None, **kwargs):
    """Launch the interactive calibration plot UI from a YAML config file.

    config_file: str
        yaml file containing configuration

    base_dir: str
        base directory for config file, if None is assumed to be same as config file directory
    """
    from shapely.geometry import Point, Polygon

    california = Polygon(
        [
            (-124.848974, 42.009518),
            (-114.131211, 42.009518),
            (-114.131211, 32.534156),
            (-124.848974, 32.534156),
        ]
    )
    manager = CalibPlotUIManager(
        config_file, base_dir=base_dir, polygon_bounds=california, **kwargs
    )

    DataUI(manager).create_view(title="DSM2 Calib Plot UI").show()
