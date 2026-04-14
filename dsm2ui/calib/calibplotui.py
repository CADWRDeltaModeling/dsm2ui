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
        if dict[key] is not None:
            dict[key] = str((pathlib.Path(base_dir) / dict[key]).resolve())
    return dict


def load_location_file(location_file):
    df = postpro.load_location_file(location_file)
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df.Longitude, df.Latitude), crs="EPSG:4326"
    )
    return gdf


# ---------------------------------------------------------------------------
# Built-in option defaults — applied before YAML values.
# ---------------------------------------------------------------------------
_DEFAULT_OPTIONS = {
    "output_folder": "./plots/",
    "include_kde_plots": True,
    "zoom_inst_plot": True,
    "write_graphics": True,
    "write_html": True,
    "mask_plot_metric_data": True,
    "tech_memo_validation_metrics": False,
    "manuscript_layout": False,
}


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


def _clear_all_caches(config):
    """Clear all diskcache post-processing caches for every DSS file referenced in *config*.

    Iterates ``observed_files_dict`` and ``study_files_dict`` values and calls
    ``postpro.PostProCache.clear()`` on each.  Returns a human-readable summary string.
    """
    dss_files = list(config.get("observed_files_dict", {}).values()) + list(
        config.get("study_files_dict", {}).values()
    )
    cleared = 0
    skipped = 0
    for f in dss_files:
        cache_dir = postpro.get_cache_dir(f)
        if pathlib.Path(cache_dir).exists():
            postpro.PostProCache(f).clear()
            cleared += 1
        else:
            skipped += 1
    parts = []
    if cleared:
        parts.append(f"{cleared} cache(s) cleared")
    if skipped:
        parts.append(f"{skipped} cache(s) had no data to clear")
    return ", ".join(parts) if parts else "No caches found"


def get_default_location_files():
    """Return paths to the bundled default station CSVs packaged with dsm2ui.

    These are snapshots of the canonical
    ``postprocessing/location_info/calibration_*_stations.csv`` files and can
    be overridden by the YAML config or CLI flags.
    """
    import importlib.resources
    data_dir = importlib.resources.files("dsm2ui.calib") / "data"
    return {
        "EC": str(data_dir / "calibration_ec_stations.csv"),
        "FLOW": str(data_dir / "calibration_flow_stations.csv"),
        "STAGE": str(data_dir / "calibration_stage_stations.csv"),
    }


def _resolve_config(raw_config, base_dir, cli_overrides=None):
    """Apply layered config resolution: built-in defaults → YAML → CLI overrides.

    *   ``options_dict``: ``_DEFAULT_OPTIONS`` is the base; YAML values override.
    *   ``location_files_dict``: bundled CSVs are the base; YAML non-null values
        override per-key; paths are resolved relative to *base_dir*.
    *   ``observed_files_dict`` / ``study_files_dict``: paths resolved relative
        to *base_dir* (null entries left as-is).
    *   *cli_overrides* (highest priority): ``{"vartypes": [...], "options": {...}}``
    """
    import copy
    config = copy.deepcopy(raw_config)

    # --- options_dict: defaults ← YAML ---
    merged_opts = dict(_DEFAULT_OPTIONS)
    merged_opts.update(config.get("options_dict") or {})
    config["options_dict"] = merged_opts

    # --- location_files_dict: bundled defaults ← YAML non-null values ---
    bundled = get_default_location_files()
    loc_files = dict(bundled)  # start from bundled
    yaml_loc = config.get("location_files_dict") or {}
    for vt, yaml_val in yaml_loc.items():
        if yaml_val:  # non-null YAML value overrides bundled
            loc_files[vt] = str((pathlib.Path(base_dir) / yaml_val).resolve())
        # null YAML value keeps bundled default
    # preserve any extra YAML keys not in bundled (e.g. TEMP)
    for vt, yaml_val in yaml_loc.items():
        if vt not in loc_files and yaml_val:
            loc_files[vt] = str((pathlib.Path(base_dir) / yaml_val).resolve())
    config["location_files_dict"] = loc_files

    # --- observed_files_dict: resolve relative paths ---
    for vt, val in (config.get("observed_files_dict") or {}).items():
        if val:
            config["observed_files_dict"][vt] = str(
                (pathlib.Path(base_dir) / val).resolve()
            )

    # --- study_files_dict: resolve relative paths ---
    for key, val in (config.get("study_files_dict") or {}).items():
        if val:
            config["study_files_dict"][key] = str(
                (pathlib.Path(base_dir) / val).resolve()
            )

    # --- CLI overrides (highest priority) ---
    if cli_overrides:
        if "vartypes" in cli_overrides:
            active = {v.upper() for v in cli_overrides["vartypes"]}
            vtw = config.get("vartype_timewindow_dict", {})
            for vt in list(vtw):
                if vt.upper() not in active:
                    vtw[vt] = None
            config["vartype_timewindow_dict"] = vtw
        if "options" in cli_overrides:
            config["options_dict"].update(cli_overrides["options"])

    return config


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

    cache_status = param.String(default="", doc="Status message from last cache-clear operation")

    def __init__(self, config_file, base_dir=None, polygon_bounds=None, cli_overrides=None, **kwargs):
        """
        config_file: str
            yaml file containing configuration

        base_dir: str
            base directory for config file, if None is assumed to be same as config file directory

        cli_overrides: dict, optional
            Overrides applied on top of the YAML config (highest priority). Supported keys:
            - ``"vartypes"``: list of active vartype names (others will be nulled)
            - ``"options"``: dict of options_dict key→value overrides
        """
        base_dir = kwargs.pop("base_dir", None)
        self.polygon_bounds = polygon_bounds
        super().__init__(**kwargs)
        self.config_file = config_file
        with open(self.config_file, "r", encoding="utf-8") as file:
            raw_config = yaml.safe_load(file)
        if base_dir is None:
            base_dir = pathlib.Path(self.config_file).parent
        # Store original vartype_timewindow (from YAML, before CLI overrides) for UI restore.
        self._vartype_timewindow_original = {
            k: v
            for k, v in raw_config.get("vartype_timewindow_dict", {}).items()
            if v is not None
        }
        self._base_dir = base_dir
        self.config = _resolve_config(raw_config, base_dir, cli_overrides=cli_overrides)
        # Build catalog once — avoids re-reading location files on every get_data_catalog() call.
        self._dvue_catalog = self._build_dvue_catalog()

    def _build_raw_catalog(self) -> gpd.GeoDataFrame:
        """Build the merged GeoDataFrame from all active location files.

        Shows every station from the configured location CSVs without any
        cache-based filtering.  On-demand post-processing is triggered in
        :meth:`create_panel` for stations that have not been pre-processed yet.
        """
        gdfs = []
        for tkey, tvalue in self.config["vartype_timewindow_dict"].items():
            if tvalue is None:
                continue
            location_file = self.config["location_files_dict"].get(tkey)
            if not location_file or not pathlib.Path(location_file).exists():
                continue
            gdf = postpro.load_location_file(location_file)
            gdf.Latitude = pd.to_numeric(gdf.Latitude, errors="coerce")
            gdf.Longitude = pd.to_numeric(gdf.Longitude, errors="coerce")
            gdf = gpd.GeoDataFrame(
                gdf,
                geometry=gpd.points_from_xy(gdf.Longitude, gdf.Latitude),
                crs="EPSG:4326",
            )
            gdf["vartype"] = str(tkey)
            gdfs.append(gdf)
        if not gdfs:
            return gpd.GeoDataFrame()
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

    def _run_postpro_on_demand(self, location, vartype):
        """Process a single station on demand and store results in the diskcache.

        Runs observed and model post-processors only for cache entries that are
        not already present.  Called automatically from :meth:`create_panel`
        when the user opens a plot for a station whose data has not yet been
        pre-processed.
        """
        import pyhecdss as _hecdss

        vartype_name = vartype.name

        # --- observed ---
        obs_dssfile = self.config["observed_files_dict"].get(vartype_name)
        if obs_dssfile:
            obs_cached = _get_cached_bparts(obs_dssfile, vartype_name)
            if location.name.upper() not in obs_cached:
                print(
                    f"[on-demand] Observed  {location.name}/{vartype_name} ..."
                )
                try:
                    _hecdss.DSSFile(obs_dssfile).catalog()
                except Exception:
                    pass
                obs_proc = postpro.PostProcessor(
                    postpro.Study("Observed", obs_dssfile),
                    postpro.Location(
                        location.name,
                        location.bpart,
                        location.description,
                        location.time_window_exclusion_list,
                        location.threshold_value,
                    ),
                    vartype,
                )
                obs_proc.do_resample_with_merge("15min")
                obs_proc.do_fill_in()
                postpro.run_processor(obs_proc)

        # --- model studies ---
        for study_name, dssfile in self.config["study_files_dict"].items():
            model_cached = _get_cached_bparts(dssfile, vartype_name)
            if location.name.upper() not in model_cached:
                print(
                    f"[on-demand] Model {study_name}  {location.name}/{vartype_name} ..."
                )
                try:
                    _hecdss.DSSFile(dssfile).catalog()
                except Exception:
                    pass
                model_proc = postpro.PostProcessor(
                    postpro.Study(study_name, dssfile),
                    postpro.Location(
                        location.name,
                        location.name,
                        location.description,
                        location.time_window_exclusion_list,
                        location.threshold_value,
                    ),
                    vartype,
                )
                postpro.run_processor(model_proc)

    def _refresh_catalog(self, status_pane=None):
        """Rebuild the catalog and push updates to the dvue DataUI table."""
        try:
            self._dvue_catalog = self._build_dvue_catalog()
            if hasattr(self, "_dataui"):
                new_df = self.get_data_catalog()
                self._dataui._dfcat = new_df
                table_cols = [
                    c for c in self.get_table_columns() if c in new_df.columns
                ]
                self._dataui.display_table.value = (
                    new_df[table_cols].reset_index(drop=True)
                )
                msg = f"Updated — {len(new_df)} stations in table"
            else:
                msg = "Catalog updated"
            if status_pane is not None:
                status_pane.object = msg
        except Exception as exc:
            msg = f"Error refreshing: {exc}"
            if status_pane is not None:
                status_pane.object = msg

    def get_widgets(self):
        # ------------------------------------------------------------------ #
        # Vartypes                                                             #
        # ------------------------------------------------------------------ #
        all_vt_options = list(self._vartype_timewindow_original.keys())
        active_vt = [
            k
            for k, v in self.config["vartype_timewindow_dict"].items()
            if v is not None
        ]
        vartype_cbs = pn.widgets.CheckBoxGroup(
            name="Active vartypes",
            options=all_vt_options,
            value=active_vt,
        )
        vartype_status = pn.pane.Str("", styles={"color": "#555", "font-size": "0.9em"})

        def _on_vartype_change(event):
            for vt, orig_val in self._vartype_timewindow_original.items():
                self.config["vartype_timewindow_dict"][vt] = (
                    orig_val if vt in event.new else None
                )
            self._refresh_catalog(vartype_status)

        vartype_cbs.param.watch(_on_vartype_change, "value")

        # ------------------------------------------------------------------ #
        # Plot options                                                         #
        # ------------------------------------------------------------------ #
        opts_dict = self.config["options_dict"]

        output_folder_w = pn.widgets.TextInput(
            name="Output folder",
            value=str(opts_dict.get("output_folder", "./plots/")),
            width=280,
        )

        def _on_output_folder(event):
            self.config["options_dict"]["output_folder"] = event.new

        output_folder_w.param.watch(_on_output_folder, "value")

        bool_opt_labels = [
            ("include_kde_plots",           "Include KDE plots"),
            ("zoom_inst_plot",              "Zoom instantaneous plot"),
            ("write_graphics",              "Write PNG files"),
            ("write_html",                  "Write HTML files"),
            ("mask_plot_metric_data",        "Mask plot metric data"),
            ("tech_memo_validation_metrics", "Tech memo validation metrics"),
            ("manuscript_layout",            "Manuscript layout"),
        ]

        def _make_opt_watcher(k):
            def _w(event):
                self.config["options_dict"][k] = event.new
            return _w

        bool_widgets = []
        for key, label in bool_opt_labels:
            w = pn.widgets.Checkbox(
                name=label,
                value=bool(opts_dict.get(key, _DEFAULT_OPTIONS.get(key, False))),
            )
            w.param.watch(_make_opt_watcher(key), "value")
            bool_widgets.append(w)

        # ------------------------------------------------------------------ #
        # Cache                                                                #
        # ------------------------------------------------------------------ #
        clear_btn = pn.widgets.Button(
            name="Clear Cache",
            button_type="warning",
            width=140,
            description="Clear all post-processing caches for every DSS file in this config.",
        )
        cache_status = pn.pane.Str("", styles={"color": "#555", "font-size": "0.9em"})

        def _on_clear(event):
            cache_status.object = "Clearing\u2026"
            try:
                msg = _clear_all_caches(self.config)
                cache_status.object = msg
                self.cache_status = msg
            except Exception as exc:
                cache_status.object = f"Error: {exc}"
                self.cache_status = f"Error: {exc}"

        clear_btn.on_click(_on_clear)

        return pn.Column(
            pn.pane.Markdown("### Vartypes"),
            vartype_cbs,
            vartype_status,
            pn.layout.Divider(),
            pn.pane.Markdown("### Plot Options"),
            pn.pane.Markdown(
                "*Changes apply on the next plot opened.*",
                styles={"font-size": "0.85em", "color": "#666"},
            ),
            output_folder_w,
            *bool_widgets,
            pn.layout.Divider(),
            pn.pane.Markdown("### Cache"),
            pn.pane.Markdown(
                "Clears cached post-processed time series for all DSS files "
                "in this config. Re-run *postpro* to rebuild.",
                styles={"font-size": "0.85em"},
            ),
            clear_btn,
            cache_status,
        )

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
                # On-demand post-processing: populate cache for uncached stations.
                if not self.config.get("skip_ondemand"):
                    self._run_postpro_on_demand(location, vartype)
                calib_plot_template_dict, metrics_df, failed_studies = postpro_dsm2.build_plot(
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
                    study_names = [s.name for s in studies]
                    missing = failed_studies if failed_studies else study_names
                    msg = (
                        f"## No plot available: {location.name} ({varname})\n\n"
                        f"**DSM2 ID (model B-part):** {location.name}  \n"
                        f"**Variable:** {varname}  \n"
                        f"**Observed DSS B-part (obs_station_id):** {location.bpart}  \n"
                        f"**Studies with missing cached data:** {', '.join(missing) if missing else 'unknown'}  \n\n"
                        f"**All studies checked:** {', '.join(study_names)}  \n\n"
                        "**Possible causes:**\n"
                        "- Post-processing has not been run yet for one or more studies at this location. "
                        "Run the postpro step to populate the cache.\n"
                        f"- The model DSS file does not contain `//{location.name}/{varname}////`.\n"
                        f"- The observed DSS file does not contain `//{location.bpart}/{varname}////`.\n"
                        "- The timewindow in the config does not overlap with available data.\n\n"
                        f"*Note: '{location.bpart}' is the observed-data station ID for this location "
                        f"(dsm2_id={location.name}). Model data is always looked up by dsm2_id.*"
                    )
                    print(
                        f"No plot found for {location.name} ({varname}). "
                        f"Studies with missing data: {', '.join(missing) if missing else 'unknown'}"
                    )
                    plots.append(
                        (
                            location.name + "@" + varname,
                            pn.pane.Markdown(msg, styles={"color": "#8B0000"}),
                        )
                    )
            except Exception as e:
                msg = (
                    f"## Error loading plot: {location.name} ({varname})\n\n"
                    f"**Exception:** {e}  \n\n"
                    f"**DSM2 ID (model B-part):** {location.name}  \n"
                    f"**Variable:** {varname}  \n"
                    f"**Observed DSS B-part (obs_station_id):** {location.bpart}  \n\n"
                    f"*Note: '{location.bpart}' is the observed-data station ID for this location "
                    f"(dsm2_id={location.name}). Model data is looked up by dsm2_id.*"
                )
                print(
                    f"Exception building plot for {location.name} ({varname}): {e}"
                )
                plots.append(
                    (
                        location.name + "@" + varname,
                        pn.pane.Markdown(msg, styles={"color": "#8B0000"}),
                    )
                )
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
@click.option(
    "--clear-cache",
    is_flag=True,
    default=False,
    help="Clear all post-processing caches before launching the UI.",
)
@click.option(
    "--vartype",
    "vartypes",
    multiple=True,
    help="Restrict active vartypes (repeat for multiple, e.g. --vartype EC --vartype FLOW).",
)
@click.option(
    "--option",
    "options",
    multiple=True,
    help="Override an options_dict entry as KEY=VALUE (e.g. --option write_html=false).",
)
def calib_plot_ui(config_file, base_dir=None, clear_cache=False, vartypes=(), options=(), **kwargs):
    """Launch the interactive calibration plot UI from a YAML config file.

    config_file: str
        yaml file containing configuration

    base_dir: str
        base directory for config file, if None is assumed to be same as config file directory
    """
    if clear_cache:
        with open(config_file, "r", encoding="utf-8") as _f:
            _cfg = yaml.safe_load(_f)
        _base = pathlib.Path(base_dir) if base_dir else pathlib.Path(config_file).parent
        _cfg["observed_files_dict"] = substitute_base_dir(
            _base, _cfg.get("observed_files_dict", {})
        )
        _cfg["study_files_dict"] = substitute_base_dir(
            _base, _cfg.get("study_files_dict", {})
        )
        msg = _clear_all_caches(_cfg)
        click.echo(msg)

    # Build CLI overrides dict from --vartype and --option flags.
    cli_overrides = {}
    if vartypes:
        cli_overrides["vartypes"] = list(vartypes)
    if options:
        parsed_opts = {}
        for item in options:
            if "=" in item:
                k, v = item.split("=", 1)
                try:
                    import yaml as _yaml
                    parsed_opts[k.strip()] = _yaml.safe_load(v.strip())
                except Exception:
                    parsed_opts[k.strip()] = v.strip()
            else:
                click.echo(f"Warning: --option '{item}' ignored (expected KEY=VALUE)")
        if parsed_opts:
            cli_overrides["options"] = parsed_opts

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
        config_file,
        base_dir=base_dir,
        polygon_bounds=california,
        cli_overrides=cli_overrides or None,
        **kwargs,
    )

    DataUI(manager).create_view(title="DSM2 Calib Plot UI").show()
