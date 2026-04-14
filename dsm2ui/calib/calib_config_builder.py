"""Build a postpro YAML config file for calib-ui from DSM2 study folders."""

import calendar
import datetime
import pathlib
import yaml

_MODULE_GLOB = {
    "hydro": "hydro*echo*.inp",
    "qual": "qual*echo*.inp",
    "gtm": "gtm*echo*.inp",
}

# Active vartype per module: the key that is non-null in vartype_timewindow_dict.
_MODULE_ACTIVE_VARTYPE = {
    "hydro": ("FLOW", "STAGE"),
    "qual": ("EC",),
    "gtm": ("EC",),
}

# Maps calib-ui vartype names to DMS Datastore param names used in the inventory.
# EC and FLOW map directly.  STAGE maps to "elev" but the datastore C-part is
# written as "elev" while postpro expects "STAGE"; the extraction helper renames
# the C-part automatically when writing the DSS file.
_VARTYPE_TO_DATASTORE_PARAM = {
    "EC": "ec",
    "FLOW": "flow",
    "STAGE": "elev",
}

# Default observed DSS filename stem per vartype.
_VARTYPE_DSS_STEM = {
    "EC": "ec_cal",
    "FLOW": "flow_cal",
    "STAGE": "stage_cal",
}


def _parse_dsm2_date(date_str):
    """Parse a DSM2-format date string (e.g. '01DEC2020') into a datetime.date."""
    return datetime.datetime.strptime(date_str.strip().upper(), "%d%b%Y").date()


def _dsm2_date_str(dt):
    """Format a date as DSM2 DDMMMYYYY (e.g. '01DEC2020'), upper-case month."""
    return dt.strftime("%d%b%Y").upper()


def _find_echo_file(study_folder, module):
    """Return the first echo .inp file matching the module pattern in output/."""
    output_dir = pathlib.Path(study_folder) / "output"
    pattern = _MODULE_GLOB.get(module)
    if pattern is None:
        raise ValueError(
            f"Unknown module '{module}'. Must be one of: {list(_MODULE_GLOB)}"
        )
    matches = sorted(output_dir.glob(pattern))
    if not matches:
        raise FileNotFoundError(
            f"No {module} echo .inp file found in {output_dir} matching '{pattern}'"
        )
    return matches[0]


def _parse_envvars(echo_file):
    """Parse the ENVVAR section of a DSM2 echo .inp file into a dict."""
    envvars = {}
    in_section = False
    with open(echo_file, "r") as f:
        for line in f:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            upper = stripped.upper()
            if upper in ("ENVVAR", "ENVVARS"):
                in_section = True
                continue
            if in_section:
                if upper == "END":
                    break
                parts = stripped.split()
                # skip header row
                if len(parts) >= 2 and parts[0].upper() != "NAME":
                    envvars[parts[0].upper()] = parts[1]
    return envvars


def _find_dss_path(study_folder, module):
    """Locate the echo file, extract DSM2MODIFIER, return (echo_file, abs DSS path)."""
    echo_file = _find_echo_file(study_folder, module)
    envvars = _parse_envvars(echo_file)
    modifier = envvars.get("DSM2MODIFIER")
    if not modifier:
        raise ValueError(
            f"DSM2MODIFIER not found in ENVVAR section of {echo_file}"
        )
    dss_path = pathlib.Path(echo_file).parent / f"{modifier}_{module}.dss"
    return echo_file, str(dss_path.resolve())


def _get_simulation_dates(echo_file):
    """Return (start_date, end_date) as datetime.date from the echo file ENVVARS.

    Returns (None, None) if START_DATE or END_DATE are missing or unparseable.
    """
    envvars = _parse_envvars(echo_file)
    try:
        start = _parse_dsm2_date(envvars["START_DATE"])
        end = _parse_dsm2_date(envvars["END_DATE"])
        return start, end
    except (KeyError, ValueError):
        return None, None


def _build_inst_plot_timewindow(end_date, module):
    """Return a one-month inst_plot_timewindow string near the end of the simulation.

    Uses the second-to-last month to avoid end-of-run edge effects.
    Format: 'YYYY-MM-01:YYYY-MM-DD'
    """
    if end_date is None:
        return None
    # Step back one month from end_date
    if end_date.month == 1:
        month, year = 12, end_date.year - 1
    else:
        month, year = end_date.month - 1, end_date.year
    last_day = calendar.monthrange(year, month)[1]
    return f"{year}-{month:02d}-01:{year}-{month:02d}-{last_day:02d}"


def build_calib_config(
    study_folders,
    postprocessing_folder=None,
    output_file="postpro_config.yml",
    module="hydro",
    output_folder="./plots/",
    observed_files=None,
    location_files=None,
    timewindow_dict=None,
    dask_options=None,
):
    """Generate a postpro YAML config file for use with ``dsm2ui calib-ui``.

    Parameters
    ----------
    study_folders : list of str or Path
        Paths to DSM2 study folders. Each must contain an ``output/`` directory
        with a ``{module}*echo*.inp`` file produced by a completed DSM2 run.
    postprocessing_folder : str or Path, optional
        Path to the postprocessing workspace (contains ``location_info/`` and
        ``observed_data/`` subdirectories).  When omitted, ``location_files_dict``
        entries are written as ``null`` so the calib-ui manager falls back to the
        bundled default CSVs; ``observed_files_dict`` entries are also ``null``
        unless overridden via *observed_files*.
    output_file : str or Path, optional
        Destination path for the generated YAML file.  Defaults to
        ``"postpro_config.yml"`` in the current directory.
    module : str, optional
        DSM2 module whose DSS output to reference: ``"hydro"``, ``"qual"``, or
        ``"gtm"``. Default is ``"hydro"``.
    output_folder : str, optional
        Folder written into ``options_dict.output_folder`` (where plots are
        saved when running calib-ui). Default is ``"./plots/"``.
    observed_files : dict, optional
        Override observed DSS paths. Keys must be a subset of
        ``{"EC", "FLOW", "STAGE"}``; missing keys fall back to defaults
        (``ec_cal.dss``, ``flow_cal.dss``, ``stage_cal.dss``) when
        *postprocessing_folder* is provided, or ``null`` otherwise.
    location_files : dict, optional
        Override location CSV paths per vartype, e.g.
        ``{"EC": "/path/to/ec.csv"}``.  Values override both the
        *postprocessing_folder* convention paths and the bundled defaults.
    timewindow_dict : dict, optional
        Override or extend the named time windows. Values not provided are
        auto-derived from the first study's echo file START_DATE / END_DATE.
    dask_options : dict, optional
        Override Dask parallel processing settings. Keys not provided fall
        back to defaults (``n_workers=8``, ``threads_per_worker=1``,
        ``memory_limit="8G"``).

    Returns
    -------
    str
        Absolute path to the written YAML config file.
    """
    if module not in _MODULE_GLOB:
        raise ValueError(
            f"Unknown module '{module}'. Must be one of: {list(_MODULE_GLOB)}"
        )

    output_file = pathlib.Path(output_file)

    # --- location and observed files ---
    if postprocessing_folder is not None:
        postprocessing_folder = pathlib.Path(postprocessing_folder).resolve()
        _location_files = {
            "EC": str(
                postprocessing_folder / "location_info" / "calibration_ec_stations.csv"
            ),
            "FLOW": str(
                postprocessing_folder / "location_info" / "calibration_flow_stations.csv"
            ),
            "STAGE": str(
                postprocessing_folder / "location_info" / "calibration_stage_stations.csv"
            ),
        }
        observed = {
            "EC": str(postprocessing_folder / "observed_data" / "ec_cal.dss"),
            "FLOW": str(postprocessing_folder / "observed_data" / "flow_cal.dss"),
            "STAGE": str(postprocessing_folder / "observed_data" / "stage_cal.dss"),
        }
    else:
        # Null entries: calib-ui manager will fall back to bundled defaults for
        # location files; observed DSS paths must be provided via observed_files.
        _location_files = {"EC": None, "FLOW": None, "STAGE": None}
        observed = {"EC": None, "FLOW": None, "STAGE": None}

    # Apply per-vartype overrides.
    if location_files:
        _location_files.update(location_files)
    if observed_files:
        observed.update(observed_files)

    # --- study DSS files (also collect echo files for date extraction) ---
    study_files = {}
    echo_files = {}
    for folder in study_folders:
        label = pathlib.Path(folder).name
        echo_file, dss_path = _find_dss_path(folder, module)
        study_files[label] = dss_path
        echo_files[label] = echo_file

    # --- derive simulation period from first study's echo file ---
    first_echo = echo_files[next(iter(echo_files))]
    start_date, end_date = _get_simulation_dates(first_echo)

    if start_date is not None and end_date is not None:
        sim_period_str = f"{_dsm2_date_str(start_date)} - {_dsm2_date_str(end_date)}"
        tw = {
            "simulation_period": sim_period_str,
            "hydro_calibration": sim_period_str,
            "qual_calibration": sim_period_str,
            "hydro_validation": sim_period_str,
            "qual_validation": sim_period_str,
        }
        active_tw_key = "simulation_period"
        inst_tw = _build_inst_plot_timewindow(end_date, module)
    else:
        # Fallback if dates cannot be parsed
        tw = {
            "hydro_calibration": "01OCT2016 - 01OCT2023",
            "qual_calibration": "01OCT2016 - 01OCT2023",
            "hydro_validation": "01OCT2000 - 01OCT2023",
            "qual_validation": "01OCT2000 - 01OCT2023",
        }
        active_tw_key = "hydro_calibration" if module == "hydro" else "qual_calibration"
        inst_tw = None

    # User overrides win
    if timewindow_dict:
        tw.update(timewindow_dict)

    # --- vartype_timewindow_dict: active vartypes get the simulation window ---
    active_vartypes = _MODULE_ACTIVE_VARTYPE[module]
    vartype_timewindow = {
        "EC": active_tw_key if "EC" in active_vartypes else None,
        "FLOW": active_tw_key if "FLOW" in active_vartypes else None,
        "STAGE": active_tw_key if "STAGE" in active_vartypes else None,
    }

    # --- inst_plot_timewindow: null for EC (non-tidal), derive for FLOW/STAGE ---
    inst_plot_timewindow = {
        "EC": None,
        "FLOW": inst_tw if "FLOW" in active_vartypes else None,
        "STAGE": inst_tw if "STAGE" in active_vartypes else None,
    }

    # --- dask ---
    dask_opts = {"n_workers": 8, "threads_per_worker": 1, "memory_limit": "8G"}
    if dask_options:
        dask_opts.update(dask_options)

    config = {
        "options_dict": {
            "output_folder": output_folder,
            "include_kde_plots": True,
            "zoom_inst_plot": True,
            "write_graphics": True,
            "write_html": True,
            "mask_plot_metric_data": True,
            "tech_memo_validation_metrics": False,
        },
        "location_files_dict": _location_files,
        "observed_files_dict": observed,
        "study_files_dict": study_files,
        "postpro_model_dict": dict(study_files),
        "vartype_dict": {"EC": "uS/cm", "FLOW": "cfs", "STAGE": "feet"},
        "vartype_timewindow_dict": vartype_timewindow,
        "timewindow_dict": tw,
        "inst_plot_timewindow_dict": inst_plot_timewindow,
        "dask_options_dict": dask_opts,
    }

    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    return str(output_file.resolve())


def extract_observed_from_datastore(
    datastore_dir,
    output_dir,
    vartypes=("EC",),
    repo_level="screened",
    unit_names=None,
):
    """Extract observed time-series from a DMS Datastore into per-vartype DSS files.

    For each requested vartype, reads all matching records from the DMS Datastore
    inventory and writes them to a ``{vartype_stem}_cal.dss`` file in *output_dir*.

    The DSS B-parts are the raw datastore ``station_id`` values, which correspond
    to the ``obs_station_id`` column in the bundled calibration station CSVs.  This
    means the extracted DSS files can be used directly as ``observed_files_dict``
    entries without any additional mapping.

    For the STAGE vartype, the datastore ``param`` name is ``"elev"`` and its DSS
    C-part is written as ``"elev"``.  This function renames those C-parts to
    ``"STAGE"`` so that ``postpro`` can locate the data correctly.

    Parameters
    ----------
    datastore_dir : str or Path
        Directory that contains the DMS Datastore (``inventory_datasets_*.csv``
        and the ``screened/`` or ``raw/`` subdirectories).
    output_dir : str or Path
        Directory where the extracted ``*_cal.dss`` files will be written.
        Created automatically if it does not exist.
    vartypes : iterable of str, optional
        Calib-UI vartype names to extract (``"EC"``, ``"FLOW"``, ``"STAGE"``).
        Default: ``("EC",)``.
    repo_level : str, optional
        Datastore repository level — ``"screened"`` (default) or ``"raw"``.
    unit_names : dict, optional
        Per-vartype unit name overrides, e.g. ``{"EC": "UMHO/CM"}``.
        Missing entries use the unit from the inventory.

    Returns
    -------
    dict
        Mapping of vartype → absolute DSS file path for each successfully
        extracted vartype.
    """
    from dsm2ui import datastore2dss

    datastore_dir = pathlib.Path(datastore_dir)
    output_dir = pathlib.Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    unit_names = unit_names or {}

    result = {}
    for vt in vartypes:
        vt_upper = vt.upper()
        param = _VARTYPE_TO_DATASTORE_PARAM.get(vt_upper)
        if param is None:
            raise ValueError(
                f"Unknown vartype '{vt}'. Supported: {list(_VARTYPE_TO_DATASTORE_PARAM)}"
            )
        stem = _VARTYPE_DSS_STEM.get(vt_upper, f"{vt_upper.lower()}_cal")
        dssfile = str(output_dir / f"{stem}.dss")

        print(f"[datastore] Extracting {vt_upper} (param={param!r}) → {dssfile}")
        datastore2dss.read_from_datastore_write_to_dss(
            str(datastore_dir),
            dssfile,
            param,
            repo_level=repo_level,
            unit_name=unit_names.get(vt_upper),
        )

        # For STAGE: rename C-part from "elev" → "STAGE" so postpro can find it.
        if vt_upper == "STAGE":
            _rename_dss_cpart(dssfile, old_cpart="elev", new_cpart="STAGE")

        result[vt_upper] = str(pathlib.Path(dssfile).resolve())
        print(f"[datastore] {vt_upper} done → {result[vt_upper]}")

    return result


def _rename_dss_cpart(dssfile, old_cpart, new_cpart):
    """Copy all records whose C-part matches *old_cpart* to paths with *new_cpart*.

    Uses pyhecdss to read and rewrite each matching pathname.  This is needed
    because the DMS Datastore writes elevation data with C-part ``"elev"`` while
    DSM2 postpro expects ``"STAGE"``.
    """
    import pyhecdss

    old_upper = old_cpart.upper()
    new_upper = new_cpart.upper()

    with pyhecdss.DSSFile(dssfile) as f:
        catalog = f.get_catalog()

    pathnames_to_rename = [
        p for p in catalog["pathname"].tolist()
        if p.split("/")[3].upper() == old_upper
    ]

    if not pathnames_to_rename:
        return  # nothing to rename

    records = []
    with pyhecdss.DSSFile(dssfile) as f:
        for old_path in pathnames_to_rename:
            try:
                df, units, ptype = f.read_rts(old_path)
                records.append((old_path, df, units, ptype))
            except Exception as exc:
                print(f"  Warning: could not read {old_path}: {exc}")

    with pyhecdss.DSSFile(dssfile) as f:
        for old_path, df, units, ptype in records:
            parts = old_path.split("/")
            parts[3] = new_upper
            new_path = "/".join(parts)
            try:
                f.write_rts(new_path, df, units, ptype)
                print(f"  Renamed {old_path!r} → {new_path!r}")
            except Exception as exc:
                print(f"  Warning: could not write {new_path}: {exc}")
