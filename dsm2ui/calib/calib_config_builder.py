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
    postprocessing_folder,
    output_file,
    module="hydro",
    output_folder="./plots/",
    observed_files=None,
    timewindow_dict=None,
    dask_options=None,
):
    """Generate a postpro YAML config file for use with ``dsm2ui calib-ui``.

    Parameters
    ----------
    study_folders : list of str or Path
        Paths to DSM2 study folders. Each must contain an ``output/`` directory
        with a ``{module}*echo*.inp`` file produced by a completed DSM2 run.
    postprocessing_folder : str or Path
        Path to the postprocessing workspace (contains ``location_info/`` and
        ``observed_data/`` subdirectories).
    output_file : str or Path
        Destination path for the generated YAML file.
    module : str, optional
        DSM2 module whose DSS output to reference: ``"hydro"``, ``"qual"``, or
        ``"gtm"``. Default is ``"hydro"``.
    output_folder : str, optional
        Folder written into ``options_dict.output_folder`` (where plots are
        saved when running calib-ui). Default is ``"./plots/"``.
    observed_files : dict, optional
        Override observed DSS paths. Keys must be a subset of
        ``{"EC", "FLOW", "STAGE"}``; missing keys fall back to defaults
        (``ec_cal.dss``, ``flow_cal.dss``, ``stage_cal.dss``).
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

    postprocessing_folder = pathlib.Path(postprocessing_folder).resolve()
    output_file = pathlib.Path(output_file)

    # --- location and observed files ---
    location_files = {
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
        "location_files_dict": location_files,
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
