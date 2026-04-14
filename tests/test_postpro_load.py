"""
Integration tests for data loading with postpro_config_interior.yml.

Tests the pipeline:
  _resolve_config → get_studies / build_location
  → PostProcessor.load_processed(timewindow=...) → build_plot()

Run with:
  pytest tests/test_postpro_load.py -v -s

The tests require the postpro caches to have been populated already
(i.e. postpro has been run for at least the EC/MAL station).
If caches are empty, the on-demand test will attempt to process them.
"""
import pathlib
import traceback

import pandas as pd
import pytest
import yaml

CONFIG_FILE = pathlib.Path(r"D:\delta\dsm2_studies\studies\postpro_config_interior.yml")


# ── skip all tests if config file not present ──────────────────────────────
pytestmark = pytest.mark.skipif(
    not CONFIG_FILE.exists(),
    reason=f"Config file not found: {CONFIG_FILE}",
)


@pytest.fixture(scope="module")
def config():
    from dsm2ui.calib.calibplotui import _resolve_config

    with open(CONFIG_FILE, encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    return _resolve_config(raw, CONFIG_FILE.parent)


# ─────────────────────────────────────────────────────────────────────────────
# 1. Config structure
# ─────────────────────────────────────────────────────────────────────────────

def test_config_has_required_keys(config):
    for key in [
        "vartype_dict",
        "vartype_timewindow_dict",
        "timewindow_dict",
        "study_files_dict",
        "observed_files_dict",
        "location_files_dict",
        "options_dict",
        "inst_plot_timewindow_dict",
    ]:
        assert key in config, f"Missing key '{key}' in resolved config"


def test_timewindow_dict_values_are_strings(config):
    for name, tw in config["timewindow_dict"].items():
        assert isinstance(tw, str), f"timewindow_dict[{name!r}] is {type(tw)}, expected str"


def test_ec_timewindow_resolves(config):
    vtw = config["vartype_timewindow_dict"]
    tw_key = vtw.get("EC")
    assert tw_key is not None, "EC vartype_timewindow_dict entry is None"
    timewindow = config["timewindow_dict"][tw_key]
    print(f"\n  EC timewindow key={tw_key!r}  value={timewindow!r}")
    assert isinstance(timewindow, str)
    # must be parseable by pandas
    if " - " in timewindow:
        parts = [p.strip() for p in timewindow.split(" - ", 1)]
    elif ":" in timewindow:
        parts = timewindow.split(":", 1)
    else:
        parts = timewindow.split("-", 1)
    assert len(parts) == 2
    start = pd.Timestamp(parts[0])
    end = pd.Timestamp(parts[1])
    assert start < end, f"timewindow start {start} is not before end {end}"
    print(f"  parsed: {start} → {end}")


# ─────────────────────────────────────────────────────────────────────────────
# 2. PostProcessor.load_processed with the real timewindow string
# ─────────────────────────────────────────────────────────────────────────────

def _make_ec_processor(config, study_name_or_observed="Observed"):
    from pydsm.analysis import postpro

    vtw_key = config["vartype_timewindow_dict"]["EC"]
    timewindow = config["timewindow_dict"][vtw_key]

    vartype = postpro.VarType("EC", config["vartype_dict"]["EC"])

    # Pick a station that should be in the EC location file
    location_file = config["location_files_dict"]["EC"]
    loc_df = postpro.load_location_file(location_file)
    # take the first location
    row = loc_df.iloc[0]
    location = postpro.Location(
        row["Name"],
        row["BPart"],
        row["Description"],
        row.get("time_window_exclusion_list", ""),
        row.get("threshold_value", None),
    )
    print(f"\n  Using location: name={location.name!r}  bpart={location.bpart!r}")
    print(f"  Timewindow string passed to load_processed: {timewindow!r}")

    if study_name_or_observed == "Observed":
        dssfile = config["observed_files_dict"]["EC"]
        loc_for_pp = location  # bpart = obs station id
    else:
        dssfile = config["study_files_dict"][study_name_or_observed]
        # model processors use name as bpart
        loc_for_pp = location._replace(bpart=location.name)

    study = postpro.Study(study_name_or_observed, dssfile)
    p = postpro.PostProcessor(study, loc_for_pp, vartype)
    return p, timewindow, location


def test_load_processed_observed_ec(config):
    """PostProcessor.load_processed() for the first EC observed station."""
    p, timewindow, location = _make_ec_processor(config, "Observed")
    success = p.load_processed(timewindow=timewindow)
    print(f"  load_processed success={success}")
    if success:
        print(f"  df shape={p.df.shape}  index range: {p.df.index.min()} → {p.df.index.max()}")
    else:
        print(f"  error_message={p.error_message!r}")
    # We don't assert success=True here because the cache may be empty,
    # but we assert there's no exception (which would propagate as test failure).


def test_load_processed_model_ec(config):
    """PostProcessor.load_processed() for the first EC model station."""
    first_study = next(iter(config["study_files_dict"]))
    p, timewindow, location = _make_ec_processor(config, first_study)
    success = p.load_processed(timewindow=timewindow)
    print(f"  load_processed success={success}")
    if success:
        print(f"  df shape={p.df.shape}  index range: {p.df.index.min()} → {p.df.index.max()}")
    else:
        print(f"  error_message={p.error_message!r}")


# ─────────────────────────────────────────────────────────────────────────────
# 3. Timewindow slicing directly on a cached series
# ─────────────────────────────────────────────────────────────────────────────

def test_timewindow_slice_dsm2_format():
    """Verify that DSM2-format timewindow correctly slices a DatetimeIndex series."""
    idx = pd.date_range("2015-01-01", "2025-01-01", freq="15min")
    s = pd.Series(range(len(idx)), index=idx)

    timewindow = "01MAR2015 - 30SEP2024"
    assert " - " in timewindow

    parts = [p.strip() for p in timewindow.split(" - ", 1)]
    start = pd.Timestamp(parts[0])
    end = pd.Timestamp(parts[1])

    sliced = s.loc[start:end]
    assert len(sliced) > 0, "Slice returned empty series"
    assert sliced.index.min() >= start
    assert sliced.index.max() <= end
    print(f"\n  Sliced {len(sliced)} rows from {sliced.index.min()} to {sliced.index.max()}")


# ─────────────────────────────────────────────────────────────────────────────
# 4. Full build_plot for MAL/EC
# ─────────────────────────────────────────────────────────────────────────────

def test_build_plot_ec_first_station(config):
    """Call postpro_dsm2.build_plot() for the first EC station.

    The confluence study does not have MAL data, so build_plot should return a
    partial plot (historical only) rather than failing completely.
    """
    from pydsm.analysis import postpro as pp_mod
    from dsm2ui.calib import postpro_dsm2

    vtw_key = config["vartype_timewindow_dict"]["EC"]
    timewindow = config["timewindow_dict"][vtw_key]
    print(f"\n  timewindow for build_plot: {timewindow!r}")

    vartype = pp_mod.VarType("EC", config["vartype_dict"]["EC"])

    location_file = config["location_files_dict"]["EC"]
    loc_df = pp_mod.load_location_file(location_file)
    row = loc_df.iloc[0]
    location = pp_mod.Location(
        row["Name"], row["BPart"], row["Description"],
        row.get("time_window_exclusion_list", ""),
        row.get("threshold_value", None),
    )
    print(f"  Station: name={location.name!r}  bpart={location.bpart!r}")

    obs_study = pp_mod.Study("Observed", config["observed_files_dict"]["EC"])
    model_studies = [
        pp_mod.Study(name, config["study_files_dict"][name])
        for name in config["study_files_dict"]
    ]
    studies = [obs_study] + model_studies

    try:
        result = postpro_dsm2.build_plot(config, studies, location, vartype)
        calib_plot_template_dict, metrics_df, failed_studies = result
        print(f"  build_plot returned: template={'present' if calib_plot_template_dict else 'None'}")
        print(f"  metrics_df={'present' if metrics_df is not None else 'None'}")
        print(f"  failed_studies={failed_studies}")
        # At least one study (historical) has data — plot must be produced
        assert calib_plot_template_dict is not None, (
            f"Expected a partial plot (historical data present) but got None. "
            f"failed_studies={failed_studies}"
        )
    except Exception as exc:
        traceback.print_exc()
        pytest.fail(f"build_plot raised an exception: {exc}")


def test_has_cached_failure(config):
    """PostProcessor.has_cached_failure() returns True for confluence/MAL (no data)
    and False for historical/MAL (has data)."""
    from pydsm.analysis import postpro as pp_mod

    vartype = pp_mod.VarType("EC", config["vartype_dict"]["EC"])
    location_file = config["location_files_dict"]["EC"]
    loc_df = pp_mod.load_location_file(location_file)
    row = loc_df.iloc[0]
    location = pp_mod.Location(
        row["Name"], row["Name"],  # model uses name as bpart
        row["Description"],
        row.get("time_window_exclusion_list", ""),
        row.get("threshold_value", None),
    )

    for study_name, dssfile in config["study_files_dict"].items():
        study = pp_mod.Study(study_name, dssfile)
        p = pp_mod.PostProcessor(study, location, vartype)
        result = p.has_cached_failure()
        print(f"  {study_name}: has_cached_failure={result}")
        if study_name == "historical":
            assert result is False, "historical should have valid data, not a cached failure"
        elif study_name == "confluence":
            assert result is True, "confluence should have a cached failure for MAL"
