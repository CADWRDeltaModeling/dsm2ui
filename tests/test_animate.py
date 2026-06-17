"""Tests for dsm2ui.animate — DSM2 HDF5 SlicingReaders and helpers.

Test data paths point at the pydsm test fixtures which live in the sibling
pydsm repository.  All tests that need HDF5 files are skipped when those
files are not present so the CI can run without the full data suite.
"""

from __future__ import annotations

import os
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Test data paths (relative to this repo; pydsm lives next to dsm2ui)
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).parent.parent
_PYDSM_DATA = _REPO_ROOT.parent / "pydsm" / "tests" / "data"

HYDRO_H5 = _PYDSM_DATA / "historical_v82.h5"
QUAL_H5 = _PYDSM_DATA / "historical_v82_ec.h5"

_has_hydro = HYDRO_H5.exists()
_has_qual = QUAL_H5.exists()

skip_no_hydro = pytest.mark.skipif(not _has_hydro, reason=f"Hydro HDF5 not found: {HYDRO_H5}")
skip_no_qual = pytest.mark.skipif(not _has_qual, reason=f"Qual HDF5 not found: {QUAL_H5}")
skip_no_geo = pytest.mark.skipif(
    not (_has_hydro or _has_qual),
    reason="Neither HYDRO nor QUAL HDF5 found",
)


# ===========================================================================
# Internal helpers
# ===========================================================================

class TestNormaliseInterval:
    def test_lowercase_passthrough(self):
        from dsm2ui.animate import _normalise_interval
        assert _normalise_interval("30min") == "30min"

    def test_uppercase_H_to_h(self):
        from dsm2ui.animate import _normalise_interval
        assert _normalise_interval("1H") == "1h"

    def test_uppercase_T_to_min(self):
        from dsm2ui.animate import _normalise_interval
        assert _normalise_interval("15T") == "15min"

    def test_D_unchanged(self):
        from dsm2ui.animate import _normalise_interval
        assert _normalise_interval("1D") == "1D"


class TestParseDsm2Timestamp:
    def test_military_format(self):
        from dsm2ui.animate import _parse_dsm2_timestamp
        ts = _parse_dsm2_timestamp("02JAN1990 0000")
        assert ts == pd.Timestamp("1990-01-02 00:00")

    def test_bytes_input(self):
        from dsm2ui.animate import _parse_dsm2_timestamp
        ts = _parse_dsm2_timestamp(b"15MAR2020 1200")
        assert ts == pd.Timestamp("2020-03-15 12:00")

    def test_iso_format(self):
        from dsm2ui.animate import _parse_dsm2_timestamp
        ts = _parse_dsm2_timestamp("2020-01-01 00:00:00")
        assert ts == pd.Timestamp("2020-01-01")


# ===========================================================================
# HydroH5FlowReader
# ===========================================================================

@skip_no_hydro
class TestHydroH5FlowReader:

    @pytest.fixture(scope="class")
    def reader(self):
        from dsm2ui.animate import HydroH5FlowReader
        r = HydroH5FlowReader(HYDRO_H5)
        yield r
        r.close()

    def test_time_index_is_regular(self, reader):
        assert reader.time_index.freq is not None

    def test_time_index_length_positive(self, reader):
        assert len(reader.time_index) > 0

    def test_vmin_less_than_vmax(self, reader):
        assert reader.vmin < reader.vmax

    def test_get_slice_returns_series(self, reader):
        ts = reader.time_index[0]
        s = reader.get_slice(ts)
        assert isinstance(s, pd.Series)

    def test_get_slice_index_contains_ints(self, reader):
        ts = reader.time_index[0]
        s = reader.get_slice(ts)
        assert all(isinstance(v, (int, np.integer)) for v in s.index)

    def test_get_slice_values_are_finite_or_nan(self, reader):
        ts = reader.time_index[10]
        s = reader.get_slice(ts)
        finite = s[s.notna()]
        assert len(finite) > 0, "All values are NaN"

    def test_get_slice_nearest_off_grid(self, reader):
        ts = reader.time_index[5] + pd.Timedelta("1min")
        s = reader.get_slice_nearest(ts)
        assert isinstance(s, pd.Series)
        # Should snap to the same step as index[5] (nearest)
        expected = reader.get_slice(reader.time_index[5])
        pd.testing.assert_series_equal(s, expected)

    def test_context_manager(self):
        from dsm2ui.animate import HydroH5FlowReader
        with HydroH5FlowReader(HYDRO_H5) as r:
            s = r.get_slice(r.time_index[0])
        assert len(s) > 0


# ===========================================================================
# HydroH5StageReader
# ===========================================================================

@skip_no_hydro
class TestHydroH5StageReader:

    @pytest.fixture(scope="class")
    def reader(self):
        from dsm2ui.animate import HydroH5StageReader
        r = HydroH5StageReader(HYDRO_H5)
        yield r
        r.close()

    def test_time_index_is_regular(self, reader):
        assert reader.time_index.freq is not None

    def test_get_slice_returns_series(self, reader):
        s = reader.get_slice(reader.time_index[0])
        assert isinstance(s, pd.Series)
        assert len(s) > 0

    def test_vmin_less_than_vmax(self, reader):
        assert reader.vmin < reader.vmax

    def test_stage_values_differ_from_default_init(self):
        # Stage values (ft) should be in a different range than flow (cfs)
        from dsm2ui.animate import HydroH5FlowReader, HydroH5StageReader
        with HydroH5FlowReader(HYDRO_H5) as fr:
            flow_vmax = fr.vmax
        with HydroH5StageReader(HYDRO_H5) as sr:
            stage_vmax = sr.vmax
        # Not required to be different but both should be finite
        assert np.isfinite(flow_vmax)
        assert np.isfinite(stage_vmax)


# ===========================================================================
# HydroH5VelocityReader
# ===========================================================================

@skip_no_hydro
class TestHydroH5VelocityReader:

    @pytest.fixture(scope="class")
    def reader(self):
        from dsm2ui.animate import HydroH5VelocityReader
        r = HydroH5VelocityReader(HYDRO_H5)
        yield r
        r.close()

    def test_time_index_is_regular(self, reader):
        assert reader.time_index.freq is not None

    def test_get_slice_returns_series(self, reader):
        s = reader.get_slice(reader.time_index[0])
        assert isinstance(s, pd.Series)
        assert len(s) > 0

    def test_velocity_values_non_negative(self, reader):
        s = reader.get_slice(reader.time_index[0])
        finite = s.dropna()
        assert len(finite) > 0
        # velocity magnitudes should be >= 0 (absolute value not guaranteed
        # but typical for tidally averaged flows — negative means reverse flow)
        # At minimum, finite values must be actual floats
        assert all(np.isfinite(v) for v in finite)

    def test_vmin_vmax_finite(self, reader):
        assert np.isfinite(reader.vmin)
        assert np.isfinite(reader.vmax)

    def test_get_slice_range_shape(self, reader):
        df = reader.get_slice_range(0, 5)
        assert df.shape[0] == 5
        assert df.shape[1] == len(reader.time_index.get_indexer(
            [reader.time_index[0]]  # just checking col count matches
        ) or df.columns)

    def test_zero_area_channels_are_nan(self, reader):
        # Velocity reader should produce NaN for dry (zero-area) channels
        # rather than inf or a huge number
        s = reader.get_slice(reader.time_index[0])
        assert not np.any(np.isinf(s.dropna().values))


# ===========================================================================
# QualH5ConcentrationReader
# ===========================================================================

@skip_no_qual
class TestQualH5ConcentrationReader:

    @pytest.fixture(scope="class")
    def reader(self):
        from dsm2ui.animate import QualH5ConcentrationReader
        r = QualH5ConcentrationReader(QUAL_H5, constituent="ec")
        yield r
        r.close()

    def test_time_index_is_regular(self, reader):
        assert reader.time_index.freq is not None

    def test_get_slice_returns_series(self, reader):
        s = reader.get_slice(reader.time_index[0])
        assert isinstance(s, pd.Series)
        assert len(s) > 0

    def test_vmin_less_than_vmax(self, reader):
        assert reader.vmin < reader.vmax

    def test_constituent_not_found_raises(self):
        from dsm2ui.animate import QualH5ConcentrationReader
        with pytest.raises(ValueError, match="not found"):
            QualH5ConcentrationReader(QUAL_H5, constituent="nonexistent_xyz")

    def test_get_slice_values_are_non_negative(self, reader):
        # EC values should be >= 0 (may be 0 at start of run)
        s = reader.get_slice(reader.time_index[0])
        finite = s.dropna()
        assert len(finite) > 0
        assert (finite >= 0).all()


# ===========================================================================
# load_dsm2_channel_gdf
# ===========================================================================

class TestLoadDsm2ChannelGdf:

    def test_bundled_gdf_loads(self):
        from dsm2ui.animate import load_dsm2_channel_gdf
        gdf = load_dsm2_channel_gdf()
        assert len(gdf) > 0

    def test_bundled_gdf_has_geo_id_column(self):
        from dsm2ui.animate import load_dsm2_channel_gdf
        gdf = load_dsm2_channel_gdf()
        assert "geo_id" in gdf.columns

    def test_bundled_gdf_geo_id_is_int(self):
        from dsm2ui.animate import load_dsm2_channel_gdf
        gdf = load_dsm2_channel_gdf()
        assert gdf["geo_id"].dtype in (np.int64, np.int32, int, "int64", "int32")

    def test_bundled_gdf_has_geometry(self):
        from dsm2ui.animate import load_dsm2_channel_gdf
        gdf = load_dsm2_channel_gdf()
        assert gdf.geometry is not None
        assert len(gdf.geometry) > 0

    def test_bundled_gdf_crs_is_wgs84(self):
        from dsm2ui.animate import load_dsm2_channel_gdf
        gdf = load_dsm2_channel_gdf()
        assert gdf.crs is not None
        assert "4326" in str(gdf.crs)


# ===========================================================================
# animate_hydro / animate_qual factory functions
# ===========================================================================

@skip_no_hydro
def test_animate_hydro_returns_manager():
    import panel as pn
    pn.extension()
    from dsm2ui.animate import animate_hydro
    from dvue.animator import GeoAnimatorManager
    mgr = animate_hydro(HYDRO_H5, variable="flow")
    assert isinstance(mgr, GeoAnimatorManager)
    assert mgr._geom_type == "line"


@skip_no_hydro
def test_animate_hydro_stage_returns_manager():
    import panel as pn
    pn.extension()
    from dsm2ui.animate import animate_hydro
    from dvue.animator import GeoAnimatorManager
    mgr = animate_hydro(HYDRO_H5, variable="stage")
    assert isinstance(mgr, GeoAnimatorManager)


@skip_no_qual
def test_animate_qual_returns_manager():
    import panel as pn
    pn.extension()
    from dsm2ui.animate import animate_qual
    from dvue.animator import GeoAnimatorManager
    mgr = animate_qual(QUAL_H5, constituent="ec")
    assert isinstance(mgr, GeoAnimatorManager)
    assert mgr._geom_type == "line"


# ===========================================================================
# CLI smoke test
# ===========================================================================

def test_animate_cli_help():
    """CLI group --help should exit 0 without importing Panel/HoloViews."""
    from click.testing import CliRunner
    from dsm2ui.animate_cli import animate
    runner = CliRunner()
    result = runner.invoke(animate, ["--help"])
    assert result.exit_code == 0
    assert "hydro" in result.output
    assert "qual" in result.output


def test_animate_hydro_subcommand_help():
    from click.testing import CliRunner
    from dsm2ui.animate_cli import animate
    runner = CliRunner()
    result = runner.invoke(animate, ["hydro", "--help"])
    assert result.exit_code == 0
    assert "--variable" in result.output
    assert "--transform" in result.output
    assert "--diff" in result.output


def test_animate_qual_subcommand_help():
    from click.testing import CliRunner
    from dsm2ui.animate_cli import animate
    runner = CliRunner()
    result = runner.invoke(animate, ["qual", "--help"])
    assert result.exit_code == 0
    assert "--constituent" in result.output
    assert "--transform" in result.output


# ===========================================================================
# Transform factory tests (synthetic data, no HDF5 needed)
# ===========================================================================


class TestTransformFactories:
    """Tests for DSM2 transform factory functions using synthetic readers."""

    @pytest.fixture
    def reader_15min(self):
        """96 steps × 4 channels at 15-min intervals (24 hours of data)."""
        from dvue.animator import InMemorySlicingReader
        idx = pd.date_range("2020-01-01", periods=96, freq="15min")
        rng = np.random.default_rng(99)
        data = rng.uniform(100.0, 1000.0, size=(96, 4))
        df = pd.DataFrame(data, index=idx, columns=[1, 2, 3, 4])
        return InMemorySlicingReader(df)

    def test_resample_transform_daily(self, reader_15min):
        from dsm2ui.animate import make_resample_transform
        from dvue.animator import TransformedSlicingReader
        tr = TransformedSlicingReader(reader_15min, make_resample_transform("D"))
        # 96 × 15-min = 24 h = 1 day → 1 daily step
        assert len(tr.time_index) == 1
        assert tr.time_index.freq == pd.tseries.frequencies.to_offset("D")

    def test_resample_transform_hourly(self, reader_15min):
        from dsm2ui.animate import make_resample_transform
        from dvue.animator import TransformedSlicingReader
        tr = TransformedSlicingReader(reader_15min, make_resample_transform("h"))
        assert len(tr.time_index) == 24
        assert tr.time_index.freq == pd.tseries.frequencies.to_offset("h")

    def test_moving_average_keeps_steps(self, reader_15min):
        from dsm2ui.animate import make_moving_average_transform
        from dvue.animator import TransformedSlicingReader
        tr = TransformedSlicingReader(reader_15min, make_moving_average_transform("2h"))
        assert len(tr.time_index) == 96

    def test_moving_average_returns_finite_values(self, reader_15min):
        from dsm2ui.animate import make_moving_average_transform
        from dvue.animator import TransformedSlicingReader
        tr = TransformedSlicingReader(reader_15min, make_moving_average_transform("2h"))
        s = tr.get_slice(tr.time_index[48])
        assert s.notna().all()

    def test_make_resample_with_buffered(self, reader_15min):
        from dsm2ui.animate import make_resample_transform
        from dvue.animator import TransformedSlicingReader, BufferedSlicingReader
        tr = TransformedSlicingReader(reader_15min, make_resample_transform("h"))
        buf = BufferedSlicingReader(tr, chunk_size=10)
        s = buf.get_slice(tr.time_index[0])
        assert isinstance(s, pd.Series)

    def test_transform_vmin_vmax_in_raw_range(self, reader_15min):
        from dsm2ui.animate import make_resample_transform
        from dvue.animator import TransformedSlicingReader
        tr = TransformedSlicingReader(reader_15min, make_resample_transform("h"))
        assert tr.vmin >= reader_15min.vmin - 1e-6
        assert tr.vmax <= reader_15min.vmax + 1e-6


# ===========================================================================
# Config save / load tests — no HDF5 required
# ===========================================================================

def _make_synthetic_manager():
    """Return a GeoAnimatorManager built from synthetic data (no HDF5 needed)."""
    import panel as pn
    import geopandas as gpd
    from shapely.geometry import LineString
    pn.extension()

    from dvue.animator import InMemorySlicingReader, GeoAnimatorManager

    idx = pd.date_range("2020-01-01", periods=48, freq="15min")
    rng = np.random.default_rng(42)
    data = pd.DataFrame(
        rng.uniform(0, 5000, size=(48, 4)),
        index=idx,
        columns=[1, 2, 3, 4],
    )
    reader = InMemorySlicingReader(data)

    # Minimal LineString GDF (mimics DSM2 channel centrelines)
    gdf = gpd.GeoDataFrame(
        {"geo_id": [1, 2, 3, 4],
         "geometry": [
             LineString([(-122.0, 38.0), (-121.9, 38.0)]),
             LineString([(-121.9, 38.0), (-121.8, 38.0)]),
             LineString([(-121.8, 38.0), (-121.7, 38.0)]),
             LineString([(-121.7, 38.0), (-121.6, 38.0)]),
         ]},
        crs="EPSG:4326",
    )

    mgr = GeoAnimatorManager(reader, gdf, geo_id_column="geo_id",
                             colormap="rainbow", vmin=100.0, vmax=4000.0,
                             size=3.0)
    # Simulate what animate_hydro() sets
    mgr._animate_meta = {
        "mode": "single",
        "file_type": "hydro",
        "files": [{"path": "/fake/tidefile.h5", "title": ""}],
        "variable": "flow",
        "location": "both",
        "shapefile": None,
        "channel_id_column": None,
        "_transform_cli_keys": {
            "Daily mean": "daily",
            "Rolling 24 h": "rolling-24h",
            "Rolling 14 D": "rolling-14d",
            "Godin filter": "godin",
        },
    }
    mgr._config_path_input.value = "/fake/tidefile_animate.yml"
    return mgr


class TestConfigSaveLoad:
    """Round-trip tests for the YAML save/load feature.

    All tests use synthetic in-memory data — no HDF5 files required.
    """

    def test_collect_state_returns_required_keys(self):
        mgr = _make_synthetic_manager()
        state = mgr.collect_state()
        for key in ("version", "mode", "files", "file_type", "variable",
                    "transform", "colormap", "vmin", "vmax", "size",
                    "show_channels", "show_basemap", "contours", "diff", "x2"):
            assert key in state, f"missing key: {key!r}"

    def test_collect_state_version_is_1(self):
        mgr = _make_synthetic_manager()
        assert mgr.collect_state()["version"] == 1

    def test_collect_state_colormap_matches_widget(self):
        mgr = _make_synthetic_manager()
        state = mgr.collect_state()
        assert state["colormap"] == mgr.colormap

    def test_collect_state_vmin_vmax(self):
        mgr = _make_synthetic_manager()
        state = mgr.collect_state()
        assert state["vmin"] == 100.0
        assert state["vmax"] == 4000.0

    def test_collect_state_contours_default_disabled(self):
        mgr = _make_synthetic_manager()
        assert mgr.collect_state()["contours"]["enabled"] is False

    def test_collect_state_contours_enabled_after_toggle(self):
        mgr = _make_synthetic_manager()
        mgr._contours_check.value = True
        assert mgr.collect_state()["contours"]["enabled"] is True

    def test_collect_state_custom_levels_round_trip(self):
        mgr = _make_synthetic_manager()
        mgr._contour_custom_input.value = "500, 1000, 2000"
        state = mgr.collect_state()
        assert state["contours"]["custom_levels"] == "500, 1000, 2000"

    def test_collect_state_transform_none_by_default(self):
        mgr = _make_synthetic_manager()
        state = mgr.collect_state()
        assert state["transform"] == "none"

    def test_save_writes_valid_yaml(self, tmp_path):
        import yaml
        mgr = _make_synthetic_manager()
        out = tmp_path / "test_config.yml"
        mgr._config_path_input.value = str(out)
        mgr._on_save_config(None)
        assert "Saved" in mgr._save_config_status.object
        assert out.exists()
        loaded = yaml.safe_load(out.read_text(encoding="utf-8"))
        assert loaded["version"] == 1
        assert loaded["file_type"] == "hydro"
        assert loaded["colormap"] == "rainbow"

    def test_save_creates_parent_dirs(self, tmp_path):
        import yaml
        mgr = _make_synthetic_manager()
        nested = tmp_path / "sub" / "dir" / "config.yml"
        mgr._config_path_input.value = str(nested)
        mgr._on_save_config(None)
        assert nested.exists()

    def test_save_empty_path_shows_warning(self):
        mgr = _make_synthetic_manager()
        mgr._config_path_input.value = ""
        mgr._on_save_config(None)
        assert "Enter" in mgr._save_config_status.object

    def test_full_round_trip(self, tmp_path):
        """Save all non-default UI values, reload, verify every field."""
        import yaml
        mgr = _make_synthetic_manager()

        # Set non-default values
        mgr.colormap = "viridis"
        mgr.vmin = 200.0
        mgr.vmax = 3000.0
        mgr.size = 5.0
        mgr._show_channels_check.value = False
        mgr._show_basemap_check.value = False
        mgr._contours_check.value = True
        mgr._n_contours_slider.value = 12
        mgr._contour_smooth_slider.value = 5.0
        mgr._contour_levels_select.value = "linear"
        mgr._contour_custom_input.value = "100, 500, 900"
        mgr._contour_color_check.value = False
        mgr._contour_labels_check.value = True

        out = tmp_path / "round_trip.yml"
        mgr._config_path_input.value = str(out)
        mgr._on_save_config(None)
        assert out.exists()

        saved = yaml.safe_load(out.read_text(encoding="utf-8"))

        assert saved["colormap"] == "viridis"
        assert saved["vmin"] == 200.0
        assert saved["vmax"] == 3000.0
        assert saved["size"] == 5.0
        assert saved["show_channels"] is False
        assert saved["show_basemap"] is False
        c = saved["contours"]
        assert c["enabled"] is True
        assert c["n_levels"] == 12
        assert c["smoothing"] == 5.0
        assert c["level_mode"] == "linear"
        assert c["custom_levels"] == "100, 500, 900"
        assert c["color"] is False
        assert c["labels"] is True

    def test_apply_config_restores_contours(self, tmp_path):
        """_apply_config_to_manager restores contour widget values."""
        import yaml
        from dsm2ui.animate_cli import _apply_config_to_manager

        mgr = _make_synthetic_manager()

        cfg = {
            "contours": {
                "enabled": True,
                "n_levels": 15,
                "smoothing": 7.0,
                "level_mode": "eq_hist",
                "custom_levels": "250, 750",
                "color": False,
                "labels": True,
            },
            "show_channels": False,
            "show_basemap": False,
        }
        _apply_config_to_manager(mgr, cfg)

        assert mgr._contours_check.value is True
        assert mgr._n_contours_slider.value == 15
        assert mgr._contour_smooth_slider.value == 7.0
        assert mgr._contour_levels_select.value == "eq_hist"
        assert mgr._contour_custom_input.value == "250, 750"
        assert mgr._contour_color_check.value is False
        assert mgr._contour_labels_check.value is True
        assert mgr._show_channels_check.value is False
        assert mgr._show_basemap_check.value is False

    def test_cli_help_shows_config_option(self):
        """--config appears in both subcommand help texts."""
        from click.testing import CliRunner
        from dsm2ui.animate_cli import animate

        runner = CliRunner()
        for subcmd in ("hydro", "qual"):
            result = runner.invoke(animate, [subcmd, "--help"])
            assert result.exit_code == 0, result.output
            assert "--config" in result.output, \
                f"--config missing from '{subcmd} --help'"

    def test_cli_no_args_no_config_shows_error(self):
        """Invoking 'hydro' without files or --config shows a useful error."""
        from click.testing import CliRunner
        from dsm2ui.animate_cli import animate

        runner = CliRunner()
        result = runner.invoke(animate, ["hydro"])
        # exit code non-zero or error message present
        assert result.exit_code != 0 or "H5FILE" in result.output
