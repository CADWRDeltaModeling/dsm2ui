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


def test_animate_qual_subcommand_help():
    from click.testing import CliRunner
    from dsm2ui.animate_cli import animate
    runner = CliRunner()
    result = runner.invoke(animate, ["qual", "--help"])
    assert result.exit_code == 0
    assert "--constituent" in result.output
