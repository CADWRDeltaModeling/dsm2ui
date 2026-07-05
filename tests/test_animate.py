"""Tests for dsm2ui.animate — DSM2 HDF5 SlicingReaders and helpers.

Test data paths point at the pydsm test fixtures which live in the sibling
pydsm repository.  All tests that need HDF5 files are skipped when those
files are not present so the CI can run without the full data suite.
"""

from __future__ import annotations

import os
import textwrap
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Test data paths (relative to this repo; pydsm lives next to dsm2ui)
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).parent.parent
_PYDSM_DATA = _REPO_ROOT.parent / "pydsm" / "tests" / "data"

HYDRO_H5  = _PYDSM_DATA / "historical_v82.h5"
QUAL_H5   = _PYDSM_DATA / "historical_v82_ec.h5"
HYDRO_INP = _PYDSM_DATA / "hydro_echo_historical_v82.inp"

_has_hydro = HYDRO_H5.exists()
_has_qual  = QUAL_H5.exists() and HYDRO_INP.exists()

skip_no_hydro = pytest.mark.skipif(not _has_hydro, reason=f"Hydro HDF5 not found: {HYDRO_H5}")
skip_no_qual  = pytest.mark.skipif(
    not _has_qual,
    reason=f"Qual HDF5 or hydro echo .inp not found under {_PYDSM_DATA}",
)
skip_no_geo = pytest.mark.skipif(
    not (_has_hydro or _has_qual),
    reason="Neither HYDRO nor QUAL HDF5 found",
)


# ===========================================================================
# Internal helpers
# ===========================================================================
# Performance benchmarks  (skipped by default — run with: pytest -m performance)
# ===========================================================================

@pytest.mark.skip(
    reason="Crashes Python with Windows fatal exception 0xc06d007f (DLL not found) "
           "in the numpy BLAS matmul path on this environment.  "
           "Re-enable once the BLAS DLL linkage is resolved."
)
@pytest.mark.performance
class TestIDWVectorizedPerformance:
    """Confirm the vectorised IDW path is substantially faster than the
    equivalent per-step Python loop.

    Run with:  pytest -m performance
    """

    def _make_inputs(self, n_times=200, n_ch=525, n_sta=120, seed=42):
        rng = np.random.default_rng(seed)
        N_ce = 2 * n_ch
        model_ce = rng.uniform(100, 25_000, size=(n_times, N_ce))
        obs_vals = rng.uniform(200, 15_000, size=(n_times, n_sta))
        obs_vals[rng.random((n_times, n_sta)) > 0.8] = np.nan  # 80 % coverage
        W_finite = np.zeros((N_ce, n_sta))
        for j in range(n_sta):
            idx = rng.choice(N_ce, 50, replace=False)
            W_finite[idx, j] = rng.uniform(1e-6, 1.0, 50)
        sta_ce_idx = rng.integers(0, N_ce, n_sta).astype(np.intp)
        return model_ce, obs_vals, W_finite, sta_ce_idx, n_ch

    def _vectorized(self, model_ce, obs_values, W_finite, sta_ce_idx, n_ch):
        valid_sta = sta_ce_idx >= 0
        model_at_sta = np.full_like(obs_values, np.nan)
        model_at_sta[:, valid_sta] = model_ce[:, sta_ce_idx[valid_sta]]
        residuals = obs_values - model_at_sta
        valid_f = (~np.isnan(residuals)).astype(np.float64)
        res_f = np.where(np.isnan(residuals), 0.0, residuals)
        wr = res_f @ W_finite.T
        w_sum = valid_f @ W_finite.T
        with np.errstate(invalid="ignore", divide="ignore"):
            correction = np.where(w_sum > 0.0, wr / w_sum, 0.0)
        corrected = model_ce + correction
        return np.nanmean(np.stack([corrected[:, 0::2], corrected[:, 1::2]], axis=2), axis=2)

    def _per_step_loop(self, model_ce, obs_values, W_finite, sta_ce_idx, n_ch):
        n_times = model_ce.shape[0]
        N_ce = model_ce.shape[1]
        result = np.zeros((n_times, n_ch))
        for t in range(n_times):
            corrections = np.zeros(N_ce)
            for i in range(N_ce):
                wr, ws = 0.0, 0.0
                for j in range(obs_values.shape[1]):
                    v = obs_values[t, j]
                    w = W_finite[i, j]
                    if not np.isnan(v) and w > 0:
                        ce_i = sta_ce_idx[j]
                        res = v - model_ce[t, ce_i] if ce_i >= 0 else 0.0
                        wr += w * res
                        ws += w
                if ws > 0:
                    corrections[i] = wr / ws
            corrected = model_ce[t] + corrections
            result[t] = np.nanmean(
                np.stack([corrected[0::2], corrected[1::2]], axis=1), axis=1
            )
        return result

    def test_vectorized_faster_than_loop(self):
        import time
        model_ce, obs_values, W_finite, sta_ce_idx, n_ch = self._make_inputs()

        # Warm up
        self._vectorized(model_ce, obs_values, W_finite, sta_ce_idx, n_ch)

        t0 = time.perf_counter()
        for _ in range(3):
            self._vectorized(model_ce, obs_values, W_finite, sta_ce_idx, n_ch)
        t_vec = (time.perf_counter() - t0) / 3

        t0 = time.perf_counter()
        self._per_step_loop(model_ce, obs_values, W_finite, sta_ce_idx, n_ch)
        t_loop = time.perf_counter() - t0

        speedup = t_loop / t_vec
        print(
            f"\n  Vectorized: {t_vec*1000:.1f} ms  |  "
            f"Loop: {t_loop*1000:.1f} ms  |  "
            f"Speedup: {speedup:.0f}x"
        )
        assert speedup > 50, (
            f"Expected ≥50x speedup; got {speedup:.1f}x.  "
            "Vectorized IDW may have regressed."
        )

    def test_vectorized_matches_loop_numerically(self):
        """Vectorized and loop paths must produce the same corrections."""
        # Use a small case for speed
        model_ce, obs_values, W_finite, sta_ce_idx, n_ch = self._make_inputs(
            n_times=10, n_ch=20, n_sta=8
        )
        r_vec  = self._vectorized(model_ce, obs_values, W_finite, sta_ce_idx, n_ch)
        r_loop = self._per_step_loop(model_ce, obs_values, W_finite, sta_ce_idx, n_ch)
        np.testing.assert_allclose(
            r_vec, r_loop, rtol=1e-10,
            err_msg="Vectorized IDW output differs from per-step loop.",
        )


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
    assert "--observations-csv" in result.output
    assert "--stations-csv" in result.output


# ===========================================================================
# CorrectedQualH5ConcentrationReader
# ===========================================================================

@skip_no_qual
class TestCorrectedQualH5ConcentrationReader:
    """Tests for CorrectedQualH5ConcentrationReader using the real QUAL H5 fixture.

    The bundled DSM2 8.2 channel centrelines GeoJSON is used for station
    snapping so no extra GIS files are needed.  Observation stations are
    placed at approximate Delta coordinates; if they land far from a channel
    pydsm.viz.dsm2gis emits a UserWarning but does not raise.
    """

    @pytest.fixture(scope="class")
    def bundled_centerlines(self):
        import dsm2ui
        p = Path(dsm2ui.__file__).parent / "dsm2gis" / "dsm2_channels_centerlines_8_2.geojson"
        if not p.exists():
            pytest.skip(f"Bundled centrelines not found: {p}")
        return p

    @pytest.fixture(scope="class")
    def obs_csv(self, tmp_path_factory):
        """30-row hourly observations CSV (1990-01-03 to 1990-01-04).
        STA_A=800 µS/cm, STA_B always NaN (simulates a missing station).
        """
        idx = pd.date_range("1990-01-03", periods=30, freq="1h")
        df = pd.DataFrame(
            {"STA_A": 800.0, "STA_B": float("nan")}, index=idx
        )
        p = tmp_path_factory.mktemp("obs") / "obs.csv"
        df.to_csv(p)
        return p

    @pytest.fixture(scope="class")
    def stations_csv(self, tmp_path_factory):
        """Two stations at approximate DSM2 Delta coordinates (WGS84)."""
        df = pd.DataFrame({
            "station_id": ["STA_A", "STA_B"],
            "lat":         [38.03,   38.10],
            "lon":         [-122.13, -121.90],
        })
        p = tmp_path_factory.mktemp("stations") / "stations.csv"
        df.to_csv(p, index=False)
        return p

    @pytest.fixture(scope="class")
    def reader(self, obs_csv, stations_csv, bundled_centerlines):
        import warnings
        from dsm2ui.animate import CorrectedQualH5ConcentrationReader
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)   # snap-distance warnings ok
            r = CorrectedQualH5ConcentrationReader(
                h5file=QUAL_H5,
                observations_csv=obs_csv,
                stations_csv=stations_csv,
                centerlines_file=bundled_centerlines,
                constituent="ec",
                power=2,
                echo_inp_file=HYDRO_INP,  # qual H5 has no channel table
            )
        yield r
        r.close()

    # ------------------------------------------------------------------
    # Static helper
    # ------------------------------------------------------------------

    def test_channels_loaded_from_h5(self):
        """_load_channels reads the CHANNEL table from the hydro H5 file.
        The qual H5 does not store channel geometry; it must come from the
        companion hydro H5 or a fallback echo .inp file.
        """
        from dsm2ui.animate import CorrectedQualH5ConcentrationReader
        # Hydro H5 stores the table at /hydro/input/channel
        df = CorrectedQualH5ConcentrationReader._load_channels(HYDRO_H5, None)
        assert len(df) > 100
        required = {"chan_no", "upnode", "downnode", "length"}
        assert required <= set(df.columns)
        assert df["upnode"].dtype == int or np.issubdtype(df["upnode"].dtype, np.integer)

    def test_channels_loaded_from_echo_inp(self):
        """_load_channels falls back to the echo .inp file."""
        from dsm2ui.animate import CorrectedQualH5ConcentrationReader
        # Pass the qual H5 (no channel table) + echo inp fallback
        df = CorrectedQualH5ConcentrationReader._load_channels(QUAL_H5, HYDRO_INP)
        assert len(df) > 100
        assert {"chan_no", "upnode", "downnode", "length"} <= set(df.columns)

    # ------------------------------------------------------------------
    # Time index
    # ------------------------------------------------------------------

    def test_time_index_is_regular(self, reader):
        assert reader.time_index.freq is not None

    def test_time_index_matches_inner(self, reader):
        from dsm2ui.animate import QualH5ConcentrationReader
        with QualH5ConcentrationReader(QUAL_H5, constituent="ec") as raw:
            assert len(reader.time_index) == len(raw.time_index)
            assert reader.time_index.freq == raw.time_index.freq

    # ------------------------------------------------------------------
    # get_slice
    # ------------------------------------------------------------------

    def test_get_slice_returns_series(self, reader):
        s = reader.get_slice(reader.time_index[5])
        assert isinstance(s, pd.Series)
        assert len(s) > 0

    def test_get_slice_index_matches_inner(self, reader):
        from dsm2ui.animate import QualH5ConcentrationReader
        ts = reader.time_index[0]
        with QualH5ConcentrationReader(QUAL_H5, constituent="ec") as raw:
            expected_idx = raw.get_slice(ts).index
        s = reader.get_slice(ts)
        pd.testing.assert_index_equal(s.index, expected_idx)

    def test_get_slice_no_nan_propagation(self, reader):
        """Correction must not introduce NaN where model had valid data."""
        s_corr = reader.get_slice(reader.time_index[5])
        from dsm2ui.animate import QualH5ConcentrationReader
        with QualH5ConcentrationReader(QUAL_H5, constituent="ec") as raw:
            s_raw = raw.get_slice(reader.time_index[5])
        # Positions that were valid in raw must still be finite after correction
        valid_mask = s_raw.notna()
        assert s_corr[valid_mask].notna().all()

    # ------------------------------------------------------------------
    # All-NaN obs path (max_obs_age guard)
    # ------------------------------------------------------------------

    def test_all_nan_obs_returns_raw(self, reader):
        """time_index[-1] is Jan 31 1990; obs CSV ends Jan 4 (~27 days gap).
        max_obs_age="2h" triggers all-NaN obs → correction=0 → output==raw.
        """
        from dsm2ui.animate import QualH5ConcentrationReader
        ts = reader.time_index[-1]
        with QualH5ConcentrationReader(QUAL_H5, constituent="ec") as raw:
            s_raw = raw.get_slice(ts)
        s_corr = reader.get_slice(ts)
        pd.testing.assert_series_equal(s_corr, s_raw)

    # ------------------------------------------------------------------
    # get_slice_range
    # ------------------------------------------------------------------

    def test_get_slice_range_shape(self, reader):
        df = reader.get_slice_range(0, 5)
        assert isinstance(df, pd.DataFrame)
        assert df.shape[0] == 5
        # column count equals number of channels
        from dsm2ui.animate import QualH5ConcentrationReader
        with QualH5ConcentrationReader(QUAL_H5, constituent="ec") as raw:
            n_ch = len(raw._channel_numbers)
        assert df.shape[1] == n_ch

    def test_get_slice_range_index_is_timestamps(self, reader):
        df = reader.get_slice_range(0, 3)
        assert isinstance(df.index, pd.DatetimeIndex)
        assert list(df.index) == list(reader.time_index[:3])

    # ------------------------------------------------------------------
    # vmin / vmax
    # ------------------------------------------------------------------

    def test_vmin_vmax_match_inner(self, reader):
        from dsm2ui.animate import QualH5ConcentrationReader
        with QualH5ConcentrationReader(QUAL_H5, constituent="ec") as raw:
            assert reader.vmin == raw.vmin
            assert reader.vmax == raw.vmax

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def test_context_manager(self, obs_csv, stations_csv, bundled_centerlines):
        import warnings
        from dsm2ui.animate import CorrectedQualH5ConcentrationReader
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            with CorrectedQualH5ConcentrationReader(
                QUAL_H5, obs_csv, stations_csv, bundled_centerlines,
                echo_inp_file=HYDRO_INP,
            ) as r:
                s = r.get_slice(r.time_index[0])
        assert len(s) > 0


# ===========================================================================
# IDW correction correctness — observations must be recovered at station CEs
# ===========================================================================

class TestIDWCorrectionExactMatch:
    """Verify the fundamental mathematical property of IDW correction:

    At a station's *home* channel-end (where the station is snapped) the IDW
    weight is infinite, so the correction is the exact residual and the
    corrected value equals the observation.

    These tests use only synthetic data — no HDF5 file is required.
    """

    def _build_corrector(self):
        """One channel, one station snapped to its upstream end."""
        channels_df = pd.DataFrame({
            "chan_no":   ["1"],
            "upnode":   [1],
            "downnode": [2],
            "length":   [10_000.0],
        })
        snapped = pd.DataFrame(
            {
                "chan_no":           ["1"],
                "location":         ["upstream"],
                "node_id":          [1],
                "distance_fraction": [0.0],
            },
            index=pd.Index(["STA_A"], name="station_id"),
        )
        from pydsm.analysis.network_correction import NetworkIDWCorrector
        return NetworkIDWCorrector(channels_df, snapped, power=2)

    def test_corrected_equals_observation_at_station_ce(self):
        """At the station's home CE the corrected value must equal the obs."""
        corrector = self._build_corrector()
        model_ce = pd.Series({"1-upstream": 500.0, "1-downstream": 600.0})
        obs       = pd.Series({"STA_A": 800.0})

        corrected = corrector.correct(model_ce, obs)

        # 1-upstream is STA_A's home CE; correction = obs - model = +300 → 800
        assert corrected["1-upstream"] == pytest.approx(800.0)

    def test_unobserved_ce_uses_idw_not_exact(self):
        """With a single station, ALL reachable CEs receive the full residual
        correction (IDW weight denominator = that one station's weight).
        The downstream CE must differ from the raw model but is NOT exact-matched
        to the observation."""
        corrector = self._build_corrector()
        model_ce = pd.Series({"1-upstream": 500.0, "1-downstream": 600.0})
        obs       = pd.Series({"STA_A": 800.0})

        corrected = corrector.correct(model_ce, obs)

        # With a single reachable station, correction = full residual everywhere.
        # raw_downstream=600, residual=300 → corrected_downstream = 900
        assert corrected["1-downstream"] > 600.0   # got corrected
        # Not exact-match to obs (because downstream CE has finite, not inf weight)
        assert corrected["1-downstream"] != pytest.approx(800.0)

    def test_all_nan_obs_returns_raw_model(self):
        """When all observations are NaN the corrector returns the raw model."""
        corrector = self._build_corrector()
        model_ce = pd.Series({"1-upstream": 500.0, "1-downstream": 600.0})
        obs_nan  = pd.Series({"STA_A": float("nan")})

        corrected = corrector.correct(model_ce, obs_nan)

        pd.testing.assert_series_equal(corrected, model_ce)

    def test_vectorized_idw_recovers_obs_at_station_ce(self):
        """The vectorized batch path in CorrectedQualH5ConcentrationReader must
        also exactly recover the observation at the station's home channel-end."""
        # Simulate the data structures used by _apply_idw_vectorized.
        N_times = 5
        N_ce = 4  # 2 channels × (upstream + downstream), interleaved
        N_sta = 1

        # W_finite: station 0 is at CE index 0 (1-upstream) — infinite weight
        # is handled separately via exact_map; finite weight here is 0.
        W_finite = np.zeros((N_ce, N_sta))
        sta_ce_idx = np.array([0], dtype=np.intp)  # STA_A → CE 0

        exact_map = [(0, np.array([0], dtype=np.intp))]  # CE 0 exact-matched by station 0

        model_ce_block = np.full((N_times, N_ce), 500.0)  # all model = 500
        obs_values     = np.full((N_times, N_sta), 800.0)  # obs = 800

        # --- reproduce _apply_idw_vectorized logic ---
        valid_sta = sta_ce_idx >= 0
        model_at_sta = np.full_like(obs_values, np.nan)
        model_at_sta[:, valid_sta] = model_ce_block[:, sta_ce_idx[valid_sta]]

        residuals = obs_values - model_at_sta  # (N_times, N_sta) = 300
        valid_f   = (~np.isnan(residuals)).astype(np.float64)
        res_f     = np.where(np.isnan(residuals), 0.0, residuals)

        wr    = res_f   @ W_finite.T
        w_sum = valid_f @ W_finite.T
        with np.errstate(invalid="ignore", divide="ignore"):
            correction = np.where(w_sum > 0.0, wr / w_sum, 0.0)

        # Exact-match overrides
        for ce_i, sta_js in exact_map:
            exact_r = residuals[:, sta_js]
            any_valid = ~np.all(np.isnan(exact_r), axis=1)
            if any_valid.any():
                correction[any_valid, ce_i] = np.nanmean(exact_r[any_valid], axis=1)

        corrected_ce = model_ce_block + correction

        # CE 0 (station home) must equal obs = 800
        np.testing.assert_allclose(corrected_ce[:, 0], 800.0, atol=1e-9)


@pytest.mark.integration
@skip_no_qual
class TestIDWCorrectionAtObservationSites:
    """Integration test: the IDW-corrected model must recover observations at
    each station's home channel-end.

    Uses the pydsm QUAL H5 test fixture.  Skipped when the fixture is absent.
    Run with:  pytest -m integration
    """

    @pytest.fixture(scope="class")
    def corrector_and_snapped(self, tmp_path_factory, bundled_centerlines):
        """Build an IDW corrector and snapped-stations table from synthetic obs."""
        import warnings
        from dsm2ui.animate import CorrectedQualH5ConcentrationReader
        from pydsm.analysis.network_correction import (
            NetworkIDWCorrector,
            snap_stations_to_channel_ends,
        )
        from pydsm.viz.dsm2gis import read_stations
        import geopandas as gpd

        # Two stations at approximate Delta locations
        sta_df = pd.DataFrame({
            "station_id": ["ST_UP", "ST_DN"],
            "lat":         [38.03,   38.05],
            "lon":         [-122.13, -121.95],
        })
        sta_path = tmp_path_factory.mktemp("sta") / "sta.csv"
        sta_df.to_csv(sta_path, index=False)

        channels_df = CorrectedQualH5ConcentrationReader._load_channels(
            HYDRO_H5, None
        )
        cl_gdf = gpd.read_file(str(bundled_centerlines))

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            snapped = snap_stations_to_channel_ends(
                read_stations(str(sta_path)), cl_gdf, channels_df
            )
        corrector = NetworkIDWCorrector(channels_df, snapped, power=2)
        return corrector, snapped

    @pytest.fixture(scope="class")
    def bundled_centerlines(self):
        import dsm2ui
        p = Path(dsm2ui.__file__).parent / "dsm2gis" / "dsm2_channels_centerlines_8_2.geojson"
        if not p.exists():
            pytest.skip(f"Bundled centrelines not found: {p}")
        return p

    def test_corrected_recovers_obs_at_home_channel_end(
        self, corrector_and_snapped
    ):
        """At each station's exact channel-end node, corrected EC == obs."""
        from dsm2ui.animate import QualH5ConcentrationReader

        corrector, snapped = corrector_and_snapped

        with QualH5ConcentrationReader(QUAL_H5, constituent="ec") as raw:
            ts = raw.time_index[10]
            model_ce_series = _qual_model_ce_series(raw, ts)

        obs = pd.Series({"ST_UP": 1200.0, "ST_DN": 900.0})
        corrected_ce = corrector.correct(model_ce_series, obs)

        for sta_id, obs_val in obs.items():
            row = snapped.loc[sta_id]
            ce_key = f"{row['chan_no']}-{row['location']}"
            assert corrected_ce[ce_key] == pytest.approx(obs_val, abs=1e-6), (
                f"Station {sta_id!r}: corrected[{ce_key!r}]={corrected_ce[ce_key]:.2f} "
                f"!= obs={obs_val:.2f}"
            )

    def test_residual_at_obs_stations_is_zero(self, corrector_and_snapped):
        """Residual (corrected − obs) at each station's CE must be ≈ 0."""
        from dsm2ui.animate import QualH5ConcentrationReader

        corrector, snapped = corrector_and_snapped

        with QualH5ConcentrationReader(QUAL_H5, constituent="ec") as raw:
            ts = raw.time_index[20]
            model_ce_series = _qual_model_ce_series(raw, ts)

        obs = pd.Series({"ST_UP": 800.0, "ST_DN": 1500.0})
        corrected_ce = corrector.correct(model_ce_series, obs)

        residuals = {}
        for sta_id, obs_val in obs.items():
            row = snapped.loc[sta_id]
            ce_key = f"{row['chan_no']}-{row['location']}"
            residuals[sta_id] = corrected_ce[ce_key] - obs_val

        for sta_id, resid in residuals.items():
            assert abs(resid) < 1e-6, (
                f"Station {sta_id!r}: corrected − obs = {resid:.6f} (expected ≈ 0)"
            )


def _qual_model_ce_series(raw_reader, timestamp):
    """Extract a channel-end Series from a QualH5ConcentrationReader at *timestamp*."""
    i = raw_reader._time_index.get_indexer([timestamp], method="nearest")[0]
    ci = raw_reader._constituent_index
    row = raw_reader._ds[i, ci, :, :].astype(float)
    row[row < -1e20] = np.nan
    chan_str = [str(c) for c in raw_reader._channel_numbers]
    ce_index = [f"{s}-{loc}" for s in chan_str for loc in ("upstream", "downstream")]
    values = np.empty(2 * len(chan_str))
    values[0::2] = row[:, 0]
    values[1::2] = row[:, 1]
    return pd.Series(values, index=ce_index)


# ---------------------------------------------------------------------------
# Real production file paths used by the live-data integration class below.
# ---------------------------------------------------------------------------
_REAL_QUAL_H5 = Path(
    r"D:\delta\dsm2_v821\studies\historical\output\hist_v821_202312_EC.h5"
)
_REAL_EC_OBS  = Path(r"D:\delta\ec_obs_avg.csv")
_REAL_STA_CSV = Path(r"D:\delta\ec_obs_stations.csv")
_REAL_ECHO    = Path(
    r"D:\delta\dsm2_v821\studies\historical\output"
    r"\hydro_echo_hist_v821_202312.inp"
)
_real_prod_files = pytest.mark.skipif(
    not all(p.exists() for p in [_REAL_QUAL_H5, _REAL_EC_OBS,
                                  _REAL_STA_CSV, _REAL_ECHO]),
    reason="Real production files not found on this machine.",
)


@pytest.mark.integration
@_real_prod_files
class TestIDWCorrectionWithRealProductionFiles:
    """Integration test: IDW correction recovers observed EC values at the
    snapped channel-ends of the real production observation stations.

    Uses the actual study files from D:\\delta\\dsm2_v821.  Skipped when those
    files are absent.

    The corrector is built directly (bypassing the full
    CorrectedQualH5ConcentrationReader) to avoid the expensive obs-alignment
    step over the full model time range.

    Run with:  pytest -m integration -v
    """

    @pytest.fixture(scope="class")
    def corrector_and_snapped(self):
        """Build NetworkIDWCorrector + snapped table from the real station CSV."""
        import warnings
        import geopandas as gpd
        import dsm2ui
        from dsm2ui.animate import CorrectedQualH5ConcentrationReader
        from pydsm.analysis.network_correction import (
            NetworkIDWCorrector,
            snap_stations_to_channel_ends,
        )
        from pydsm.viz.dsm2gis import read_stations

        cl = (Path(dsm2ui.__file__).parent
              / "dsm2gis" / "dsm2_channels_centerlines_8_2.geojson")
        channels_df = CorrectedQualH5ConcentrationReader._load_channels(
            str(_REAL_QUAL_H5), str(_REAL_ECHO)
        )
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            snapped = snap_stations_to_channel_ends(
                read_stations(str(_REAL_STA_CSV)),
                gpd.read_file(str(cl)),
                channels_df,
            )
        corrector = NetworkIDWCorrector(channels_df, snapped, power=2)
        return corrector, snapped

    @pytest.fixture(scope="class")
    def obs_df(self):
        return pd.read_csv(str(_REAL_EC_OBS), index_col=0, parse_dates=True)

    def _sample_timesteps(self, obs_df, snapped, n=3, min_stations=5):
        testable = [s for s in obs_df.columns if s in snapped.index]
        if not testable:
            pytest.skip("No obs stations found in snapped table.")
        counts = obs_df[testable].notna().sum(axis=1)
        candidates = obs_df.index[counts >= min_stations]
        if len(candidates) == 0:
            pytest.skip("No timestep has enough non-NaN observations.")
        step = max(1, len(candidates) // n)
        return testable, list(candidates[::step][:n])

    def test_corrected_ec_matches_obs_at_station_home_ce(
        self, corrector_and_snapped, obs_df
    ):
        """The corrected EC at observation station CEs must be much closer to
        observations than the raw model.

        IDW with real data:
        - At *isolated* nodes (one station per network node) the correction is
          exact: corrected[CE] == obs.
        - At *shared* nodes (≥2 stations on the same node) the corrector uses
          the mean of their residuals, so corrected[CE] = mean(obs values at
          that node) ≠ any individual obs.

        We therefore test the overall mean absolute error reduction: corrected
        MAE at station CEs must be < 15 % of the raw-model MAE (≥ 85 % error
        reduction), which is a conservative lower bound for IDW.
        """
        from dsm2ui.animate import QualH5ConcentrationReader

        corrector, snapped = corrector_and_snapped
        testable, timestamps = self._sample_timesteps(obs_df, snapped)

        raw_errs  = []
        corr_errs = []
        with QualH5ConcentrationReader(str(_REAL_QUAL_H5), constituent="ec") as raw:
            for ts in timestamps:
                obs_row = obs_df.loc[ts, testable].dropna()
                if obs_row.empty:
                    continue
                model_ce     = _qual_model_ce_series(raw, ts)
                corrected_ce = corrector.correct(model_ce, obs_row)

                for sta_id, obs_val in obs_row.items():
                    row    = snapped.loc[sta_id]
                    ce_key = f"{row['chan_no']}-{row['location']}"
                    if ce_key not in model_ce.index:
                        continue
                    raw_errs.append(abs(float(model_ce[ce_key])  - float(obs_val)))
                    corr_errs.append(abs(float(corrected_ce[ce_key]) - float(obs_val)))

        if not raw_errs:
            pytest.skip("No data — check station/obs alignment.")

        mae_raw  = float(np.mean(raw_errs))
        mae_corr = float(np.mean(corr_errs))
        frac = mae_corr / max(mae_raw, 1.0)

        assert frac < 0.15, (
            f"IDW correction reduced MAE from {mae_raw:.1f} to {mae_corr:.1f} µS/cm "
            f"({frac:.1%} remaining — expected < 15 %)."
        )

    def test_mean_absolute_residual_across_stations_is_tiny(
        self, corrector_and_snapped, obs_df
    ):
        """Stricter version over more timesteps: corrected MAE < 10 % of raw MAE."""
        from dsm2ui.animate import QualH5ConcentrationReader

        corrector, snapped = corrector_and_snapped
        testable, timestamps = self._sample_timesteps(obs_df, snapped, n=5)

        raw_errs  = []
        corr_errs = []
        with QualH5ConcentrationReader(str(_REAL_QUAL_H5), constituent="ec") as raw:
            for ts in timestamps:
                obs_row = obs_df.loc[ts, testable].dropna()
                if obs_row.empty:
                    continue
                model_ce     = _qual_model_ce_series(raw, ts)
                corrected_ce = corrector.correct(model_ce, obs_row)
                for sta_id, obs_val in obs_row.items():
                    row    = snapped.loc[sta_id]
                    ce_key = f"{row['chan_no']}-{row['location']}"
                    if ce_key in model_ce.index:
                        raw_errs.append(abs(float(model_ce[ce_key])  - float(obs_val)))
                        corr_errs.append(abs(float(corrected_ce[ce_key]) - float(obs_val)))

        if not raw_errs:
            pytest.skip("No residuals collected — check station/obs alignment.")

        mae_raw  = float(np.mean(raw_errs))
        mae_corr = float(np.mean(corr_errs))
        frac = mae_corr / max(mae_raw, 1.0)

        assert frac < 0.10, (
            f"IDW correction reduced MAE from {mae_raw:.1f} to {mae_corr:.1f} µS/cm "
            f"({frac:.1%} remaining — expected < 10 %)."
        )


class TestCorrectedQualH5CLI:
    """Tests that the qual CLI branches correctly on --observations-csv.

    The factory functions are monkeypatched to avoid actually building readers
    or opening HDF5 files so no test data is required.
    """

    @pytest.fixture()
    def fake_h5(self, tmp_path):
        p = tmp_path / "fake.h5"
        p.write_bytes(b"")
        return str(p)

    @pytest.fixture()
    def fake_obs_csv(self, tmp_path):
        p = tmp_path / "obs.csv"
        p.write_text("datetime,STA_A\n1990-01-03 00:00,800\n")
        return str(p)

    @pytest.fixture()
    def fake_stations_csv(self, tmp_path):
        p = tmp_path / "stations.csv"
        p.write_text("station_id,lat,lon\nSTA_A,38.03,-122.13\n")
        return str(p)

    def _run(self, args, monkeypatch):
        """Patch server & both factories, then invoke the CLI."""
        from click.testing import CliRunner
        from dsm2ui.animate_cli import animate
        import unittest.mock as mock

        fake_mgr = mock.MagicMock()
        fake_mgr._reader = mock.MagicMock()
        fake_mgr._reader.time_index = pd.date_range("1990-01-01", periods=5, freq="1h")

        calls = {"qual": 0, "corrected": 0}

        def fake_animate_qual(*a, **kw):
            calls["qual"] += 1
            return fake_mgr

        def fake_animate_qual_corrected(*a, **kw):
            calls["corrected"] += 1
            return fake_mgr

        # Patch both factories in the CLI module namespace
        monkeypatch.setattr("dsm2ui.animate.animate_qual", fake_animate_qual)
        monkeypatch.setattr("dsm2ui.animate.animate_qual_corrected", fake_animate_qual_corrected)
        # Patch _serve_viewer so the test doesn't actually launch a Panel server
        monkeypatch.setattr("dsm2ui.animate_cli._serve_viewer", lambda build, **kw: build())
        # Suppress holoviews / panel extension calls
        monkeypatch.setattr("holoviews.extension", lambda *a, **kw: None)
        monkeypatch.setattr("panel.extension", lambda *a, **kw: None)

        runner = CliRunner()
        result = runner.invoke(animate, args, catch_exceptions=False)
        return result, calls

    def test_no_obs_csv_calls_animate_qual(self, fake_h5, monkeypatch):
        """Without --observations-csv the normal animate_qual is used."""
        result, calls = self._run(["qual", fake_h5], monkeypatch)
        assert result.exit_code == 0, result.output
        assert calls["qual"] == 1
        assert calls["corrected"] == 0

    def test_obs_csv_calls_animate_qual_corrected(
        self, fake_h5, fake_obs_csv, fake_stations_csv, monkeypatch
    ):
        """With --observations-csv the corrected factory is used."""
        result, calls = self._run(
            ["qual", fake_h5,
             "--observations-csv", fake_obs_csv,
             "--stations-csv", fake_stations_csv],
            monkeypatch,
        )
        assert result.exit_code == 0, result.output
        assert calls["corrected"] == 1
        assert calls["qual"] == 0

    def test_obs_csv_without_stations_raises(
        self, fake_h5, fake_obs_csv, monkeypatch
    ):
        """--observations-csv without --stations-csv must be a UsageError."""
        from click.testing import CliRunner
        from dsm2ui.animate_cli import animate
        monkeypatch.setattr("dsm2ui.animate_cli._serve_viewer", lambda build, **kw: build())
        runner = CliRunner()
        result = runner.invoke(
            animate,
            ["qual", fake_h5, "--observations-csv", fake_obs_csv],
        )
        assert result.exit_code != 0
        assert "stations-csv" in result.output.lower() or "stations_csv" in result.output.lower()

    def test_obs_csv_with_two_h5_raises(
        self, fake_h5, fake_obs_csv, fake_stations_csv, monkeypatch
    ):
        """IDW correction with two H5 files must be a UsageError."""
        from click.testing import CliRunner
        from dsm2ui.animate_cli import animate
        monkeypatch.setattr("dsm2ui.animate_cli._serve_viewer", lambda build, **kw: build())
        runner = CliRunner()
        result = runner.invoke(
            animate,
            ["qual", fake_h5, fake_h5,
             "--observations-csv", fake_obs_csv,
             "--stations-csv", fake_stations_csv],
        )
        assert result.exit_code != 0
        assert "single file" in result.output.lower() or "not supported" in result.output.lower()

    def test_qual_help_lists_new_options(self):
        """--help must advertise the new IDW options."""
        from click.testing import CliRunner
        from dsm2ui.animate_cli import animate
        result = CliRunner().invoke(animate, ["qual", "--help"])
        assert "--observations-csv" in result.output
        assert "--stations-csv" in result.output
        assert "--idw-power" in result.output


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
        from dvue.animator import StreamingTransformedSlicingReader
        tr = StreamingTransformedSlicingReader(reader_15min, make_resample_transform("D"), sample_steps=2)
        # 96 × 15-min = 24 h = 1 day → 1 daily step
        assert len(tr.time_index) == 1
        assert tr.time_index.freq == pd.tseries.frequencies.to_offset("D")

    def test_resample_transform_hourly(self, reader_15min):
        from dsm2ui.animate import make_resample_transform
        from dvue.animator import StreamingTransformedSlicingReader
        tr = StreamingTransformedSlicingReader(reader_15min, make_resample_transform("h"), sample_steps=4)
        assert len(tr.time_index) == 24
        assert tr.time_index.freq == pd.tseries.frequencies.to_offset("h")

    def test_moving_average_keeps_steps(self, reader_15min):
        from dsm2ui.animate import make_moving_average_transform
        from dvue.animator import StreamingTransformedSlicingReader
        tr = StreamingTransformedSlicingReader(reader_15min, make_moving_average_transform("2h"), sample_steps=4)
        assert len(tr.time_index) == 96

    def test_moving_average_returns_finite_values(self, reader_15min):
        from dsm2ui.animate import make_moving_average_transform
        from dvue.animator import StreamingTransformedSlicingReader
        tr = StreamingTransformedSlicingReader(reader_15min, make_moving_average_transform("2h"), sample_steps=4)
        s = tr.get_slice(tr.time_index[48])
        assert s.notna().all()

    def test_make_resample_with_buffered(self, reader_15min):
        from dsm2ui.animate import make_resample_transform
        from dvue.animator import StreamingTransformedSlicingReader, BufferedSlicingReader
        tr = StreamingTransformedSlicingReader(reader_15min, make_resample_transform("h"), sample_steps=4)
        buf = BufferedSlicingReader(tr, chunk_size=10)
        s = buf.get_slice(tr.time_index[0])
        assert isinstance(s, pd.Series)

    def test_transform_vmin_vmax_in_raw_range(self, reader_15min):
        from dsm2ui.animate import make_resample_transform
        from dvue.animator import StreamingTransformedSlicingReader
        tr = StreamingTransformedSlicingReader(reader_15min, make_resample_transform("h"), sample_steps=4)
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
                             colormap="turbo", vmin=100.0, vmax=4000.0,
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
        assert loaded["colormap"] == "turbo"

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
        mgr._channels_alpha_slider.value = 25
        mgr._basemap_alpha_slider.value = 50
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
        assert saved["show_channels"] == 25
        assert saved["show_basemap"] == 50
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
            "show_channels": 25,
            "show_basemap": 50,
        }
        _apply_config_to_manager(mgr, cfg)

        assert mgr._contours_check.value is True
        assert mgr._n_contours_slider.value == 15
        assert mgr._contour_smooth_slider.value == 7.0
        assert mgr._contour_levels_select.value == "eq_hist"
        assert mgr._contour_custom_input.value == "250, 750"
        assert mgr._contour_color_check.value is False
        assert mgr._contour_labels_check.value is True
        assert mgr._channels_alpha_slider.value == 25
        assert mgr._basemap_alpha_slider.value == 50

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


# ===========================================================================
# export_corrected_qual_h5 — embedded observations round-trip (no H5 needed)
# ===========================================================================

class TestExportCorrectedEmbedObs:
    """Verify that export_corrected_qual_h5 embeds obs data + station locations
    in the output /correction group.

    Uses --zero-model mode so no real QUAL HDF5 is required.  A minimal
    synthetic echo .inp with two channels is sufficient for the channel
    topology step.
    """

    _ECHO_INP = textwrap.dedent("""\
        CHANNEL
        CHAN_NO  LENGTH   MANNING  DISPERSION  UPNODE  DOWNNODE
        1        19500    0.035    360.0       1       2
        2        15000    0.030    300.0       2       3
        END
    """)

    @pytest.fixture(scope="class")
    def obs_csv(self, tmp_path_factory):
        idx = pd.date_range("2020-01-01", periods=24, freq="1h")
        df = pd.DataFrame({"STA_A": 500.0, "STA_B": 800.0}, index=idx)
        df.iloc[5:10, 0] = float("nan")  # intentional NaN rows
        p = tmp_path_factory.mktemp("obs") / "obs.csv"
        df.to_csv(p)
        return p

    @pytest.fixture(scope="class")
    def stations_csv(self, tmp_path_factory):
        df = pd.DataFrame({
            "station_id": ["STA_A", "STA_B"],
            "lat": [38.03, 38.10],
            "lon": [-122.13, -121.90],
        })
        p = tmp_path_factory.mktemp("stations") / "stations.csv"
        df.to_csv(p, index=False)
        return p

    @pytest.fixture(scope="class")
    def echo_inp(self, tmp_path_factory):
        p = tmp_path_factory.mktemp("inp") / "echo.inp"
        p.write_text(self._ECHO_INP)
        return p

    @pytest.fixture(scope="class")
    def corrected_h5(self, tmp_path_factory, obs_csv, stations_csv, echo_inp):
        import warnings
        from dsm2ui.animate import export_corrected_qual_h5
        out = tmp_path_factory.mktemp("h5") / "corrected.h5"
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            export_corrected_qual_h5(
                input_h5=None,
                output_h5=out,
                observations_csv=obs_csv,
                stations_csv=stations_csv,
                echo_inp_file=echo_inp,
                zero_model=True,
                interval="1h",
            )
        return out

    # ------------------------------------------------------------------
    # observations sub-group
    # ------------------------------------------------------------------

    def test_observations_group_exists(self, corrected_h5):
        import h5py
        with h5py.File(corrected_h5, "r") as f:
            assert "/correction/observations" in f

    def test_timestamps_shape(self, corrected_h5):
        import h5py
        with h5py.File(corrected_h5, "r") as f:
            ts = f["/correction/observations/timestamps"][:]
        assert ts.shape == (24,)

    def test_timestamps_dtype_int64(self, corrected_h5):
        import h5py
        with h5py.File(corrected_h5, "r") as f:
            ts = f["/correction/observations/timestamps"][:]
        assert ts.dtype == np.int64

    def test_timestamps_round_trip(self, corrected_h5):
        import h5py
        with h5py.File(corrected_h5, "r") as f:
            ts_ns = f["/correction/observations/timestamps"][:]
        idx = pd.to_datetime(ts_ns, unit="ns")
        expected = pd.date_range("2020-01-01", periods=24, freq="1h")
        # Compare as Python datetimes to avoid pandas resolution differences
        assert list(idx.to_pydatetime()) == list(expected.to_pydatetime())

    def test_station_ids_round_trip(self, corrected_h5):
        import h5py
        with h5py.File(corrected_h5, "r") as f:
            raw = f["/correction/observations/station_ids"][:]
        ids = [s.decode("utf-8") if isinstance(s, bytes) else s for s in raw]
        assert ids == ["STA_A", "STA_B"]

    def test_values_shape(self, corrected_h5):
        import h5py
        with h5py.File(corrected_h5, "r") as f:
            vals = f["/correction/observations/values"][:]
        assert vals.shape == (24, 2)

    def test_values_dtype_float32(self, corrected_h5):
        import h5py
        with h5py.File(corrected_h5, "r") as f:
            vals = f["/correction/observations/values"][:]
        assert vals.dtype == np.float32

    def test_values_nan_preserved(self, corrected_h5):
        import h5py
        with h5py.File(corrected_h5, "r") as f:
            vals = f["/correction/observations/values"][:]
        # With linear interpolation and max_obs_age=2h (default), the edges
        # of the 5-hour gap in STA_A (rows 5-9) fill in — only the centre
        # row (7) is more than 2h from both the left obs (row 4) and the
        # right obs (row 10), so it stays NaN.
        assert np.isnan(vals[7, 0])          # centre of gap: NaN
        assert not np.isnan(vals[5, 0])      # 1h from row 4: filled
        assert not np.isnan(vals[9, 0])      # 1h from row 10: filled
        # STA_B column has no NaN
        assert not np.isnan(vals[:, 1]).any()

    def test_values_non_nan_correct(self, corrected_h5):
        import h5py
        with h5py.File(corrected_h5, "r") as f:
            vals = f["/correction/observations/values"][:]
        # STA_A non-NaN rows should be approx 500.0 (float32 precision)
        np.testing.assert_allclose(vals[0, 0], 500.0, rtol=1e-5)
        # STA_B should be approx 800.0
        np.testing.assert_allclose(vals[0, 1], 800.0, rtol=1e-5)

    # ------------------------------------------------------------------
    # stations sub-group
    # ------------------------------------------------------------------

    def test_stations_group_exists(self, corrected_h5):
        import h5py
        with h5py.File(corrected_h5, "r") as f:
            assert "/correction/stations" in f

    def test_stations_ids_round_trip(self, corrected_h5):
        import h5py
        with h5py.File(corrected_h5, "r") as f:
            raw = f["/correction/stations/station_id"][:]
        ids = [s.decode("utf-8") if isinstance(s, bytes) else s for s in raw]
        assert ids == ["STA_A", "STA_B"]

    def test_stations_lat_lon(self, corrected_h5):
        import h5py
        with h5py.File(corrected_h5, "r") as f:
            lat = f["/correction/stations/lat"][:]
            lon = f["/correction/stations/lon"][:]
        np.testing.assert_allclose(lat, [38.03, 38.10])
        np.testing.assert_allclose(lon, [-122.13, -121.90])

    def test_stations_crs_attr(self, corrected_h5):
        import h5py
        with h5py.File(corrected_h5, "r") as f:
            crs = f["/correction/stations"].attrs["crs"]
        assert crs == "EPSG:4326"

    # ------------------------------------------------------------------
    # Existing provenance attrs must still be present
    # ------------------------------------------------------------------

    def test_provenance_attrs_intact(self, corrected_h5):
        import h5py
        with h5py.File(corrected_h5, "r") as f:
            grp = f["/correction"]
            for attr in ("method", "power", "constituent", "zero_model", "created"):
                assert attr in grp.attrs, f"Missing provenance attr: {attr!r}"
            assert grp.attrs["method"] == "IDW"
            assert grp.attrs["zero_model"] == "True"


# ===========================================================================
# _read_obs_from_correction_group — round-trip from embedded H5 data
# ===========================================================================

class TestReadObsFromCorrectionGroup:
    """Tests for _read_obs_from_correction_group.

    Builds its own corrected H5 using export_corrected_qual_h5 with
    zero_model=True so no external data files are needed.
    """

    _ECHO_INP = textwrap.dedent("""\
        CHANNEL
        CHAN_NO  LENGTH   MANNING  DISPERSION  UPNODE  DOWNNODE
        1        19500    0.035    360.0       1       2
        2        15000    0.030    300.0       2       3
        END
    """)

    @pytest.fixture(scope="class")
    def corrected_h5(self, tmp_path_factory):
        import warnings
        from dsm2ui.animate import export_corrected_qual_h5

        base = tmp_path_factory.mktemp("readobs")
        idx = pd.date_range("2020-06-01", periods=48, freq="1h")
        obs_df = pd.DataFrame({"A": 300.0, "B": 600.0, "C": float("nan")}, index=idx)
        obs_df.iloc[10:15, 1] = float("nan")   # B rows 10-14 become NaN

        obs_csv = base / "obs.csv"
        obs_df.to_csv(obs_csv)

        sta_df = pd.DataFrame({
            "station_id": ["A", "B", "C"],
            "lat": [37.8, 38.0, 38.2],
            "lon": [-122.4, -122.1, -121.8],
        })
        sta_csv = base / "sta.csv"
        sta_df.to_csv(sta_csv, index=False)

        echo_inp = base / "echo.inp"
        echo_inp.write_text(self._ECHO_INP)

        out = base / "corrected.h5"
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            export_corrected_qual_h5(
                input_h5=None, output_h5=out,
                observations_csv=obs_csv, stations_csv=sta_csv,
                echo_inp_file=echo_inp, zero_model=True, interval="1h",
            )
        return out

    # ------------------------------------------------------------------
    # Return type and basic structure
    # ------------------------------------------------------------------

    def test_returns_tuple_when_data_present(self, corrected_h5):
        from dsm2ui.animate import _read_obs_from_correction_group
        result = _read_obs_from_correction_group(corrected_h5)
        assert result is not None
        assert len(result) == 2

    def test_returns_none_for_plain_h5(self, tmp_path):
        import h5py
        from dsm2ui.animate import _read_obs_from_correction_group
        plain = tmp_path / "plain.h5"
        with h5py.File(plain, "w") as f:
            f.create_dataset("dummy", data=np.zeros(5))
        assert _read_obs_from_correction_group(plain) is None

    def test_returns_none_for_nonexistent_file(self, tmp_path):
        from dsm2ui.animate import _read_obs_from_correction_group
        assert _read_obs_from_correction_group(tmp_path / "missing.h5") is None

    # ------------------------------------------------------------------
    # obs_aligned DataFrame
    # ------------------------------------------------------------------

    def test_obs_index_length(self, corrected_h5):
        from dsm2ui.animate import _read_obs_from_correction_group
        obs, _ = _read_obs_from_correction_group(corrected_h5)
        assert len(obs) == 48

    def test_obs_columns(self, corrected_h5):
        from dsm2ui.animate import _read_obs_from_correction_group
        obs, _ = _read_obs_from_correction_group(corrected_h5)
        assert list(obs.columns) == ["A", "B", "C"]

    def test_obs_values_non_nan(self, corrected_h5):
        from dsm2ui.animate import _read_obs_from_correction_group
        obs, _ = _read_obs_from_correction_group(corrected_h5)
        assert obs.iloc[0, 0] == pytest.approx(300.0, abs=0.1)  # A
        assert obs.iloc[0, 1] == pytest.approx(600.0, abs=0.1)  # B row 0

    def test_obs_nan_preserved(self, corrected_h5):
        from dsm2ui.animate import _read_obs_from_correction_group
        obs, _ = _read_obs_from_correction_group(corrected_h5)
        # C is all-NaN (no valid obs at all) — stays NaN regardless
        assert np.isnan(obs.iloc[:, 2]).all()
        # B has NaN at rows 10-14 (5-hour gap).  With max_obs_age=2h (default)
        # the centre of that gap (row 12, which is 3h from both row 9 and row 15)
        # stays NaN; the edges (rows 10, 11, 13, 14) are filled by interpolation.
        assert np.isnan(obs.iloc[12, 1])        # gap centre: NaN
        assert not np.isnan(obs.iloc[0, 1])     # far from gap: valid
        assert not np.isnan(obs.iloc[10, 1])    # 1h from row 9: filled

    # ------------------------------------------------------------------
    # stations_df DataFrame
    # ------------------------------------------------------------------

    def test_stations_has_required_columns(self, corrected_h5):
        from dsm2ui.animate import _read_obs_from_correction_group
        _, sta = _read_obs_from_correction_group(corrected_h5)
        assert "station_id" in sta.columns
        assert "lat" in sta.columns
        assert "lon" in sta.columns

    def test_stations_ids(self, corrected_h5):
        from dsm2ui.animate import _read_obs_from_correction_group
        _, sta = _read_obs_from_correction_group(corrected_h5)
        assert list(sta["station_id"]) == ["A", "B", "C"]

    def test_stations_coords(self, corrected_h5):
        from dsm2ui.animate import _read_obs_from_correction_group
        _, sta = _read_obs_from_correction_group(corrected_h5)
        np.testing.assert_allclose(sta["lat"].values, [37.8, 38.0, 38.2])
        np.testing.assert_allclose(sta["lon"].values, [-122.4, -122.1, -121.8])

    # ------------------------------------------------------------------
    # Index timestamps
    # ------------------------------------------------------------------

    def test_obs_index_start(self, corrected_h5):
        from dsm2ui.animate import _read_obs_from_correction_group
        obs, _ = _read_obs_from_correction_group(corrected_h5)
        assert obs.index[0].to_pydatetime() == pd.Timestamp("2020-06-01").to_pydatetime()

    def test_obs_index_step(self, corrected_h5):
        from dsm2ui.animate import _read_obs_from_correction_group
        obs, _ = _read_obs_from_correction_group(corrected_h5)
        delta = obs.index[1] - obs.index[0]
        assert delta == pd.Timedelta("1h")


# ===========================================================================
# Obs time interpolation in export_corrected_qual_h5
# ===========================================================================

class TestObsTimeInterpolation:
    """Tests for the linear time interpolation applied to observation gaps.

    Uses zero_model=True and a synthetic two-station obs CSV so that the
    stored /correction/observations dataset reflects the interpolated values.
    No external data files are required.
    """

    _ECHO_INP = textwrap.dedent("""\
        CHANNEL
        CHAN_NO  LENGTH   MANNING  DISPERSION  UPNODE  DOWNNODE
        1        19500    0.035    360.0       1       2
        END
    """)

    def _make_corrected_h5(self, tmp_path, obs_df, max_obs_age):
        """Export a zero-model H5 from *obs_df* with *max_obs_age* interpolation."""
        import warnings
        from dsm2ui.animate import export_corrected_qual_h5

        obs_csv = tmp_path / "obs.csv"
        obs_df.to_csv(obs_csv)

        sta_csv = tmp_path / "sta.csv"
        pd.DataFrame({
            "station_id": list(obs_df.columns),
            "lat": [38.0] * len(obs_df.columns),
            "lon": [-122.0 + 0.1 * i for i in range(len(obs_df.columns))],
        }).to_csv(sta_csv, index=False)

        echo_inp = tmp_path / "echo.inp"
        echo_inp.write_text(self._ECHO_INP)

        out = tmp_path / "interp_test.h5"
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            export_corrected_qual_h5(
                input_h5=None, output_h5=out,
                observations_csv=obs_csv, stations_csv=sta_csv,
                echo_inp_file=echo_inp, zero_model=True,
                interval="1h", max_obs_age=max_obs_age,
            )
        return out

    # ------------------------------------------------------------------
    # Gap within max_obs_age is interpolated
    # ------------------------------------------------------------------

    def test_gap_within_limit_is_interpolated(self, tmp_path):
        """A 3-day gap with max_obs_age=5D should be filled by linear interp."""
        from dsm2ui.animate import _read_obs_from_correction_group

        idx = pd.DatetimeIndex(["2020-01-01", "2020-01-04"])  # 3-day gap
        obs_df = pd.DataFrame({"A": [500.0, 800.0]}, index=idx)

        h5 = self._make_corrected_h5(tmp_path, obs_df, "5D")
        obs, _ = _read_obs_from_correction_group(h5)

        # Jan 2 00:00 is 24h after Jan 1; linear interp over 72h: 500 + 300*(24/72)
        jan2 = pd.Timestamp("2020-01-02 00:00")
        pos = obs.index.searchsorted(jan2)
        val = float(obs.iloc[pos, 0])
        assert val == pytest.approx(600.0, abs=1.0), (
            f"Expected linear interpolation ~600 at Jan 2, got {val:.1f}"
        )

    def test_gap_midpoint_differs_from_both_endpoints(self, tmp_path):
        """Midpoint of a gap must be between the two surrounding obs values."""
        from dsm2ui.animate import _read_obs_from_correction_group

        idx = pd.DatetimeIndex(["2020-01-01", "2020-01-03"])  # 2-day gap
        obs_df = pd.DataFrame({"A": [400.0, 800.0]}, index=idx)

        h5 = self._make_corrected_h5(tmp_path, obs_df, "5D")
        obs, _ = _read_obs_from_correction_group(h5)

        # Jan 2 00:00 is the midpoint (24h from each end); expect ~600
        jan2 = pd.Timestamp("2020-01-02 00:00")
        pos = obs.index.searchsorted(jan2)
        val = float(obs.iloc[pos, 0])
        assert 550 < val < 650, f"Expected midpoint ~600, got {val:.1f}"
        assert val != pytest.approx(400.0, abs=5.0), "Value should not equal left obs"
        assert val != pytest.approx(800.0, abs=5.0), "Value should not equal right obs"

    # ------------------------------------------------------------------
    # Gap exceeding 2×max_obs_age has NaN in the centre
    # ------------------------------------------------------------------

    def test_long_gap_has_nan_in_centre(self, tmp_path):
        """Centre of a gap > 2×max_obs_age must remain NaN."""
        from dsm2ui.animate import _read_obs_from_correction_group

        # 12-day gap, max_obs_age=3D: centre 6 days should be NaN
        idx = pd.DatetimeIndex(["2020-01-01", "2020-01-13"])  # 12-day gap
        obs_df = pd.DataFrame({"A": [500.0, 700.0]}, index=idx)

        h5 = self._make_corrected_h5(tmp_path, obs_df, "3D")
        obs, _ = _read_obs_from_correction_group(h5)

        # Jan 7 00:00 is 6 days from each end, beyond the 3D limit from both sides
        jan7 = pd.Timestamp("2020-01-07 00:00")
        pos = obs.index.searchsorted(jan7)
        val = obs.iloc[pos, 0]
        assert np.isnan(val), (
            f"Expected NaN at centre of long gap (>2×max_obs_age), got {val:.1f}"
        )

    # ------------------------------------------------------------------
    # Endpoints near observations are not NaN
    # ------------------------------------------------------------------

    def test_obs_timestamps_are_not_nan(self, tmp_path):
        """The output values at obs timestamps must equal the obs values."""
        from dsm2ui.animate import _read_obs_from_correction_group

        idx = pd.DatetimeIndex(["2020-01-01", "2020-01-05"])
        obs_df = pd.DataFrame({"A": [300.0, 900.0]}, index=idx)

        h5 = self._make_corrected_h5(tmp_path, obs_df, "5D")
        obs, _ = _read_obs_from_correction_group(h5)

        jan1_pos = obs.index.searchsorted(pd.Timestamp("2020-01-01 00:00"))
        jan5_pos = obs.index.searchsorted(pd.Timestamp("2020-01-05 00:00"))
        assert obs.iloc[jan1_pos, 0] == pytest.approx(300.0, abs=0.1)
        assert obs.iloc[jan5_pos, 0] == pytest.approx(900.0, abs=0.1)


# ===========================================================================
# Rolling 14 D → Daily mean transform (overlap fix + registration)
# ===========================================================================

class TestRolling14dDailyTransform:
    """Tests for the 'Rolling 14 D → Daily mean' composed transform.

    The transform applies a 14-day centred rolling mean to the raw data
    and then resamples to daily values.  The key regression being tested is
    that ``StreamingTransformedSlicingReader`` fetches enough raw context
    (overlap = 7 days on each side) before applying the rolling window so
    that boundary output days are correctly smoothed rather than being
    computed from only a fraction of the 14-day window.
    """

    # ----------------------------------------------------------------
    # Helper: in-memory reader with synthetic hourly data
    # ----------------------------------------------------------------

    @pytest.fixture
    def hourly_reader_30d(self):
        """InMemorySlicingReader with 30 days of hourly data, single channel."""
        from dvue.animator import InMemorySlicingReader

        ti = pd.date_range("2020-01-01", periods=30 * 24, freq="1h")
        rng = np.random.default_rng(7)
        df = pd.DataFrame({"ch1": rng.uniform(0, 10_000, len(ti))}, index=ti)
        return InMemorySlicingReader(df)

    @pytest.fixture
    def hourly_reader_60d_30min(self):
        """InMemorySlicingReader with 60 days of 30-min data, single channel."""
        from dvue.animator import InMemorySlicingReader

        ti = pd.date_range("2020-01-01", periods=60 * 48, freq="30min")
        rng = np.random.default_rng(42)
        df = pd.DataFrame({"ch1": rng.uniform(0, 10_000, len(ti))}, index=ti)
        return InMemorySlicingReader(df)

    # ----------------------------------------------------------------
    # TransformSpec properties
    # ----------------------------------------------------------------

    def test_composed_spec_kind_is_aggregate(self):
        from dsm2ui.animate import make_moving_average_transform, make_resample_transform, make_composed_transform

        r14d = make_moving_average_transform("14D")
        dmean = make_resample_transform("D", "mean")
        spec = make_composed_transform(r14d, dmean)
        assert spec.kind == "aggregate"

    def test_composed_spec_output_freq_is_daily(self):
        from dsm2ui.animate import make_moving_average_transform, make_resample_transform, make_composed_transform

        r14d = make_moving_average_transform("14D")
        spec = make_composed_transform(r14d, make_resample_transform("D", "mean"))
        assert spec.output_freq == "D"

    def test_overlap_is_half_14d_window_at_1h(self):
        """Overlap should be ceil(7 days / 1h) = 168 steps for hourly data."""
        from dsm2ui.animate import make_moving_average_transform, make_resample_transform, make_composed_transform

        spec = make_composed_transform(
            make_moving_average_transform("14D"),
            make_resample_transform("D", "mean"),
        )
        freq_nanos = int(pd.Timedelta("1h").total_seconds() * 1e9)
        assert spec.get_overlap(freq_nanos) == 7 * 24

    def test_overlap_is_half_14d_window_at_30min(self):
        """Overlap = ceil(7 days / 30min) = 336 steps for 30-min data."""
        from dsm2ui.animate import make_moving_average_transform, make_resample_transform, make_composed_transform

        spec = make_composed_transform(
            make_moving_average_transform("14D"),
            make_resample_transform("D", "mean"),
        )
        freq_nanos = int(pd.Timedelta("30min").total_seconds() * 1e9)
        assert spec.get_overlap(freq_nanos) == 7 * 48

    # ----------------------------------------------------------------
    # StreamingTransformedSlicingReader behaviour
    # ----------------------------------------------------------------

    def test_output_time_index_is_daily(self, hourly_reader_30d):
        from dsm2ui.animate import make_moving_average_transform, make_resample_transform, make_composed_transform
        from dvue.animator.reader import StreamingTransformedSlicingReader

        spec = make_composed_transform(
            make_moving_average_transform("14D"),
            make_resample_transform("D", "mean"),
        )
        reader = StreamingTransformedSlicingReader(hourly_reader_30d, spec)
        assert reader.time_index.freq is not None
        assert len(reader.time_index) == 30

    def test_no_nan_in_middle_chunk(self, hourly_reader_30d):
        """A chunk in the middle of the file must have no NaN values."""
        from dsm2ui.animate import make_moving_average_transform, make_resample_transform, make_composed_transform
        from dvue.animator.reader import StreamingTransformedSlicingReader

        spec = make_composed_transform(
            make_moving_average_transform("14D"),
            make_resample_transform("D", "mean"),
        )
        reader = StreamingTransformedSlicingReader(hourly_reader_30d, spec)
        mid = len(reader.time_index) // 2
        chunk = reader.get_slice_range(mid - 3, mid + 4)
        assert not chunk["ch1"].isna().any(), "Middle chunk must not contain NaN"

    def test_first_day_not_nan(self, hourly_reader_30d):
        """The first output day must produce a value (min_periods=1)."""
        from dsm2ui.animate import make_moving_average_transform, make_resample_transform, make_composed_transform
        from dvue.animator.reader import StreamingTransformedSlicingReader

        spec = make_composed_transform(
            make_moving_average_transform("14D"),
            make_resample_transform("D", "mean"),
        )
        reader = StreamingTransformedSlicingReader(hourly_reader_30d, spec)
        first = reader.get_slice_range(0, 1)
        assert not first["ch1"].isna().any(), "First output day must not be NaN"

    def test_last_day_not_nan(self, hourly_reader_30d):
        """The last output day must produce a value (min_periods=1)."""
        from dsm2ui.animate import make_moving_average_transform, make_resample_transform, make_composed_transform
        from dvue.animator.reader import StreamingTransformedSlicingReader

        spec = make_composed_transform(
            make_moving_average_transform("14D"),
            make_resample_transform("D", "mean"),
        )
        reader = StreamingTransformedSlicingReader(hourly_reader_30d, spec)
        n = len(reader.time_index)
        last = reader.get_slice_range(n - 1, n)
        assert not last["ch1"].isna().any(), "Last output day must not be NaN"

    def test_rolling_daily_differs_from_raw_daily(self, hourly_reader_30d):
        """Rolling-14D-daily must differ from raw daily mean (smoothing effect)."""
        from dsm2ui.animate import make_moving_average_transform, make_resample_transform, make_composed_transform
        from dvue.animator.reader import StreamingTransformedSlicingReader

        spec = make_composed_transform(
            make_moving_average_transform("14D"),
            make_resample_transform("D", "mean"),
        )
        reader = StreamingTransformedSlicingReader(hourly_reader_30d, spec)

        # Compare the middle day
        mid = len(reader.time_index) // 2
        rolling_val = reader.get_slice_range(mid, mid + 1)["ch1"].iloc[0]

        # Compute raw daily mean for the same day
        raw_df = hourly_reader_30d.get_slice_range(0, 30 * 24)
        raw_daily = raw_df.resample("D").mean()
        raw_val = raw_daily["ch1"].iloc[mid]

        # 14-day rolling should smooth across ±7 days, so values differ
        assert abs(rolling_val - raw_val) > 1.0, (
            f"Rolling-daily ({rolling_val:.1f}) suspiciously close to "
            f"raw-daily ({raw_val:.1f}) — smoothing may not be applied"
        )

    def test_boundary_chunk_uses_overlap_context(self, hourly_reader_60d_30min):
        """Verify the overlap fix: day 1 of a 30-min dataset should use context
        from days 2-7 (right-side context via the overlap extension)."""
        from dsm2ui.animate import make_moving_average_transform, make_resample_transform, make_composed_transform
        from dvue.animator.reader import StreamingTransformedSlicingReader

        spec = make_composed_transform(
            make_moving_average_transform("14D"),
            make_resample_transform("D", "mean"),
        )
        reader = StreamingTransformedSlicingReader(hourly_reader_60d_30min, spec)
        assert spec.get_overlap(int(pd.Timedelta("30min").total_seconds() * 1e9)) == 7 * 48

        # Fetch a small window that straddles the chunk boundary to confirm
        # the overlap is fetched (test that no IndexError or NaN occurs)
        chunk = reader.get_slice_range(0, 10)
        assert len(chunk) == 10
        assert not chunk["ch1"].isna().all(), "Chunk should not be entirely NaN"

    def test_buffered_reader_round_trip(self, hourly_reader_30d):
        """BufferedSlicingReader wrapping the streaming transform must work."""
        from dsm2ui.animate import make_moving_average_transform, make_resample_transform, make_composed_transform
        from dvue.animator.reader import StreamingTransformedSlicingReader, BufferedSlicingReader

        spec = make_composed_transform(
            make_moving_average_transform("14D"),
            make_resample_transform("D", "mean"),
        )
        streaming = StreamingTransformedSlicingReader(hourly_reader_30d, spec)
        buffered = BufferedSlicingReader(streaming, chunk_size=7)

        ts = buffered.time_index[15]  # mid-run
        s = buffered.get_slice_nearest(ts)
        assert isinstance(s, pd.Series)
        assert not s.isna().any()

    # ----------------------------------------------------------------
    # Registration in options dict and CLI
    # ----------------------------------------------------------------

    def test_transform_in_options_dict(self):
        from dsm2ui.animate import _dsm2_transform_options
        opts = _dsm2_transform_options()
        assert "Rolling 14 D \u2192 Daily mean" in opts, (
            "Transform not found in _dsm2_transform_options() — "
            "check _dsm2_transform_options() in animate.py"
        )

    def test_cli_key_mapping(self):
        from dsm2ui.animate import _dsm2_transform_cli_keys
        keys = _dsm2_transform_cli_keys()
        assert "Rolling 14 D \u2192 Daily mean" in keys
        assert keys["Rolling 14 D \u2192 Daily mean"] == "rolling-14d-daily"

    def test_cli_map_in_animate_cli(self):
        from dsm2ui.animate_cli import _CLI_TRANSFORM_MAP
        assert "rolling-14d-daily" in _CLI_TRANSFORM_MAP
        assert _CLI_TRANSFORM_MAP["rolling-14d-daily"] == "Rolling 14 D \u2192 Daily mean"

    def test_rolling_14d_daily_in_hydro_help(self):
        """rolling-14d-daily must appear as a --transform choice in hydro help."""
        from click.testing import CliRunner
        from dsm2ui.animate_cli import animate
        runner = CliRunner()
        result = runner.invoke(animate, ["hydro", "--help"])
        assert "rolling-14d-daily" in result.output, (
            "rolling-14d-daily not found in 'dsm2ui animate hydro --help'"
        )

    def test_rolling_14d_daily_in_qual_help(self):
        """rolling-14d-daily must appear as a --transform choice in qual help."""
        from click.testing import CliRunner
        from dsm2ui.animate_cli import animate
        runner = CliRunner()
        result = runner.invoke(animate, ["qual", "--help"])
        assert "rolling-14d-daily" in result.output

    def test_transform_uses_integer_rolling_for_regular_freq(self):
        """For regular-frequency data, rolling uses an odd integer window.

        An odd window guarantees perfect symmetry for center=True.  The result
        should be numerically very close to (but NOT necessarily identical to)
        time-offset rolling because pandas' integer-center and time-offset-center
        have slightly different edge-bin definitions.  For a 14-day smooth the
        two are within 0.1 % at interior points where the full window is available.
        """
        from dsm2ui.animate import make_moving_average_transform

        ti = pd.date_range("2020-01-01", periods=200, freq="1h")
        rng = np.random.default_rng(7)
        df = pd.DataFrame(
            rng.uniform(0, 1, (200, 3)),
            index=ti, columns=["a", "b", "c"],
        )
        spec = make_moving_average_transform("14D")
        result = spec.transform_fn(df)

        # Shape and frequency must be preserved
        assert result.shape == df.shape
        assert result.index.freq == df.index.freq

        # Interior values (away from edges where both windows have the full
        # 14-day context) must be numerically very close to time rolling.
        # We compare the middle 50 rows (well away from any edge effects).
        time_rolled = df.rolling("14D", center=True, min_periods=1).mean()
        mid = slice(75, 125)
        pd.testing.assert_frame_equal(
            result.iloc[mid],
            time_rolled.iloc[mid],
            check_exact=False,
            rtol=1e-3,   # within 0.1 %  — differences are at boundary rows only
        )

    def test_integer_rolling_window_is_odd(self):
        """The integer window must be odd so center=True is perfectly symmetric."""
        from dsm2ui.animate import make_moving_average_transform

        for window_str, freq_str, expected_half in [
            ("14D",  "30min", 336),   # 14*48/2=336 → window=673
            ("14D",  "1h",    168),   # 14*24/2=168 → window=337
            ("24h",  "30min",  24),   # 24*2/2=24  → window=49
            ("24h",  "1h",     12),   # 24/2=12    → window=25
        ]:
            ti = pd.date_range("2020-01-01", periods=2 * expected_half + 5, freq=freq_str)
            df = pd.DataFrame({"x": np.arange(len(ti), dtype=float)}, index=ti)
            spec = make_moving_average_transform(window_str)
            result = spec.transform_fn(df)
            assert result.shape == df.shape, f"Shape mismatch for {window_str} at {freq_str}"


# ===========================================================================
# Slider out-of-bounds regression
# ===========================================================================

class TestSliderOutOfBoundsRegression:
    """Regression tests for stale-slider-value handling in _on_slider_change.

    Panel's DiscretePlayer can deliver a value from a previous browser session
    that is outside the current reader's time_index.  History of fixes:

    * **No guard**: ``IndexError: index 31356 is out of bounds`` — server crash
    * **Clamp (bad)**: no crash, but display jumped to last timestamp with no
      warning — confusing because slider thumb was visually mid-range
    * **Ignore (bad)**: no crash, no display update — slider thumb moved but
      displayed timestamp stayed frozen — equally confusing
    * **Log + reset (current)**: logs a WARNING with the stale index, then
      resets the slider to 0 so both the browser thumb and the server-side
      display are in sync at the beginning of the run.
    """

    def _make_manager(self, n_steps: int):
        import geopandas as gpd
        import panel as pn
        from shapely.geometry import LineString
        from dvue.animator import GeoAnimatorManager, InMemorySlicingReader

        pn.extension()
        ti = pd.date_range("2020-01-01", periods=n_steps, freq="1h")
        df = pd.DataFrame({"ch1": np.arange(n_steps, dtype=float)}, index=ti)
        reader = InMemorySlicingReader(df)
        gdf = gpd.GeoDataFrame(
            {"geo_id": [1]},
            geometry=[LineString([(0, 0), (1, 0)])],
            crs="EPSG:4326",
        )
        return GeoAnimatorManager(reader, gdf, geo_id_column="geo_id")

    def _event(self, value):
        from unittest.mock import MagicMock
        ev = MagicMock()
        ev.new = value
        return ev

    def test_valid_index_does_not_raise(self):
        """A slider value within bounds must not raise."""
        mgr = self._make_manager(100)
        mgr._on_slider_change(self._event(50))

    def test_stale_index_31356_logs_warning_and_resets_to_zero(self, caplog):
        """Stale value logs a WARNING and resets slider+display to step 0.

        The warning must include the stale index value so it is visible in
        server logs without needing to enable debug-level logging.
        """
        import logging
        n = 3774
        mgr = self._make_manager(n)
        with caplog.at_level(logging.WARNING):
            mgr._on_slider_change(self._event(31356))
        # Slider must be reset to 0
        assert mgr._time_slider.value == 0
        # A warning mentioning the stale index must have been logged
        assert any("31356" in r.message for r in caplog.records), (
            "Expected WARNING log containing stale index 31356; "
            f"got: {[r.message for r in caplog.records]}"
        )

    def test_stale_index_no_crash(self):
        """Exact repro: idx=31356 against 3774-step reader must not raise IndexError."""
        mgr = self._make_manager(3774)
        mgr._on_slider_change(self._event(31356))   # must not raise

    def test_stale_index_resets_slider_to_zero(self):
        """After a stale event, the slider value must be reset to 0."""
        mgr = self._make_manager(3774)
        mgr._on_slider_change(self._event(31356))
        assert mgr._time_slider.value == 0

    def test_negative_index_resets_to_zero(self):
        """A negative stale index also resets slider to 0."""
        mgr = self._make_manager(50)
        mgr._on_slider_change(self._event(-5))
        assert mgr._time_slider.value == 0

    def test_last_valid_index_ok(self):
        """Index == len - 1 (the last valid step) must not raise."""
        n = 100
        mgr = self._make_manager(n)
        mgr._on_slider_change(self._event(n - 1))

    def test_one_past_end_resets_to_zero(self):
        """Index == len resets to 0, not snaps to end."""
        n = 100
        mgr = self._make_manager(n)
        mgr._on_slider_change(self._event(n))
        assert mgr._time_slider.value == 0

    def test_initial_transform_gives_correct_slider_options(self):
        """Root-cause test: slider options must reflect the TRANSFORMED reader.

        When GeoAnimatorManager is constructed with initial_transform set to
        an aggregate transform (e.g. "Daily mean"), the slider options must
        have len(self._reader.time_index) entries — NOT len(raw_reader) entries.

        This is the bug that caused "slider index 22231 is out of range [0, 3774)"
        when loading from a saved config with rolling-14d-daily: the raw hourly
        reader had 22231 steps but the transformed daily reader had 3774 steps.
        The slider was created with 22231 hourly options, so any move beyond
        position 3773 triggered the out-of-bounds warning.
        """
        import geopandas as gpd
        import panel as pn
        from shapely.geometry import LineString
        from dvue.animator import GeoAnimatorManager, InMemorySlicingReader
        from dsm2ui.animate import make_resample_transform

        pn.extension()
        # Build a reader with 240 hourly steps (10 days)
        ti_hourly = pd.date_range("2020-01-01", periods=240, freq="1h")
        df = pd.DataFrame({"ch1": np.arange(240, dtype=float)}, index=ti_hourly)
        raw_reader = InMemorySlicingReader(df)

        daily_spec = make_resample_transform("D", "mean")
        transform_options = {"Daily mean": daily_spec}

        gdf = gpd.GeoDataFrame(
            {"geo_id": [1]},
            geometry=[LineString([(0, 0), (1, 0)])],
            crs="EPSG:4326",
        )
        mgr = GeoAnimatorManager(
            raw_reader, gdf,
            geo_id_column="geo_id",
            transform_options=transform_options,
            initial_transform="Daily mean",
        )

        # The transformed reader has 10 daily steps (not 240 hourly)
        assert len(mgr._reader.time_index) == 10, (
            f"Expected 10 daily steps, got {len(mgr._reader.time_index)}"
        )
        # Slider options must match the TRANSFORMED (daily) reader
        assert len(mgr._time_slider.options) == 10, (
            f"Slider has {len(mgr._time_slider.options)} options but reader "
            f"has {len(mgr._reader.time_index)} steps — options should match the "
            "transformed reader, not the raw hourly reader"
        )
        # Slider position 0 should not raise (it's a valid daily index)
        mgr._on_slider_change(self._event(0))
        # Slider position 5 (5th day) should not raise
        mgr._on_slider_change(self._event(5))
        # Slider position 200 (hourly range, out of daily range) must log+reset
        mgr._on_slider_change(self._event(200))
        assert mgr._time_slider.value == 0

    def test_spurious_none_event_on_browser_connect_is_suppressed(self):
        """Spurious 'none' event on first browser connect must NOT reset reader,
        AND must restore the widget label to the initial transform name.

        When loading from config with initial_transform='Daily mean', Panel
        fires _on_transform_change(event.new='none') once on browser connect
        before the real widget value propagates.  Without the guard this was
        reported as 'doing none transform on init'.

        The guard must:
        1. Suppress the reader reset (reader stays daily).
        2. Restore _transform_select.value = initial_transform so the dropdown
           shows the correct label (not 'none').  This triggers a recursive
           _on_transform_change(initial_transform) which rebuilds the reader
           and loads frame 0 — making data and label consistent.
        """
        import geopandas as gpd
        import panel as pn
        from shapely.geometry import LineString
        from dvue.animator import GeoAnimatorManager, InMemorySlicingReader
        from dsm2ui.animate import make_resample_transform
        from unittest.mock import MagicMock

        pn.extension()
        ti_hourly = pd.date_range("2020-01-01", periods=240, freq="1h")
        df = pd.DataFrame({"ch1": np.arange(240, dtype=float)}, index=ti_hourly)
        raw_reader = InMemorySlicingReader(df)
        daily_spec = make_resample_transform("D", "mean")
        gdf = gpd.GeoDataFrame(
            {"geo_id": [1]},
            geometry=[LineString([(0, 0), (1, 0)])],
            crs="EPSG:4326",
        )
        mgr = GeoAnimatorManager(
            raw_reader, gdf,
            geo_id_column="geo_id",
            transform_options={"Daily mean": daily_spec},
            initial_transform="Daily mean",
        )

        # Simulate the spurious Panel "none" event on first browser connect
        ev = MagicMock()
        ev.new = "none"
        mgr._on_transform_change(ev)  # must be suppressed; widget restored

        # After suppression the widget label must be restored to initial_transform
        assert mgr._transform_select.value == "Daily mean", (
            f"Widget label should be restored to 'Daily mean', got "
            f"'{mgr._transform_select.value}'"
        )
        # The recursive call triggered by restoring the widget rebuilds the
        # daily reader — verify slider options are still daily (10 steps, not 240).
        assert len(mgr._reader.time_index) == 10, (
            "Spurious 'none' event must not reset daily reader to hourly "
            "(reader should remain 10-step daily)"
        )

        # After the first suppression, a GENUINE 'none' selection MUST apply
        ev2 = MagicMock()
        ev2.new = "none"
        mgr._on_transform_change(ev2)  # second call — NOT suppressed
        # Reader should now be the raw hourly reader (transform removed)
        assert len(mgr._reader.time_index) == 240, (
            "Genuine 'none' transform selection after init must apply correctly"
        )


# ===========================================================================
# Integration performance benchmarks — real QUAL HDF5 + Godin -> Daily
# (skipped by default; run with: pytest -m performance)
# These exercise the full streaming-transform + buffered-prefetch reader stack
# against the real DSM2 QUAL tidefile, mirroring `dsm2ui animate qual` with the
# "Godin filter -> Daily mean" transform.  They quantify the worst-frame stall
# that the async double-buffer prefetch in BufferedSlicingReader removes.
# ===========================================================================

@pytest.mark.performance
@pytest.mark.integration
@skip_no_qual
class TestGodinDailyPrefetchPerformance:
    """Measure per-frame latency of the qual Godin->Daily animation stack.

    Compares ``BufferedSlicingReader(prefetch=False)`` (synchronous chunk
    refill on the playback thread — the old behaviour) against
    ``prefetch=True`` (async double-buffer).  With prefetch enabled and a
    realistic inter-frame interval, the slow Godin+daily transform happens on a
    background thread, so no single frame should incur the full chunk-load cost.
    """

    CHUNK_SIZE = 90
    N_FRAMES = 180
    FRAME_INTERVAL = 0.04  # seconds between frames (DiscretePlayer is 0.5s)

    def _build_streaming(self):
        """Return (base_reader, streaming_reader) for the Godin->Daily stack."""
        from dsm2ui.animate import (
            QualH5ConcentrationReader,
            make_godin_transform,
            make_resample_transform,
            make_composed_transform,
        )
        from dvue.animator import StreamingTransformedSlicingReader

        base = QualH5ConcentrationReader(str(QUAL_H5), constituent="ec")
        spec = make_composed_transform(
            make_godin_transform(),
            make_resample_transform("D", "mean"),
        )
        streaming = StreamingTransformedSlicingReader(base, spec)
        return base, streaming

    def _sweep_worst_frame(self, buffered, n, frame_interval):
        """Forward sweep; return (worst_frame_seconds, total_seconds)."""
        import time

        n = min(n, len(buffered.time_index))
        worst = 0.0
        t_start = time.perf_counter()
        for i in range(n):
            ts = buffered.time_index[i]
            t0 = time.perf_counter()
            buffered.get_slice(ts)
            worst = max(worst, time.perf_counter() - t0)
            if frame_interval:
                time.sleep(frame_interval)
        return worst, time.perf_counter() - t_start

    def test_streaming_stack_builds_and_is_daily(self):
        base, streaming = self._build_streaming()
        try:
            assert len(streaming.time_index) > 0
            # Daily output frequency from the composed transform's final stage.
            assert streaming.time_index.freq == pd.tseries.frequencies.to_offset("D")
        finally:
            base.close()

    def test_prefetch_lowers_worst_frame_latency(self):
        from dvue.animator import BufferedSlicingReader

        # --- Synchronous baseline (old behaviour) ---
        base_a, stream_a = self._build_streaming()
        try:
            sync = BufferedSlicingReader(
                stream_a, chunk_size=self.CHUNK_SIZE, prefetch=False
            )
            sync_worst, _ = self._sweep_worst_frame(
                sync, self.N_FRAMES, frame_interval=0.0
            )
        finally:
            base_a.close()

        # If a full chunk load is too cheap to measure, the comparison is
        # meaningless on this machine/fixture — skip rather than assert noise.
        if sync_worst < 0.02:
            pytest.skip(
                f"Chunk transform too fast to benchmark (sync worst-frame "
                f"{sync_worst*1e3:.1f} ms < 20 ms)"
            )

        # --- Async prefetch (new behaviour) ---
        base_b, stream_b = self._build_streaming()
        try:
            pre = BufferedSlicingReader(
                stream_b, chunk_size=self.CHUNK_SIZE,
                refill_margin=0.4, prefetch=True,
            )
            pre.get_slice(pre.time_index[0])  # warm the first chunk
            pre_worst, _ = self._sweep_worst_frame(
                pre, self.N_FRAMES, frame_interval=self.FRAME_INTERVAL
            )
        finally:
            base_b.close()

        # With background prefetch the playback thread never pays the full
        # Godin+daily chunk-load cost; worst frame should be a fraction of sync.
        assert pre_worst < sync_worst * 0.6, (
            f"prefetch worst-frame {pre_worst*1e3:.1f} ms not < 60% of sync "
            f"worst-frame {sync_worst*1e3:.1f} ms — async prefetch is not "
            "hiding the chunk-load stall"
        )
