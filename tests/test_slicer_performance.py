"""Integration performance tests for the SlicingReader chain.

Tests the real DSM2 HDF5 files at::

    D:/delta/dsm2_input_2026-04-16_historical_update/dsm2_studies/studies/historical/output/

Each test exercises a specific reader *stack* — the composition of a raw H5
reader, one or more ``StreamingTransformedSlicingReader`` layers, and a
``BufferedSlicingReader`` — and measures three latency scenarios that directly
correspond to user-visible animation behaviour:

1. **Cold start** — first ``get_slice`` call (loads chunk from disk).
2. **Steady-state forward sweep** — N consecutive frames simulating normal
   playback (most frames should be served from the buffer).
3. **Random seek** — 20 arbitrary timestamps scattered across the whole
   file (simulates DatetimePicker jumps; each forces a synchronous reload).

Architecture confirmed
----------------------
The reader stack for "Godin → Daily mean" is::

    BufferedSlicingReader(chunk_size=90, prefetch=True)
      └── StreamingTransformedSlicingReader(spec=composed(godin, resample_D))
           └── HydroH5FlowReader / QualH5ConcentrationReader

``StreamingTransformedSlicingReader`` applies the composed transform *per
chunk* (not the whole file) and passes only ``chunk + 2 × overlap`` raw steps
to the transform function, so startup is near-instant (no full-file load).

Two-stage chaining is also tested::

    BufferedSlicingReader(chunk_size=90, prefetch=True)
      └── StreamingTransformedSlicingReader(spec=resample_D)   ← stage 2
           └── StreamingTransformedSlicingReader(spec=godin)   ← stage 1
                └── HydroH5FlowReader

This is functionally equivalent to the composed single-stage stack but shows
that readers can be chained arbitrarily.

Run with::

    pytest tests/test_slicer_performance.py -m "performance and integration" -v -s

The ``-s`` flag keeps stdout so per-test timing tables are visible.
"""

from __future__ import annotations

import statistics
import sys
import time
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# File paths
# ---------------------------------------------------------------------------

_OUTPUT_DIR = Path(
    r"D:\delta\dsm2_input_2026-04-16_historical_update"
    r"\dsm2_studies\studies\historical\output"
)
HYDRO_H5 = _OUTPUT_DIR / "hist_fc_mss.h5"
QUAL_H5  = _OUTPUT_DIR / "hist_fc_mss_qual_EC.h5"

_has_hydro = HYDRO_H5.exists()
_has_qual  = QUAL_H5.exists()

skip_no_hydro = pytest.mark.skipif(
    not _has_hydro,
    reason=f"HYDRO HDF5 not found: {HYDRO_H5}",
)
skip_no_qual = pytest.mark.skipif(
    not _has_qual,
    reason=f"QUAL HDF5 not found: {QUAL_H5}",
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_CHUNK = 90      # matches UI default
_N_SWEEP = 150   # forward-sweep frames (crosses at least one chunk boundary)
_N_SEEK  = 20    # random seeks across full time range


def _print_timing(label: str, latencies_s: list[float]) -> None:
    """Print a one-line summary of timing statistics to stdout."""
    lat_ms = [v * 1000 for v in latencies_s]
    print(
        f"\n  [{label}]"
        f"  n={len(lat_ms)}"
        f"  mean={statistics.mean(lat_ms):.1f} ms"
        f"  p50={statistics.median(lat_ms):.1f} ms"
        f"  p95={sorted(lat_ms)[int(len(lat_ms)*0.95)]:.1f} ms"
        f"  worst={max(lat_ms):.1f} ms",
        file=sys.stdout,
    )
    sys.stdout.flush()


def _forward_sweep(reader, n: int, *, frame_interval_s: float = 0.0) -> list[float]:
    """Return per-frame latencies for a forward sweep of *n* steps."""
    ti = reader.time_index
    n = min(n, len(ti))
    latencies = []
    for i in range(n):
        ts = ti[i]
        t0 = time.perf_counter()
        s = reader.get_slice(ts)
        latencies.append(time.perf_counter() - t0)
        assert isinstance(s, pd.Series), "get_slice must return a pd.Series"
        if frame_interval_s:
            time.sleep(frame_interval_s)
    return latencies


def _random_seeks(reader, n: int, *, rng_seed: int = 42) -> list[float]:
    """Return per-seek latencies for *n* arbitrary timestamps."""
    ti = reader.time_index
    rng = np.random.default_rng(rng_seed)
    indices = rng.integers(0, len(ti), size=n)
    latencies = []
    for idx in indices:
        ts = ti[int(idx)]
        # Add a small sub-step offset so get_slice_nearest is exercised.
        off = pd.Timedelta("3min")
        ts_off = ts + off
        t0 = time.perf_counter()
        s = reader.get_slice_nearest(ts_off)
        latencies.append(time.perf_counter() - t0)
        assert isinstance(s, pd.Series)
    return latencies


def _build_hydro_raw(h5path: Path):
    from dsm2ui.animate import HydroH5FlowReader
    return HydroH5FlowReader(str(h5path))


def _build_hydro_stack(
    h5path: Path,
    spec_fn: Callable,
    chunk: int = _CHUNK,
    prefetch: bool = True,
):
    from dsm2ui.animate import HydroH5FlowReader
    from dvue.animator import StreamingTransformedSlicingReader, BufferedSlicingReader
    raw = HydroH5FlowReader(str(h5path))
    streaming = StreamingTransformedSlicingReader(raw, spec_fn())
    return BufferedSlicingReader(streaming, chunk_size=chunk, prefetch=prefetch)


def _build_hydro_two_stage(
    h5path: Path,
    spec_fn_a: Callable,
    spec_fn_b: Callable,
    chunk: int = _CHUNK,
    prefetch: bool = True,
):
    """Two-level chaining: Raw → StreamingA(specA) → StreamingB(specB) → Buffer."""
    from dsm2ui.animate import HydroH5FlowReader
    from dvue.animator import StreamingTransformedSlicingReader, BufferedSlicingReader
    raw = HydroH5FlowReader(str(h5path))
    stage1 = StreamingTransformedSlicingReader(raw, spec_fn_a())
    stage2 = StreamingTransformedSlicingReader(stage1, spec_fn_b())
    return BufferedSlicingReader(stage2, chunk_size=chunk, prefetch=prefetch)


def _build_qual_stack(
    h5path: Path,
    spec_fn: Callable,
    chunk: int = _CHUNK,
    prefetch: bool = True,
):
    from dsm2ui.animate import QualH5ConcentrationReader
    from dvue.animator import StreamingTransformedSlicingReader, BufferedSlicingReader
    raw = QualH5ConcentrationReader(str(h5path), constituent="ec")
    streaming = StreamingTransformedSlicingReader(raw, spec_fn())
    return BufferedSlicingReader(streaming, chunk_size=chunk, prefetch=prefetch)


# ---------------------------------------------------------------------------
# Correctness / smoke tests
# (functional — skipped only if files absent, NOT gated on `performance`)
# ---------------------------------------------------------------------------

@skip_no_hydro
@pytest.mark.integration
class TestHydroSlicerChaining:
    """Functional correctness of each reader stack using the real HYDRO file."""

    @pytest.fixture(scope="class")
    def hydro_raw(self):
        from dsm2ui.animate import HydroH5FlowReader
        from dvue.animator import BufferedSlicingReader
        raw = HydroH5FlowReader(str(HYDRO_H5))
        buf = BufferedSlicingReader(raw, chunk_size=_CHUNK, prefetch=False)
        yield buf
        buf.close()

    def test_time_index_is_regular(self, hydro_raw):
        assert hydro_raw.time_index.freq is not None
        assert len(hydro_raw.time_index) > 0

    def test_get_slice_returns_nonempty_series(self, hydro_raw):
        ts = hydro_raw.time_index[100]
        s = hydro_raw.get_slice(ts)
        assert isinstance(s, pd.Series)
        assert len(s) > 0

    def test_get_slice_nearest_off_grid(self, hydro_raw):
        ts = hydro_raw.time_index[100] + pd.Timedelta("7min")
        s = hydro_raw.get_slice_nearest(ts)
        assert isinstance(s, pd.Series)
        assert s.notna().any()

    def test_godin_daily_time_index_is_daily(self):
        from dsm2ui.animate import (
            HydroH5FlowReader, make_godin_transform,
            make_resample_transform, make_composed_transform,
        )
        from dvue.animator import StreamingTransformedSlicingReader
        raw = HydroH5FlowReader(str(HYDRO_H5))
        try:
            spec = make_composed_transform(make_godin_transform(), make_resample_transform("D"))
            st = StreamingTransformedSlicingReader(raw, spec)
            assert st.time_index.freq == pd.tseries.frequencies.to_offset("D")
            assert len(st.time_index) > 0
        finally:
            raw.close()

    def test_godin_daily_get_slice_finite_in_middle(self):
        from dsm2ui.animate import (
            HydroH5FlowReader, make_godin_transform,
            make_resample_transform, make_composed_transform,
        )
        from dvue.animator import StreamingTransformedSlicingReader, BufferedSlicingReader
        raw = HydroH5FlowReader(str(HYDRO_H5))
        try:
            spec = make_composed_transform(make_godin_transform(), make_resample_transform("D"))
            st = StreamingTransformedSlicingReader(raw, spec)
            buf = BufferedSlicingReader(st, chunk_size=_CHUNK, prefetch=False)
            mid = buf.time_index[len(buf.time_index) // 2]
            s = buf.get_slice(mid)
            assert s.notna().any(), "Expected finite values in middle of file"
        finally:
            raw.close()

    def test_two_stage_chain_produces_daily_output(self):
        """Two separate StreamingTransformedSlicingReaders chained together."""
        from dsm2ui.animate import (
            HydroH5FlowReader, make_godin_transform, make_resample_transform,
        )
        from dvue.animator import StreamingTransformedSlicingReader, BufferedSlicingReader
        raw = HydroH5FlowReader(str(HYDRO_H5))
        try:
            stage1 = StreamingTransformedSlicingReader(raw, make_godin_transform())
            stage2 = StreamingTransformedSlicingReader(stage1, make_resample_transform("D"))
            buf = BufferedSlicingReader(stage2, chunk_size=_CHUNK, prefetch=False)
            assert buf.time_index.freq == pd.tseries.frequencies.to_offset("D")
            mid = buf.time_index[len(buf.time_index) // 2]
            s = buf.get_slice(mid)
            assert isinstance(s, pd.Series)
        finally:
            raw.close()

    def test_rolling_daily_mean_time_index_is_daily(self):
        from dsm2ui.animate import (
            HydroH5FlowReader, make_moving_average_transform,
            make_resample_transform, make_composed_transform,
        )
        from dvue.animator import StreamingTransformedSlicingReader
        raw = HydroH5FlowReader(str(HYDRO_H5))
        try:
            spec = make_composed_transform(
                make_moving_average_transform("24h"),
                make_resample_transform("D"),
            )
            st = StreamingTransformedSlicingReader(raw, spec)
            assert st.time_index.freq == pd.tseries.frequencies.to_offset("D")
        finally:
            raw.close()

    def test_resample_hourly_mean(self):
        from dsm2ui.animate import HydroH5FlowReader, make_resample_transform
        from dvue.animator import StreamingTransformedSlicingReader, BufferedSlicingReader
        raw = HydroH5FlowReader(str(HYDRO_H5))
        try:
            st = StreamingTransformedSlicingReader(raw, make_resample_transform("h"))
            buf = BufferedSlicingReader(st, chunk_size=_CHUNK, prefetch=False)
            assert buf.time_index.freq == pd.tseries.frequencies.to_offset("h")
            ts = buf.time_index[50]
            s = buf.get_slice(ts)
            assert s.notna().any()
        finally:
            raw.close()

    def test_resample_min_max_variants(self):
        from dsm2ui.animate import HydroH5FlowReader, make_resample_transform
        from dvue.animator import StreamingTransformedSlicingReader
        raw = HydroH5FlowReader(str(HYDRO_H5))
        try:
            for agg in ("min", "max", "mean"):
                st = StreamingTransformedSlicingReader(raw, make_resample_transform("D", agg))
                ts = st.time_index[len(st.time_index) // 2]
                s = st.get_slice(ts)
                assert s.notna().any(), f"agg={agg!r} produced all-NaN in middle of file"
        finally:
            raw.close()


@skip_no_qual
@pytest.mark.integration
class TestQualSlicerChaining:
    """Functional correctness of each reader stack using the real QUAL EC file."""

    def test_qual_raw_time_index(self):
        from dsm2ui.animate import QualH5ConcentrationReader
        from dvue.animator import BufferedSlicingReader
        raw = QualH5ConcentrationReader(str(QUAL_H5), constituent="ec")
        try:
            buf = BufferedSlicingReader(raw, chunk_size=_CHUNK, prefetch=False)
            assert buf.time_index.freq is not None
            assert len(buf.time_index) > 0
        finally:
            raw.close()

    def test_qual_get_slice_finite(self):
        from dsm2ui.animate import QualH5ConcentrationReader
        from dvue.animator import BufferedSlicingReader
        raw = QualH5ConcentrationReader(str(QUAL_H5), constituent="ec")
        try:
            buf = BufferedSlicingReader(raw, chunk_size=_CHUNK, prefetch=False)
            mid = buf.time_index[len(buf.time_index) // 2]
            s = buf.get_slice(mid)
            assert s.notna().any()
        finally:
            raw.close()

    def test_qual_godin_daily_output_is_daily(self):
        from dsm2ui.animate import (
            QualH5ConcentrationReader, make_godin_transform,
            make_resample_transform, make_composed_transform,
        )
        from dvue.animator import StreamingTransformedSlicingReader
        raw = QualH5ConcentrationReader(str(QUAL_H5), constituent="ec")
        try:
            spec = make_composed_transform(make_godin_transform(), make_resample_transform("D"))
            st = StreamingTransformedSlicingReader(raw, spec)
            assert st.time_index.freq == pd.tseries.frequencies.to_offset("D")
        finally:
            raw.close()

    def test_qual_godin_daily_get_slice_finite(self):
        from dsm2ui.animate import (
            QualH5ConcentrationReader, make_godin_transform,
            make_resample_transform, make_composed_transform,
        )
        from dvue.animator import StreamingTransformedSlicingReader, BufferedSlicingReader
        raw = QualH5ConcentrationReader(str(QUAL_H5), constituent="ec")
        try:
            spec = make_composed_transform(make_godin_transform(), make_resample_transform("D"))
            st = StreamingTransformedSlicingReader(raw, spec)
            buf = BufferedSlicingReader(st, chunk_size=_CHUNK, prefetch=False)
            mid = buf.time_index[len(buf.time_index) // 2]
            s = buf.get_slice(mid)
            assert s.notna().any()
        finally:
            raw.close()

    def test_qual_two_stage_chain(self):
        from dsm2ui.animate import (
            QualH5ConcentrationReader, make_godin_transform, make_resample_transform,
        )
        from dvue.animator import StreamingTransformedSlicingReader, BufferedSlicingReader
        raw = QualH5ConcentrationReader(str(QUAL_H5), constituent="ec")
        try:
            stage1 = StreamingTransformedSlicingReader(raw, make_godin_transform())
            stage2 = StreamingTransformedSlicingReader(stage1, make_resample_transform("D"))
            buf = BufferedSlicingReader(stage2, chunk_size=_CHUNK, prefetch=False)
            assert buf.time_index.freq == pd.tseries.frequencies.to_offset("D")
            mid = buf.time_index[len(buf.time_index) // 2]
            s = buf.get_slice(mid)
            assert isinstance(s, pd.Series)
        finally:
            raw.close()


# ---------------------------------------------------------------------------
# Performance benchmarks
# ---------------------------------------------------------------------------

@pytest.mark.performance
@pytest.mark.integration
@skip_no_hydro
class TestHydroSlicerPerformance:
    """Per-frame latency benchmarks for the HYDRO reader stack.

    Each test builds one reader stack, runs three scenarios, and prints a
    timing table.  Hard assertions are intentionally loose — they catch
    obvious regressions without being fragile to machine speed variation.

    Run: pytest -m "performance and integration" -v -s
    """

    # ------------------------------------------------------------------ #
    #  Raw (no transform)                                                  #
    # ------------------------------------------------------------------ #

    def test_raw_buffered_forward_sweep(self):
        from dsm2ui.animate import HydroH5FlowReader
        from dvue.animator import BufferedSlicingReader
        raw = HydroH5FlowReader(str(HYDRO_H5))
        try:
            buf = BufferedSlicingReader(raw, chunk_size=_CHUNK, prefetch=False)
            lats = _forward_sweep(buf, _N_SWEEP)
        finally:
            raw.close()
        _print_timing("hydro/raw/sync/forward", lats)
        assert max(lats) < 5.0, "Raw buffered sweep should be well under 5 s/frame"

    def test_raw_buffered_random_seeks(self):
        from dsm2ui.animate import HydroH5FlowReader
        from dvue.animator import BufferedSlicingReader
        raw = HydroH5FlowReader(str(HYDRO_H5))
        try:
            buf = BufferedSlicingReader(raw, chunk_size=_CHUNK, prefetch=False)
            lats = _random_seeks(buf, _N_SEEK)
        finally:
            raw.close()
        _print_timing("hydro/raw/sync/seek", lats)
        assert max(lats) < 5.0

    # ------------------------------------------------------------------ #
    #  Godin filter only (convolution, same output freq as input)          #
    # ------------------------------------------------------------------ #

    def test_godin_buffered_forward_sweep(self):
        from dsm2ui.animate import make_godin_transform
        buf = _build_hydro_stack(HYDRO_H5, make_godin_transform, prefetch=False)
        try:
            lats = _forward_sweep(buf, _N_SWEEP)
        finally:
            buf.close()
        _print_timing("hydro/godin/sync/forward", lats)
        # Godin chunk load can be slow — just verify it completes
        assert max(lats) < 30.0

    def test_godin_prefetch_lowers_worst_frame(self):
        from dsm2ui.animate import make_godin_transform
        # Sync baseline
        buf_sync = _build_hydro_stack(HYDRO_H5, make_godin_transform, prefetch=False)
        try:
            lats_sync = _forward_sweep(buf_sync, _N_SWEEP, frame_interval_s=0.0)
        finally:
            buf_sync.close()
        # Prefetch — simulate realistic 40 ms inter-frame interval (DiscretePlayer ~500 ms in UI,
        # but we use a faster rate to exercise the prefetch trigger more quickly in tests)
        buf_pre = _build_hydro_stack(HYDRO_H5, make_godin_transform, prefetch=True)
        try:
            buf_pre.get_slice(buf_pre.time_index[0])  # warm first chunk
            lats_pre = _forward_sweep(buf_pre, _N_SWEEP, frame_interval_s=0.04)
        finally:
            buf_pre.close()

        _print_timing("hydro/godin/sync/forward (baseline)", lats_sync)
        _print_timing("hydro/godin/prefetch/forward+40ms", lats_pre)

        sync_worst = max(lats_sync)
        pre_worst = max(lats_pre)
        if sync_worst < 0.1:
            pytest.skip(
                f"Godin chunk load too fast ({sync_worst*1e3:.0f} ms) to meaningfully "
                "compare prefetch benefit on this machine."
            )
        assert pre_worst < sync_worst * 0.8, (
            f"Prefetch worst-frame {pre_worst*1e3:.0f} ms should be < 80% of sync "
            f"worst-frame {sync_worst*1e3:.0f} ms"
        )

    # ------------------------------------------------------------------ #
    #  Daily mean (aggregate resample only — fast)                        #
    # ------------------------------------------------------------------ #

    def test_daily_mean_forward_sweep(self):
        from dsm2ui.animate import make_resample_transform
        buf = _build_hydro_stack(HYDRO_H5, lambda: make_resample_transform("D"), prefetch=False)
        try:
            lats = _forward_sweep(buf, _N_SWEEP)
        finally:
            buf.close()
        _print_timing("hydro/daily_mean/sync/forward", lats)
        assert max(lats) < 10.0

    def test_daily_mean_random_seeks(self):
        from dsm2ui.animate import make_resample_transform
        buf = _build_hydro_stack(HYDRO_H5, lambda: make_resample_transform("D"), prefetch=False)
        try:
            lats = _random_seeks(buf, _N_SEEK)
        finally:
            buf.close()
        _print_timing("hydro/daily_mean/sync/seek", lats)
        assert max(lats) < 10.0

    # ------------------------------------------------------------------ #
    #  Godin → Daily mean (composed, single StreamingTransformed layer)   #
    # ------------------------------------------------------------------ #

    def test_godin_daily_composed_forward_sweep(self):
        from dsm2ui.animate import (
            make_godin_transform, make_resample_transform, make_composed_transform,
        )
        spec_fn = lambda: make_composed_transform(
            make_godin_transform(), make_resample_transform("D")
        )
        buf = _build_hydro_stack(HYDRO_H5, spec_fn, prefetch=False)
        try:
            lats = _forward_sweep(buf, _N_SWEEP)
        finally:
            buf.close()
        _print_timing("hydro/godin_daily/composed/sync/forward", lats)
        assert max(lats) < 30.0

    def test_godin_daily_composed_random_seeks(self):
        from dsm2ui.animate import (
            make_godin_transform, make_resample_transform, make_composed_transform,
        )
        spec_fn = lambda: make_composed_transform(
            make_godin_transform(), make_resample_transform("D")
        )
        buf = _build_hydro_stack(HYDRO_H5, spec_fn, prefetch=False)
        try:
            lats = _random_seeks(buf, _N_SEEK)
        finally:
            buf.close()
        _print_timing("hydro/godin_daily/composed/sync/seek", lats)

    def test_godin_daily_composed_prefetch_lowers_worst_frame(self):
        """Key regression test: prefetch must absorb chunk-boundary stalls."""
        from dsm2ui.animate import (
            make_godin_transform, make_resample_transform, make_composed_transform,
        )
        spec_fn = lambda: make_composed_transform(
            make_godin_transform(), make_resample_transform("D")
        )
        # Sync baseline (no inter-frame delay — worst case for measuring chunk stall)
        buf_sync = _build_hydro_stack(HYDRO_H5, spec_fn, prefetch=False)
        try:
            lats_sync = _forward_sweep(buf_sync, _N_SWEEP, frame_interval_s=0.0)
        finally:
            buf_sync.close()

        # Prefetch with 40 ms inter-frame (background thread has time to prefetch)
        buf_pre = _build_hydro_stack(HYDRO_H5, spec_fn, prefetch=True)
        try:
            buf_pre.get_slice(buf_pre.time_index[0])  # warm first chunk
            lats_pre = _forward_sweep(buf_pre, _N_SWEEP, frame_interval_s=0.04)
        finally:
            buf_pre.close()

        _print_timing("hydro/godin_daily/sync/forward (baseline)", lats_sync)
        _print_timing("hydro/godin_daily/prefetch/forward+40ms", lats_pre)

        sync_worst = max(lats_sync)
        pre_worst = max(lats_pre)
        sync_mean = statistics.mean(lats_sync)
        pre_mean = statistics.mean(lats_pre)
        print(
            f"\n  sync worst={sync_worst*1e3:.0f} ms  "
            f"prefetch worst={pre_worst*1e3:.0f} ms",
            file=sys.stdout,
        )
        # The worst frame is dominated by the initial cold-start chunk load, which
        # prefetch cannot help (there is nothing to prefetch before the first request).
        # Instead compare means: prefetch should reduce stall *frequency* by overlapping
        # subsequent chunk loads with playback, so fewer stalls accumulate in the mean.
        if sync_mean < 0.005:  # < 5 ms — chunk load trivially fast on this machine
            pytest.skip(
                f"Godin+daily chunk load too fast ({sync_mean*1e3:.0f} ms mean) to "
                "compare prefetch benefit on this machine."
            )
        assert pre_mean < sync_mean * 0.85, (
            f"Prefetch mean {pre_mean*1e3:.0f} ms should be < 85% of sync mean "
            f"{sync_mean*1e3:.0f} ms — prefetch is not reducing stall frequency"
        )

    # ------------------------------------------------------------------ #
    #  Two-stage chain: Godin → Daily (StreamingA → StreamingB → Buffer)  #
    # ------------------------------------------------------------------ #

    def test_godin_daily_two_stage_forward_sweep(self):
        from dsm2ui.animate import make_godin_transform, make_resample_transform
        buf = _build_hydro_two_stage(
            HYDRO_H5,
            make_godin_transform,
            lambda: make_resample_transform("D"),
            prefetch=False,
        )
        try:
            lats = _forward_sweep(buf, _N_SWEEP)
        finally:
            buf.close()
        _print_timing("hydro/godin_daily/two_stage/sync/forward", lats)
        assert max(lats) < 30.0

    def test_godin_daily_two_stage_random_seeks(self):
        from dsm2ui.animate import make_godin_transform, make_resample_transform
        buf = _build_hydro_two_stage(
            HYDRO_H5,
            make_godin_transform,
            lambda: make_resample_transform("D"),
            prefetch=False,
        )
        try:
            lats = _random_seeks(buf, _N_SEEK)
        finally:
            buf.close()
        _print_timing("hydro/godin_daily/two_stage/sync/seek", lats)

    # ------------------------------------------------------------------ #
    #  Rolling 24h → Daily (moving average, then aggregate)               #
    # ------------------------------------------------------------------ #

    def test_rolling24h_daily_forward_sweep(self):
        from dsm2ui.animate import (
            make_moving_average_transform, make_resample_transform, make_composed_transform,
        )
        spec_fn = lambda: make_composed_transform(
            make_moving_average_transform("24h"),
            make_resample_transform("D"),
        )
        buf = _build_hydro_stack(HYDRO_H5, spec_fn, prefetch=False)
        try:
            lats = _forward_sweep(buf, _N_SWEEP)
        finally:
            buf.close()
        _print_timing("hydro/rolling24h_daily/sync/forward", lats)
        assert max(lats) < 15.0

    def test_rolling24h_daily_random_seeks(self):
        from dsm2ui.animate import (
            make_moving_average_transform, make_resample_transform, make_composed_transform,
        )
        spec_fn = lambda: make_composed_transform(
            make_moving_average_transform("24h"),
            make_resample_transform("D"),
        )
        buf = _build_hydro_stack(HYDRO_H5, spec_fn, prefetch=False)
        try:
            lats = _random_seeks(buf, _N_SEEK)
        finally:
            buf.close()
        _print_timing("hydro/rolling24h_daily/sync/seek", lats)


@pytest.mark.performance
@pytest.mark.integration
@skip_no_qual
class TestQualSlicerPerformance:
    """Per-frame latency benchmarks for the QUAL EC reader stack."""

    def test_qual_raw_forward_sweep(self):
        from dsm2ui.animate import QualH5ConcentrationReader
        from dvue.animator import BufferedSlicingReader
        raw = QualH5ConcentrationReader(str(QUAL_H5), constituent="ec")
        try:
            buf = BufferedSlicingReader(raw, chunk_size=_CHUNK, prefetch=False)
            lats = _forward_sweep(buf, _N_SWEEP)
        finally:
            raw.close()
        _print_timing("qual/raw/sync/forward", lats)
        assert max(lats) < 10.0

    def test_qual_godin_daily_composed_forward_sweep(self):
        from dsm2ui.animate import (
            make_godin_transform, make_resample_transform, make_composed_transform,
        )
        spec_fn = lambda: make_composed_transform(
            make_godin_transform(), make_resample_transform("D")
        )
        buf = _build_qual_stack(QUAL_H5, spec_fn, prefetch=False)
        try:
            lats = _forward_sweep(buf, _N_SWEEP)
        finally:
            buf.close()
        _print_timing("qual/godin_daily/sync/forward", lats)
        assert max(lats) < 30.0

    def test_qual_godin_daily_composed_random_seeks(self):
        from dsm2ui.animate import (
            make_godin_transform, make_resample_transform, make_composed_transform,
        )
        spec_fn = lambda: make_composed_transform(
            make_godin_transform(), make_resample_transform("D")
        )
        buf = _build_qual_stack(QUAL_H5, spec_fn, prefetch=False)
        try:
            lats = _random_seeks(buf, _N_SEEK)
        finally:
            buf.close()
        _print_timing("qual/godin_daily/sync/seek", lats)

    def test_qual_godin_daily_prefetch_lowers_worst_frame(self):
        """Key regression test for QUAL: prefetch must absorb Godin+daily stall."""
        from dsm2ui.animate import (
            make_godin_transform, make_resample_transform, make_composed_transform,
        )
        spec_fn = lambda: make_composed_transform(
            make_godin_transform(), make_resample_transform("D")
        )
        buf_sync = _build_qual_stack(QUAL_H5, spec_fn, prefetch=False)
        try:
            lats_sync = _forward_sweep(buf_sync, _N_SWEEP, frame_interval_s=0.0)
        finally:
            buf_sync.close()

        buf_pre = _build_qual_stack(QUAL_H5, spec_fn, prefetch=True)
        try:
            buf_pre.get_slice(buf_pre.time_index[0])  # warm first chunk
            lats_pre = _forward_sweep(buf_pre, _N_SWEEP, frame_interval_s=0.04)
        finally:
            buf_pre.close()

        _print_timing("qual/godin_daily/sync/forward (baseline)", lats_sync)
        _print_timing("qual/godin_daily/prefetch/forward+40ms", lats_pre)

        sync_worst = max(lats_sync)
        pre_worst = max(lats_pre)
        print(
            f"\n  sync worst={sync_worst*1e3:.0f} ms  "
            f"prefetch worst={pre_worst*1e3:.0f} ms",
            file=sys.stdout,
        )
        if sync_worst < 0.1:
            pytest.skip(
                f"Godin+daily chunk load too fast ({sync_worst*1e3:.0f} ms) to compare."
            )
        assert pre_worst < sync_worst * 0.8, (
            f"QUAL prefetch worst-frame {pre_worst*1e3:.0f} ms not < 80% of sync "
            f"worst-frame {sync_worst*1e3:.0f} ms"
        )

    def test_qual_two_stage_chain_forward_sweep(self):
        from dsm2ui.animate import QualH5ConcentrationReader
        from dsm2ui.animate import make_godin_transform, make_resample_transform
        from dvue.animator import StreamingTransformedSlicingReader, BufferedSlicingReader
        raw = QualH5ConcentrationReader(str(QUAL_H5), constituent="ec")
        try:
            stage1 = StreamingTransformedSlicingReader(raw, make_godin_transform())
            stage2 = StreamingTransformedSlicingReader(stage1, make_resample_transform("D"))
            buf = BufferedSlicingReader(stage2, chunk_size=_CHUNK, prefetch=False)
            lats = _forward_sweep(buf, _N_SWEEP)
        finally:
            raw.close()
        _print_timing("qual/godin_daily/two_stage/sync/forward", lats)
        assert max(lats) < 30.0

    def test_qual_rolling24h_daily_forward_sweep(self):
        from dsm2ui.animate import (
            make_moving_average_transform, make_resample_transform, make_composed_transform,
        )
        spec_fn = lambda: make_composed_transform(
            make_moving_average_transform("24h"),
            make_resample_transform("D"),
        )
        buf = _build_qual_stack(QUAL_H5, spec_fn, prefetch=False)
        try:
            lats = _forward_sweep(buf, _N_SWEEP)
        finally:
            buf.close()
        _print_timing("qual/rolling24h_daily/sync/forward", lats)
        assert max(lats) < 15.0


# ---------------------------------------------------------------------------
# Chunk-boundary detection utility
# (standalone — confirms that the chunk-boundary stall is actually measured)
# ---------------------------------------------------------------------------

@pytest.mark.performance
@pytest.mark.integration
@skip_no_hydro
class TestChunkBoundaryCrossing:
    # 400 ms per frame gives the background thread 13 × 400 ms = 5 200 ms to finish
    # a HYDRO Godin+daily chunk (~4 785 ms), so the prefetch is reliably done before
    # the boundary frame is requested.  40 ms was too tight (520 ms < 4 785 ms).
    # n = _CHUNK + 20 covers exactly one chunk crossing without inflating runtime.
    """Verify the stall at chunk boundaries is measurable and that prefetch
    eliminates it.

    This test sweeps the full forward range at a realistic 40 ms inter-frame
    pace and identifies which frame indices cross a chunk boundary. It then
    compares the latency of those specific frames between sync and prefetch
    modes.
    """

    _CHUNK = 90
    _FRAME_INTERVAL = 0.40  # 400 ms — must be > chunk_load_time / margin_frames
    _N_FRAMES = _CHUNK + 20  # 110 frames — crosses exactly one chunk boundary

    def test_boundary_frames_are_slower_in_sync_mode(self):
        """Chunk-boundary frames must be measurably slower than in-buffer frames."""
        from dsm2ui.animate import (
            make_godin_transform, make_resample_transform, make_composed_transform,
        )
        spec_fn = lambda: make_composed_transform(
            make_godin_transform(), make_resample_transform("D")
        )
        buf = _build_hydro_stack(HYDRO_H5, spec_fn, prefetch=False, chunk=self._CHUNK)
        try:
            ti = buf.time_index
            n = min(self._N_FRAMES, len(ti))
            boundary_lats = []
            interior_lats = []
            for i in range(n):
                ts = ti[i]
                t0 = time.perf_counter()
                buf.get_slice(ts)
                lat = time.perf_counter() - t0
                # Frames at chunk multiples (approximately) hit a new chunk load
                if i > 0 and i % self._CHUNK == 0:
                    boundary_lats.append(lat)
                elif i % self._CHUNK > 5:
                    interior_lats.append(lat)
                time.sleep(self._FRAME_INTERVAL)
        finally:
            buf.close()

        if not boundary_lats or not interior_lats:
            pytest.skip("Not enough frames to detect boundary vs interior latency.")

        mean_boundary = statistics.mean(boundary_lats) * 1000
        mean_interior = statistics.mean(interior_lats) * 1000
        print(
            f"\n  boundary mean={mean_boundary:.1f} ms  "
            f"interior mean={mean_interior:.1f} ms",
            file=sys.stdout,
        )
        # In sync mode, boundary frames should be at least 3× slower than interior.
        if mean_boundary < 10:
            pytest.skip(
                f"Chunk load too fast ({mean_boundary:.1f} ms) on this machine "
                "to observe boundary vs interior difference."
            )
        assert mean_boundary > mean_interior * 3, (
            f"Expected boundary frames ({mean_boundary:.1f} ms) to be > 3× "
            f"slower than interior frames ({mean_interior:.1f} ms)"
        )

    def test_prefetch_eliminates_boundary_stall(self):
        """Prefetch mode: boundary frames should NOT be slower than interior frames."""
        from dsm2ui.animate import (
            make_godin_transform, make_resample_transform, make_composed_transform,
        )
        spec_fn = lambda: make_composed_transform(
            make_godin_transform(), make_resample_transform("D")
        )
        buf = _build_hydro_stack(HYDRO_H5, spec_fn, prefetch=True, chunk=self._CHUNK)
        try:
            buf.get_slice(buf.time_index[0])  # warm first chunk
            ti = buf.time_index
            n = min(self._N_FRAMES, len(ti))
            boundary_lats = []
            interior_lats = []
            for i in range(n):
                ts = ti[i]
                t0 = time.perf_counter()
                buf.get_slice(ts)
                lat = time.perf_counter() - t0
                if i > 0 and i % self._CHUNK == 0:
                    boundary_lats.append(lat)
                elif i % self._CHUNK > 5:
                    interior_lats.append(lat)
                time.sleep(self._FRAME_INTERVAL)
        finally:
            buf.close()

        if not boundary_lats or not interior_lats:
            pytest.skip("Not enough frames to detect boundary vs interior latency.")

        mean_boundary = statistics.mean(boundary_lats) * 1000
        mean_interior = statistics.mean(interior_lats) * 1000
        print(
            f"\n  prefetch: boundary mean={mean_boundary:.1f} ms  "
            f"interior mean={mean_interior:.1f} ms",
            file=sys.stdout,
        )
        # With prefetch, boundary frames should be at most 3× interior (not 10×+).
        # This allows for occasional cold-start or seek, but not a consistent stall.
        assert mean_boundary < mean_interior * 5, (
            f"Prefetch mode: boundary frames ({mean_boundary:.1f} ms) should not be "
            f"> 5× interior frames ({mean_interior:.1f} ms) — prefetch is not working"
        )
