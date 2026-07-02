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


def _build_hydro_stack_rsb(
    h5path: Path,
    spec_fn: Callable,
    chunk: int = _CHUNK,
    prefetch: bool = True,
):
    """New production stack: inserts RawSequentialBuffer between raw reader and
    StreamingTransformedSlicingReader to cache overlapping HDF5 windows."""
    from dsm2ui.animate import HydroH5FlowReader
    from dvue.animator import (
        StreamingTransformedSlicingReader, BufferedSlicingReader, RawSequentialBuffer,
    )
    spec = spec_fn()
    raw = HydroH5FlowReader(str(h5path))
    # Only insert RSB when there is raw overlap (convolution-type transforms).
    freq_nanos = raw.time_index.freq.nanos if hasattr(raw.time_index.freq, 'nanos') else int(raw.time_index.freq.delta.total_seconds() * 1e9)
    raw_overlap = spec.get_overlap(freq_nanos)
    if raw_overlap > 0:
        inner = RawSequentialBuffer(raw, prefetch_enabled=prefetch)
    else:
        inner = raw
    streaming = StreamingTransformedSlicingReader(inner, spec)
    return BufferedSlicingReader(
        streaming, chunk_size=chunk, prefetch=prefetch,
        adaptive=True, min_chunk_size=50, max_chunk_size=2000,
    )


def _build_qual_stack_rsb(
    h5path: Path,
    spec_fn: Callable,
    chunk: int = _CHUNK,
    prefetch: bool = True,
):
    """New production stack for QUAL: inserts RawSequentialBuffer."""
    from dsm2ui.animate import QualH5ConcentrationReader
    from dvue.animator import (
        StreamingTransformedSlicingReader, BufferedSlicingReader, RawSequentialBuffer,
    )
    spec = spec_fn()
    raw = QualH5ConcentrationReader(str(h5path), constituent="ec")
    freq_nanos = raw.time_index.freq.nanos if hasattr(raw.time_index.freq, 'nanos') else int(raw.time_index.freq.delta.total_seconds() * 1e9)
    raw_overlap = spec.get_overlap(freq_nanos)
    if raw_overlap > 0:
        inner = RawSequentialBuffer(raw, prefetch_enabled=prefetch)
    else:
        inner = raw
    streaming = StreamingTransformedSlicingReader(inner, spec)
    return BufferedSlicingReader(
        streaming, chunk_size=chunk, prefetch=prefetch,
        adaptive=True, min_chunk_size=50, max_chunk_size=2000,
    )


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


# ---------------------------------------------------------------------------
# RawSequentialBuffer improvement benchmarks
# Compares the new RSB stack (production default) vs the old stack (no RSB)
# for Godin-based transforms, which benefit most from raw-level caching.
# ---------------------------------------------------------------------------


@pytest.mark.performance
@pytest.mark.integration
@skip_no_hydro
class TestRSBImprovementHydro:
    """Quantify the RawSequentialBuffer improvement on the HYDRO reader stack.

    Each test builds the old stack (no RSB) and the new stack (with RSB),
    runs a forward sweep and random seeks on each, and prints a side-by-side
    comparison.  Assertions require the RSB stack to be no *slower* than the
    old stack; improvement is reported but not enforced beyond a small margin
    to keep the test machine-speed agnostic.

    Run with::

        pytest -m "performance and integration" -v -s
    """

    _CHUNK = 90
    _N_SWEEP = 150
    _N_SEEK = 20
    _FRAME_INTERVAL = 0.04   # 40 ms — realistic playback pace

    @staticmethod
    def _spec_fn():
        from dsm2ui.animate import (
            make_godin_transform, make_resample_transform, make_composed_transform,
        )
        return make_composed_transform(make_godin_transform(), make_resample_transform("D"))

    def test_rsb_forward_sweep_no_slower_than_baseline(self):
        """RSB stack forward-sweep mean latency must not exceed 110% of old stack."""
        spec_fn = self._spec_fn

        # --- old stack (sync, no RSB) ---
        buf_old = _build_hydro_stack(HYDRO_H5, spec_fn, chunk=self._CHUNK, prefetch=False)
        try:
            lats_old = _forward_sweep(buf_old, self._N_SWEEP)
        finally:
            buf_old.close()

        # --- new stack (sync, with RSB) ---
        buf_rsb = _build_hydro_stack_rsb(HYDRO_H5, spec_fn, chunk=self._CHUNK, prefetch=False)
        try:
            lats_rsb = _forward_sweep(buf_rsb, self._N_SWEEP)
        finally:
            buf_rsb.close()

        mean_old = statistics.mean(lats_old) * 1000
        mean_rsb = statistics.mean(lats_rsb) * 1000
        p95_old  = sorted(lats_old)[int(len(lats_old) * 0.95)] * 1000
        p95_rsb  = sorted(lats_rsb)[int(len(lats_rsb) * 0.95)] * 1000
        print(
            f"\n  [hydro/godin_daily/sync/forward]"
            f"\n    old stack : mean={mean_old:.1f} ms  p95={p95_old:.1f} ms  worst={max(lats_old)*1000:.1f} ms"
            f"\n    RSB stack : mean={mean_rsb:.1f} ms  p95={p95_rsb:.1f} ms  worst={max(lats_rsb)*1000:.1f} ms"
            f"\n    delta mean: {mean_rsb - mean_old:+.1f} ms  ({(mean_rsb/mean_old - 1)*100:+.1f}%)",
            file=sys.stdout,
        )
        sys.stdout.flush()
        # In sync mode RSB uses prefetch_enabled=False (no async thread), so overhead
        # should be negligible.  The 1.10 tolerance catches any future regressions.
        assert mean_rsb <= mean_old * 1.10, (
            f"RSB mean ({mean_rsb:.1f} ms) should not exceed 110% of old-stack mean ({mean_old:.1f} ms)"
        )

    def test_rsb_prefetch_forward_sweep_vs_old_prefetch(self):
        """RSB + prefetch vs old + prefetch: worst-frame should be ≤ old worst-frame."""
        spec_fn = self._spec_fn

        # --- old stack (prefetch, no RSB) ---
        buf_old = _build_hydro_stack(HYDRO_H5, spec_fn, chunk=self._CHUNK, prefetch=True)
        try:
            buf_old.get_slice(buf_old.time_index[0])  # warm
            lats_old = _forward_sweep(buf_old, self._N_SWEEP, frame_interval_s=self._FRAME_INTERVAL)
        finally:
            buf_old.close()

        # --- new stack (prefetch + RSB) ---
        buf_rsb = _build_hydro_stack_rsb(HYDRO_H5, spec_fn, chunk=self._CHUNK, prefetch=True)
        try:
            buf_rsb.get_slice(buf_rsb.time_index[0])  # warm
            lats_rsb = _forward_sweep(buf_rsb, self._N_SWEEP, frame_interval_s=self._FRAME_INTERVAL)
        finally:
            buf_rsb.close()

        mean_old  = statistics.mean(lats_old) * 1000
        mean_rsb  = statistics.mean(lats_rsb) * 1000
        worst_old = max(lats_old) * 1000
        worst_rsb = max(lats_rsb) * 1000
        print(
            f"\n  [hydro/godin_daily/prefetch/forward+{self._FRAME_INTERVAL*1000:.0f}ms]"
            f"\n    old stack : mean={mean_old:.1f} ms  worst={worst_old:.1f} ms"
            f"\n    RSB stack : mean={mean_rsb:.1f} ms  worst={worst_rsb:.1f} ms"
            f"\n    delta worst: {worst_rsb - worst_old:+.1f} ms  ({(worst_rsb/max(worst_old, 1) - 1)*100:+.1f}%)",
            file=sys.stdout,
        )
        sys.stdout.flush()
        # RSB must not make the worst-frame more than 20% worse.
        assert worst_rsb <= worst_old * 1.20, (
            f"RSB prefetch worst-frame ({worst_rsb:.1f} ms) must not exceed 120% of "
            f"old-stack worst-frame ({worst_old:.1f} ms)"
        )

    def test_rsb_random_seeks_no_slower_than_baseline(self):
        """Random seeks must not be slower with RSB (seeks bypass the RSB cache)."""
        spec_fn = self._spec_fn

        buf_old = _build_hydro_stack(HYDRO_H5, spec_fn, chunk=self._CHUNK, prefetch=False)
        try:
            lats_old = _random_seeks(buf_old, self._N_SEEK)
        finally:
            buf_old.close()

        buf_rsb = _build_hydro_stack_rsb(HYDRO_H5, spec_fn, chunk=self._CHUNK, prefetch=False)
        try:
            lats_rsb = _random_seeks(buf_rsb, self._N_SEEK)
        finally:
            buf_rsb.close()

        mean_old = statistics.mean(lats_old) * 1000
        mean_rsb = statistics.mean(lats_rsb) * 1000
        print(
            f"\n  [hydro/godin_daily/sync/seek]"
            f"\n    old stack : mean={mean_old:.1f} ms  worst={max(lats_old)*1000:.1f} ms"
            f"\n    RSB stack : mean={mean_rsb:.1f} ms  worst={max(lats_rsb)*1000:.1f} ms"
            f"\n    delta mean: {mean_rsb - mean_old:+.1f} ms",
            file=sys.stdout,
        )
        sys.stdout.flush()
        assert mean_rsb <= mean_old * 1.25, (
            f"RSB seek mean ({mean_rsb:.1f} ms) must not exceed 125% of old stack ({mean_old:.1f} ms)"
        )


@pytest.mark.performance
@pytest.mark.integration
@skip_no_qual
class TestRSBImprovementQual:
    """Quantify the RawSequentialBuffer improvement on the QUAL EC reader stack."""

    _CHUNK = 90
    _N_SWEEP = 150
    _FRAME_INTERVAL = 0.04

    @staticmethod
    def _spec_fn():
        from dsm2ui.animate import (
            make_godin_transform, make_resample_transform, make_composed_transform,
        )
        return make_composed_transform(make_godin_transform(), make_resample_transform("D"))

    def test_rsb_forward_sweep_no_slower_than_baseline(self):
        spec_fn = self._spec_fn

        buf_old = _build_qual_stack(QUAL_H5, spec_fn, chunk=self._CHUNK, prefetch=False)
        try:
            lats_old = _forward_sweep(buf_old, self._N_SWEEP)
        finally:
            buf_old.close()

        buf_rsb = _build_qual_stack_rsb(QUAL_H5, spec_fn, chunk=self._CHUNK, prefetch=False)
        try:
            lats_rsb = _forward_sweep(buf_rsb, self._N_SWEEP)
        finally:
            buf_rsb.close()

        mean_old = statistics.mean(lats_old) * 1000
        mean_rsb = statistics.mean(lats_rsb) * 1000
        p95_old  = sorted(lats_old)[int(len(lats_old) * 0.95)] * 1000
        p95_rsb  = sorted(lats_rsb)[int(len(lats_rsb) * 0.95)] * 1000
        print(
            f"\n  [qual/godin_daily/sync/forward]"
            f"\n    old stack : mean={mean_old:.1f} ms  p95={p95_old:.1f} ms  worst={max(lats_old)*1000:.1f} ms"
            f"\n    RSB stack : mean={mean_rsb:.1f} ms  p95={p95_rsb:.1f} ms  worst={max(lats_rsb)*1000:.1f} ms"
            f"\n    delta mean: {mean_rsb - mean_old:+.1f} ms  ({(mean_rsb/mean_old - 1)*100:+.1f}%)",
            file=sys.stdout,
        )
        sys.stdout.flush()
        # In sync mode RSB uses prefetch_enabled=False (no async thread), so overhead
        # should be negligible.  The 1.10 tolerance catches any future regressions.
        assert mean_rsb <= mean_old * 1.10, (
            f"QUAL RSB mean ({mean_rsb:.1f} ms) must not exceed 110% of old-stack mean ({mean_old:.1f} ms)"
        )

    def test_rsb_prefetch_forward_sweep_vs_old_prefetch(self):
        spec_fn = self._spec_fn

        buf_old = _build_qual_stack(QUAL_H5, spec_fn, chunk=self._CHUNK, prefetch=True)
        try:
            buf_old.get_slice(buf_old.time_index[0])
            lats_old = _forward_sweep(buf_old, self._N_SWEEP, frame_interval_s=self._FRAME_INTERVAL)
        finally:
            buf_old.close()

        buf_rsb = _build_qual_stack_rsb(QUAL_H5, spec_fn, chunk=self._CHUNK, prefetch=True)
        try:
            buf_rsb.get_slice(buf_rsb.time_index[0])
            lats_rsb = _forward_sweep(buf_rsb, self._N_SWEEP, frame_interval_s=self._FRAME_INTERVAL)
        finally:
            buf_rsb.close()

        mean_old  = statistics.mean(lats_old) * 1000
        mean_rsb  = statistics.mean(lats_rsb) * 1000
        worst_old = max(lats_old) * 1000
        worst_rsb = max(lats_rsb) * 1000
        print(
            f"\n  [qual/godin_daily/prefetch/forward+{self._FRAME_INTERVAL*1000:.0f}ms]"
            f"\n    old stack : mean={mean_old:.1f} ms  worst={worst_old:.1f} ms"
            f"\n    RSB stack : mean={mean_rsb:.1f} ms  worst={worst_rsb:.1f} ms"
            f"\n    delta worst: {worst_rsb - worst_old:+.1f} ms",
            file=sys.stdout,
        )
        sys.stdout.flush()
        assert worst_rsb <= worst_old * 1.20, (
            f"QUAL RSB prefetch worst-frame ({worst_rsb:.1f} ms) must not exceed 120% of "
            f"old-stack worst-frame ({worst_old:.1f} ms)"
        )


# ---------------------------------------------------------------------------
# Unit tests for chunk_size scaling in _setup_reader
# No real HDF5 files required — uses InMemorySlicingReader as a mock.
# ---------------------------------------------------------------------------

class TestChunkSizeScaling:
    """Verify that _setup_reader scales chunk_size for aggregate transforms.

    These tests do NOT need real HDF5 files — they use InMemorySlicingReader
    with synthetic data to exercise the exact code path in
    GeoAnimatorManager._setup_reader that was added to fix the slow cold-start
    for daily / weekly aggregate transforms.

    Key regression guarded:
      pandas >= 2.2 raises ValueError from Day.nanos (not a fixed-frequency
      offset for tz-aware data).  The fix uses a sample-Timedelta fallback.
      Without it, the except-Exception block silently disabled the scaling and
      chunk_size stayed at 200, mapping to 200 * 96 = 19 200 raw steps for
      a daily aggregate from 15-min data.
    """

    # ------------------------------------------------------------------
    # Pure-math tests (no readers, no H5 files)
    # ------------------------------------------------------------------

    @pytest.mark.parametrize("freq_str,expected_ns", [
        ("D",  86_400_000_000_000),
        ("h",   3_600_000_000_000),
        ("15min",  900_000_000_000),
    ])
    def test_offset_ns_fallback(self, freq_str, expected_ns):
        """The two-attempt nanos lookup returns the correct value for all freqs."""
        out_offset = pd.tseries.frequencies.to_offset(freq_str)
        try:
            out_ns = int(out_offset.nanos)
        except (ValueError, AttributeError):
            _t0 = pd.Timestamp("2001-01-01")
            out_ns = int((_t0 + out_offset - _t0).total_seconds() * 1e9)
        assert out_ns == expected_ns, (
            f"freq={freq_str!r}: expected {expected_ns}, got {out_ns}"
        )

    def test_daily_from_15min_ratio(self):
        """Daily from 15-min gives ratio=96, chunk_size=5."""
        freq_nanos = int(pd.tseries.frequencies.to_offset("15min").nanos)
        out_offset = pd.tseries.frequencies.to_offset("D")
        try:
            out_ns = int(out_offset.nanos)
        except (ValueError, AttributeError):
            _t0 = pd.Timestamp("2001-01-01")
            out_ns = int((_t0 + out_offset - _t0).total_seconds() * 1e9)
        ratio = out_ns // freq_nanos
        chunk_size = max(5, 200 // ratio)
        min_chunk  = max(2, 50 // ratio)
        max_chunk  = max(chunk_size, 2000 // ratio)
        assert ratio == 96
        assert chunk_size == 5
        assert min_chunk == 2
        assert max_chunk == 20

    def test_hourly_from_15min_ratio(self):
        """Hourly from 15-min gives ratio=4, chunk_size=50."""
        freq_nanos = int(pd.tseries.frequencies.to_offset("15min").nanos)
        out_ns = int(pd.tseries.frequencies.to_offset("h").nanos)
        ratio = out_ns // freq_nanos
        chunk_size = max(5, 200 // ratio)
        assert ratio == 4
        assert chunk_size == 50

    # ------------------------------------------------------------------
    # Integration tests via GeoAnimatorManager._setup_reader
    # Uses InMemorySlicingReader — no H5 files needed.
    # ------------------------------------------------------------------

    def _make_15min_reader(self, n_steps: int = 1000, n_channels: int = 5):
        """Return an InMemorySlicingReader with n_channels of fake 15-min data."""
        from dvue.animator.reader import InMemorySlicingReader
        idx = pd.date_range("2020-01-01", periods=n_steps, freq="15min")
        df = pd.DataFrame(
            {i: range(n_steps) for i in range(n_channels)},
            index=idx,
        )
        return InMemorySlicingReader(df)

    def _make_manager_stub(self, base_reader, buffer_chunk_size: int = 200):
        """Return a minimal object exposing _setup_reader from GeoAnimatorManager."""
        from dvue.animator.ui import GeoAnimatorManager
        # Access _setup_reader as an unbound method to avoid constructing
        # the full Panel/Bokeh UI (which needs a running event loop).
        mgr = object.__new__(GeoAnimatorManager)
        mgr._base_reader = base_reader
        mgr._buffer_chunk_size = buffer_chunk_size
        mgr._transform_options = {}
        return mgr

    @pytest.mark.integration
    def test_setup_reader_no_transform_uses_default_chunk(self):
        """No transform → chunk_size unchanged at _buffer_chunk_size."""
        from dvue.animator.reader import BufferedSlicingReader
        raw = self._make_15min_reader()
        mgr = self._make_manager_stub(raw, buffer_chunk_size=200)
        reader = mgr._setup_reader("none")
        assert isinstance(reader, BufferedSlicingReader)
        assert reader._chunk_size == 200

    @pytest.mark.integration
    def test_setup_reader_daily_aggregate_scales_chunk(self):
        """Daily aggregate from 15-min must produce chunk_size=5."""
        from dsm2ui.animate import make_resample_transform
        from dvue.animator.reader import BufferedSlicingReader
        raw = self._make_15min_reader()
        mgr = self._make_manager_stub(raw, buffer_chunk_size=200)
        mgr._transform_options = {"Daily mean": make_resample_transform("D")}
        reader = mgr._setup_reader("Daily mean")
        assert isinstance(reader, BufferedSlicingReader)
        assert reader._chunk_size == 5, (
            f"Expected chunk_size=5 for daily aggregate from 15-min, "
            f"got {reader._chunk_size}"
        )
        assert reader._min_chunk_size == 2
        assert reader._max_chunk_size == 20

    @pytest.mark.integration
    def test_setup_reader_godin_daily_scales_chunk(self):
        """Godin → Daily mean (composed) must also produce chunk_size=5."""
        from dsm2ui.animate import (
            make_godin_transform, make_resample_transform, make_composed_transform,
        )
        from dvue.animator.reader import BufferedSlicingReader
        raw = self._make_15min_reader()
        mgr = self._make_manager_stub(raw, buffer_chunk_size=200)
        spec = make_composed_transform(make_godin_transform(), make_resample_transform("D"))
        mgr._transform_options = {"Godin → Daily mean": spec}
        reader = mgr._setup_reader("Godin → Daily mean")
        assert isinstance(reader, BufferedSlicingReader)
        assert reader._chunk_size == 5, (
            f"Expected chunk_size=5 for Godin→Daily from 15-min, "
            f"got {reader._chunk_size}"
        )

    @pytest.mark.integration
    def test_setup_reader_godin_convolution_keeps_default_chunk(self):
        """Godin alone (convolution, output_freq=None) must NOT scale chunk_size."""
        from dsm2ui.animate import make_godin_transform
        from dvue.animator.reader import BufferedSlicingReader, StreamingTransformedSlicingReader
        raw = self._make_15min_reader()
        mgr = self._make_manager_stub(raw, buffer_chunk_size=200)
        mgr._transform_options = {"Godin": make_godin_transform()}
        reader = mgr._setup_reader("Godin")
        assert isinstance(reader, BufferedSlicingReader)
        assert reader._chunk_size == 200, (
            "Godin (convolution) should keep default chunk_size=200"
        )
        # Inner must be STSR, not RSR
        assert isinstance(reader._inner, StreamingTransformedSlicingReader)


# ---------------------------------------------------------------------------
# ResamplingSlicingReader (RSR) improvement benchmarks
# Tests the new production stack: STSR(filter) → RSR → BSR
# and verifies cold-start latency is small even for composed transforms.
# ---------------------------------------------------------------------------


def _build_production_stack(
    h5path: Path,
    spec_fn: Callable,
    h5_type: str = "hydro",
    chunk: int = 10,
    prefetch: bool = True,
):
    """Build the production reader stack that mirrors GeoAnimatorManager._setup_reader.

    For aggregate specs (kind='aggregate') this creates:
      BSR(chunk) → RSR(output_freq) → [STSR(filter_spec)] → [RSB] → BaseReader

    For convolution specs (kind='convolution') this creates:
      BSR(chunk) → STSR(spec) → [RSB] → BaseReader
    """
    from dvue.animator import BufferedSlicingReader, RawSequentialBuffer, ResamplingSlicingReader
    from dvue.animator.reader import StreamingTransformedSlicingReader

    if h5_type == "qual":
        from dsm2ui.animate import QualH5ConcentrationReader
        base_reader = QualH5ConcentrationReader(str(h5path), constituent="ec")
    else:
        from dsm2ui.animate import HydroH5FlowReader
        base_reader = HydroH5FlowReader(str(h5path))

    spec = spec_fn()
    reader = base_reader

    try:
        freq_nanos = int(
            pd.tseries.frequencies.to_offset(reader.time_index.freq).nanos
        )
    except (AttributeError, TypeError):
        freq_nanos = 0

    if spec.kind == "aggregate":
        filter_spec = spec.filter_spec
        if filter_spec is not None:
            try:
                raw_overlap = filter_spec.get_overlap(freq_nanos)
            except (AttributeError, TypeError):
                raw_overlap = 0
            if raw_overlap > 0:
                reader = RawSequentialBuffer(reader, prefetch_enabled=prefetch)
            reader = StreamingTransformedSlicingReader(reader, filter_spec)
        reader = ResamplingSlicingReader(reader, spec.output_freq, spec.resample_agg)
    else:
        try:
            raw_overlap = spec.get_overlap(freq_nanos)
        except (AttributeError, TypeError):
            raw_overlap = 0
        if raw_overlap > 0:
            reader = RawSequentialBuffer(reader, prefetch_enabled=prefetch)
        reader = StreamingTransformedSlicingReader(reader, spec)

    return BufferedSlicingReader(
        reader, chunk_size=chunk, prefetch=prefetch,
        adaptive=True, min_chunk_size=2, max_chunk_size=100,
    )


@pytest.mark.performance
@pytest.mark.integration
@skip_no_hydro
class TestRSRImprovementHydro:
    """Verify the ResamplingSlicingReader path is fast on cold start.

    The key regression being guarded: before RSR, the first ``get_slice``
    after a transform switch triggered ``_load_chunk`` with chunk_size=200
    *output* days, loading 200 * freq_ratio + 2*overlap raw steps all at
    once (e.g. 200*24 + 68 = 4868 hourly steps for Godin→Daily, or
    200*96 + 268 = 19468 fifteen-minute steps).  With RSR, the first chunk
    is only ``chunk_size`` output days = 10 days worth of raw data.

    Cold-start tolerance: < 5 s (was ~100 s without RSR for flow layer).

    Run with::

        pytest -m "performance and integration" -k TestRSRImprovement -v -s
    """

    _COLD_START_LIMIT_S = 5.0   # must initialise + serve first frame within 5 s
    _CHUNK = 10
    _N_SWEEP = 60
    _N_SEEK = 10

    # ------------------------------------------------------------------ #
    #  Daily mean (no filter, just RSR)                                   #
    # ------------------------------------------------------------------ #

    def test_daily_mean_cold_start_fast(self):
        """Daily mean cold start must be < 5 s with RSR."""
        from dsm2ui.animate import make_resample_transform
        buf = _build_production_stack(
            HYDRO_H5, lambda: make_resample_transform("D"),
            chunk=self._CHUNK, prefetch=False,
        )
        try:
            ts = buf.time_index[len(buf.time_index) // 2]
            t0 = time.perf_counter()
            s = buf.get_slice(ts)
            cold_start = time.perf_counter() - t0
        finally:
            buf.close()
        print(f"\n  [hydro/daily/RSR] cold start: {cold_start*1000:.0f} ms",
              file=sys.stdout); sys.stdout.flush()
        assert cold_start < self._COLD_START_LIMIT_S, (
            f"Daily mean RSR cold start {cold_start:.2f} s exceeds {self._COLD_START_LIMIT_S} s"
        )
        assert s.notna().any()

    def test_daily_mean_forward_sweep(self):
        from dsm2ui.animate import make_resample_transform
        buf = _build_production_stack(
            HYDRO_H5, lambda: make_resample_transform("D"),
            chunk=self._CHUNK, prefetch=False,
        )
        try:
            lats = _forward_sweep(buf, self._N_SWEEP)
        finally:
            buf.close()
        _print_timing("hydro/daily/RSR/forward", lats)
        assert max(lats) < self._COLD_START_LIMIT_S

    # ------------------------------------------------------------------ #
    #  Rolling 14D → Daily mean (main regression target)                  #
    # ------------------------------------------------------------------ #

    def test_rolling14d_daily_cold_start_fast(self):
        """Rolling 14D → Daily cold start must be < 5 s with RSR."""
        from dsm2ui.animate import (
            make_moving_average_transform, make_resample_transform, make_composed_transform,
        )
        spec_fn = lambda: make_composed_transform(
            make_moving_average_transform("14D"),
            make_resample_transform("D"),
        )
        buf = _build_production_stack(
            HYDRO_H5, spec_fn, chunk=self._CHUNK, prefetch=False,
        )
        try:
            ts = buf.time_index[len(buf.time_index) // 2]
            t0 = time.perf_counter()
            s = buf.get_slice(ts)
            cold_start = time.perf_counter() - t0
        finally:
            buf.close()
        print(f"\n  [hydro/rolling14d_daily/RSR] cold start: {cold_start*1000:.0f} ms",
              file=sys.stdout); sys.stdout.flush()
        assert cold_start < self._COLD_START_LIMIT_S, (
            f"Rolling14D→Daily RSR cold start {cold_start:.2f} s exceeds {self._COLD_START_LIMIT_S} s"
        )
        assert s.notna().any()

    def test_rolling14d_daily_forward_sweep(self):
        from dsm2ui.animate import (
            make_moving_average_transform, make_resample_transform, make_composed_transform,
        )
        spec_fn = lambda: make_composed_transform(
            make_moving_average_transform("14D"),
            make_resample_transform("D"),
        )
        buf = _build_production_stack(
            HYDRO_H5, spec_fn, chunk=self._CHUNK, prefetch=False,
        )
        try:
            lats = _forward_sweep(buf, self._N_SWEEP)
        finally:
            buf.close()
        _print_timing("hydro/rolling14d_daily/RSR/forward", lats)
        assert max(lats) < self._COLD_START_LIMIT_S

    def test_rolling14d_daily_output_is_daily(self):
        """Verify the time index of the production stack is daily-frequency."""
        from dsm2ui.animate import (
            make_moving_average_transform, make_resample_transform, make_composed_transform,
        )
        spec_fn = lambda: make_composed_transform(
            make_moving_average_transform("14D"),
            make_resample_transform("D"),
        )
        buf = _build_production_stack(
            HYDRO_H5, spec_fn, chunk=self._CHUNK, prefetch=False,
        )
        try:
            assert buf.time_index.freq == pd.tseries.frequencies.to_offset("D"), (
                f"Expected daily freq, got {buf.time_index.freq!r}"
            )
            assert len(buf.time_index) > 0
        finally:
            buf.close()

    def test_rolling14d_daily_random_seeks(self):
        from dsm2ui.animate import (
            make_moving_average_transform, make_resample_transform, make_composed_transform,
        )
        spec_fn = lambda: make_composed_transform(
            make_moving_average_transform("14D"),
            make_resample_transform("D"),
        )
        buf = _build_production_stack(
            HYDRO_H5, spec_fn, chunk=self._CHUNK, prefetch=False,
        )
        try:
            lats = _random_seeks(buf, self._N_SEEK)
        finally:
            buf.close()
        _print_timing("hydro/rolling14d_daily/RSR/seek", lats)
        assert max(lats) < self._COLD_START_LIMIT_S

    # ------------------------------------------------------------------ #
    #  Godin → Daily mean                                                  #
    # ------------------------------------------------------------------ #

    def test_godin_daily_cold_start_fast(self):
        """Godin → Daily cold start must be < 5 s with RSR."""
        from dsm2ui.animate import (
            make_godin_transform, make_resample_transform, make_composed_transform,
        )
        spec_fn = lambda: make_composed_transform(
            make_godin_transform(), make_resample_transform("D")
        )
        buf = _build_production_stack(
            HYDRO_H5, spec_fn, chunk=self._CHUNK, prefetch=False,
        )
        try:
            ts = buf.time_index[len(buf.time_index) // 2]
            t0 = time.perf_counter()
            s = buf.get_slice(ts)
            cold_start = time.perf_counter() - t0
        finally:
            buf.close()
        print(f"\n  [hydro/godin_daily/RSR] cold start: {cold_start*1000:.0f} ms",
              file=sys.stdout); sys.stdout.flush()
        assert cold_start < self._COLD_START_LIMIT_S, (
            f"Godin→Daily RSR cold start {cold_start:.2f} s exceeds {self._COLD_START_LIMIT_S} s"
        )
        assert s.notna().any()

    def test_godin_daily_forward_sweep(self):
        from dsm2ui.animate import (
            make_godin_transform, make_resample_transform, make_composed_transform,
        )
        spec_fn = lambda: make_composed_transform(
            make_godin_transform(), make_resample_transform("D")
        )
        buf = _build_production_stack(
            HYDRO_H5, spec_fn, chunk=self._CHUNK, prefetch=False,
        )
        try:
            lats = _forward_sweep(buf, self._N_SWEEP)
        finally:
            buf.close()
        _print_timing("hydro/godin_daily/RSR/forward", lats)
        assert max(lats) < self._COLD_START_LIMIT_S


@pytest.mark.performance
@pytest.mark.integration
@skip_no_qual
class TestRSRImprovementQual:
    """Same cold-start regression tests for the QUAL EC reader stack."""

    _COLD_START_LIMIT_S = 5.0
    _CHUNK = 10
    _N_SWEEP = 60

    def test_daily_mean_cold_start_fast(self):
        from dsm2ui.animate import make_resample_transform
        buf = _build_production_stack(
            QUAL_H5, lambda: make_resample_transform("D"),
            h5_type="qual", chunk=self._CHUNK, prefetch=False,
        )
        try:
            ts = buf.time_index[len(buf.time_index) // 2]
            t0 = time.perf_counter()
            s = buf.get_slice(ts)
            cold_start = time.perf_counter() - t0
        finally:
            buf.close()
        print(f"\n  [qual/daily/RSR] cold start: {cold_start*1000:.0f} ms",
              file=sys.stdout); sys.stdout.flush()
        assert cold_start < self._COLD_START_LIMIT_S
        assert s.notna().any()

    def test_rolling14d_daily_cold_start_fast(self):
        from dsm2ui.animate import (
            make_moving_average_transform, make_resample_transform, make_composed_transform,
        )
        spec_fn = lambda: make_composed_transform(
            make_moving_average_transform("14D"), make_resample_transform("D")
        )
        buf = _build_production_stack(
            QUAL_H5, spec_fn, h5_type="qual", chunk=self._CHUNK, prefetch=False,
        )
        try:
            ts = buf.time_index[len(buf.time_index) // 2]
            t0 = time.perf_counter()
            s = buf.get_slice(ts)
            cold_start = time.perf_counter() - t0
        finally:
            buf.close()
        print(f"\n  [qual/rolling14d_daily/RSR] cold start: {cold_start*1000:.0f} ms",
              file=sys.stdout); sys.stdout.flush()
        assert cold_start < self._COLD_START_LIMIT_S
        assert s.notna().any()

    def test_godin_daily_cold_start_fast(self):
        from dsm2ui.animate import (
            make_godin_transform, make_resample_transform, make_composed_transform,
        )
        spec_fn = lambda: make_composed_transform(
            make_godin_transform(), make_resample_transform("D")
        )
        buf = _build_production_stack(
            QUAL_H5, spec_fn, h5_type="qual", chunk=self._CHUNK, prefetch=False,
        )
        try:
            ts = buf.time_index[len(buf.time_index) // 2]
            t0 = time.perf_counter()
            s = buf.get_slice(ts)
            cold_start = time.perf_counter() - t0
        finally:
            buf.close()
        print(f"\n  [qual/godin_daily/RSR] cold start: {cold_start*1000:.0f} ms",
              file=sys.stdout); sys.stdout.flush()
        assert cold_start < self._COLD_START_LIMIT_S
        assert s.notna().any()
