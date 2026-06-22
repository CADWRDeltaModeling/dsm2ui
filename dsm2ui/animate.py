"""DSM2 HDF5 SlicingReaders and animation helpers.

This module provides three :class:`dvue.animator.SlicingReader` subclasses
that read directly from DSM2 HDF5 tidefiles using ``h5py`` (no pydsm UI
layer; no Panel/HoloViews imports).

Supported file types
--------------------
- **HYDRO** tidefile (``historical_v82.h5``, ``hist_fc_mss.h5``, …):
  :class:`HydroH5FlowReader` — ``/hydro/data/channel flow``
  :class:`HydroH5StageReader` — ``/hydro/data/channel stage``
  :class:`HydroH5VelocityReader` — flow ÷ area (ft/s, computed on-the-fly)
- **QUAL / GTM** tidefile (``historical_v82_ec.h5``, ``historical_gtm.h5``, …):
  :class:`QualH5ConcentrationReader` — ``/output/channel concentration``

Convenience factories
---------------------
:func:`animate_hydro` and :func:`animate_qual` return a fully configured
:class:`dvue.animator.GeoAnimatorManager` ready to serve.

GeoDataFrame helper
-------------------
:func:`load_dsm2_channel_gdf` loads the bundled DSM2 8.2 channel centreline
GeoJSON (or a user-supplied override) and returns a ``gpd.GeoDataFrame``
with ``geo_id`` column = integer channel number.

Usage example
-------------
>>> import panel as pn
>>> pn.extension()
>>> mgr = animate_hydro("path/to/tidefile.h5", variable="flow")
>>> mgr.servable()
"""

from __future__ import annotations

import importlib.resources
import re
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from dvue.animator import SlicingReader

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_FREQ_ALIAS_MAP = {
    # pandas 2.x dropped uppercase aliases — normalise on read
    "T": "min",
    "H": "h",
    "D": "D",
    "MS": "MS",
    "A": "YE",
    "Y": "YE",
}

_INTERVAL_RE = re.compile(r"(\d+)\s*([a-zA-Z]+)")


def _normalise_interval(interval_str: str) -> str:
    """Convert a DSM2/HECDSS interval string to a pandas 2.x-safe freq alias.

    Examples: ``"30min"`` → ``"30min"``, ``"1H"`` → ``"1h"``,
    ``"60min"`` → ``"60min"``, ``"1DAY"`` → ``"1D"``.
    """
    s = interval_str.strip()
    # Already lowercase pandas alias? pass through
    m = _INTERVAL_RE.match(s)
    if not m:
        return s
    n, unit = m.group(1), m.group(2)
    unit_norm = _FREQ_ALIAS_MAP.get(unit.upper(), unit)
    # DSM2 uses 'min' (already fine), 'H'→'h', 'D'→'D', etc.
    return f"{n}{unit_norm}"


def _parse_dsm2_timestamp(raw) -> pd.Timestamp:
    """Parse a DSM2 ``start_time`` HDF5 attribute to ``pd.Timestamp``.

    Accepts bytes, str, numpy bytes, or already a Timestamp/datetime.
    DSM2 format: ``"02JAN1990 0000"`` (military date).
    """
    if isinstance(raw, (bytes, np.bytes_)):
        raw = raw.decode("utf-8")
    if isinstance(raw, (list, np.ndarray)):
        raw = raw[0]
        return _parse_dsm2_timestamp(raw)
    if isinstance(raw, pd.Timestamp):
        return raw
    s = str(raw).strip()
    # Try pandas first (handles ISO and many formats)
    try:
        return pd.Timestamp(s)
    except Exception:
        pass
    # DSM2 military format: 02JAN1990 0000
    month_map = {
        "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
        "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
    }
    pat = re.match(r"(\d{1,2})([A-Za-z]{3})(\d{4})\s+(\d{4})", s)
    if pat:
        day, mon, year, hhmm = pat.groups()
        hh, mm = int(hhmm[:2]), int(hhmm[2:])
        return pd.Timestamp(int(year), month_map[mon.upper()], int(day), hh, mm)
    raise ValueError(f"Cannot parse DSM2 timestamp: {s!r}")


def _decode_string_array(arr) -> list[str]:
    """Decode an HDF5 byte-string dataset to a list of Python str."""
    out = []
    for item in arr:
        if isinstance(item, (bytes, np.bytes_)):
            out.append(item.decode("utf-8").strip())
        else:
            out.append(str(item).strip())
    return out


# ---------------------------------------------------------------------------
# Base class
# ---------------------------------------------------------------------------

class _DSM2BaseH5Reader(SlicingReader):
    """Internal base: opens an HDF5 file and builds the common time index.

    Parameters
    ----------
    filepath : str or Path
        Path to the DSM2 HDF5 tidefile.
    dataset_path : str
        HDF5 path to the time-series dataset (used to read time attributes).
    channel_number_path : str
        HDF5 path to the 1-D channel number dataset.
    """

    def __init__(
        self,
        filepath: "str | Path",
        dataset_path: str,
        channel_number_path: str,
    ) -> None:
        import h5py

        self._filepath = Path(filepath)
        self._h5 = h5py.File(self._filepath, "r")
        if dataset_path not in self._h5:
            # Diagnose the mismatch — tell the user which top-level groups the
            # file actually contains so they can pick the right sub-command.
            top_keys = list(self._h5.keys())
            expected_root = dataset_path.split("/")[1]  # e.g. "hydro" or "output"
            if expected_root not in self._h5:
                alt_roots = [k for k in top_keys if k in ("hydro", "qual", "gtm", "output")]
                hint = (
                    "The file appears to be a QUAL/GTM tidefile — try 'dsm2ui animate qual'."
                    if "output" in top_keys or "qual" in top_keys
                    else (
                        "The file appears to be a HYDRO tidefile — try 'dsm2ui animate hydro'."
                        if "hydro" in top_keys
                        else f"Top-level groups found: {top_keys}"
                    )
                )
            else:
                hint = f"Dataset '{dataset_path}' not found; top-level groups: {top_keys}"
            self._h5.close()
            raise ValueError(
                f"Wrong HDF5 file type for this command.\n"
                f"File   : {self._filepath}\n"
                f"Wanted : {dataset_path}\n"
                f"Hint   : {hint}"
            )
        self._ds = self._h5[dataset_path]

        # Build DatetimeIndex from dataset attributes
        attrs = dict(self._ds.attrs)
        start = _parse_dsm2_timestamp(attrs["start_time"])
        # interval attr may be a scalar str, bytes, or a 1-element array
        raw_interval = attrs["interval"]
        if hasattr(raw_interval, "__len__") and not isinstance(raw_interval, (str, bytes)):
            raw_interval = raw_interval[0]
        if isinstance(raw_interval, (bytes, np.bytes_)):
            raw_interval = raw_interval.decode("utf-8")
        interval = _normalise_interval(str(raw_interval).strip())
        n_times = self._ds.shape[0]
        idx = pd.date_range(start=start, periods=n_times, freq=interval)

        # Channel numbers as int (matches the bundled GeoJSON "id" column)
        chan_raw = self._h5[channel_number_path][:]
        self._channel_numbers: list[int] = [
            int(s.decode("utf-8").strip()) if isinstance(s, (bytes, np.bytes_)) else int(s)
            for s in chan_raw
        ]

        # vmin/vmax: compute from full array (subclasses may override)
        self._vmin, self._vmax = self._compute_global_range()

        super().__init__(idx)

    def _compute_global_range(self, n_sample: int = 20) -> tuple[float, float]:
        """Estimate (vmin, vmax) from the first *n_sample* time steps.

        Reading the full dataset for a multi-year tidefile at 15-min intervals
        can load gigabytes.  Sampling the first few frames is fast and gives a
        good enough initial colour-scale range; the user can override via the
        ``vmin``/``vmax`` controls in the UI.

        Subclasses that need to select a constituent dimension should override.
        """
        n = min(n_sample, self._ds.shape[0])
        data = self._ds[:n].astype(float)
        data[data < -1e20] = np.nan  # DSM2 missing-value sentinel
        if np.all(np.isnan(data)):
            return 0.0, 1.0
        return float(np.nanmin(data)), float(np.nanmax(data))

    @property
    def vmin(self) -> float:
        return self._vmin

    @property
    def vmax(self) -> float:
        return self._vmax

    def close(self) -> None:
        """Close the underlying HDF5 file handle."""
        if self._h5.id.valid:
            self._h5.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


# ---------------------------------------------------------------------------
# Hydro readers
# ---------------------------------------------------------------------------

_HYDRO_CHAN_PATH = "/hydro/geometry/channel_number"


class HydroH5FlowReader(_DSM2BaseH5Reader):
    """SlicingReader for DSM2 HYDRO tidefile channel flow.

    Returns a ``pd.Series(index=channel_numbers_as_int, values=float_cfs)``
    at each time step.  Values are averaged across the upstream/downstream
    location dimension.

    Parameters
    ----------
    filepath : str or Path
        HYDRO HDF5 tidefile.
    location : {"both", "upstream", "downstream"}, optional
        Which location to use.  ``"both"`` (default) averages the two ends.
    """

    _DATASET = "/hydro/data/channel flow"

    def __init__(
        self,
        filepath: "str | Path",
        location: str = "both",
    ) -> None:
        self._location = location
        super().__init__(filepath, self._DATASET, _HYDRO_CHAN_PATH)

    def _location_index(self) -> "int | None":
        """Return the location array index, or None to average all."""
        if self._location == "both":
            return None
        loc_ds = self._h5["/hydro/geometry/channel_location"][:]
        loc_names = _decode_string_array(loc_ds)
        loc = self._location.lower()
        if loc not in loc_names:
            raise ValueError(
                f"location {loc!r} not in file; available: {loc_names}"
            )
        return loc_names.index(loc)

    def get_slice(self, timestamp: pd.Timestamp) -> pd.Series:
        i = self._time_index.get_indexer([timestamp], method="nearest")[0]
        row = self._ds[i, :, :]  # (n_channels, n_locations)
        row = row.astype(float)
        row[row < -1e20] = np.nan
        loc_idx = self._location_index()
        if loc_idx is None:
            values = np.nanmean(row, axis=1)
        else:
            values = row[:, loc_idx]
        return pd.Series(values, index=self._channel_numbers, dtype=float)

    def get_slice_range(self, start_idx: int, end_idx: int) -> pd.DataFrame:
        """Read a contiguous block of time steps in one HDF5 call."""
        rows = self._ds[start_idx:end_idx, :, :].astype(float)  # (t, ch, loc)
        rows[rows < -1e20] = np.nan
        loc_idx = self._location_index()
        if loc_idx is None:
            values = np.nanmean(rows, axis=2)   # average u/d
        else:
            values = rows[:, :, loc_idx]
        timestamps = self._time_index[start_idx:end_idx]
        return pd.DataFrame(values, index=timestamps, columns=self._channel_numbers)


class HydroH5StageReader(HydroH5FlowReader):
    """SlicingReader for DSM2 HYDRO tidefile channel stage (water depth, ft).

    Same interface as :class:`HydroH5FlowReader` but reads stage data.
    """

    _DATASET = "/hydro/data/channel stage"


class HydroH5VelocityReader(_DSM2BaseH5Reader):
    """SlicingReader for DSM2 HYDRO tidefile channel velocity (ft/s).

    Velocity is not stored directly — it is computed as flow / area.
    Both datasets are read for each time step (or chunk) and divided
    element-wise.  Zero or near-zero areas (dry channels) produce NaN.

    Parameters
    ----------
    filepath : str or Path
        HYDRO HDF5 tidefile.
    location : {"both", "upstream", "downstream"}, optional
        Which channel end to use.  ``"both"`` (default) averages the two ends
        before dividing.
    zero_area_threshold : float, optional
        Areas below this value (ft²) are treated as zero and yield NaN
        velocity.  Default ``0.01``.
    """

    _FLOW_DS = "/hydro/data/channel flow"
    _AREA_DS = "/hydro/data/channel area"

    def __init__(
        self,
        filepath: "str | Path",
        location: str = "both",
        zero_area_threshold: float = 0.01,
    ) -> None:
        self._location = location
        self._zero_area_threshold = zero_area_threshold
        # Base class opens the file and reads the flow dataset for time attrs
        # and channel numbers; we also need the area dataset handle.
        super().__init__(filepath, self._FLOW_DS, _HYDRO_CHAN_PATH)
        self._area_ds = self._h5[self._AREA_DS]

    def _location_index(self) -> "int | None":
        if self._location == "both":
            return None
        loc_ds = self._h5["/hydro/geometry/channel_location"][:]
        loc_names = _decode_string_array(loc_ds)
        loc = self._location.lower()
        if loc not in loc_names:
            raise ValueError(
                f"location {loc!r} not in file; available: {loc_names}"
            )
        return loc_names.index(loc)

    def _velocity(self, flow: np.ndarray, area: np.ndarray) -> np.ndarray:
        """Compute velocity from (n_channels, n_locations) arrays."""
        loc_idx = self._location_index()
        if loc_idx is None:
            f = np.nanmean(flow, axis=1)
            a = np.nanmean(area, axis=1)
        else:
            f = flow[:, loc_idx]
            a = area[:, loc_idx]
        # Zero-area channels (dry) become NaN
        a[a < self._zero_area_threshold] = np.nan
        return f / a

    def _compute_global_range(self, n_sample: int = 20) -> tuple[float, float]:
        n = min(n_sample, self._ds.shape[0])
        flow = self._ds[:n].astype(float)
        flow[flow < -1e20] = np.nan
        # Area dataset may not yet be open on first call; access via h5 dict
        area = self._h5[self._AREA_DS][:n].astype(float)
        area[area < -1e20] = np.nan
        # Vectorised velocity for the sample block
        area[area < self._zero_area_threshold] = np.nan
        vel = flow / area
        if np.all(np.isnan(vel)):
            return 0.0, 1.0
        return float(np.nanmin(vel)), float(np.nanmax(vel))

    def get_slice(self, timestamp: pd.Timestamp) -> pd.Series:
        i = self._time_index.get_indexer([timestamp], method="nearest")[0]
        flow = self._ds[i, :, :].astype(float)
        flow[flow < -1e20] = np.nan
        area = self._area_ds[i, :, :].astype(float)
        area[area < -1e20] = np.nan
        values = self._velocity(flow, area)
        return pd.Series(values, index=self._channel_numbers, dtype=float)

    def get_slice_range(self, start_idx: int, end_idx: int) -> pd.DataFrame:
        """Batch read: one HDF5 call each for flow and area."""
        flow = self._ds[start_idx:end_idx, :, :].astype(float)  # (t, ch, loc)
        flow[flow < -1e20] = np.nan
        area = self._area_ds[start_idx:end_idx, :, :].astype(float)
        area[area < -1e20] = np.nan
        loc_idx = self._location_index()
        if loc_idx is None:
            f = np.nanmean(flow, axis=2)
            a = np.nanmean(area, axis=2)
        else:
            f = flow[:, :, loc_idx]
            a = area[:, :, loc_idx]
        a[a < self._zero_area_threshold] = np.nan
        vel = f / a
        timestamps = self._time_index[start_idx:end_idx]
        return pd.DataFrame(vel, index=timestamps, columns=self._channel_numbers)


# ---------------------------------------------------------------------------
# Qual / GTM reader
# ---------------------------------------------------------------------------

_QUAL_CHAN_PATH = "/output/channel_number"
_QUAL_CONC_PATH = "/output/channel concentration"
_QUAL_CONSTIT_PATH = "/output/constituent_names"


class QualH5ConcentrationReader(_DSM2BaseH5Reader):
    """SlicingReader for DSM2 QUAL/GTM tidefile channel concentrations.

    Works for both QUAL (``historical_v82_ec.h5``) and GTM
    (``historical_gtm.h5``) files — the dataset layout is identical.

    Parameters
    ----------
    filepath : str or Path
        QUAL or GTM HDF5 tidefile.
    constituent : str, optional
        Constituent name to extract (case-insensitive).  Default ``"ec"``.
    """

    def __init__(
        self,
        filepath: "str | Path",
        constituent: str = "ec",
    ) -> None:
        self._constituent_name = constituent.strip().lower()
        # Store filepath to resolve constituent index after base __init__
        # (we need the file open first)
        import h5py
        _h5_tmp = h5py.File(Path(filepath), "r")
        constit_names = _decode_string_array(_h5_tmp[_QUAL_CONSTIT_PATH][:])
        _h5_tmp.close()
        constit_names_lower = [n.lower() for n in constit_names]
        if self._constituent_name not in constit_names_lower:
            raise ValueError(
                f"Constituent {constituent!r} not found. "
                f"Available: {constit_names}"
            )
        self._constituent_index = constit_names_lower.index(self._constituent_name)
        super().__init__(filepath, _QUAL_CONC_PATH, _QUAL_CHAN_PATH)

    def _compute_global_range(self, n_sample: int = 20) -> tuple[float, float]:
        # Sample only the first n_sample frames for the relevant constituent.
        ci = self._constituent_index
        n = min(n_sample, self._ds.shape[0])
        data = self._ds[:n, ci, :, :].astype(float)
        data[data < -1e20] = np.nan
        if np.all(np.isnan(data)):
            return 0.0, 1.0
        return float(np.nanmin(data)), float(np.nanmax(data))

    def get_slice(self, timestamp: pd.Timestamp) -> pd.Series:
        i = self._time_index.get_indexer([timestamp], method="nearest")[0]
        ci = self._constituent_index
        row = self._ds[i, ci, :, :].astype(float)  # (n_channels, n_locations)
        row[row < -1e20] = np.nan
        values = np.nanmean(row, axis=1)  # average u/d
        return pd.Series(values, index=self._channel_numbers, dtype=float)

    def get_slice_range(self, start_idx: int, end_idx: int) -> pd.DataFrame:
        """Read a contiguous block of time steps in one HDF5 call."""
        ci = self._constituent_index
        rows = self._ds[start_idx:end_idx, ci, :, :].astype(float)  # (t, ch, loc)
        rows[rows < -1e20] = np.nan
        values = np.nanmean(rows, axis=2)   # average u/d
        timestamps = self._time_index[start_idx:end_idx]
        return pd.DataFrame(values, index=timestamps, columns=self._channel_numbers)


# ---------------------------------------------------------------------------
# IDW-corrected QUAL reader
# ---------------------------------------------------------------------------

class CorrectedQualH5ConcentrationReader(SlicingReader):
    """SlicingReader wrapping :class:`QualH5ConcentrationReader` with network IDW correction.

    Sparse observations from a time-indexed CSV are used to compute additive
    residual corrections (via :class:`~pydsm.analysis.network_correction.NetworkIDWCorrector`)
    that are applied to the **raw upstream/downstream channel-end values** before
    the two ends are averaged and returned.  This means the correction happens
    *before* any downstream transform (moving-average, Godin filter, etc.) that
    is layered on top via :class:`~dvue.animator.TransformedSlicingReader`.

    When *observations_csv* is not supplied use :class:`QualH5ConcentrationReader`
    directly — this class is only constructed when observations are explicitly
    provided.

    Network initialisation
    ----------------------
    The CHANNEL table (chan_no, upnode, downnode, length) is read from the H5
    file itself — tried at ``/input/channel`` first, then
    ``/hydro/input/channel``.  If neither path exists, *echo_inp_file* is used
    as a fallback.

    Parameters
    ----------
    h5file : str or Path
        QUAL or GTM HDF5 tidefile.
    observations_csv : str or Path
        Time-indexed CSV.  First column = datetime index; remaining columns =
        station IDs (matching ``station_id`` values in *stations_csv*).
        NaN or missing cells are treated as absent for that time step.
    stations_csv : str or Path
        CSV with a ``station_id`` column and either ``lat``/``lon`` or
        ``x``/``y`` columns.  Loaded via ``pydsm.viz.dsm2gis.read_stations``.
    centerlines_file : str or Path
        GeoJSON or shapefile of DSM2 channel centrelines.
    constituent : str, optional
        Constituent to extract (case-insensitive).  Default ``"ec"``.
    power : float, optional
        IDW distance exponent.  Default ``2``.
    max_distance : float or None, optional
        Maximum network distance in feet beyond which an observation has no
        influence.  ``None`` means no cutoff.
    max_obs_age : str or pd.Timedelta, optional
        Maximum allowable difference between a model time step and the
        nearest observation row.  When exceeded all observations for that
        step are treated as missing (correction = 0).  Default ``"2h"``.
    echo_inp_file : str or Path or None, optional
        Fallback DSM2 echo ``.inp`` file to read the CHANNEL table from if
        the H5 file does not contain the table.
    """

    def __init__(
        self,
        h5file: "str | Path",
        observations_csv: "str | Path",
        stations_csv: "str | Path",
        centerlines_file: "str | Path",
        constituent: str = "ec",
        power: float = 2,
        max_distance: "float | None" = None,
        max_obs_age: "str | pd.Timedelta" = "2h",
        echo_inp_file: "str | Path | None" = None,
        corrector=None,
    ) -> None:
        import geopandas as gpd
        from pydsm.analysis.network_correction import (
            NetworkIDWCorrector,
            snap_stations_to_channel_ends,
        )
        from pydsm.viz.dsm2gis import read_stations

        # --- inner raw reader (keeps the HDF5 file open) --------------------
        self._inner = QualH5ConcentrationReader(h5file, constituent=constituent)

        # --- observations ---------------------------------------------------
        self._obs_df = pd.read_csv(
            observations_csv, index_col=0, parse_dates=True
        )
        self._max_obs_age = pd.Timedelta(max_obs_age)
        # Pre-align observations to the model time index once so every
        # per-step lookup is O(1) rather than O(log N) binary search.
        self._obs_aligned = self._align_obs(self._obs_df)

        # --- CHANNEL table (network topology) --------------------------------
        channels_df = self._load_channels(h5file, echo_inp_file)

        # --- snap observation stations onto channel-ends --------------------
        stations_gdf = read_stations(str(stations_csv))
        centerlines_gdf = gpd.read_file(str(centerlines_file))
        snapped = snap_stations_to_channel_ends(
            stations_gdf, centerlines_gdf, channels_df
        )

        # --- corrector -------------------------------------------------------
        # Use a caller-supplied NetworkCorrector when provided; otherwise
        # fall back to IDW so the interface is backward-compatible.
        if corrector is not None:
            self._corrector = corrector
            # Snapping is not needed when the corrector is pre-built.
            # Build a minimal snapped DataFrame from the corrector's own
            # snapped_stations_df so _get_obs_at works correctly.
            self._snapped_passthrough = True
        else:
            self._corrector = NetworkIDWCorrector(
                channels_df, snapped, power=power, max_distance=max_distance
            )
            self._snapped_passthrough = False

        # --- channel-end index in interleaved order -------------------------
        # Layout: [ch0-upstream, ch0-downstream, ch1-upstream, ch1-downstream, ...]
        # values[0::2] = upstream array, values[1::2] = downstream array,
        # both in the same order as self._chan_numbers.
        self._chan_numbers: list = self._inner._channel_numbers  # list[int]
        self._chan_str: list = [str(c) for c in self._chan_numbers]
        self._ce_index: list = [
            f"{s}-{loc}"
            for s in self._chan_str
            for loc in ("upstream", "downstream")
        ]

        # Pre-build numpy weight matrices for vectorized IDW correction.
        self._precompute_idw_matrices()

        super().__init__(self._inner.time_index)

    def rebuild_corrector(self, new_corrector) -> None:
        """Replace the active corrector in-place and rebuild vectorised matrices."""
        self._corrector = new_corrector
        self._precompute_idw_matrices()

    def _align_obs(self, obs_df: "pd.DataFrame") -> "pd.DataFrame":
        """Reindex *obs_df* onto the model time index with nearest-match and
        tolerance so every per-step lookup is an O(1) label access."""
        if obs_df.empty:
            return obs_df
        return obs_df.reindex(
            self._inner.time_index,
            method="nearest",
            tolerance=self._max_obs_age,
        )

    def _reload_observations(self, obs_path: str) -> None:
        """Reload the observations CSV, rebuild the pre-aligned cache and
        the IDW weight matrices (station order may change)."""
        self._obs_df = pd.read_csv(obs_path, index_col=0, parse_dates=True)
        self._obs_aligned = self._align_obs(self._obs_df)
        self._precompute_idw_matrices()

    def _precompute_idw_matrices(self) -> None:
        """Convert the IDW weights dict to numpy arrays for vectorised correction.

        Sets ``self._use_vectorized_idw = True`` when the corrector exposes
        ``_weights`` and ``_snapped`` (NetworkIDWCorrector).  Falls back to
        per-step loop for other corrector types (e.g. OI).
        """
        self._use_vectorized_idw = False
        corr = self._corrector
        if not (hasattr(corr, "_weights") and hasattr(corr, "_snapped")):
            return
        if self._obs_aligned.empty:
            return
        obs_cols = list(self._obs_aligned.columns)
        if not obs_cols:
            return

        N_sta = len(obs_cols)
        N_ce = len(self._ce_index)
        sta_to_j = {sid: j for j, sid in enumerate(obs_cols)}
        ce_to_i  = {ce:  i for i, ce  in enumerate(self._ce_index)}

        # W_finite[i, j] = d^(-power) weight from station j to channel-end i.
        # Exact-match (inf weight) entries are kept separate.
        W_finite = np.zeros((N_ce, N_sta), dtype=np.float64)
        exact_map: list = []  # list of (ce_index, np.array of station_j indices)

        for ce_key, ce_weights in corr._weights.items():
            if ce_key not in ce_to_i:
                continue
            i = ce_to_i[ce_key]
            exact_js: list = []
            for sid, w in ce_weights.items():
                if sid not in sta_to_j:
                    continue
                j = sta_to_j[sid]
                if np.isinf(w):
                    exact_js.append(j)
                elif w > 0.0 and np.isfinite(w):
                    W_finite[i, j] = w
            if exact_js:
                exact_map.append((i, np.asarray(exact_js, dtype=np.intp)))

        # For each obs station, the CE index of its snapped channel-end
        # (used to gather the model value for the residual computation).
        sta_ce_idx = np.full(N_sta, -1, dtype=np.intp)
        for sid, row in corr._snapped.iterrows():
            if sid not in sta_to_j:
                continue
            j = sta_to_j[sid]
            ce_key = f"{row['chan_no']}-{row['location']}"
            if ce_key in ce_to_i:
                sta_ce_idx[j] = ce_to_i[ce_key]

        self._W_finite   = W_finite
        self._exact_map  = exact_map
        self._sta_ce_idx = sta_ce_idx
        self._use_vectorized_idw = True

    def _apply_idw_vectorized(
        self,
        model_ce_block: "np.ndarray",
        obs_values: "np.ndarray",
    ) -> "np.ndarray":
        """Vectorised IDW for a whole chunk using pre-built numpy matrices.

        Parameters
        ----------
        model_ce_block : ndarray, shape (N_times, N_ce)
            Interleaved upstream/downstream model values per channel.
        obs_values : ndarray, shape (N_times, N_sta)
            Pre-aligned observations (NaN where no obs within max_obs_age).

        Returns
        -------
        ndarray, shape (N_times, N_ch)
            Corrected channel values (mean of upstream and downstream).
        """
        n_ch = len(self._chan_numbers)

        # ---- 1. Gather model values at each station's home channel-end ----
        valid_sta = self._sta_ce_idx >= 0
        model_at_sta = np.full_like(obs_values, np.nan)
        if valid_sta.any():
            model_at_sta[:, valid_sta] = model_ce_block[:, self._sta_ce_idx[valid_sta]]

        # ---- 2. Residuals (obs − model at station CE); NaN = missing obs ----
        residuals = obs_values - model_at_sta   # (N_times × N_sta)
        valid_f   = (~np.isnan(residuals)).astype(np.float64)
        res_f     = np.where(np.isnan(residuals), 0.0, residuals)

        # ---- 3. Finite IDW: two matrix multiplies -------------------------
        # wr[t,i]    = Σ_j  W[i,j] · valid[t,j] · res[t,j]
        # w_sum[t,i] = Σ_j  W[i,j] · valid[t,j]
        W = self._W_finite  # (N_ce × N_sta)
        wr    = res_f   @ W.T   # (N_times × N_ce)
        w_sum = valid_f @ W.T   # (N_times × N_ce)
        with np.errstate(invalid="ignore", divide="ignore"):
            correction = np.where(w_sum > 0.0, wr / w_sum, 0.0)  # (N_times × N_ce)

        # ---- 4. Exact-match overrides (inf-weight stations) ---------------
        for ce_i, sta_js in self._exact_map:
            exact_r = residuals[:, sta_js]          # (N_times × n_exact)
            any_valid = ~np.all(np.isnan(exact_r), axis=1)
            if any_valid.any():
                correction[any_valid, ce_i] = np.nanmean(
                    exact_r[any_valid], axis=1
                )

        # ---- 5. Apply corrections and average upstream / downstream -------
        corrected_ce = model_ce_block + correction           # (N_times × N_ce)
        up = corrected_ce[:, 0::2]                           # (N_times × N_ch)
        dn = corrected_ce[:, 1::2]                           # (N_times × N_ch)
        return np.nanmean(np.stack([up, dn], axis=2), axis=2)  # (N_times × N_ch)

    # ------------------------------------------------------------------
    # Static helper: load CHANNEL table
    # ------------------------------------------------------------------

    @staticmethod
    def _load_channels(h5file: "str | Path", echo_inp_file: "str | Path | None"):
        """Return a normalised CHANNEL DataFrame (lowercase columns, typed).

        Tries ``/input/channel`` then ``/hydro/input/channel`` in the H5 file.
        Falls back to parsing *echo_inp_file* if neither is present.
        """
        import h5py
        from pydsm.output.dsm2h5 import read_table_as_df

        with h5py.File(Path(h5file), "r") as h5:
            for tpath in ("/input/channel", "/hydro/input/channel"):
                if tpath in h5:
                    df = read_table_as_df(h5, tpath)
                    if df is not None and len(df) > 0:
                        df = df.rename(columns=lambda c: c.lower())
                        df["chan_no"]  = df["chan_no"].astype(str)
                        df["upnode"]   = df["upnode"].astype(int)
                        df["downnode"] = df["downnode"].astype(int)
                        df["length"]   = df["length"].astype(float)
                        return df

        if echo_inp_file is None:
            raise ValueError(
                "CHANNEL table not found in the H5 file (tried /input/channel "
                "and /hydro/input/channel) and no echo_inp_file was supplied."
            )
        from pydsm.input.parser import parse

        with open(echo_inp_file, "r") as fh:
            tables = parse(fh.read())
        df = tables["CHANNEL"].rename(columns=lambda c: c.lower())
        df["chan_no"]  = df["chan_no"].astype(str)
        df["upnode"]   = df["upnode"].astype(int)
        df["downnode"] = df["downnode"].astype(int)
        df["length"]   = df["length"].astype(float)
        return df

    # ------------------------------------------------------------------
    # Observation lookup
    # ------------------------------------------------------------------

    def _get_obs_at(self, timestamp: pd.Timestamp) -> pd.Series:
        """Return the pre-aligned observation row for *timestamp* (O(1) lookup).

        Returns an all-NaN Series when the model step has no observation within
        *max_obs_age* (the pre-alignment sets those rows to NaN already).
        """
        if self._obs_aligned.empty:
            return pd.Series(dtype=float)
        try:
            return self._obs_aligned.loc[timestamp]
        except KeyError:
            return pd.Series(np.nan, index=self._obs_aligned.columns, dtype=float)

    # ------------------------------------------------------------------
    # Channel-end helpers
    # ------------------------------------------------------------------

    def _build_model_ce(
        self, upstream: np.ndarray, downstream: np.ndarray
    ) -> pd.Series:
        """Pack per-channel upstream/downstream arrays into a channel-end Series.

        Even positions (0, 2, 4...) = upstream; odd (1, 3, 5...) = downstream.
        """
        n = len(self._chan_str)
        values = np.empty(2 * n, dtype=float)
        values[0::2] = upstream
        values[1::2] = downstream
        return pd.Series(values, index=self._ce_index, dtype=float)

    def _corrected_avg(self, corrected_ce: pd.Series) -> np.ndarray:
        """Average corrected upstream/downstream back to one value per channel."""
        v = corrected_ce.values
        return np.nanmean(np.stack([v[0::2], v[1::2]], axis=1), axis=1)

    # ------------------------------------------------------------------
    # SlicingReader interface
    # ------------------------------------------------------------------

    @property
    def vmin(self) -> float:
        return self._inner.vmin

    @property
    def vmax(self) -> float:
        return self._inner.vmax

    def get_slice(self, timestamp: pd.Timestamp) -> pd.Series:
        i = self._inner._time_index.get_indexer([timestamp], method="nearest")[0]
        ci = self._inner._constituent_index
        row = self._inner._ds[i, ci, :, :].astype(float)   # (n_ch, 2)
        row[row < -1e20] = np.nan
        model_ce = self._build_model_ce(row[:, 0], row[:, 1])
        obs = self._get_obs_at(timestamp)
        corrected_ce = self._corrector.correct(model_ce, obs)
        return pd.Series(
            self._corrected_avg(corrected_ce),
            index=self._chan_numbers,
            dtype=float,
        )

    def get_slice_range(self, start_idx: int, end_idx: int) -> pd.DataFrame:
        """Read a block of time steps and apply IDW correction.

        For IDW correctors the correction is fully vectorised via pre-built
        numpy weight matrices (two matrix multiplies for the whole chunk).
        For other corrector types (e.g. OI) a per-step loop is used.
        """
        ci = self._inner._constituent_index
        # One HDF5 read: shape (n_times, n_ch, 2)
        block = self._inner._ds[start_idx:end_idx, ci, :, :].astype(float)
        block[block < -1e20] = np.nan
        timestamps = self._inner.time_index[start_idx:end_idx]
        n_times = end_idx - start_idx
        n_ch = block.shape[1]

        # Build interleaved model CE block: (n_times, 2*n_ch)
        # col 0::2 = upstream, col 1::2 = downstream (matches self._ce_index order)
        model_ce_block = np.empty((n_times, 2 * n_ch), dtype=np.float64)
        model_ce_block[:, 0::2] = block[:, :, 0]
        model_ce_block[:, 1::2] = block[:, :, 1]

        if self._use_vectorized_idw and not self._obs_aligned.empty:
            # Fast path: single reindex + two matrix multiplies
            obs_values = self._obs_aligned.reindex(timestamps).values
            result = self._apply_idw_vectorized(model_ce_block, obs_values)
        else:
            # Fallback: per-step loop (used for OI or when IDW matrices not built)
            if not self._obs_aligned.empty:
                obs_block = self._obs_aligned.reindex(timestamps)
            else:
                obs_block = pd.DataFrame(
                    index=timestamps, columns=pd.Index([], dtype=str), dtype=float
                )
            result = np.zeros((n_times, n_ch), dtype=float)
            for t_idx in range(n_times):
                model_ce = self._build_model_ce(
                    block[t_idx, :, 0], block[t_idx, :, 1]
                )
                obs = obs_block.iloc[t_idx]
                corrected_ce = self._corrector.correct(model_ce, obs)
                result[t_idx, :] = self._corrected_avg(corrected_ce)

        return pd.DataFrame(result, index=timestamps, columns=self._chan_numbers)

    def close(self) -> None:
        """Close the underlying HDF5 file."""
        self._inner.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def _make_correction_card(
    mgr,
    reader: "CorrectedQualH5ConcentrationReader",
    channels_df: "pd.DataFrame",
    centerlines_file: "str | Path",
    initial_obs_csv: str = "",
    initial_stations_csv: str = "",
    initial_method: str = "IDW",
    initial_power: float = 2.0,
    initial_sigma_obs: float = 10.0,
    initial_kernel: str = "exponential",
    initial_resistance: float = 3.0,
    refresh_fn=None,
):
    """Build a Panel Card that lets the user switch IDW/OI correction live.

    The card is appended to ``mgr._controls`` by :func:`animate_qual_corrected`.
    Clicking **Apply** rebuilds the corrector, calls :meth:`rebuild_corrector`,
    invalidates the reader buffer, and re-renders the current frame.
    """
    import threading
    import panel as pn

    # --- widgets ---
    obs_input   = pn.widgets.TextInput(
        name="Observations CSV", value=initial_obs_csv,
        placeholder="/path/to/obs.csv", sizing_mode="stretch_width",
    )
    sta_input   = pn.widgets.TextInput(
        name="Stations CSV", value=initial_stations_csv,
        placeholder="/path/to/stations.csv", sizing_mode="stretch_width",
    )
    method_sel  = pn.widgets.Select(
        name="Correction method", options=["IDW", "OI"],
        value=initial_method, sizing_mode="stretch_width",
    )
    power_sl    = pn.widgets.FloatSlider(
        name="IDW power", value=initial_power,
        start=0.5, end=5.0, step=0.5, sizing_mode="stretch_width",
    )
    sigma_in    = pn.widgets.FloatInput(
        name="OI \u03c3_obs (\u00b5S/cm)", value=initial_sigma_obs,
        start=0.1, step=1.0, sizing_mode="stretch_width",
    )
    kernel_sel  = pn.widgets.Select(
        name="OI kernel",
        options=["exponential", "channel_direction"],
        value=initial_kernel, sizing_mode="stretch_width",
    )
    resist_sl   = pn.widgets.FloatSlider(
        name="Resistance (against-flow penalty)",
        value=initial_resistance, start=1.0, end=20.0, step=0.5,
        sizing_mode="stretch_width",
    )
    apply_btn   = pn.widgets.Button(
        name="\u21ba Apply", button_type="success",
        sizing_mode="stretch_width",
    )
    status_md   = pn.pane.Markdown("", sizing_mode="stretch_width")

    # --- dynamic visibility helpers ---
    resist_row = pn.Column(resist_sl, visible=(initial_kernel == "channel_direction"))
    idw_col    = pn.Column(power_sl, visible=(initial_method == "IDW"))
    oi_col     = pn.Column(sigma_in, kernel_sel, resist_row, visible=(initial_method == "OI"))

    def _on_kernel(event):
        resist_row.visible = (event.new == "channel_direction")

    def _on_method(event):
        idw_col.visible = (event.new == "IDW")
        oi_col.visible  = (event.new == "OI")

    kernel_sel.param.watch(_on_kernel, "value")
    method_sel.param.watch(_on_method, "value")

    # --- apply callback ---
    def _on_apply(_event):
        apply_btn.disabled = True
        status_md.object = "\u23f3 Building corrector\u2026"

        obs_path = obs_input.value.strip()
        sta_path = sta_input.value.strip()
        if not obs_path or not sta_path:
            status_md.object = "\u26a0 Both CSV paths are required."
            apply_btn.disabled = False
            return

        def _build():
            try:
                import geopandas as gpd
                from pydsm.analysis.network_correction import (
                    NetworkIDWCorrector,
                    NetworkOICorrector,
                    snap_stations_to_channel_ends,
                    exponential_kernel,
                    channel_direction_kernel,
                )
                from pydsm.viz.dsm2gis import read_stations

                stations_gdf = read_stations(sta_path)
                cl_gdf = gpd.read_file(str(centerlines_file))
                snapped = snap_stations_to_channel_ends(
                    stations_gdf, cl_gdf, channels_df
                )

                method = method_sel.value
                if method == "IDW":
                    new_corrector = NetworkIDWCorrector(
                        channels_df, snapped, power=power_sl.value
                    )
                else:
                    kfn = (
                        exponential_kernel()
                        if kernel_sel.value == "exponential"
                        else channel_direction_kernel(resistance=resist_sl.value)
                    )
                    new_corrector = NetworkOICorrector(
                        channels_df, snapped,
                        sigma_obs=sigma_in.value,
                        corr_fn=kfn,
                    )

                # Reload observations CSV if path changed
                import pandas as _pd
                reader._reload_observations(obs_path)
                reader.rebuild_corrector(new_corrector)

                # Invalidate buffer and re-render inside the Bokeh document.
                doc = getattr(mgr, "_bk_figure", None)
                doc = doc.document if doc is not None else None

                def _in_doc():
                    try:
                        current = (
                            mgr._transform_select.value
                            if hasattr(mgr, "_transform_select")
                            else "none"
                        )
                        if refresh_fn is not None:
                            refresh_fn(mgr, current)
                        else:
                            mgr._reader = mgr._setup_reader(current)
                            mgr._time_slider.param.trigger("value")
                        # Update meta so Save Config reflects new settings
                        mgr._animate_meta.setdefault("correction", {})
                        mgr._animate_meta["correction"].update({
                            "enabled": True,
                            "observations_csv": obs_path,
                            "stations_csv": sta_path,
                            "method": method.lower(),
                            "idw": {"power": power_sl.value},
                            "oi": {
                                "sigma_obs": sigma_in.value,
                                "kernel": kernel_sel.value,
                                "resistance": resist_sl.value,
                            },
                        })
                        status_md.object = f"\u2713 {method} correction applied."
                    except Exception as _exc:
                        status_md.object = f"\u2717 Render error: {_exc}"
                    finally:
                        apply_btn.disabled = False

                if doc is not None and hasattr(doc, "add_next_tick_callback"):
                    doc.add_next_tick_callback(_in_doc)
                else:
                    _in_doc()

            except Exception as exc:
                status_md.object = f"\u2717 {exc}"
                apply_btn.disabled = False

        threading.Thread(target=_build, daemon=True).start()

    apply_btn.on_click(_on_apply)

    return pn.Card(
        obs_input,
        sta_input,
        method_sel,
        idw_col,
        oi_col,
        apply_btn,
        status_md,
        title="Observation correction",
        collapsed=False,
        sizing_mode="stretch_width",
    )


# ---------------------------------------------------------------------------
# GeoDataFrame loader
# ---------------------------------------------------------------------------

def load_dsm2_channel_gdf(
    shapefile: "str | Path | None" = None,
    simplify_tolerance: float = 50.0,
    channel_id_column: "str | None" = None,
) -> "geopandas.GeoDataFrame":
    """Load DSM2 channel centreline geometry.

    Parameters
    ----------
    shapefile : str or Path, optional
        Path to an alternative GeoJSON or shapefile.  When ``None`` (default)
        the bundled ``dsm2_channels_centerlines_8_2.geojson`` is used.
    simplify_tolerance : float, optional
        Simplification tolerance in metres (applied in EPSG:3857).  Set to
        ``0`` to disable.  Default ``50`` m.
    channel_id_column : str or None, optional
        Name of the column in *shapefile* that contains integer channel numbers.
        When ``None`` (default) the function tries the standard auto-detected
        names: ``'id'``, ``'channel_nu'``, ``'CHAN_NO'``.
        Use this when your shapefile uses a non-standard column name.

    Returns
    -------
    geopandas.GeoDataFrame
        Has column ``"geo_id"`` (int) matching DSM2 channel numbers, plus
        ``"geometry"`` (LineString, EPSG:4326).
    """
    import logging
    import os
    import geopandas as gpd

    log = logging.getLogger(__name__)

    # SHAPE_RESTORE_SHX=YES tells GDAL/pyogrio to auto-recreate the .shx
    # index file if it is missing from an ESRI Shapefile.
    _old_shx = os.environ.get("SHAPE_RESTORE_SHX")
    os.environ["SHAPE_RESTORE_SHX"] = "YES"
    try:
        if shapefile is not None:
            gdf = gpd.read_file(shapefile)
            log.info(
                "Loaded shapefile %s: %d features, CRS=%s, columns=%s",
                shapefile, len(gdf),
                getattr(gdf, 'crs', 'unknown'),
                list(gdf.columns),
            )
        else:
            pkg_path = Path(__file__).parent / "dsm2gis" / "dsm2_channels_centerlines_8_2.geojson"
            gdf = gpd.read_file(str(pkg_path))
    finally:
        if _old_shx is None:
            os.environ.pop("SHAPE_RESTORE_SHX", None)
        else:
            os.environ["SHAPE_RESTORE_SHX"] = _old_shx

    # Resolve the channel-number column.
    # Priority: (1) explicit channel_id_column arg, (2) auto-detect standard names.
    _AUTO_NAMES = ["id", "channel_nu", "CHAN_NO"]
    resolved_col: "str | None" = None

    if channel_id_column is not None:
        if channel_id_column in gdf.columns:
            resolved_col = channel_id_column
        else:
            col_info = ", ".join(
                f"{c} ({gdf[c].dtype})" for c in gdf.columns if c != "geometry"
            ) or "<no non-geometry columns>"
            raise ValueError(
                f"--channel-id-column {channel_id_column!r} not found in shapefile.\n"
                f"Available columns: {col_info}\n"
                f"File: {shapefile}"
            )
    else:
        for name in _AUTO_NAMES:
            if name in gdf.columns:
                resolved_col = name
                break

    if resolved_col is None:
        non_geom = [c for c in gdf.columns if c != "geometry"]
        col_info = ", ".join(
            f"{c} ({gdf[c].dtype})" for c in non_geom
        ) or "<no non-geometry columns>"
        raise ValueError(
            f"Cannot identify the channel number column in the shapefile.\n"
            f"Tried auto-detect names: {_AUTO_NAMES!r}\n"
            f"Available non-geometry columns: {col_info}\n"
            f"File: {shapefile}\n"
            f"Hint: re-run with --channel-id-column <name> to specify it explicitly."
        )

    log.info("Using column %r as channel id (dtype=%s)", resolved_col, gdf[resolved_col].dtype)
    if resolved_col != "geo_id":
        gdf = gdf.rename(columns={resolved_col: "geo_id"})

    # Coerce geo_id to int — handles both int32/int64 and string representations
    # (e.g. GeoJSON may load integer-valued strings as object dtype on some
    # fiona/pyogrio versions).
    try:
        gdf["geo_id"] = gdf["geo_id"].astype(int)
    except (ValueError, TypeError) as exc:
        raise ValueError(
            f"Channel ID column {resolved_col!r} could not be coerced to integers: {exc}\n"
            f"Sample values: {gdf['geo_id'].head(5).tolist()}"
        ) from exc

    # Ensure a valid CRS before any reprojection.  If the file has no CRS,
    # issue a warning and assume EPSG:4326 only as a last resort (this is
    # almost certainly wrong for shapefiles in a projected system, but lets
    # the user get a partial result rather than a hard crash).
    if gdf.crs is None:
        log.warning(
            "Shapefile/GeoJSON has no CRS; assuming EPSG:4326. "
            "If the geometry looks wrong, set the CRS in your file or supply "
            "a properly georeferenced shapefile with --shapefile."
        )
        gdf = gdf.set_crs("EPSG:4326")

    # Simplify geometry in EPSG:3857 (metres) then reproject to WGS84.
    # This removes redundant vertices from complex channel centrelines,
    # giving a significant rendering speed-up for large tidefiles.
    if simplify_tolerance and simplify_tolerance > 0:
        gdf = gdf.to_crs("EPSG:3857")
        # Drop rows whose geometry has non-finite coordinates (NaN / inf).
        # Such geometries cause a GEOS IllegalArgumentException during
        # simplification and typically come from NULL / corrupt shapefile rows.
        import shapely
        valid_mask = gdf.geometry.apply(
            lambda g: g is not None
            and not g.is_empty
            and np.all(np.isfinite(shapely.get_coordinates(g)))
        )
        n_dropped = (~valid_mask).sum()
        if n_dropped:
            log.warning(
                "Dropping %d shapefile row(s) with non-finite geometry coordinates "
                "before simplification (channel ids: %s).",
                n_dropped,
                gdf.loc[~valid_mask, "geo_id"].tolist(),
            )
            gdf = gdf.loc[valid_mask].copy()
        if gdf.empty:
            raise ValueError(
                f"All {n_dropped} row(s) in the shapefile/GeoJSON were dropped because "
                "their geometry coordinates are non-finite (NaN / inf / zero).\n"
                "This usually means the bundled channel centreline GeoJSON does not "
                "match the DSM2 grid used by this HDF5 file (e.g. a planning study "
                "uses a different grid from the historical base).\n"
                "Fix: supply a shapefile that matches your grid with\n"
                "  --shapefile path/to/channels.shp"
            )
        gdf["geometry"] = gdf.geometry.simplify(
            tolerance=simplify_tolerance, preserve_topology=True
        )
        gdf = gdf.to_crs("EPSG:4326")
        return gdf

    # Ensure WGS84
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    elif not gdf.crs.equals("EPSG:4326"):
        gdf = gdf.to_crs("EPSG:4326")

    return gdf


# ---------------------------------------------------------------------------
# Convenience factories
# ---------------------------------------------------------------------------


def make_composed_transform(
    spec_a: "TransformSpec",
    spec_b: "TransformSpec",
) -> "TransformSpec":
    """Return a :class:`~dvue.animator.TransformSpec` that applies *spec_a*
    then *spec_b* in sequence.

    The warmup overlap (``get_overlap``) from *spec_a* is preserved so the
    streaming reader prefetches enough context for *spec_a*'s window (e.g.
    Godin's 33.5 h).  The output frequency is taken from *spec_b* (the final
    resampling step).

    Parameters
    ----------
    spec_a : TransformSpec
        First transform (e.g. Godin filter).
    spec_b : TransformSpec
        Second transform applied to the result of *spec_a*
        (e.g. daily resample).

    Returns
    -------
    TransformSpec
        Composed specification.
    """
    from dvue.animator.reader import TransformSpec

    fn_a = spec_a.transform_fn
    fn_b = spec_b.transform_fn

    def _composed(df: pd.DataFrame) -> pd.DataFrame:
        return fn_b(fn_a(df))

    _composed.__name__ = (
        f"{getattr(fn_a, '__name__', 'a')}+{getattr(fn_b, '__name__', 'b')}"
    )

    return TransformSpec(
        transform_fn=_composed,
        # The output kind is determined by the final stage: if spec_b is
        # aggregate (e.g. daily resample), the composed transform must also be
        # aggregate so dvue uses the correct code path.  Using spec_a's kind
        # here would trigger dvue's convolution path which assumes input_len ==
        # output_len — that assumption is violated when spec_b resamples.
        kind=spec_b.kind if spec_b.kind == "aggregate" else spec_a.kind,
        get_overlap=spec_a.get_overlap,
        output_freq=spec_b.output_freq or spec_a.output_freq,
    )


def _dsm2_transform_options() -> dict:
    """Return the standard DSM2 transform options dict for GeoAnimatorManager.

    Keys are display names; values are ``TransformSpec`` instances.
    """
    godin  = make_godin_transform()
    dmean  = make_resample_transform("D", "mean")
    dmin   = make_resample_transform("D", "min")
    dmax   = make_resample_transform("D", "max")
    r24h   = make_moving_average_transform("24h")
    r14d   = make_moving_average_transform("14D")
    return {
        "Daily mean":           dmean,
        "Daily min":            dmin,
        "Daily max":            dmax,
        "Rolling 24 h":         r24h,
        "Rolling 14 D":         r14d,
        "Godin filter":         godin,
        "Godin \u2192 Daily mean":   make_composed_transform(godin, dmean),
        "Godin \u2192 Daily min":    make_composed_transform(godin, dmin),
        "Godin \u2192 Daily max":    make_composed_transform(godin, dmax),
    }


def _dsm2_transform_cli_keys() -> dict:
    """Reverse-map from display name to CLI key for all standard DSM2 transforms."""
    return {
        "Daily mean":               "daily",
        "Daily min":                "daily-min",
        "Daily max":                "daily-max",
        "Rolling 24 h":             "rolling-24h",
        "Rolling 14 D":             "rolling-14d",
        "Godin filter":             "godin",
        "Godin \u2192 Daily mean":  "godin-daily",
        "Godin \u2192 Daily min":   "godin-daily-min",
        "Godin \u2192 Daily max":   "godin-daily-max",
    }


def _make_resample_card(mgr) -> "pn.Card":
    """Build a Panel Card for interactive custom-period resampling.

    Lets the user compose any period + aggregation on top of the current
    transform.  Clicking **Apply** adds a new option to the transform
    selector and selects it, which triggers the manager's normal
    ``_on_transform_change`` path.
    """
    import panel as pn

    PERIOD_PRESETS = ["1h", "3h", "6h", "12h", "1D", "2D", "7D", "1ME"]

    period_select = pn.widgets.Select(
        name="Resample period", options=PERIOD_PRESETS + ["custom"],
        value="1D", sizing_mode="stretch_width",
    )
    period_custom = pn.widgets.TextInput(
        name="Custom period", value="1D",
        placeholder="e.g. 3D, 12h, 1ME",
        sizing_mode="stretch_width", visible=False,
    )
    agg_select = pn.widgets.Select(
        name="Aggregation", options=["mean", "min", "max"],
        value="mean", sizing_mode="stretch_width",
    )
    base_select = pn.widgets.Select(
        name="Base transform",
        options=["(current)"] + list(mgr._transform_options.keys()),
        value="(current)", sizing_mode="stretch_width",
    )
    apply_btn = pn.widgets.Button(
        name="\u21ba Apply resample",
        button_type="success", sizing_mode="stretch_width",
    )
    status_md = pn.pane.Markdown("", sizing_mode="stretch_width")

    def _on_period_change(event):
        period_custom.visible = (event.new == "custom")

    period_select.param.watch(_on_period_change, "value")

    def _on_apply(_):
        apply_btn.disabled = True
        freq = (
            period_custom.value.strip()
            if period_select.value == "custom"
            else period_select.value
        )
        if not freq:
            status_md.object = "\u26a0 Specify a valid period."
            apply_btn.disabled = False
            return

        agg = agg_select.value
        base_name = base_select.value
        if base_name == "(current)":
            base_name = getattr(mgr._transform_select, "value", "none")

        try:
            resample_spec = make_resample_transform(freq, agg)
            if base_name != "none" and base_name in mgr._transform_options:
                composed = make_composed_transform(
                    mgr._transform_options[base_name], resample_spec
                )
                display_name = f"{base_name} \u2192 {agg}({freq})"
            else:
                composed = resample_spec
                display_name = f"{agg}({freq})"

            mgr._transform_options[display_name] = composed
            if display_name not in mgr._transform_select.options:
                mgr._transform_select.options = (
                    mgr._transform_select.options + [display_name]
                )
            # Selecting this value triggers the manager's own transform change
            # handler — no extra re-render needed.
            mgr._transform_select.value = display_name

            # Keep base_select in sync with any new options
            base_select.options = (
                ["(current)"] + list(mgr._transform_options.keys())
            )

            mgr._animate_meta.setdefault("resample", {})
            mgr._animate_meta["resample"] = {
                "enabled": True,
                "freq": freq,
                "agg": agg,
                "base_transform": base_name if base_name != "(current)" else None,
            }
            status_md.object = f"\u2713 Applied: {display_name}"
        except Exception as exc:
            status_md.object = f"\u2717 {exc}"
        finally:
            apply_btn.disabled = False

    apply_btn.on_click(_on_apply)

    return pn.Card(
        period_select,
        period_custom,
        agg_select,
        base_select,
        apply_btn,
        status_md,
        title="Resample",
        collapsed=True,
        sizing_mode="stretch_width",
    )


def _add_resample_card_to_manager(mgr) -> None:
    """Patch ``collect_state`` so custom resample settings survive a
    Save Config → reload cycle.  The Resample card is intentionally not
    added to the sidebar (resample options are available through the
    Transform selector: Godin → Daily mean/min/max etc.)."""
    _orig = mgr.collect_state

    def _patched_with_resample():
        state = _orig()
        if mgr._animate_meta.get("resample", {}).get("enabled"):
            state["resample"] = mgr._animate_meta["resample"]
        return state

    mgr.collect_state = _patched_with_resample


def animate_hydro(
    h5file: "str | Path",
    variable: str = "flow",
    location: str = "both",
    shapefile: "str | Path | None" = None,
    simplify_tolerance: float = 50.0,
    channel_id_column: "str | None" = None,
    **mgr_kwargs,
) -> "dvue.animator.GeoAnimatorManager":
    """Create a :class:`~dvue.animator.GeoAnimatorManager` for HYDRO channel data.

    Parameters
    ----------
    h5file : str or Path
    variable : {"flow", "stage", "velocity"}, optional
    location : {"both", "upstream", "downstream"}, optional
    shapefile : str or Path, optional
    simplify_tolerance : float, optional
    channel_id_column : str or None, optional
        Column in *shapefile* holding integer channel IDs.  Auto-detected when
        ``None`` (tries ``'id'``, ``'channel_nu'``, ``'CHAN_NO'``).
    **mgr_kwargs
        Forwarded to :class:`~dvue.animator.GeoAnimatorManager`.
    """
    from dvue.animator import GeoAnimatorManager

    variable = variable.lower()
    if variable == "flow":
        reader = HydroH5FlowReader(h5file, location=location)
    elif variable == "stage":
        reader = HydroH5StageReader(h5file, location=location)
    elif variable == "velocity":
        reader = HydroH5VelocityReader(h5file, location=location)
    else:
        raise ValueError(f"variable must be 'flow', 'stage', or 'velocity', got {variable!r}")

    gdf = load_dsm2_channel_gdf(
        shapefile,
        simplify_tolerance=simplify_tolerance,
        channel_id_column=channel_id_column,
    )
    mgr_kwargs.setdefault("title", f"DSM2 Hydro \u2014 {variable.title()}")
    mgr_kwargs.setdefault("colormap", "rainbow")
    mgr_kwargs.setdefault("transform_options", _dsm2_transform_options())
    mgr_kwargs.setdefault("buffer_chunk_size", 200)
    mgr = GeoAnimatorManager(reader, gdf, geo_id_column="geo_id", **mgr_kwargs)
    # Attach metadata so the Save config card can write a complete YAML.
    h5abs = str(Path(h5file).absolute())
    mgr._animate_meta = {
        "mode": "single",
        "file_type": "hydro",
        "files": [{"path": h5abs, "title": ""}],
        "variable": variable,
        "location": location,
        "shapefile": str(Path(shapefile).absolute()) if shapefile else None,
        "channel_id_column": channel_id_column,
        "_transform_cli_keys": _dsm2_transform_cli_keys(),
    }
    mgr._config_path_input.value = str(
        Path(h5abs).with_name(Path(h5abs).stem + "_animate.yml")
    )
    _add_resample_card_to_manager(mgr)
    return mgr


class QualH5X2Callback:
    """Computes the X2 isohaline line from a QUAL/GTM HDF5 concentration dataset.

    X2 is the distance from the Golden Gate where salinity equals a threshold
    (typically 2 psu ≈ 2000–2700 µS/cm EC).  For each time step this callback:

    1. Reads the upstream and downstream EC values for every channel.
    2. Finds channels where the threshold is crossed between the two ends.
    3. Linearly interpolates the exact crossing point along each channel geometry.
    4. Sorts the crossing points by easting (x) and returns them as a single
       connected ``multi_line`` path.

    Parameters
    ----------
    h5_handle : h5py.File
        Open HDF5 file handle (kept open by the reader).
    ds_handle : h5py dataset
        ``/output/channel concentration`` dataset.
    constituent_index : int
        Index of the constituent to use (e.g. 0 for EC).
    channel_numbers : list of int
        Channel numbers corresponding to dataset axis 1.
    gdf_proj : gpd.GeoDataFrame
        Channel centerlines in EPSG:3857 with ``geo_id`` column (int).
        Should be the *unsimplified* geometry for accurate interpolation.
    geo_id_column : str, optional
        Column in *gdf_proj* holding channel ids.  Default ``"geo_id"``.
    """

    def __init__(
        self,
        h5_handle,
        ds_handle,
        constituent_index: int,
        channel_numbers: list,
        gdf_proj: "gpd.GeoDataFrame",
        geo_id_column: str = "geo_id",
    ) -> None:
        self._h5 = h5_handle
        self._ds = ds_handle
        self._ci = constituent_index
        self._channel_numbers = channel_numbers
        # Pre-build a dict: channel_id → geometry for fast lookup
        self._chan_geom: dict = {
            int(row[geo_id_column]): row.geometry
            for _, row in gdf_proj.iterrows()
        }

    def __call__(self, step_idx: int, threshold: float = 2700.0) -> tuple[list, list]:
        """Return (xs, ys) lists-of-lists for the X2 isohaline at *step_idx*.

        Parameters
        ----------
        step_idx : int
            Time step index.
        threshold : float
            EC threshold in µS/cm (default 2700).

        Returns
        -------
        (xs, ys) : tuple[list, list]
            Each is a list of lists suitable for Bokeh ``multi_line``.
            Typically a single connected path, so ``xs = [[x0, x1, …]]``.
        """
        row = self._ds[step_idx, self._ci, :, :].astype(float)  # (n_ch, 2)
        row[row < -1e20] = np.nan

        # Determine location order from HDF5 (upstream=0, downstream=1 by convention)
        upstream = row[:, 0]
        downstream = row[:, 1]

        # Find channels where the threshold is crossed between u and d ends
        cond = (
            (np.isfinite(upstream) & np.isfinite(downstream)) &
            (
                ((upstream < threshold) & (downstream >= threshold)) |
                ((upstream >= threshold) & (downstream < threshold))
            )
        )

        if not np.any(cond):
            return [], []

        chan_indices = np.where(cond)[0]
        points: list[tuple[float, float]] = []

        for idx in chan_indices:
            u_val = upstream[idx]
            d_val = downstream[idx]
            chan_id = int(self._channel_numbers[idx])
            geom = self._chan_geom.get(chan_id)
            if geom is None or geom.is_empty:
                continue

            denom = d_val - u_val
            if denom == 0.0:
                continue
            # Normalized distance (0 = upstream end, 1 = downstream end)
            norm = float((threshold - u_val) / denom)
            norm = max(0.0, min(1.0, norm))

            pt = geom.interpolate(norm, normalized=True)
            points.append((pt.x, pt.y))

        if len(points) < 2:
            return [], []

        # Sort crossing points by x (easting) to form a connected line
        points.sort(key=lambda p: p[0])
        return [[p[0] for p in points]], [[p[1] for p in points]]


def animate_qual(
    h5file: "str | Path",
    constituent: str = "ec",
    shapefile: "str | Path | None" = None,
    simplify_tolerance: float = 50.0,
    x2_threshold: "float | None" = None,
    channel_id_column: "str | None" = None,
    **mgr_kwargs,
) -> "dvue.animator.GeoAnimatorManager":
    """Create a :class:`~dvue.animator.GeoAnimatorManager` for QUAL/GTM concentrations.

    Parameters
    ----------
    h5file : str or Path
        QUAL or GTM HDF5 tidefile.
    constituent : str, optional
        Constituent name (case-insensitive).  Default ``"ec"``.
    shapefile : str or Path, optional
        Custom channel geometry.  Defaults to bundled GeoJSON.
    simplify_tolerance : float, optional
        Geometry simplification tolerance in metres.  Default 50 m.
    x2_threshold : float or None, optional
        When provided, enables the X2 isohaline control in the UI.
    channel_id_column : str or None, optional
        Column in *shapefile* holding integer channel IDs.  Auto-detected when
        ``None`` (tries ``'id'``, ``'channel_nu'``, ``'CHAN_NO'``).
    **mgr_kwargs
        Forwarded to :class:`~dvue.animator.GeoAnimatorManager`.
    """
    from dvue.animator import GeoAnimatorManager

    reader = QualH5ConcentrationReader(h5file, constituent=constituent)

    x2_callback = None
    if x2_threshold is not None:
        x2_gdf = load_dsm2_channel_gdf(
            shapefile, simplify_tolerance=0, channel_id_column=channel_id_column
        )
        x2_callback = QualH5X2Callback(
            reader._h5,
            reader._ds,
            reader._constituent_index,
            reader._channel_numbers,
            x2_gdf.to_crs("EPSG:3857"),
        )

    gdf = load_dsm2_channel_gdf(
        shapefile,
        simplify_tolerance=simplify_tolerance,
        channel_id_column=channel_id_column,
    )
    mgr_kwargs.setdefault("title", f"DSM2 QUAL/GTM \u2014 {constituent.upper()}")
    mgr_kwargs.setdefault("colormap", "rainbow")
    mgr_kwargs.setdefault("transform_options", _dsm2_transform_options())
    mgr_kwargs.setdefault("buffer_chunk_size", 200)
    mgr = GeoAnimatorManager(
        reader, gdf, geo_id_column="geo_id",
        x2_callback=x2_callback, **mgr_kwargs,
    )
    if x2_threshold is not None:
        mgr._x2_threshold_input.value = float(x2_threshold)
    # Attach metadata so the Save config card can write a complete YAML.
    h5abs = str(Path(h5file).absolute())
    mgr._animate_meta = {
        "mode": "single",
        "file_type": "qual",
        "files": [{"path": h5abs, "title": ""}],
        "variable": constituent,
        "location": "both",
        "shapefile": str(Path(shapefile).absolute()) if shapefile else None,
        "channel_id_column": channel_id_column,
        "_transform_cli_keys": _dsm2_transform_cli_keys(),
    }
    mgr._config_path_input.value = str(
        Path(h5abs).with_name(Path(h5abs).stem + "_animate.yml")
    )
    _add_resample_card_to_manager(mgr)
    return mgr


def animate_qual_corrected(
    h5file: "str | Path",
    observations_csv: "str | Path",
    stations_csv: "str | Path",
    centerlines_file: "str | Path | None" = None,
    constituent: str = "ec",
    power: float = 2,
    max_distance: "float | None" = None,
    max_obs_age: str = "2h",
    echo_inp_file: "str | Path | None" = None,
    corrector=None,
    shapefile: "str | Path | None" = None,
    simplify_tolerance: float = 50.0,
    channel_id_column: "str | None" = None,
    **mgr_kwargs,
) -> "dvue.animator.GeoAnimatorManager":
    """Create a :class:`~dvue.animator.GeoAnimatorManager` for QUAL/GTM concentrations
    with network bias correction from sparse observations.

    The correction is applied to the raw channel-end values **before** any
    downstream transform (moving-average, Godin filter, etc.).

    When no observations are available for a given time step (all-NaN or outside
    *max_obs_age*) the model values are returned unchanged for that step.

    Two correction methods are available:

    * **IDW** (default) — inverse-distance weighting on a directed network graph;
      corrections propagate only downstream.  Controlled by *power* and
      *max_distance*.
    * **OI** — optimal interpolation with a symmetric exponential or
      channel-direction kernel; de-weights redundant nearby stations.  Pass a
      pre-built :class:`~pydsm.analysis.network_correction.NetworkOICorrector`
      via the *corrector* argument.

    Parameters
    ----------
    h5file : str or Path
        QUAL or GTM HDF5 tidefile.
    observations_csv : str or Path
        Time-indexed CSV.  First column = datetime index; remaining columns =
        station IDs.  NaN cells are treated as missing.
    stations_csv : str or Path
        CSV with ``station_id`` and lat/lon or x/y columns.
    centerlines_file : str or Path or None, optional
        GeoJSON or shapefile of DSM2 channel centrelines used for snapping
        observation stations.  When ``None`` (default) the bundled
        ``dsm2_channels_centerlines_8_2.geojson`` is used.
    constituent : str, optional
        Constituent name.  Default ``"ec"``.
    power : float, optional
        IDW distance exponent.  Default ``2``.  Ignored when *corrector* is
        supplied.
    max_distance : float or None, optional
        Maximum network distance (ft) for IDW correction influence.  Ignored
        when *corrector* is supplied.
    max_obs_age : str, optional
        Maximum time difference for observation matching.  Default ``"2h"``.
    echo_inp_file : str or Path or None, optional
        Fallback DSM2 echo ``.inp`` file for the CHANNEL table.
    corrector : NetworkCorrector or None, optional
        A pre-built :class:`~pydsm.analysis.network_correction.NetworkCorrector`
        instance (e.g. :class:`~pydsm.analysis.network_correction.NetworkOICorrector`).
        When ``None`` (default) a :class:`~pydsm.analysis.network_correction.NetworkIDWCorrector`
        is built from *power* and *max_distance*.
    shapefile : str or Path, optional
        Custom channel geometry for the map display.  Defaults to bundled GeoJSON.
    simplify_tolerance : float, optional
        Geometry simplification tolerance in metres.  Default 50 m.
    channel_id_column : str or None, optional
        Column in *shapefile* holding integer channel IDs.
    **mgr_kwargs
        Forwarded to :class:`~dvue.animator.GeoAnimatorManager`.
    """
    from dvue.animator import GeoAnimatorManager

    # Resolve centerlines: default to the bundled GeoJSON
    if centerlines_file is None:
        centerlines_file = (
            Path(__file__).parent / "dsm2gis" / "dsm2_channels_centerlines_8_2.geojson"
        )

    reader = CorrectedQualH5ConcentrationReader(
        h5file=h5file,
        observations_csv=observations_csv,
        stations_csv=stations_csv,
        centerlines_file=centerlines_file,
        constituent=constituent,
        power=power,
        max_distance=max_distance,
        max_obs_age=max_obs_age,
        echo_inp_file=echo_inp_file,
        corrector=corrector,
    )
    gdf = load_dsm2_channel_gdf(
        shapefile,
        simplify_tolerance=simplify_tolerance,
        channel_id_column=channel_id_column,
    )
    mgr_kwargs.setdefault(
        "title", f"DSM2 QUAL/GTM \u2014 {constituent.upper()} (IDW corrected)"
    )
    mgr_kwargs.setdefault("colormap", "rainbow")
    mgr_kwargs.setdefault("transform_options", _dsm2_transform_options())
    mgr_kwargs.setdefault("buffer_chunk_size", 200)
    mgr = GeoAnimatorManager(reader, gdf, geo_id_column="geo_id", **mgr_kwargs)

    # --- detect effective method name for meta ----------------------------
    _corr_type = type(reader._corrector).__name__   # 'NetworkIDWCorrector' etc.
    _method_name = "oi" if "OI" in _corr_type else "idw"
    _power  = getattr(reader._corrector, "power", 2.0)
    _so     = getattr(reader._corrector, "_sigma_obs", 10.0)
    _kfn    = getattr(reader._corrector, "_corr_fn", None)
    _kernel = getattr(_kfn, "_kind", "exponential") if _kfn is not None else "exponential"
    _resist = getattr(_kfn, "_resistance", 3.0) if _kfn is not None else 3.0

    # --- store correction settings in _animate_meta so Save Config works --
    h5abs  = str(Path(next(
        e["path"] for e in mgr._animate_meta.get("files", [{"path": ""}])
    )).absolute()) if mgr._animate_meta.get("files") else ""

    corr_meta = {
        "enabled": True,
        "observations_csv": str(observations_csv),
        "stations_csv": str(stations_csv),
        "centerlines_file": str(centerlines_file) if centerlines_file else None,
        "echo_inp_file": str(echo_inp_file) if echo_inp_file else None,
        "max_obs_age": max_obs_age,
        "method": _method_name,
        "idw": {"power": _power},
        "oi": {
            "sigma_obs": _so,
            "kernel": _kernel,
            "resistance": _resist,
            "length_scale": None,
        },
    }
    mgr._animate_meta["correction"] = corr_meta

    # --- patch collect_state to emit the correction section ---------------
    _orig_collect = mgr.collect_state

    def _patched_collect_state():
        state = _orig_collect()
        state["correction"] = mgr._animate_meta.get("correction", {})
        return state

    mgr.collect_state = _patched_collect_state

    # --- wire correction card into the controls panel ---------------------
    _channels_df = reader._corrector._snapped.reset_index() if hasattr(
        reader._corrector, "_snapped"
    ) else pd.DataFrame()
    # Load channels from the H5 file (same path used by the reader)
    try:
        from dsm2ui.animate import CorrectedQualH5ConcentrationReader as _CQHR
        _channels_df_full = _CQHR._load_channels(
            Path(observations_csv).parent  # dummy — we need to get channels_df
            if False else
            # Actually load from the reader's inner H5
            reader._inner._filepath,
            echo_inp_file,
        )
    except Exception:
        _channels_df_full = pd.DataFrame()  # fallback — card still works for param changes

    _cl_for_card = (
        centerlines_file
        if centerlines_file is not None
        else Path(__file__).parent / "dsm2gis" / "dsm2_channels_centerlines_8_2.geojson"
    )
    correction_card = _make_correction_card(
        mgr=mgr,
        reader=reader,
        channels_df=_channels_df_full,
        centerlines_file=_cl_for_card,
        initial_obs_csv=str(observations_csv),
        initial_stations_csv=str(stations_csv),
        initial_method=_method_name.upper(),
        initial_power=_power,
        initial_sigma_obs=_so,
        initial_kernel=_kernel,
        initial_resistance=_resist,
    )
    # Insert the correction card just before the Save Config card (last item)
    mgr._controls.insert(-1, correction_card)
    # Ensure _transform_cli_keys is present so config save round-trips correctly.
    mgr._animate_meta.setdefault("_transform_cli_keys", _dsm2_transform_cli_keys())
    _add_resample_card_to_manager(mgr)
    return mgr


def animate_qual_corrected_multi(
    h5file: "str | Path",
    observations_csv: "str | Path",
    stations_csv: "str | Path",
    centerlines_file: "str | Path | None" = None,
    constituent: str = "ec",
    power: float = 2,
    max_distance: "float | None" = None,
    max_obs_age: str = "2h",
    echo_inp_file: "str | Path | None" = None,
    corrector=None,
    shapefile: "str | Path | None" = None,
    simplify_tolerance: float = 50.0,
    channel_id_column: "str | None" = None,
    **mgr_kwargs,
) -> "dvue.animator.MultiGeoAnimatorManager":
    """Side-by-side: Model Only vs Model + Bias-Corrected.

    Creates a :class:`~dvue.animator.MultiGeoAnimatorManager` where

    * **Panel A** — raw model values from the QUAL tidefile.
    * **Panel B** — model values after network bias correction from sparse
      observations (IDW or OI depending on *corrector*).

    Both panels share the same channel geometry and colour scale so differences
    due to the correction are immediately visible.

    Parameters
    ----------
    h5file : str or Path
        QUAL or GTM HDF5 tidefile.
    observations_csv : str or Path
        Time-indexed CSV of observations (station IDs as columns).
    stations_csv : str or Path
        CSV with ``station_id`` and lat/lon or x/y columns.
    centerlines_file : str or Path or None, optional
        Channel centrelines for station snapping.  Defaults to the bundled GeoJSON.
    constituent : str, optional
        Constituent name.  Default ``"ec"``.
    power : float, optional
        IDW distance exponent.  Ignored when *corrector* is supplied.
    max_distance : float or None, optional
        Max IDW influence distance (ft).  Ignored when *corrector* is supplied.
    max_obs_age : str, optional
        Maximum observation age for matching.  Default ``"2h"``.
    echo_inp_file : str or Path or None, optional
        DSM2 echo ``.inp`` file as fallback CHANNEL table source.
    corrector : NetworkCorrector or None, optional
        Pre-built corrector (e.g. NetworkOICorrector).  When ``None`` an IDW
        corrector is built automatically.
    shapefile : str or Path or None, optional
        Custom channel geometry.  Defaults to bundled GeoJSON.
    simplify_tolerance : float, optional
        Geometry simplification tolerance in metres.  Default 50 m.
    channel_id_column : str or None, optional
        Column in *shapefile* holding integer channel IDs.
    **mgr_kwargs
        Forwarded to :class:`~dvue.animator.MultiGeoAnimatorManager`.
    """
    from dvue.animator import MultiGeoAnimatorManager

    if centerlines_file is None:
        centerlines_file = (
            Path(__file__).parent / "dsm2gis" / "dsm2_channels_centerlines_8_2.geojson"
        )

    # Panel A — raw model (no correction applied)
    reader_raw = QualH5ConcentrationReader(h5file, constituent=constituent)

    # Panel B — bias-corrected model
    reader_corrected = CorrectedQualH5ConcentrationReader(
        h5file=h5file,
        observations_csv=observations_csv,
        stations_csv=stations_csv,
        centerlines_file=centerlines_file,
        constituent=constituent,
        power=power,
        max_distance=max_distance,
        max_obs_age=max_obs_age,
        echo_inp_file=echo_inp_file,
        corrector=corrector,
    )

    gdf = load_dsm2_channel_gdf(
        shapefile,
        simplify_tolerance=simplify_tolerance,
        channel_id_column=channel_id_column,
    )

    _corr_type = type(reader_corrected._corrector).__name__
    _method = "OI" if "OI" in _corr_type else "IDW"

    title_a = f"Model \u2014 {constituent.upper()}"
    title_b = f"Model + {_method} Correction \u2014 {constituent.upper()}"

    mgr_kwargs.setdefault("colormap", "rainbow")
    mgr_kwargs.setdefault("transform_options", _dsm2_transform_options())
    mgr_kwargs.setdefault("buffer_chunk_size", 200)

    mgr = MultiGeoAnimatorManager(
        reader_raw, reader_corrected,
        gdf_a=gdf, gdf_b=gdf,
        title_a=title_a, title_b=title_b,
        geo_id_column="geo_id",
        show_diff=False,
        **mgr_kwargs,
    )

    # Metadata for the Save Config card
    h5abs = str(Path(h5file).absolute())
    mgr._animate_meta = {
        "mode": "corrected_multi",
        "file_type": "qual",
        "files": [{"path": h5abs, "title": title_a}],
        "variable": constituent,
        "shapefile": str(Path(shapefile).absolute()) if shapefile else None,
        "channel_id_column": channel_id_column,
        "correction": {
            "enabled": True,
            "observations_csv": str(observations_csv),
            "stations_csv": str(stations_csv),
            "centerlines_file": str(centerlines_file),
            "echo_inp_file": str(echo_inp_file) if echo_inp_file else None,
            "max_obs_age": max_obs_age,
            "method": _method.lower(),
            "idw": {"power": power},
        },
        "_transform_cli_keys": _dsm2_transform_cli_keys(),
    }
    mgr._config_path_input.value = str(
        Path(h5file).absolute().with_name(
            Path(h5file).stem + "_animate.yml"
        )
    )

    # --- wire the correction card into the sidebar -----------------------
    # The card controls the corrector on panel B (reader_corrected).
    # When the user clicks Apply, panel B's reader is rebuilt; panel A
    # (raw model) is left unchanged so the comparison remains valid.

    _cl_for_card = (
        centerlines_file
        if centerlines_file is not None
        else Path(__file__).parent / "dsm2gis" / "dsm2_channels_centerlines_8_2.geojson"
    )
    try:
        _channels_df_full = CorrectedQualH5ConcentrationReader._load_channels(
            reader_corrected._inner._filepath, echo_inp_file
        )
    except Exception:
        _channels_df_full = pd.DataFrame()

    _power  = getattr(reader_corrected._corrector, "power", 2.0)
    _so     = getattr(reader_corrected._corrector, "_sigma_obs", 10.0)
    _kfn    = getattr(reader_corrected._corrector, "_corr_fn", None)
    _kernel = getattr(_kfn, "_kind", "exponential") if _kfn is not None else "exponential"
    _resist = getattr(_kfn, "_resistance", 3.0) if _kfn is not None else 3.0

    def _refresh_panel_b(m, current_transform):
        """Rebuild panel B's reader pipeline and trigger a re-render."""
        m._reader_b = m._setup_reader(m._base_reader_b, current_transform)
        m._time_slider.param.trigger("value")

    correction_card = _make_correction_card(
        mgr=mgr,
        reader=reader_corrected,
        channels_df=_channels_df_full,
        centerlines_file=_cl_for_card,
        initial_obs_csv=str(observations_csv),
        initial_stations_csv=str(stations_csv),
        initial_method=_method,
        initial_power=_power,
        initial_sigma_obs=_so,
        initial_kernel=_kernel,
        initial_resistance=_resist,
        refresh_fn=_refresh_panel_b,
    )
    # Insert the correction card just before the Save Config card (last item)
    mgr._controls.insert(-1, correction_card)

    # --- patch collect_state so Save Config includes the correction block ---
    # MultiGeoAnimatorManager's default collect_state doesn't know about the
    # correction metadata; patch it the same way animate_qual_corrected does.
    _orig_collect_multi = mgr.collect_state

    def _patched_collect_multi():
        state = _orig_collect_multi()
        state["correction"] = mgr._animate_meta.get("correction", {})
        return state

    mgr.collect_state = _patched_collect_multi
    _add_resample_card_to_manager(mgr)
    return mgr

# Godin filter warmup in minutes — the filter needs 33.5 h of data before the
# first valid output.  At 15-min intervals that is 134 raw steps.
_GODIN_WARMUP_MINUTES = int(33.5 * 60)


def _freq_minutes(reader: "SlicingReader") -> float:
    """Return the reader's time step in minutes."""
    nanos = pd.tseries.frequencies.to_offset(reader.time_index.freq).nanos
    return nanos / 60e9


def make_resample_transform(freq: str = "D", agg: str = "mean"):
    """Return a :class:`~dvue.animator.TransformSpec` for resampling to a coarser step.

    Parameters
    ----------
    freq : str, optional
        Target pandas frequency string.  Default ``"D"``.
    agg : {"mean", "sum", "max", "min"}, optional
        Default ``"mean"``.

    Returns
    -------
    TransformSpec
    """
    from dvue.animator.reader import TransformSpec

    def _transform(df: pd.DataFrame) -> pd.DataFrame:
        resampled = getattr(df.resample(freq), agg)()
        if resampled.index.freq is None:
            resampled.index.freq = pd.tseries.frequencies.to_offset(freq)
        return resampled
    _transform.__name__ = f"resample_{freq}_{agg}"

    return TransformSpec(
        transform_fn=_transform,
        kind="aggregate",
        get_overlap=lambda _freq_nanos: 0,
        output_freq=freq,
    )


def make_moving_average_transform(window: str = "24h", min_periods: int = 1):
    """Return a transform that applies a centred rolling mean.

    The time index and frequency are **unchanged** — the same number of
    timesteps are returned.  Values at the edges of the series (where the
    full window is not available) are computed from ``min_periods`` samples.

    Parameters
    ----------
    window : str, optional
        Rolling window size as a pandas offset string (e.g. ``"24h"``,
        ``"48h"``, ``"7D"``).  Default ``"24h"``.
    min_periods : int, optional
        Minimum observations required to produce a non-NaN output.
        Default ``1`` (use what is available at the edges).

    Returns
    -------
    TransformSpec
    """
    import math as _math
    from dvue.animator.reader import TransformSpec

    window_nanos = int(pd.to_timedelta(window).total_seconds() * 1e9)

    def _transform(df: pd.DataFrame) -> pd.DataFrame:
        rolled = df.rolling(window, center=True, min_periods=min_periods).mean()
        rolled.index.freq = df.index.freq
        return rolled
    _transform.__name__ = f"rolling_{window}_mean"

    return TransformSpec(
        transform_fn=_transform,
        kind="convolution",
        get_overlap=lambda freq_nanos: _math.ceil(window_nanos / 2 / freq_nanos),
        output_freq=None,
    )
    return _transform


def make_godin_transform():
    """Return a transform that applies the Godin tidal filter via vtools3.

    The Godin filter is a cascaded cosine-Lanczos low-pass filter that removes
    tidal variability (periods < ~25 h) while preserving subtidal signals.  It
    requires **vtools3** which is a DSM2-specific dependency not bundled with
    dvue.

    The output has the **same time index** as the input but with NaN for the
    ~33.5 h warmup period at each end.  To remove the NaN edges, pass
    ``warmup_steps`` to :class:`~dvue.animator.TransformedSlicingReader`.

    The convenience helper :func:`apply_godin` constructs a
    ``TransformedSlicingReader`` with the correct warmup automatically.

    Returns
    -------
    TransformSpec
    """
    import math as _math
    from dvue.animator.reader import TransformSpec

    _WARMUP_NANOS = int(33.5 * 3600 * 1e9)  # 33.5 h per side

    def _transform(df: pd.DataFrame) -> pd.DataFrame:
        try:
            from vtools.functions.filter import godin
        except ImportError as exc:
            raise ImportError(
                "The Godin tidal filter requires vtools3.  "
                "Install it with: conda install -c cadwr-dms vtools3"
            ) from exc

        out_cols = {}
        for col in df.columns:
            series = df[col].copy()
            try:
                filtered = godin(series)
                if hasattr(filtered, "squeeze"):
                    filtered = filtered.squeeze()
                if not isinstance(filtered, pd.Series):
                    filtered = pd.Series(np.asarray(filtered).ravel(),
                                         index=series.index)
            except Exception:
                filtered = pd.Series(np.nan, index=series.index)
            out_cols[col] = filtered

        result = pd.DataFrame(out_cols, index=df.index)
        result.index.freq = df.index.freq
        return result

    _transform.__name__ = "godin_filter"

    return TransformSpec(
        transform_fn=_transform,
        kind="convolution",
        get_overlap=lambda freq_nanos: _math.ceil(_WARMUP_NANOS / freq_nanos),
        output_freq=None,
    )


def apply_godin(
    inner: "SlicingReader",
) -> "dvue.animator.StreamingTransformedSlicingReader":
    """Wrap *inner* with a Godin tidal filter using streaming chunk-by-chunk mode.

    The overlap (warm-up window) is computed automatically from the reader's
    time step, so no full-file load occurs at startup.

    Returns
    -------
    StreamingTransformedSlicingReader
    """
    from dvue.animator import StreamingTransformedSlicingReader
    return StreamingTransformedSlicingReader(inner, make_godin_transform())


# ---------------------------------------------------------------------------
# Multi-file factories (side-by-side + diff)
# ---------------------------------------------------------------------------

def animate_hydro_multi(
    h5file_a: "str | Path",
    h5file_b: "str | Path",
    variable: str = "flow",
    location: str = "both",
    shapefiles: "list[str | Path | None] | None" = None,
    simplify_tolerance: float = 50.0,
    channel_id_column: "str | None" = None,
    show_diff: bool = False,
    title_a: "str | None" = None,
    title_b: "str | None" = None,
    **mgr_kwargs,
) -> "dvue.animator.MultiGeoAnimatorManager":
    """Create a :class:`~dvue.animator.MultiGeoAnimatorManager` for two HYDRO files.

    Parameters
    ----------
    h5file_a, h5file_b : str or Path
        Two HYDRO HDF5 tidefiles (Study A and Study B).
    variable : {"flow", "stage", "velocity"}, optional
    location : {"both", "upstream", "downstream"}, optional
    shapefiles : list of 1 or 2 paths, optional
        If one path (or ``None``), the same shapefile is used for both maps.
        If two paths, the first is used for A, the second for B.
    simplify_tolerance : float, optional
    channel_id_column : str or None, optional
    show_diff : bool, optional
        Start in diff (A − B) mode.  Default ``False``.
    title_a, title_b : str or None, optional
        Map titles.  Default to the H5 file basenames.
    **mgr_kwargs
        Forwarded to :class:`~dvue.animator.MultiGeoAnimatorManager`.
    """
    from dvue.animator import MultiGeoAnimatorManager
    from pathlib import Path as _Path

    variable = variable.lower()

    def _make_reader(h5):
        if variable == "flow":
            return HydroH5FlowReader(h5, location=location)
        if variable == "stage":
            return HydroH5StageReader(h5, location=location)
        if variable == "velocity":
            return HydroH5VelocityReader(h5, location=location)
        raise ValueError(f"variable must be flow/stage/velocity, got {variable!r}")

    reader_a = _make_reader(h5file_a)
    reader_b = _make_reader(h5file_b)

    sf_a, sf_b = _resolve_shapefiles(shapefiles)
    gdf_a = load_dsm2_channel_gdf(sf_a, simplify_tolerance=simplify_tolerance,
                                   channel_id_column=channel_id_column)
    gdf_b = load_dsm2_channel_gdf(sf_b, simplify_tolerance=simplify_tolerance,
                                   channel_id_column=channel_id_column) if sf_b != sf_a else gdf_a

    ta = title_a or _Path(h5file_a).stem
    tb = title_b or _Path(h5file_b).stem

    mgr_kwargs.setdefault("colormap", "rainbow")
    mgr_kwargs.setdefault("transform_options", _dsm2_transform_options())
    mgr_kwargs.setdefault("buffer_chunk_size", 200)
    mgr = MultiGeoAnimatorManager(
        reader_a, reader_b,
        gdf_a=gdf_a, gdf_b=gdf_b,
        title_a=ta, title_b=tb,
        geo_id_column="geo_id",
        show_diff=show_diff,
        **mgr_kwargs,
    )
    # Attach metadata for the Save config card.
    sf_a_abs = str(_Path(sf_a).absolute()) if sf_a else None
    sf_b_abs = str(_Path(sf_b).absolute()) if sf_b and sf_b != sf_a else None
    mgr._animate_meta = {
        "mode": "multi",
        "file_type": "hydro",
        "files": [
            {"path": str(_Path(h5file_a).absolute()), "title": ta},
            {"path": str(_Path(h5file_b).absolute()), "title": tb},
        ],
        "variable": variable,
        "location": location,
        "shapefile": sf_a_abs,
        "shapefile_b": sf_b_abs,
        "channel_id_column": channel_id_column,
        "_transform_cli_keys": _dsm2_transform_cli_keys(),
    }
    mgr._config_path_input.value = str(
        _Path(h5file_a).absolute().with_name(
            _Path(h5file_a).stem + "_animate.yml"
        )
    )
    _add_resample_card_to_manager(mgr)
    return mgr


def animate_qual_multi(
    h5file_a: "str | Path",
    h5file_b: "str | Path",
    constituent: str = "ec",
    shapefiles: "list[str | Path | None] | None" = None,
    simplify_tolerance: float = 50.0,
    channel_id_column: "str | None" = None,
    show_diff: bool = False,
    title_a: "str | None" = None,
    title_b: "str | None" = None,
    **mgr_kwargs,
) -> "dvue.animator.MultiGeoAnimatorManager":
    """Create a :class:`~dvue.animator.MultiGeoAnimatorManager` for two QUAL files.

    Parameters
    ----------
    h5file_a, h5file_b : str or Path
        Two QUAL/GTM HDF5 tidefiles.
    constituent : str, optional
    shapefiles : list of 1 or 2 paths, optional
    simplify_tolerance : float, optional
    channel_id_column : str or None, optional
    show_diff : bool, optional
        Start in diff (A − B) mode.  Default ``False``.
    title_a, title_b : str or None, optional
    **mgr_kwargs
        Forwarded to :class:`~dvue.animator.MultiGeoAnimatorManager`.
    """
    from dvue.animator import MultiGeoAnimatorManager
    from pathlib import Path as _Path

    reader_a = QualH5ConcentrationReader(h5file_a, constituent=constituent)
    reader_b = QualH5ConcentrationReader(h5file_b, constituent=constituent)

    sf_a, sf_b = _resolve_shapefiles(shapefiles)
    gdf_a = load_dsm2_channel_gdf(sf_a, simplify_tolerance=simplify_tolerance,
                                   channel_id_column=channel_id_column)
    gdf_b = load_dsm2_channel_gdf(sf_b, simplify_tolerance=simplify_tolerance,
                                   channel_id_column=channel_id_column) if sf_b != sf_a else gdf_a

    ta = title_a or _Path(h5file_a).stem
    tb = title_b or _Path(h5file_b).stem

    mgr_kwargs.setdefault("colormap", "rainbow")
    mgr_kwargs.setdefault("transform_options", _dsm2_transform_options())
    mgr_kwargs.setdefault("buffer_chunk_size", 200)
    mgr = MultiGeoAnimatorManager(
        reader_a, reader_b,
        gdf_a=gdf_a, gdf_b=gdf_b,
        title_a=ta, title_b=tb,
        geo_id_column="geo_id",
        show_diff=show_diff,
        **mgr_kwargs,
    )
    # Attach metadata for the Save config card.
    sf_a_abs = str(_Path(sf_a).absolute()) if sf_a else None
    sf_b_abs = str(_Path(sf_b).absolute()) if sf_b and sf_b != sf_a else None
    mgr._animate_meta = {
        "mode": "multi",
        "file_type": "qual",
        "files": [
            {"path": str(_Path(h5file_a).absolute()), "title": ta},
            {"path": str(_Path(h5file_b).absolute()), "title": tb},
        ],
        "variable": constituent,
        "location": "both",
        "shapefile": sf_a_abs,
        "shapefile_b": sf_b_abs,
        "channel_id_column": channel_id_column,
        "_transform_cli_keys": _dsm2_transform_cli_keys(),
    }
    mgr._config_path_input.value = str(
        _Path(h5file_a).absolute().with_name(
            _Path(h5file_a).stem + "_animate.yml"
        )
    )
    _add_resample_card_to_manager(mgr)
    return mgr


# ---------------------------------------------------------------------------
# Export corrected QUAL HDF5
# ---------------------------------------------------------------------------

_MONTHS_3 = ["JAN","FEB","MAR","APR","MAY","JUN",
             "JUL","AUG","SEP","OCT","NOV","DEC"]


def _ts_to_dsm2(ts: "pd.Timestamp") -> str:
    """Format a Timestamp as a DSM2 military string ``DDMMMYYYY HHMM``."""
    return (f"{ts.day:02d}{_MONTHS_3[ts.month - 1]}{ts.year} "
            f"{ts.hour:02d}{ts.minute:02d}")


def export_corrected_qual_h5(
    input_h5: "str | Path",
    output_h5: "str | Path",
    observations_csv: "str | Path",
    stations_csv: "str | Path",
    constituent: str = "ec",
    centerlines_file: "str | Path | None" = None,
    power: float = 2.0,
    max_obs_age: str = "2h",
    echo_inp_file: "str | Path | None" = None,
    start: "pd.Timestamp | None" = None,
    end: "pd.Timestamp | None" = None,
    chunk_size: int = 1000,
) -> None:
    """Pre-compute IDW-corrected concentrations and write a new QUAL HDF5 file.

    The output file is a drop-in replacement for the raw QUAL HDF5 — same
    dataset paths (``/output/channel concentration``, ``/output/channel_number``,
    ``/output/constituent_names``) and time attributes — so it can be compared
    directly with the raw model using the standard two-file animation:

        dsm2ui animate qual RAW.h5 CORRECTED.h5 --constituent ec

    Corrections are applied at the raw upstream/downstream channel-end level
    before averaging, matching what :class:`CorrectedQualH5ConcentrationReader`
    does during live animation.  The output file stores no pre-applied
    transforms so Godin / daily options in the comparison UI work normally.

    Parameters
    ----------
    input_h5 : str or Path
        Source QUAL/GTM HDF5 tidefile.
    output_h5 : str or Path
        Destination HDF5 path (created or overwritten).
    observations_csv : str or Path
        Time-indexed CSV of observations (station IDs as columns).
    stations_csv : str or Path
        CSV with ``station_id`` and lat/lon or x/y columns.
    constituent : str, optional
        Constituent to correct (case-insensitive).  Default ``"ec"``.
    centerlines_file : str or Path or None, optional
        GeoJSON or shapefile of DSM2 channel centrelines used for station
        snapping.  When ``None`` (default) the bundled DSM2 8.2 centrelines
        are used.
    power : float, optional
        IDW distance exponent.  Default ``2.0``.
    max_obs_age : str, optional
        Maximum observation age for matching.  Default ``"2h"``.
    echo_inp_file : str or Path or None, optional
        Fallback CHANNEL table source when the H5 lacks ``/input/channel``.
    start, end : pd.Timestamp or None, optional
        Time window to export.  Default: full model range.
    chunk_size : int, optional
        Timesteps per write chunk.  Default ``1000``.
    """
    import h5py
    import datetime
    import warnings
    import tqdm as _tqdm
    import geopandas as gpd
    from pydsm.analysis.network_correction import (
        NetworkIDWCorrector,
        snap_stations_to_channel_ends,
    )
    from pydsm.viz.dsm2gis import read_stations

    input_h5  = Path(input_h5)
    output_h5 = Path(output_h5)

    # ------------------------------------------------------------------ #
    # 1. Read source structure (no bulk data loaded yet)                   #
    # ------------------------------------------------------------------ #
    print(f"Source : {input_h5}")
    with h5py.File(input_h5, "r") as src:
        constit_names = _decode_string_array(src[_QUAL_CONSTIT_PATH][:])
        cname_lc = constituent.strip().lower()
        ci_map = {n.lower(): i for i, n in enumerate(constit_names)}
        if cname_lc not in ci_map:
            raise ValueError(
                f"Constituent {constituent!r} not found. "
                f"Available: {constit_names}"
            )
        ci = ci_map[cname_lc]

        src_ds    = src[_QUAL_CONC_PATH]
        src_attrs = dict(src_ds.attrs)
        n_times_src, _, n_channels, n_loc = src_ds.shape
        chan_raw = src[_QUAL_CHAN_PATH][:]

    # Reconstruct full time index from H5 attributes
    start_ts_src = _parse_dsm2_timestamp(src_attrs["start_time"])
    raw_iv = src_attrs["interval"]
    if hasattr(raw_iv, "__len__") and not isinstance(raw_iv, (str, bytes)):
        raw_iv = raw_iv[0]
    if isinstance(raw_iv, (bytes, np.bytes_)):
        raw_iv = raw_iv.decode("utf-8")
    interval_str = _normalise_interval(str(raw_iv).strip())
    time_index_full = pd.date_range(
        start=start_ts_src, periods=n_times_src, freq=interval_str
    )

    # ------------------------------------------------------------------ #
    # 2. Determine output time window                                      #
    # ------------------------------------------------------------------ #
    t0 = int(time_index_full.searchsorted(start, side="left"))  if start else 0
    t1 = int(time_index_full.searchsorted(end,   side="right")) if end   else n_times_src
    t1 = min(t1, n_times_src)
    time_index_out = time_index_full[t0:t1]
    n_times_out    = len(time_index_out)
    if n_times_out == 0:
        raise ValueError("No timesteps in the requested range.")
    print(f"Output : {n_times_out} steps  "
          f"({time_index_out[0]} -> {time_index_out[-1]})")

    # ------------------------------------------------------------------ #
    # 3. Build IDW corrector                                               #
    # ------------------------------------------------------------------ #
    print("Building IDW corrector ...")
    channels_df = CorrectedQualH5ConcentrationReader._load_channels(
        input_h5, echo_inp_file
    )
    import dsm2ui as _dsm2ui_pkg
    _cl = (
        Path(centerlines_file)
        if centerlines_file is not None
        else Path(_dsm2ui_pkg.__file__).parent
             / "dsm2gis" / "dsm2_channels_centerlines_8_2.geojson"
    )
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        snapped = snap_stations_to_channel_ends(
            read_stations(str(stations_csv)),
            gpd.read_file(str(_cl)),
            channels_df,
        )
    corrector = NetworkIDWCorrector(channels_df, snapped, power=power)

    # ------------------------------------------------------------------ #
    # 4. Pre-align observations and build vectorised IDW weight matrices   #
    # ------------------------------------------------------------------ #
    print(f"Loading observations: {observations_csv}")
    obs_df = pd.read_csv(observations_csv, index_col=0, parse_dates=True)
    obs_aligned = obs_df.reindex(
        time_index_out, method="nearest",
        tolerance=pd.Timedelta(max_obs_age),
    )
    obs_cols = list(obs_aligned.columns) if not obs_aligned.empty else []

    # Build CE index matching channel order in the H5
    chan_str = [
        (c.decode("utf-8").strip()
         if isinstance(c, (bytes, np.bytes_)) else str(int(c)))
        for c in chan_raw
    ]
    ce_index = [
        f"{s}-{loc}"
        for s in chan_str
        for loc in ("upstream", "downstream")
    ]

    # Same logic as CorrectedQualH5ConcentrationReader._precompute_idw_matrices
    use_vec    = False
    W_finite   = None
    exact_map: list = []
    sta_ce_idx = None

    if (obs_cols
            and hasattr(corrector, "_weights")
            and hasattr(corrector, "_snapped")):
        N_sta  = len(obs_cols)
        N_ce   = len(ce_index)
        sta_j  = {sid: j for j, sid in enumerate(obs_cols)}
        ce_i   = {ce:  i for i, ce  in enumerate(ce_index)}
        W_finite = np.zeros((N_ce, N_sta), dtype=np.float64)

        for ce_key, wts in corrector._weights.items():
            if ce_key not in ce_i:
                continue
            row_i = ce_i[ce_key]
            exact_js: list = []
            for sid, w in wts.items():
                if sid not in sta_j:
                    continue
                j = sta_j[sid]
                if np.isinf(w):
                    exact_js.append(j)
                elif w > 0.0 and np.isfinite(w):
                    W_finite[row_i, j] = w
            if exact_js:
                exact_map.append((row_i, np.asarray(exact_js, dtype=np.intp)))

        sta_ce_idx = np.full(N_sta, -1, dtype=np.intp)
        for sid, row in corrector._snapped.iterrows():
            if sid not in sta_j:
                continue
            j  = sta_j[sid]
            ck = f"{row['chan_no']}-{row['location']}"
            if ck in ce_i:
                sta_ce_idx[j] = ce_i[ck]

        use_vec = True

    # ------------------------------------------------------------------ #
    # 5. Create output H5 and write corrected data in chunks               #
    # ------------------------------------------------------------------ #
    output_h5.parent.mkdir(parents=True, exist_ok=True)
    print(f"Writing : {output_h5}")

    with h5py.File(output_h5, "w") as dst:
        # Channel numbers (verbatim copy)
        dst.create_dataset(_QUAL_CHAN_PATH, data=chan_raw)

        # Single constituent name
        dst.create_dataset(
            _QUAL_CONSTIT_PATH,
            data=np.array([constituent.upper().encode("utf-8")]),
        )

        # Concentration dataset: shape (n_times_out, 1, n_channels, 2)
        out_ds = dst.create_dataset(
            _QUAL_CONC_PATH,
            shape=(n_times_out, 1, n_channels, n_loc),
            dtype=np.float32,
            chunks=(min(chunk_size, n_times_out), 1, n_channels, n_loc),
        )
        # Time attributes matching what _DSM2BaseH5Reader expects
        out_ds.attrs["start_time"] = np.bytes_(
            _ts_to_dsm2(time_index_out[0]).encode("utf-8")
        )
        out_ds.attrs["interval"] = src_attrs["interval"]

        # Chunk loop: read raw → apply IDW → write corrected
        with h5py.File(input_h5, "r") as src:
            src_ds = src[_QUAL_CONC_PATH]
            for cs in _tqdm.tqdm(
                range(0, n_times_out, chunk_size),
                desc="Exporting corrected EC",
                unit="chunk",
            ):
                ce = min(cs + chunk_size, n_times_out)
                cn = ce - cs

                # Raw upstream/downstream: (cn, n_channels, 2)
                block = src_ds[t0 + cs:t0 + ce, ci, :, :].astype(np.float64)
                block[block < -1e20] = np.nan

                # Interleaved CE block: (cn, 2*n_channels)
                mcb = np.empty((cn, 2 * n_channels), dtype=np.float64)
                mcb[:, 0::2] = block[:, :, 0]  # upstream
                mcb[:, 1::2] = block[:, :, 1]  # downstream

                if use_vec and obs_cols:
                    ts_chunk = time_index_out[cs:ce]
                    ov = obs_aligned.reindex(ts_chunk).values  # (cn, N_sta)

                    valid_sta = sta_ce_idx >= 0
                    mat = np.full((cn, len(obs_cols)), np.nan)
                    if valid_sta.any():
                        mat[:, valid_sta] = mcb[:, sta_ce_idx[valid_sta]]

                    resid = ov - mat
                    vf    = (~np.isnan(resid)).astype(np.float64)
                    rf    = np.where(np.isnan(resid), 0.0, resid)

                    wr  = rf @ W_finite.T
                    ws  = vf @ W_finite.T
                    with np.errstate(invalid="ignore", divide="ignore"):
                        corr = np.where(ws > 0.0, wr / ws, 0.0)

                    for ci_e, sjs in exact_map:
                        er = resid[:, sjs]
                        av = ~np.all(np.isnan(er), axis=1)
                        if av.any():
                            corr[av, ci_e] = np.nanmean(er[av], axis=1)

                    ccb = mcb + corr
                else:
                    ccb = mcb  # no obs available → pass through raw model

                # Reshape to (cn, n_channels, 2) and write
                out_chunk = np.stack(
                    [ccb[:, 0::2], ccb[:, 1::2]], axis=2
                ).astype(np.float32)
                out_ds[cs:ce, 0, :, :] = out_chunk

        # Provenance group so the file is self-documenting
        grp = dst.create_group("correction")
        grp.attrs["method"]           = "IDW"
        grp.attrs["power"]            = float(power)
        grp.attrs["max_obs_age"]      = str(max_obs_age)
        grp.attrs["constituent"]      = constituent.upper()
        grp.attrs["observations_csv"] = str(observations_csv)
        grp.attrs["stations_csv"]     = str(stations_csv)
        grp.attrs["echo_inp_file"]    = str(echo_inp_file) if echo_inp_file else ""
        grp.attrs["source_h5"]        = str(input_h5.absolute())
        grp.attrs["created"]          = datetime.datetime.now().isoformat()

    print(f"Done -> {output_h5}")


def _resolve_shapefiles(
    shapefiles: "list | None",
) -> "tuple[str | None, str | None]":
    """Return (sf_a, sf_b) from a list of 0–2 shapefile paths.

    Rules:
    - None or []     → both None  (use bundled GeoJSON)
    - [sf1]          → sf_a = sf1, sf_b = sf1
    - [sf1, sf2]     → sf_a = sf1, sf_b = sf2
    """
    if not shapefiles:
        return None, None
    if len(shapefiles) == 1:
        return shapefiles[0], shapefiles[0]
    return shapefiles[0], shapefiles[1]

