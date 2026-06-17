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
# GeoDataFrame loader
# ---------------------------------------------------------------------------

def load_dsm2_channel_gdf(
    shapefile: "str | Path | None" = None,
    simplify_tolerance: float = 50.0,
) -> "geopandas.GeoDataFrame":
    """Load DSM2 channel centreline geometry.

    Parameters
    ----------
    shapefile : str or Path, optional
        Path to an alternative GeoJSON or shapefile.  When ``None`` (default)
        the bundled ``dsm2_channels_centerlines_8_2.geojson`` is used.
    simplify_tolerance : float, optional
        Simplification tolerance in metres (applied in EPSG:3857).  Set to
        ``0`` to disable.  Default ``50`` m — removes redundant vertices from
        complex channel centrelines while preserving overall shape, giving a
        significant rendering speed-up for multi-year animations.

    Returns
    -------
    geopandas.GeoDataFrame
        Has column ``"geo_id"`` (int) matching DSM2 channel numbers, plus
        ``"geometry"`` (LineString, EPSG:4326).
    """
    import geopandas as gpd

    if shapefile is not None:
        gdf = gpd.read_file(shapefile)
    else:
        pkg_path = Path(__file__).parent / "dsm2gis" / "dsm2_channels_centerlines_8_2.geojson"
        gdf = gpd.read_file(str(pkg_path))

    # Normalise the channel number column to "geo_id" (int)
    # The bundled GeoJSON uses "id"; shapefiles from pydsm use "channel_nu"
    if "id" in gdf.columns:
        gdf = gdf.rename(columns={"id": "geo_id"})
    elif "channel_nu" in gdf.columns:
        gdf = gdf.rename(columns={"channel_nu": "geo_id"})
    elif "CHAN_NO" in gdf.columns:
        gdf = gdf.rename(columns={"CHAN_NO": "geo_id"})

    if "geo_id" not in gdf.columns:
        raise ValueError(
            "Cannot identify channel number column in shapefile. "
            "Expected one of: 'id', 'channel_nu', 'CHAN_NO'. "
            f"Available columns: {list(gdf.columns)}"
        )

    gdf["geo_id"] = gdf["geo_id"].astype(int)

    # Simplify geometry in EPSG:3857 (metres) then reproject to WGS84.
    # This removes redundant vertices from complex channel centrelines,
    # giving a significant rendering speed-up for large tidefiles.
    if simplify_tolerance and simplify_tolerance > 0:
        gdf = gdf.to_crs("EPSG:3857")
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

def animate_hydro(
    h5file: "str | Path",
    variable: str = "flow",
    location: str = "both",
    shapefile: "str | Path | None" = None,
    simplify_tolerance: float = 50.0,
    **mgr_kwargs,
) -> "dvue.animator.GeoAnimatorManager":
    """Create a :class:`~dvue.animator.GeoAnimatorManager` for HYDRO channel data.

    Parameters
    ----------
    h5file : str or Path
        HYDRO HDF5 tidefile.
    variable : {"flow", "stage"}, optional
        Which variable to animate.  Default ``"flow"``.
    location : {"both", "upstream", "downstream"}, optional
        Location to use.  ``"both"`` averages upstream and downstream.
    shapefile : str or Path, optional
        Custom channel geometry.  Defaults to bundled GeoJSON.
    simplify_tolerance : float, optional
        Geometry simplification tolerance in metres.  Default 50 m.
        Set to 0 to disable.
    **mgr_kwargs
        Forwarded to :class:`~dvue.animator.GeoAnimatorManager`
        (``vmin``, ``vmax``, ``colormap``, ``title``, etc.).

    Returns
    -------
    dvue.animator.GeoAnimatorManager
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

    gdf = load_dsm2_channel_gdf(shapefile, simplify_tolerance=simplify_tolerance)
    mgr_kwargs.setdefault("title", f"DSM2 Hydro — {variable.title()}")
    mgr_kwargs.setdefault("colormap", "rainbow")
    return GeoAnimatorManager(reader, gdf, geo_id_column="geo_id", **mgr_kwargs)


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
        Set to 0 to disable.
    x2_threshold : float or None, optional
        When provided, attaches a :class:`QualH5X2Callback` so the
        X2 isohaline control appears in the UI.  Pass the initial EC
        threshold value (e.g. ``2700.0`` µS/cm).  Default ``None``.
    **mgr_kwargs
        Forwarded to :class:`~dvue.animator.GeoAnimatorManager`.

    Returns
    -------
    dvue.animator.GeoAnimatorManager
    """
    from dvue.animator import GeoAnimatorManager

    reader = QualH5ConcentrationReader(h5file, constituent=constituent)

    x2_callback = None
    if x2_threshold is not None:
        # Load *unsimplified* centerlines for accurate interpolation
        x2_gdf = load_dsm2_channel_gdf(shapefile, simplify_tolerance=0)
        x2_callback = QualH5X2Callback(
            reader._h5,
            reader._ds,
            reader._constituent_index,
            reader._channel_numbers,
            x2_gdf.to_crs("EPSG:3857"),
        )

    gdf = load_dsm2_channel_gdf(shapefile, simplify_tolerance=simplify_tolerance)
    mgr_kwargs.setdefault("title", f"DSM2 QUAL/GTM \u2014 {constituent.upper()}")
    mgr_kwargs.setdefault("colormap", "rainbow")
    mgr = GeoAnimatorManager(
        reader, gdf, geo_id_column="geo_id",
        x2_callback=x2_callback, **mgr_kwargs,
    )
    if x2_threshold is not None:
        mgr._x2_threshold_input.value = float(x2_threshold)
    return mgr

