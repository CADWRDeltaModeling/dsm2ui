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


def _dsm2_transform_options() -> dict:
    """Return the standard DSM2 transform options dict for GeoAnimatorManager.

    Keys are display names; values are ``transform_fn`` callables accepted by
    :class:`dvue.animator.TransformedSlicingReader`.
    """
    return {
        "Daily mean":     make_resample_transform("D", "mean"),
        "Rolling 24 h":   make_moving_average_transform("24h"),
        "Rolling 14 D":   make_moving_average_transform("14D"),
        "Godin filter":   make_godin_transform(),
    }


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
        "_transform_cli_keys": {
            "Daily mean": "daily",
            "Rolling 24 h": "rolling-24h",
            "Rolling 14 D": "rolling-14d",
            "Godin filter": "godin",
        },
    }
    mgr._config_path_input.value = str(
        Path(h5abs).with_name(Path(h5abs).stem + "_animate.yml")
    )
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
        "_transform_cli_keys": {
            "Daily mean": "daily",
            "Rolling 24 h": "rolling-24h",
            "Rolling 14 D": "rolling-14d",
            "Godin filter": "godin",
        },
    }
    mgr._config_path_input.value = str(
        Path(h5abs).with_name(Path(h5abs).stem + "_animate.yml")
    )
    return mgr


# ---------------------------------------------------------------------------
# Transform factories for use with TransformedSlicingReader
# ---------------------------------------------------------------------------

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
        "_transform_cli_keys": {
            "Daily mean": "daily",
            "Rolling 24 h": "rolling-24h",
            "Rolling 14 D": "rolling-14d",
            "Godin filter": "godin",
        },
    }
    mgr._config_path_input.value = str(
        _Path(h5file_a).absolute().with_name(
            _Path(h5file_a).stem + "_animate.yml"
        )
    )
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
        "_transform_cli_keys": {
            "Daily mean": "daily",
            "Rolling 24 h": "rolling-24h",
            "Rolling 14 D": "rolling-14d",
            "Godin filter": "godin",
        },
    }
    mgr._config_path_input.value = str(
        _Path(h5file_a).absolute().with_name(
            _Path(h5file_a).stem + "_animate.yml"
        )
    )
    return mgr


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

