import os
import glob as _glob
import fnmatch
import pandas as pd
import dms_datastore
from dms_datastore import read_ts
import pyhecdss as dss
from pathlib import Path
import tqdm

_DEFAULT_CHANNELS = (
    Path(__file__).parent / "dsm2gis" / "dsm2_channels_centerlines_8_2.geojson"
)


def _filter_inventory_by_polygon(inventory, polygon_file):
    """Return rows of *inventory* whose (lon, lat) falls within *polygon_file*.

    The polygon file can be any format readable by geopandas (GeoJSON,
    shapefile, etc.).  It is reprojected to EPSG:4326 before the point-in-
    polygon test so that it is always compared against the inventory's WGS84
    lat/lon columns.
    """
    import geopandas as gpd

    if "lat" not in inventory.columns or "lon" not in inventory.columns:
        print("Warning: inventory has no lat/lon columns — clip filter skipped.")
        return inventory

    poly_gdf = gpd.read_file(polygon_file)
    if poly_gdf.crs is not None and poly_gdf.crs.to_epsg() != 4326:
        poly_gdf = poly_gdf.to_crs("EPSG:4326")
    try:
        polygon = poly_gdf.union_all()
    except AttributeError:
        polygon = poly_gdf.unary_union

    gdf = gpd.GeoDataFrame(
        inventory,
        geometry=gpd.points_from_xy(inventory["lon"], inventory["lat"]),
        crs="EPSG:4326",
    )
    within = gdf[gdf.within(polygon)].drop(columns=["geometry"])
    n_removed = len(inventory) - len(within)
    print(
        f"  Clip: {len(within)} of {len(inventory)} stations inside polygon"
        f" ({n_removed} outside removed)."
    )
    return within.reset_index(drop=True)


# this should be a util function
def find_lastest_fname(pattern, dir="."):
    d = Path(dir)
    fname, mtime = None, 0
    for f in d.glob(pattern):
        fmtime = f.stat().st_mtime
        if fmtime > mtime:
            mtime = fmtime
            fname = f.absolute()
    return fname, mtime


def read_from_datastore_write_to_dss(
    datastore_dir, dssfile, param, repo_level="screened", unit_name=None
):
    """
    Reads datastore timeseries files and writes to a DSS file

    Parameters
    ----------
    datastore_dir : str
        Directory where Datastore files are stored
    repo_level : str
        default is screened
    dssfile : str
        Filename to write to
    param : str
        e.g one of "flow","elev", "ec", etc.
    """
    inventory_file, mtime = find_lastest_fname(
        f"inventory_datasets_{repo_level}*.csv", datastore_dir
    )
    print("Using inventory file:", inventory_file)
    inventory = pd.read_csv(inventory_file)
    param_inventory = inventory[inventory["param"] == param]
    apart = "DMS-DATASTORE"
    fpart = os.path.basename(inventory_file).split("_")[-1].split(".csv")[0]
    with dss.DSSFile(dssfile, create_new=True) as f:
        for idx, row in tqdm.tqdm(
            param_inventory.iterrows(), total=len(param_inventory)
        ):
            filepattern = os.path.join(datastore_dir, repo_level, row["file_pattern"])
            ts = read_ts(filepattern)
            print("Reading ", filepattern)
            if pd.isna(row["subloc"]):
                bpart = row["station_id"]
            else:
                bpart = row["station_id"] + row["subloc"]
            epart = ts.index.freqstr
            pathname = f'/{apart}/{bpart}/{row["param"]}///{fpart}/'
            print("Writing to ", pathname)
            f.write_rts(
                pathname,
                ts,
                unit_name if unit_name is not None else row["unit"],
                "INST-VAL",
            )
    print("Done")


def write_station_lat_lng(datastore_dir, station_file, param, repo_level="screened",
                          clip_polygon_file=None):
    """
    Writes station metadata to a csv file.

    Columns written: dsm2_id, obs_station_id, station_name, agency, lat, lon,
    utm_easting, utm_northing (x/y from the inventory, UTM Zone 10N).

    ``obs_station_id`` is the DSS B-part used when writing the DSS file, i.e.
    ``station_id + subloc`` (uppercased) when a subloc is present, otherwise
    just ``station_id``.  When a station has both an *upper* and a *lower*
    sensor, the *upper* subloc is preferred.

    Parameters
    ----------
    datastore_dir : str
        Directory where Datastore files are stored
    station_file : str
        Filename to write to
    param : str
        e.g one of "flow","elev", "ec", etc.
    repo_level : str
        default is screened
    clip_polygon_file : str or None
        Optional path to a polygon file (GeoJSON, shapefile, etc.) used to
        restrict output to stations whose location falls within the polygon.
    """
    inventory_file, mtime = find_lastest_fname(
        f"inventory_datasets_{repo_level}*.csv", datastore_dir
    )
    print("Using inventory file:", inventory_file)
    inventory = pd.read_csv(inventory_file)
    inventory = inventory[inventory["param"] == param].copy()

    if clip_polygon_file is not None:
        inventory = _filter_inventory_by_polygon(inventory, clip_polygon_file)

    # Build obs_station_id = station_id + subloc (uppercased) to match the DSS
    # B-part written by read_from_datastore_write_to_dss.
    inventory["obs_station_id"] = inventory.apply(
        lambda r: (r["station_id"] + r["subloc"]).upper()
        if pd.notna(r.get("subloc"))
        else r["station_id"].upper(),
        axis=1,
    )

    # When a station has multiple sublocs, prefer 'upper' > 'lower' > other.
    _subloc_rank = {"upper": 0, "lower": 1}
    inventory["_subloc_rank"] = inventory["subloc"].map(
        lambda s: _subloc_rank.get(str(s).lower(), 2) if pd.notna(s) else 3
    )
    inventory = (
        inventory.sort_values("_subloc_rank")
        .drop_duplicates(subset=["station_id"])
        .drop(columns=["_subloc_rank"])
    )

    cols = ["station_id", "obs_station_id"]
    rename = {}  # keep station_id as-is — required by the animate --stations-csv reader
    if "name" in inventory.columns:
        cols.append("name")
        rename["name"] = "station_name"
    if "agency" in inventory.columns:
        cols.append("agency")
    cols += ["lat", "lon"]
    if "x" in inventory.columns and "y" in inventory.columns:
        cols += ["x", "y"]
        rename.update({"x": "utm_easting", "y": "utm_northing"})
    inventory = inventory[cols].rename(columns=rename)
    inventory.to_csv(station_file, index=False)
    print("Wrote to ", station_file)
    print("Done")


def read_from_datastore_write_to_csv(
    datastore_dir, csvfile, param, repo_level="screened", start=None, end=None,
    clip_polygon_file=None,
):
    """
    Reads datastore timeseries files and writes to a wide-format CSV file.

    Each station becomes a column; rows are the union of all timestamps across
    stations (NaN where a station has no observation at a given time).

    Data is processed one calendar year at a time.  The screened directory
    is scanned once with a single glob, and the resulting filename→path dict
    is searched in-memory with ``fnmatch`` per station/year so that only one
    glob call is made regardless of how many stations or years are requested.
    Each matched shard file is then opened by its exact path (no wildcard),
    so ``read_ts`` performs a single stat rather than a full directory scan.
    Memory is bounded by one year of aligned data across all stations.

    Parameters
    ----------
    datastore_dir : str
        Directory where Datastore files are stored.
    csvfile : str
        Output CSV file path.
    param : str
        Parameter to extract, e.g. "flow", "elev", "ec".
    repo_level : str
        Data repository level, default "screened".
    start : pandas.Timestamp or None
        If provided, only rows at or after this timestamp are included.
    end : pandas.Timestamp or None
        If provided, only rows at or before this timestamp are included.
    clip_polygon_file : str or None
        Optional path to a polygon file (GeoJSON, shapefile, etc.) used to
        restrict extraction to stations whose location falls within the polygon.
    """
    inventory_file, mtime = find_lastest_fname(
        f"inventory_datasets_{repo_level}*.csv", datastore_dir
    )
    print("Using inventory file:", inventory_file)
    inventory = pd.read_csv(inventory_file)
    param_inventory = inventory[inventory["param"] == param]

    if clip_polygon_file is not None:
        param_inventory = _filter_inventory_by_polygon(param_inventory, clip_polygon_file)

    if param_inventory.empty:
        print("No stations found for param:", param)
        return

    # Determine year range from inventory columns, then clamp to user start/end
    if "min_year" in param_inventory.columns:
        year_min = int(param_inventory["min_year"].min())
    else:
        year_min = start.year if start is not None else 1990
    if "max_year" in param_inventory.columns:
        year_max = int(param_inventory["max_year"].max())
    else:
        year_max = end.year if end is not None else pd.Timestamp.now().year
    if start is not None:
        year_min = max(year_min, start.year)
    if end is not None:
        year_max = min(year_max, end.year)

    # Build column name for each inventory row once
    col_names = {}
    for idx, row in param_inventory.iterrows():
        subloc = row.get("subloc")
        if pd.notna(subloc) and str(subloc).strip():
            col_names[idx] = f"{row['station_id']}@{subloc}"
        else:
            col_names[idx] = row["station_id"]

    # Scan the shard directory exactly once to build an in-memory filename map.
    # The screened directory can have 20k+ files on a network drive; a wildcard
    # glob against it takes ~10 s per call.  One scan here avoids that cost on
    # every station x year iteration.
    shard_dir = os.path.join(datastore_dir, repo_level)
    print("Scanning shard directory (one-time) ...", end=" ", flush=True)
    all_shards = _glob.glob(os.path.join(shard_dir, "*.csv"))
    file_by_name = {os.path.basename(f): f for f in all_shards}
    print(f"{len(file_by_name)} files found.")

    def _resolve_year_path(file_pat, year):
        """Return exact file path for (file_pat, year), or None if not found."""
        yp = file_pat[:-6] + f"_{year}.csv" if file_pat.endswith("_*.csv") else file_pat
        for fname, fpath in file_by_name.items():
            if fnmatch.fnmatch(fname, yp):
                return fpath
        return None

    # Pre-scan: determine the ordered union of station columns across all years.
    # This is O(N_stations × N_years) with no disk I/O — only in-memory fnmatch
    # — and ensures every chunk written to CSV has identical columns so the file
    # is always valid regardless of which stations have data in which year.
    all_columns = []
    seen_cols = set()
    for year in range(year_min, year_max + 1):
        for idx, row in param_inventory.iterrows():
            col = col_names[idx]
            if col in seen_cols:
                continue
            if _resolve_year_path(row["file_pattern"], year) is not None:
                all_columns.append(col)
                seen_cols.add(col)

    print(f"Extracting {param} for {len(all_columns)} stations with data, "
          f"years {year_min}\u2013{year_max} ...")
    first_chunk = True
    total_rows = 0

    for year in range(year_min, year_max + 1):
        chunk_start = pd.Timestamp(f"{year}-01-01")
        chunk_end = pd.Timestamp(f"{year}-12-31 23:59:59")
        if start is not None and chunk_start < start:
            chunk_start = start
        if end is not None and chunk_end > end:
            chunk_end = end
        if chunk_start > chunk_end:
            continue

        print(f"  Year {year} ...", end=" ", flush=True)
        series_dict = {}
        for idx, row in tqdm.tqdm(
            param_inventory.iterrows(), total=len(param_inventory), leave=False
        ):
            file_pat = row["file_pattern"]
            fpath = _resolve_year_path(file_pat, year)
            if fpath is None:
                continue
            # Exact path: read_ts calls glob.glob on a literal path which is
            # a single stat() call, not a full directory enumeration.
            try:
                ts = read_ts(fpath)
            except Exception:
                continue
            if ts is None or len(ts) == 0:
                continue
            if isinstance(ts, pd.DataFrame):
                ts = ts.iloc[:, 0]
            ts = ts.loc[chunk_start:chunk_end]
            if len(ts) == 0:
                continue
            series_dict[col_names[idx]] = ts

        if not series_dict:
            print("no data.")
            continue

        chunk_df = pd.concat(series_dict, axis=1)
        # Reindex to the full column union so every chunk has identical columns.
        # Stations with no data for this year get an all-NaN column.
        chunk_df = chunk_df.reindex(columns=all_columns)
        chunk_df.index.name = "datetime"
        chunk_df.to_csv(csvfile, mode="w" if first_chunk else "a", header=first_chunk)
        total_rows += len(chunk_df)
        first_chunk = False
        n_with_data = series_dict.__len__()
        print(f"{len(chunk_df)} rows × {n_with_data}/{len(all_columns)} stations with data")
        del chunk_df

    if first_chunk:
        print("No data found for param:", param)
    else:
        print(f"Wrote {total_rows} total rows to {csvfile}")
    print("Done")


def extend_obs_csv(
    csvfile,
    datastore_dir,
    param,
    repo_level="screened",
    start=None,
    end=None,
    clip_polygon_file=None,
):
    """Extend an existing wide-format observation CSV with new data from the
    datastore, covering a wider time window.

    Only the time periods **outside** the existing file's date range are
    extracted from the datastore; the covered period is never re-read.
    The new and existing data are then merged into a single CSV with the
    union of all timestamps and columns (stations).

    When there is no existing file the function falls back to a plain
    :func:`read_from_datastore_write_to_csv` extraction.

    Parameters
    ----------
    csvfile : str
        Path to the CSV file to extend (created if absent).
    datastore_dir : str
        DMS Datastore root directory.
    param : str
        Parameter to extract, e.g. ``"ec"``.
    repo_level : str
        Data repository level.  Default ``"screened"``.
    start, end : pandas.Timestamp or None
        Desired full time range after extension.
    clip_polygon_file : str or None
        Optional spatial clip polygon (forwarded to extractor).
    """
    p = Path(csvfile)

    if not p.exists():
        # Nothing to merge — full extraction
        read_from_datastore_write_to_csv(
            datastore_dir, csvfile, param, repo_level,
            start=start, end=end,
            clip_polygon_file=clip_polygon_file,
        )
        return

    print(f"Loading existing data from {csvfile} ...")
    existing = pd.read_csv(csvfile, index_col=0, parse_dates=True)
    ex_start = existing.index.min()
    ex_end   = existing.index.max()
    print(f"  Existing range : {ex_start} to {ex_end}  ({len(existing)} rows)")
    print(f"  Requested range: {start} to {end}")

    parts = [existing]
    _tmpfiles = []

    try:
        # ---- early extension (before existing start) --------------------
        if start is not None and pd.Timestamp(start) < ex_start:
            tf = p.with_suffix(".extend_early.csv")
            _tmpfiles.append(tf)
            early_end = ex_start - pd.Timedelta("1ns")
            print(f"\nExtracting early extension: {start} -> {ex_start.date()} ...")
            read_from_datastore_write_to_csv(
                datastore_dir, str(tf), param, repo_level,
                start=pd.Timestamp(start), end=early_end,
                clip_polygon_file=clip_polygon_file,
            )
            if tf.exists() and tf.stat().st_size > 0:
                parts.insert(0, pd.read_csv(tf, index_col=0, parse_dates=True))

        # ---- late extension (after existing end) ------------------------
        if end is not None and pd.Timestamp(end) > ex_end:
            tf = p.with_suffix(".extend_late.csv")
            _tmpfiles.append(tf)
            late_start = ex_end + pd.Timedelta("1ns")
            print(f"\nExtracting late extension: {ex_end.date()} -> {end} ...")
            read_from_datastore_write_to_csv(
                datastore_dir, str(tf), param, repo_level,
                start=late_start, end=pd.Timestamp(end),
                clip_polygon_file=clip_polygon_file,
            )
            if tf.exists() and tf.stat().st_size > 0:
                parts.append(pd.read_csv(tf, index_col=0, parse_dates=True))

    finally:
        for tf in _tmpfiles:
            if tf.exists():
                tf.unlink()

    if len(parts) == 1:
        print("No extension needed — requested range is already covered.")
        return

    print("\nMerging ...")
    # outer concat: union of all timestamps AND all station columns
    merged = pd.concat(parts, axis=0, sort=True).sort_index()
    # At overlapping timestamps prefer the existing (first-added) values.
    merged = merged[~merged.index.duplicated(keep="first")]
    merged.index.name = "datetime"
    merged.to_csv(csvfile)
    print(
        f"Extended CSV written to {csvfile}  "
        f"({len(merged)} rows x {len(merged.columns)} stations)"
    )
    print("Done")


def average_sublocs_csv(input_csv, output_csv=None):
    """
    Post-process a wide-format datastore CSV: collapse multi-sublocation
    stations into a single averaged column.

    For each station that appears with more than one sub-location (e.g.
    ``anh@upper`` and ``anh@lower``), those columns are replaced by a single
    column holding the row-wise mean.  Stations with only one sub-location
    have the ``@subloc`` suffix stripped.  Stations with no sub-location
    are left unchanged.

    NaN-safe: ``mean(axis=1)`` ignores NaN so that if one sensor has a gap
    the other reading is used as-is for that time step.  Only when *all*
    sensors for a station are NaN at a given time does the output become NaN.

    Parameters
    ----------
    input_csv : str
        Wide-format CSV with a ``datetime`` index column, as produced by
        ``read_from_datastore_write_to_csv``.
    output_csv : str or None
        Output path.  Defaults to ``{stem}_avg.csv`` in the same directory
        as the input file.

    Returns
    -------
    str
        Path of the written output file.
    """
    df = pd.read_csv(input_csv, index_col=0, parse_dates=True)
    df.index.name = "datetime"

    # Group columns by base station_id (everything before the first "@")
    groups = {}  # base_id → [col, ...]  — insertion-ordered (Python 3.7+)
    for col in df.columns:
        base = col.split("@", 1)[0]
        groups.setdefault(base, []).append(col)

    out_cols = {}
    averaged = []
    for base, cols in groups.items():
        if len(cols) > 1 and all("@" in c for c in cols):
            # All members carry an explicit subloc — collapse to mean
            sublocs = [c.split("@", 1)[1] for c in cols]
            print(f"  Averaging {base}: {', '.join(sublocs)}")
            out_cols[base] = df[cols].mean(axis=1)
            averaged.append(base)
        elif len(cols) > 1:
            # Mixed (some with subloc, some without) — keep columns separate
            for c in cols:
                out_cols[c] = df[c]
        else:
            # Single column — strip @subloc suffix if present
            out_cols[base] = df[cols[0]]

    out_df = pd.DataFrame(out_cols)
    out_df.index.name = "datetime"

    if output_csv is None:
        p = Path(input_csv)
        output_csv = str(p.parent / (p.stem + "_avg" + p.suffix))

    out_df.to_csv(output_csv)
    if averaged:
        print(f"Averaged sublocs for: {', '.join(averaged)}")
    print(f"Wrote {len(out_df.columns)} stations to {output_csv}")
    print("Done")
    return output_csv


def make_dsm2_clip_polygon(output_file, buffer_m=5000, channels_file=None):
    """
    Create a buffered clip polygon from DSM2 channel centerlines.

    The centerlines are buffered by *buffer_m* metres in the projected
    coordinate system (EPSG:26910 UTM Zone 10N), dissolved into a single
    polygon, then reprojected to WGS84 (EPSG:4326) for direct use with the
    inventory lat/lon columns.

    Parameters
    ----------
    output_file : str
        Output GeoJSON path.
    buffer_m : float
        Buffer distance in metres.  Default 5000 m (5 km).
    channels_file : str or None
        Path to a channel centrelines GeoJSON file.  Defaults to the bundled
        DSM2 8.2 centrelines (EPSG:26910).
    """
    import geopandas as gpd

    if channels_file is None:
        channels_file = str(_DEFAULT_CHANNELS)

    print(f"Loading channel centrelines from {channels_file} ...")
    gdf = gpd.read_file(channels_file)

    # Ensure a projected CRS for accurate metre-based buffering
    if gdf.crs is None or not gdf.crs.is_projected:
        gdf = gdf.to_crs("EPSG:26910")

    print(f"Buffering {len(gdf)} channels by {buffer_m:,} m ...")
    buffered = gdf.buffer(buffer_m)
    try:
        union_geom = buffered.union_all()
    except AttributeError:          # geopandas < 0.14
        union_geom = buffered.unary_union

    # Reproject to WGS84 so the polygon matches inventory lat/lon columns
    result = gpd.GeoDataFrame(geometry=[union_geom], crs=gdf.crs)
    result = result.to_crs("EPSG:4326")

    result.to_file(output_file, driver="GeoJSON")
    print(f"Clip polygon written to {output_file}")
    print("Done")
