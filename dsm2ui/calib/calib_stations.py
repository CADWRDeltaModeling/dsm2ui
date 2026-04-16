"""calib_stations.py — Build calibration_ec_stations.csv from datastore station CSV.

Takes the enriched stations CSV produced by ``dsm2ui datastore extract --stations``
and a DSM2 channel centerlines GeoJSON, snaps each station to the nearest channel,
and writes a ``calibration_ec_stations.csv`` ready for use in calibration runs.

Stations that cannot be snapped within the distance tolerance are written to a
separate *_unmatched.csv* so they can be reviewed and fed back into
``dsm2ui station-map to-dsm2`` once corrected.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd


# Columns in calibration_ec_stations.csv, in order.
_CALIB_COLUMNS = [
    "dsm2_id",
    "obs_station_id",
    "station_name",
    "note",
    "subtract",
    "time_window_exclusion_list",
    "threshold_value",
    "agency",
    "in cont repo",
    "Elevation",
    "lat",
    "lon",
    "utm_easting",
    "utm_northing",
    "Calibration Period",
    "Validation Period",
    "Data Availability (cdec_flow_cleaned.dss)",
    "Data Availability (2012 Calibration)",
]


def build_calib_stations_csv(
    stations_csv: str,
    centerlines_file: str,
    output_csv: str,
    unmatched_csv: str | None = None,
    distance_tolerance: int = 100,
) -> None:
    """Snap datastore stations to DSM2 channels and write calibration_ec_stations.csv.

    Parameters
    ----------
    stations_csv:
        Path to the enriched stations CSV produced by
        ``dsm2ui datastore extract --stations``.
        Required columns: ``station_id``, ``lat``, ``lon``.
        Optional columns (written through if present): ``station_name``,
        ``agency``, ``utm_easting``, ``utm_northing``.
    centerlines_file:
        Path to the DSM2 channel centerlines GeoJSON (UTM Zone 10N).
    output_csv:
        Destination path for the calibration_ec_stations.csv output.
    unmatched_csv:
        Destination path for stations that could not be snapped to any channel.
        Defaults to ``<output_csv stem>_unmatched.csv``.
    distance_tolerance:
        Maximum distance (ft) from a channel centerline for a station to be
        considered a match.  Default 100.
    """
    from pydsm.viz import dsm2gis

    stations = pd.read_csv(stations_csv)
    output_path = Path(output_csv)
    if unmatched_csv is None:
        unmatched_csv = str(output_path.parent / (output_path.stem + "_unmatched.csv"))

    # Use a temp file for the snap step so we can capture matched/unmatched.
    import tempfile, os
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        dsm2gis.create_stations_output_file(
            stations_file=stations_csv,
            centerlines_file=centerlines_file,
            output_file=tmp_path,
            distance_tolerance=distance_tolerance,
        )
        snapped = pd.read_csv(tmp_path, sep=" ")
    finally:
        os.unlink(tmp_path)

    # Merge snapped results back onto the input stations by station_id (both lowercase)
    merged = stations.merge(
        snapped[["NAME"]],
        left_on="station_id",
        right_on="NAME",
        how="left",
    )

    matched_mask = merged["NAME"].notna()
    matched = merged[matched_mask].copy()
    # Uppercase DSM2 names after merge
    matched["dsm2_id"] = matched["NAME"].str.upper()
    unmatched = stations[~stations["station_id"].isin(matched["station_id"])].copy()

    # Build calibration CSV rows
    rows = []
    for _, row in matched.iterrows():
        r = {
            "dsm2_id": row["dsm2_id"],
            "obs_station_id": row["station_id"],
            "station_name": row.get("station_name", ""),
            "note": "",
            "subtract": "NO",
            "time_window_exclusion_list": "",
            "threshold_value": "",
            "agency": row.get("agency", ""),
            "in cont repo": "",
            "Elevation": 0,
            "lat": row.get("lat", ""),
            "lon": row.get("lon", ""),
            "utm_easting": row.get("utm_easting", ""),
            "utm_northing": row.get("utm_northing", ""),
            "Calibration Period": "",
            "Validation Period": "",
            "Data Availability (cdec_flow_cleaned.dss)": "",
            "Data Availability (2012 Calibration)": "",
        }
        rows.append(r)

    out_df = pd.DataFrame(rows, columns=_CALIB_COLUMNS)
    out_df.to_csv(output_csv, index=False)
    print(f"Calibration stations CSV written to: {output_csv}  ({len(out_df)} stations)")

    if not unmatched.empty:
        unmatched.to_csv(unmatched_csv, index=False)
        print(
            f"WARNING: {len(unmatched)} station(s) could not be snapped to any channel "
            f"(tolerance={distance_tolerance} ft)."
        )
        print(f"  Unmatched stations written to: {unmatched_csv}")
        print("  Review and re-run with 'dsm2ui station-map to-dsm2' after adjusting locations.")
    else:
        print("All stations matched successfully.")
