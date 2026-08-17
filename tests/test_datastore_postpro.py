"""Tests for dsm2ui.datastore2dss post-processing functions.

Tests are self-contained (no network drive / live datastore required).
"""
import io
import json
import textwrap
import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from unittest.mock import patch, MagicMock

from dsm2ui.datastore2dss import (
    average_sublocs_csv,
    _filter_inventory_by_polygon,
    _filter_inventory_by_station_ids,
    make_dsm2_clip_polygon,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_wide_csv(tmp_path, data: dict, index_name="datetime") -> Path:
    """Write a minimal wide-format CSV to *tmp_path* and return its Path."""
    idx = pd.date_range("2015-01-01", periods=5, freq="h")
    df = pd.DataFrame(data, index=idx)
    df.index.name = index_name
    fpath = tmp_path / "obs.csv"
    df.to_csv(fpath)
    return fpath


def _make_inventory(stations: list[dict]) -> pd.DataFrame:
    """Build a minimal inventory DataFrame from a list of station dicts."""
    return pd.DataFrame(stations)


# ---------------------------------------------------------------------------
# average_sublocs_csv
# ---------------------------------------------------------------------------

class TestAverageSublocsCsv:
    def test_averages_upper_lower_pair(self, tmp_path):
        """Columns anh@upper and anh@lower → single column anh = mean."""
        fpath = _make_wide_csv(
            tmp_path,
            {
                "anh@upper": [100.0, 200.0, np.nan, 400.0, 500.0],
                "anh@lower": [200.0, 300.0, 300.0, np.nan, 500.0],
            },
        )
        out = average_sublocs_csv(str(fpath))
        df = pd.read_csv(out, index_col=0, parse_dates=True)
        assert "anh" in df.columns
        assert "anh@upper" not in df.columns
        assert "anh@lower" not in df.columns
        # row 0: (100+200)/2 = 150
        assert df["anh"].iloc[0] == pytest.approx(150.0)
        # row 1: (200+300)/2 = 250
        assert df["anh"].iloc[1] == pytest.approx(250.0)

    def test_nan_safe_one_missing(self, tmp_path):
        """If one subloc is NaN the other value is used (mean ignores NaN)."""
        idx = pd.date_range("2015-01-01", periods=2, freq="h")
        df = pd.DataFrame(
            {"msd@upper": [100.0, np.nan], "msd@lower": [np.nan, 200.0]},
            index=idx,
        )
        df.index.name = "datetime"
        fpath = tmp_path / "obs.csv"
        df.to_csv(fpath)
        out = average_sublocs_csv(str(fpath))
        result = pd.read_csv(out, index_col=0, parse_dates=True)
        assert result["msd"].iloc[0] == pytest.approx(100.0)
        assert result["msd"].iloc[1] == pytest.approx(200.0)

    def test_both_nan_stays_nan(self, tmp_path):
        """Both sublocs NaN → output NaN."""
        fpath = _make_wide_csv(
            tmp_path,
            {
                "msd@upper": [np.nan],
                "msd@lower": [np.nan],
            },
        )
        out = average_sublocs_csv(str(fpath))
        df = pd.read_csv(out, index_col=0, parse_dates=True)
        assert pd.isna(df["msd"].iloc[0])

    def test_single_subloc_stripped(self, tmp_path):
        """Columns with one subloc have @subloc removed."""
        fpath = _make_wide_csv(tmp_path, {"bks@upper": [1.0, 2.0, 3.0, 4.0, 5.0]})
        out = average_sublocs_csv(str(fpath))
        df = pd.read_csv(out, index_col=0, parse_dates=True)
        assert "bks" in df.columns
        assert "bks@upper" not in df.columns

    def test_no_subloc_unchanged(self, tmp_path):
        """Columns without @ are passed through unchanged."""
        fpath = _make_wide_csv(tmp_path, {"rsac075": [1.0, 2.0, 3.0, 4.0, 5.0]})
        out = average_sublocs_csv(str(fpath))
        df = pd.read_csv(out, index_col=0, parse_dates=True)
        assert "rsac075" in df.columns

    def test_default_output_name(self, tmp_path):
        """Default output path is {stem}_avg.csv next to the input file."""
        fpath = _make_wide_csv(tmp_path, {"a@x": [1.0, 2.0, 3.0, 4.0, 5.0]})
        out = average_sublocs_csv(str(fpath))
        assert Path(out).name == "obs_avg.csv"
        assert Path(out).parent == tmp_path

    def test_explicit_output_path(self, tmp_path):
        """Explicit --output path is respected."""
        fpath = _make_wide_csv(tmp_path, {"a@x": [1.0, 2.0, 3.0, 4.0, 5.0]})
        explicit = str(tmp_path / "custom.csv")
        out = average_sublocs_csv(str(fpath), explicit)
        assert out == explicit
        assert Path(explicit).exists()

    def test_three_sublocs_averaged(self, tmp_path):
        """Three sub-locations are all included in the mean."""
        fpath = _make_wide_csv(
            tmp_path,
            {
                "sta@a": [10.0, 20.0, 30.0, 40.0, 50.0],
                "sta@b": [20.0, 30.0, 40.0, 50.0, 60.0],
                "sta@c": [30.0, 40.0, 50.0, 60.0, 70.0],
            },
        )
        out = average_sublocs_csv(str(fpath))
        df = pd.read_csv(out, index_col=0, parse_dates=True)
        assert "sta" in df.columns
        assert df["sta"].iloc[0] == pytest.approx(20.0)  # (10+20+30)/3

    def test_column_order_preserved(self, tmp_path):
        """Output column order follows first-appearance of each base station."""
        fpath = _make_wide_csv(
            tmp_path,
            {
                "z_station": [1.0, 2.0, 3.0, 4.0, 5.0],
                "a_station@upper": [1.0, 2.0, 3.0, 4.0, 5.0],
                "a_station@lower": [2.0, 3.0, 4.0, 5.0, 6.0],
            },
        )
        out = average_sublocs_csv(str(fpath))
        df = pd.read_csv(out, index_col=0, parse_dates=True)
        assert list(df.columns) == ["z_station", "a_station"]

    def test_mixed_subloc_no_subloc_kept_separate(self, tmp_path):
        """If a base has both @subloc and no-subloc cols, keep them separate."""
        fpath = _make_wide_csv(
            tmp_path,
            {
                "sta": [1.0, 2.0, 3.0, 4.0, 5.0],
                "sta@lower": [2.0, 3.0, 4.0, 5.0, 6.0],
            },
        )
        out = average_sublocs_csv(str(fpath))
        df = pd.read_csv(out, index_col=0, parse_dates=True)
        # Both columns should be preserved as-is — no averaging of mixed group
        assert "sta" in df.columns
        assert "sta@lower" in df.columns


# ---------------------------------------------------------------------------
# _filter_inventory_by_polygon
# ---------------------------------------------------------------------------

class TestFilterInventoryByPolygon:
    def _write_polygon(self, tmp_path, coords) -> str:
        """Write a simple GeoJSON polygon file and return path."""
        geojson = {
            "type": "FeatureCollection",
            "features": [{
                "type": "Feature",
                "properties": {},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [coords],
                },
            }],
        }
        fpath = tmp_path / "clip.geojson"
        fpath.write_text(json.dumps(geojson))
        return str(fpath)

    def test_stations_inside_kept(self, tmp_path):
        geopandas = pytest.importorskip("geopandas")
        # Small square polygon centred on the Delta (~-121.8, 38.1)
        coords = [
            [-122.0, 38.0], [-121.5, 38.0],
            [-121.5, 38.3], [-122.0, 38.3], [-122.0, 38.0],
        ]
        poly_file = self._write_polygon(tmp_path, coords)
        inv = _make_inventory([
            {"station_id": "inside",  "lat": 38.1, "lon": -121.7, "param": "ec"},
            {"station_id": "outside", "lat": 39.0, "lon": -120.0, "param": "ec"},
        ])
        result = _filter_inventory_by_polygon(inv, poly_file)
        assert list(result["station_id"]) == ["inside"]

    def test_no_lat_lon_returns_unchanged(self, tmp_path):
        geopandas = pytest.importorskip("geopandas")
        coords = [[-122.0, 38.0], [-121.5, 38.0], [-121.5, 38.3],
                  [-122.0, 38.3], [-122.0, 38.0]]
        poly_file = self._write_polygon(tmp_path, coords)
        inv = pd.DataFrame([{"station_id": "x", "param": "ec"}])
        result = _filter_inventory_by_polygon(inv, poly_file)
        assert len(result) == 1   # unchanged

    def test_all_outside_returns_empty(self, tmp_path):
        geopandas = pytest.importorskip("geopandas")
        coords = [[-122.0, 38.0], [-121.5, 38.0], [-121.5, 38.3],
                  [-122.0, 38.3], [-122.0, 38.0]]
        poly_file = self._write_polygon(tmp_path, coords)
        inv = _make_inventory([
            {"station_id": "far1", "lat": 40.0, "lon": -119.0, "param": "ec"},
            {"station_id": "far2", "lat": 39.0, "lon": -118.0, "param": "ec"},
        ])
        result = _filter_inventory_by_polygon(inv, poly_file)
        assert len(result) == 0


# ---------------------------------------------------------------------------
# make_dsm2_clip_polygon
# ---------------------------------------------------------------------------

class TestMakeDsm2ClipPolygon:
    def test_output_is_valid_geojson_in_wgs84(self, tmp_path):
        geopandas = pytest.importorskip("geopandas")
        out = str(tmp_path / "clip.geojson")
        make_dsm2_clip_polygon(out, buffer_m=1000)
        import geopandas as gpd
        gdf = gpd.read_file(out)
        assert len(gdf) == 1
        assert gdf.crs.to_epsg() == 4326
        # Polygon should cover the Delta region
        bounds = gdf.total_bounds  # [minx, miny, maxx, maxy]
        assert bounds[0] < -121.5    # west of -121.5
        assert bounds[1] < 38.0     # south of 38.0
        assert bounds[2] > -121.5   # east of -121.5
        assert bounds[3] > 38.0     # north of 38.0

    def test_larger_buffer_gives_larger_polygon(self, tmp_path):
        geopandas = pytest.importorskip("geopandas")
        import geopandas as gpd
        small = str(tmp_path / "small.geojson")
        large = str(tmp_path / "large.geojson")
        make_dsm2_clip_polygon(small, buffer_m=100)
        make_dsm2_clip_polygon(large, buffer_m=5000)
        small_area = gpd.read_file(small).to_crs("EPSG:26910").area.sum()
        large_area = gpd.read_file(large).to_crs("EPSG:26910").area.sum()
        assert large_area > small_area

    def test_custom_channels_file(self, tmp_path):
        """Using the bundled file explicitly should give same result."""
        geopandas = pytest.importorskip("geopandas")
        default_channels = str(
            Path(__file__).parent.parent
            / "dsm2ui" / "dsm2gis" / "dsm2_channels_centerlines_8_2.geojson"
        )
        out = str(tmp_path / "clip.geojson")
        make_dsm2_clip_polygon(out, buffer_m=500, channels_file=default_channels)
        import geopandas as gpd
        gdf = gpd.read_file(out)
        assert len(gdf) == 1


# ---------------------------------------------------------------------------
# _filter_inventory_by_station_ids
# ---------------------------------------------------------------------------

class TestFilterInventoryByStationIds:
    def test_none_returns_unchanged(self):
        inv = _make_inventory([
            {"station_id": "sta1", "param": "elev"},
            {"station_id": "sta2", "param": "elev"},
        ])
        result = _filter_inventory_by_station_ids(inv, None)
        assert len(result) == 2

    def test_empty_list_returns_unchanged(self):
        inv = _make_inventory([{"station_id": "sta1", "param": "elev"}])
        result = _filter_inventory_by_station_ids(inv, [])
        assert len(result) == 1

    def test_filters_to_matching_station(self):
        inv = _make_inventory([
            {"station_id": "sta1", "param": "elev"},
            {"station_id": "sta2", "param": "elev"},
        ])
        result = _filter_inventory_by_station_ids(inv, ["sta1"])
        assert list(result["station_id"]) == ["sta1"]

    def test_case_insensitive(self):
        inv = _make_inventory([{"station_id": "srv", "param": "elev"}])
        result = _filter_inventory_by_station_ids(inv, ["SRV"])
        assert list(result["station_id"]) == ["srv"]

    def test_multiple_station_ids(self):
        inv = _make_inventory([
            {"station_id": "sta1", "param": "elev"},
            {"station_id": "sta2", "param": "elev"},
            {"station_id": "sta3", "param": "elev"},
        ])
        result = _filter_inventory_by_station_ids(inv, ["sta1", "sta3"])
        assert sorted(result["station_id"]) == ["sta1", "sta3"]

    def test_no_match_returns_empty(self):
        inv = _make_inventory([{"station_id": "sta1", "param": "elev"}])
        result = _filter_inventory_by_station_ids(inv, ["nonexistent"])
        assert len(result) == 0


# ---------------------------------------------------------------------------
# read_from_datastore_write_to_dss — resilience + station filtering
# ---------------------------------------------------------------------------

class TestReadFromDatastoreWriteToDss:
    def _write_station_csv(self, dir_path, fname, values, freq="h", start="2020-01-01"):
        """Write a minimal dms_datastore-format CSV under *dir_path*/*fname*."""
        from dms_datastore.write_ts import write_ts_csv

        idx = pd.date_range(start, periods=len(values), freq=freq)
        s = pd.Series(values, index=idx, name="value")
        write_ts_csv(s, str(dir_path / fname))

    def _write_inventory(self, datastore_dir, rows):
        inv_path = datastore_dir / "inventory_datasets_screened.csv"
        pd.DataFrame(rows).to_csv(inv_path, index=False)
        return inv_path

    def test_short_series_skipped_others_written(self, tmp_path):
        """A station with too little data to infer frequency (the original
        crash) is skipped, and the remaining stations are still written."""
        pyhecdss = pytest.importorskip("pyhecdss")
        screened = tmp_path / "screened"
        screened.mkdir()
        # good: 5 hourly points → freq inferrable
        self._write_station_csv(screened, "good_STA1_elev_2020.csv", [1.0, 2.0, 3.0, 4.0, 5.0])
        # bad: only 2 points → pd.infer_freq raises ValueError
        self._write_station_csv(screened, "bad_STA2_elev_2020.csv", [1.0, 2.0])
        self._write_inventory(tmp_path, [
            {"station_id": "STA1", "subloc": float("nan"), "param": "elev",
             "unit": "feet", "file_pattern": "good_STA1_elev_2020.csv"},
            {"station_id": "STA2", "subloc": float("nan"), "param": "elev",
             "unit": "feet", "file_pattern": "bad_STA2_elev_2020.csv"},
        ])

        from dsm2ui.datastore2dss import read_from_datastore_write_to_dss

        dssfile = str(tmp_path / "out.dss")
        # Must not raise despite STA2's short series.
        read_from_datastore_write_to_dss(str(tmp_path), dssfile, "elev")

        with pyhecdss.DSSFile(dssfile) as f:
            paths = f.get_pathnames()
        joined = "\n".join(paths)
        assert "STA1" in joined
        assert "STA2" not in joined

    def test_station_id_filter(self, tmp_path):
        """--station-id restricts extraction to the requested station only."""
        pyhecdss = pytest.importorskip("pyhecdss")
        screened = tmp_path / "screened"
        screened.mkdir()
        self._write_station_csv(screened, "a_STA1_elev_2020.csv", [1.0, 2.0, 3.0, 4.0, 5.0])
        self._write_station_csv(screened, "a_STA2_elev_2020.csv", [1.0, 2.0, 3.0, 4.0, 5.0])
        self._write_inventory(tmp_path, [
            {"station_id": "STA1", "subloc": float("nan"), "param": "elev",
             "unit": "feet", "file_pattern": "a_STA1_elev_2020.csv"},
            {"station_id": "STA2", "subloc": float("nan"), "param": "elev",
             "unit": "feet", "file_pattern": "a_STA2_elev_2020.csv"},
        ])

        from dsm2ui.datastore2dss import read_from_datastore_write_to_dss

        dssfile = str(tmp_path / "out.dss")
        read_from_datastore_write_to_dss(
            str(tmp_path), dssfile, "elev", station_ids=["sta1"]
        )

        with pyhecdss.DSSFile(dssfile) as f:
            paths = f.get_pathnames()
        joined = "\n".join(paths)
        assert "STA1" in joined
        assert "STA2" not in joined
