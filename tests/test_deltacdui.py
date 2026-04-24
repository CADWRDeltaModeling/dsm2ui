"""Tests for DeltaCDUIManager and DeltaCDAreaReader.

``xr.open_dataset`` is mocked with in-memory ``xr.Dataset`` objects so
no real .nc files are required.
"""

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from unittest.mock import patch

from dsm2ui.deltacdui.deltacduimgr import DeltaCDAreaReader, DeltaCDUIManager


# ---------------------------------------------------------------------------
# Helpers — in-memory xr.Dataset factories
# ---------------------------------------------------------------------------

def _make_ds_no_crop(n_area=2, n_time=10, variables=("et", "precip")):
    times = pd.date_range("2020-01-01", periods=n_time, freq="D")
    area_ids = np.array([str(i + 1) for i in range(n_area)])
    rng = np.random.default_rng(0)
    data_vars = {
        v: xr.Variable(
            dims=["time", "area_id"],
            data=rng.random((n_time, n_area)),
            attrs={"units": "mm/day"},
        )
        for v in variables
    }
    return xr.Dataset(data_vars, coords={"time": times, "area_id": area_ids})


def _make_ds_with_crop(n_area=2, n_time=10, n_crop=2, variables=("et", "precip")):
    times = pd.date_range("2020-01-01", periods=n_time, freq="D")
    area_ids = np.array([str(i + 1) for i in range(n_area)])
    crops = np.array([f"crop{j}" for j in range(n_crop)])
    rng = np.random.default_rng(1)
    data_vars = {
        v: xr.Variable(
            dims=["time", "area_id", "crop"],
            data=rng.random((n_time, n_area, n_crop)),
            attrs={"units": "mm/day"},
        )
        for v in variables
    }
    return xr.Dataset(
        data_vars, coords={"time": times, "area_id": area_ids, "crop": crops}
    )


# ---------------------------------------------------------------------------
# DeltaCDAreaReader
# ---------------------------------------------------------------------------

class TestDeltaCDAreaReader:
    def test_load_no_crop_returns_dataframe_with_datetimeindex(self):
        ds = _make_ds_no_crop()
        reader = DeltaCDAreaReader({"fake.nc": ds})
        df = reader.load(source="fake.nc", area_id="1", variable="et")
        assert isinstance(df, pd.DataFrame)
        assert isinstance(df.index, pd.DatetimeIndex)
        assert len(df) == 10

    def test_load_with_crop_returns_correct_length(self):
        ds = _make_ds_with_crop()
        reader = DeltaCDAreaReader({"fake.nc": ds})
        df = reader.load(source="fake.nc", area_id="1", variable="et", crop="crop0")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 10

    def test_load_with_time_range_slices_result(self):
        ds = _make_ds_no_crop(n_time=30)
        reader = DeltaCDAreaReader({"fake.nc": ds})
        df = reader.load(
            source="fake.nc",
            area_id="1",
            variable="et",
            time_range=("2020-01-05", "2020-01-15"),
        )
        assert len(df) > 0
        assert df.index.max() <= pd.Timestamp("2020-01-15")

    def test_load_bad_area_id_returns_empty_dataframe(self):
        ds = _make_ds_no_crop()
        reader = DeltaCDAreaReader({"fake.nc": ds})
        df = reader.load(source="fake.nc", area_id="nonexistent_99", variable="et")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

    def test_load_all_area_ids_have_data(self):
        ds = _make_ds_no_crop(n_area=3)
        reader = DeltaCDAreaReader({"fake.nc": ds})
        for area_id in ["1", "2", "3"]:
            df = reader.load(source="fake.nc", area_id=area_id, variable="et")
            assert len(df) == 10

    def test_repr_contains_filename(self):
        ds = _make_ds_no_crop()
        reader = DeltaCDAreaReader({"myfile.nc": ds})
        assert "myfile.nc" in repr(reader)


# ---------------------------------------------------------------------------
# DeltaCDUIManager — catalog shape
# ---------------------------------------------------------------------------

class TestDeltaCDUIManagerCatalog:
    def test_catalog_without_crop_row_count(self, tmp_path):
        ds = _make_ds_no_crop(n_area=2, variables=("et", "precip"))
        nc_path = str(tmp_path / "test.nc")
        with patch("dsm2ui.deltacdui.deltacduimgr.xr.open_dataset", return_value=ds):
            mgr = DeltaCDUIManager(nc_path)
        cat = mgr.get_data_catalog()
        # 2 area_ids × 2 variables = 4 rows
        assert len(cat) == 4

    def test_catalog_without_crop_has_required_columns(self, tmp_path):
        ds = _make_ds_no_crop(n_area=2, variables=("et", "precip"))
        nc_path = str(tmp_path / "test.nc")
        with patch("dsm2ui.deltacdui.deltacduimgr.xr.open_dataset", return_value=ds):
            mgr = DeltaCDUIManager(nc_path)
        cat = mgr.get_data_catalog()
        assert "area_id" in cat.columns
        assert "variable" in cat.columns
        assert "source" in cat.columns

    def test_catalog_without_crop_has_no_crop_column(self, tmp_path):
        ds = _make_ds_no_crop()
        nc_path = str(tmp_path / "test.nc")
        with patch("dsm2ui.deltacdui.deltacduimgr.xr.open_dataset", return_value=ds):
            mgr = DeltaCDUIManager(nc_path)
        cat = mgr.get_data_catalog()
        assert "crop" not in cat.columns

    def test_catalog_with_crop_row_count(self, tmp_path):
        ds = _make_ds_with_crop(n_area=2, n_crop=2, variables=("et", "precip"))
        nc_path = str(tmp_path / "test.nc")
        with patch("dsm2ui.deltacdui.deltacduimgr.xr.open_dataset", return_value=ds):
            mgr = DeltaCDUIManager(nc_path)
        cat = mgr.get_data_catalog()
        # 2 area_ids × 2 crops × 2 variables = 8 rows
        assert len(cat) == 8

    def test_catalog_with_crop_has_crop_column(self, tmp_path):
        ds = _make_ds_with_crop(n_area=2, n_crop=2, variables=("et",))
        nc_path = str(tmp_path / "test.nc")
        with patch("dsm2ui.deltacdui.deltacduimgr.xr.open_dataset", return_value=ds):
            mgr = DeltaCDUIManager(nc_path)
        cat = mgr.get_data_catalog()
        assert "crop" in cat.columns

    def test_invalid_file_type_raises_valueerror(self, tmp_path):
        with pytest.raises(ValueError, match="Invalid file type"):
            DeltaCDUIManager(str(tmp_path / "data.csv"))

    def test_catalog_source_column_matches_path(self, tmp_path):
        ds = _make_ds_no_crop()
        nc_path = str(tmp_path / "mydata.nc")
        with patch("dsm2ui.deltacdui.deltacduimgr.xr.open_dataset", return_value=ds):
            mgr = DeltaCDUIManager(nc_path)
        cat = mgr.get_data_catalog()
        assert (cat["source"] == nc_path).all()

    def test_multifile_catalog_combines_rows(self, tmp_path):
        ds1 = _make_ds_no_crop(n_area=2, variables=("et",))
        ds2 = _make_ds_no_crop(n_area=3, variables=("precip",))
        nc1 = str(tmp_path / "f1.nc")
        nc2 = str(tmp_path / "f2.nc")
        datasets = {nc1: ds1, nc2: ds2}
        with patch(
            "dsm2ui.deltacdui.deltacduimgr.xr.open_dataset",
            side_effect=lambda p, **kw: datasets[p],
        ):
            mgr = DeltaCDUIManager(nc1, nc2)
        cat = mgr.get_data_catalog()
        # f1: 2 areas × 1 var = 2 rows; f2: 3 areas × 1 var = 3 rows → total 5
        assert len(cat) == 5

    def test_catalog_variable_values_match_dataset_vars(self, tmp_path):
        ds = _make_ds_no_crop(n_area=2, variables=("et", "precip"))
        nc_path = str(tmp_path / "test.nc")
        with patch("dsm2ui.deltacdui.deltacduimgr.xr.open_dataset", return_value=ds):
            mgr = DeltaCDUIManager(nc_path)
        cat = mgr.get_data_catalog()
        assert set(cat["variable"].unique()) == {"et", "precip"}

    def test_catalog_area_id_values_match_dataset(self, tmp_path):
        ds = _make_ds_no_crop(n_area=3, variables=("et",))
        nc_path = str(tmp_path / "test.nc")
        with patch("dsm2ui.deltacdui.deltacduimgr.xr.open_dataset", return_value=ds):
            mgr = DeltaCDUIManager(nc_path)
        cat = mgr.get_data_catalog()
        assert set(cat["area_id"].unique()) == {"1", "2", "3"}


# ---------------------------------------------------------------------------
# DeltaCDUIManager — data retrieval
# ---------------------------------------------------------------------------

class TestDeltaCDUIManagerDataRetrieval:
    @pytest.fixture
    def mgr_and_cat(self, tmp_path):
        ds = _make_ds_no_crop(n_area=2, variables=("et",))
        nc_path = str(tmp_path / "data.nc")
        with patch("dsm2ui.deltacdui.deltacduimgr.xr.open_dataset", return_value=ds):
            mgr = DeltaCDUIManager(nc_path)
        return mgr, mgr.get_data_catalog()

    def test_get_data_reference_returns_ref(self, mgr_and_cat):
        mgr, cat = mgr_and_cat
        row = cat.iloc[0]
        ref = mgr.get_data_reference(row)
        assert ref is not None

    def test_data_reference_loads_dataframe(self, mgr_and_cat):
        mgr, cat = mgr_and_cat
        row = cat.iloc[0]
        ref = mgr.get_data_reference(row)
        df = ref.getData()
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0

    def test_get_time_range_returns_timestamps(self, tmp_path):
        # Use a dataset spanning two calendar years so start_year != max_year.
        ds = _make_ds_no_crop(n_area=1, n_time=400, variables=("et",))
        nc_path = str(tmp_path / "data2yr.nc")
        with patch("dsm2ui.deltacdui.deltacduimgr.xr.open_dataset", return_value=ds):
            mgr = DeltaCDUIManager(nc_path)
        cat = mgr.get_data_catalog()
        start, end = mgr.get_time_range(cat)
        assert isinstance(start, pd.Timestamp)
        assert isinstance(end, pd.Timestamp)
        assert start < end


# ---------------------------------------------------------------------------
# DeltaCDUIManager — metadata helpers
# ---------------------------------------------------------------------------

class TestDeltaCDUIManagerMetadata:
    @pytest.fixture
    def mgr(self, tmp_path):
        ds = _make_ds_no_crop(n_area=2, variables=("et",))
        nc_path = str(tmp_path / "data.nc")
        with patch("dsm2ui.deltacdui.deltacduimgr.xr.open_dataset", return_value=ds):
            return DeltaCDUIManager(nc_path)

    def test_build_station_name_no_crop(self, mgr):
        row = {"area_id": "42", "variable": "et"}
        name = mgr.build_station_name(row)
        assert "42" in name

    def test_get_table_column_width_map_has_area_id(self, mgr):
        col_map = mgr._get_table_column_width_map()
        assert "area_id" in col_map

    def test_get_table_filters_has_area_id(self, mgr):
        filters = mgr.get_table_filters()
        assert "area_id" in filters

    def test_get_table_filters_has_variable(self, mgr):
        filters = mgr.get_table_filters()
        assert "variable" in filters
