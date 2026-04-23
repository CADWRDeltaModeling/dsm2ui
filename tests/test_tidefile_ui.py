"""Tests for DSM2TidefileUIManager catalog creation and data retrieval."""
import os

import pandas as pd
import pytest

from pydsm.output.hydroh5 import HydroH5
from pydsm.output.qualh5 import QualH5

HYDRO_H5 = os.path.join(os.path.dirname(__file__), "..", "..", "pydsm", "tests", "data", "historical_v82.h5")
QUAL_H5 = os.path.join(os.path.dirname(__file__), "..", "..", "pydsm", "tests", "data", "historical_v82_ec.h5")


@pytest.fixture(scope="module")
def hydro():
    return HydroH5(HYDRO_H5)


@pytest.fixture(scope="module")
def qual():
    return QualH5(QUAL_H5)


# ---------------------------------------------------------------------------
# HydroH5 catalog
# ---------------------------------------------------------------------------


class TestHydroCatalog:
    def test_catalog_has_expected_variables(self, hydro):
        cat = hydro.create_catalog()
        variables = set(cat["variable"].str.upper().unique())
        assert {"FLOW", "AREA", "STAGE", "HEIGHT"}.issubset(variables)

    def test_catalog_has_id_and_filename(self, hydro):
        cat = hydro.create_catalog()
        assert "id" in cat.columns
        assert "filename" in cat.columns

    def test_channel_flow_has_up_and_down(self, hydro):
        cat = hydro.create_catalog()
        flow_ids = cat.loc[cat["variable"] == "flow", "id"]
        assert any("_UP" in i for i in flow_ids), "Expected upstream entries"
        assert any("_DOWN" in i for i in flow_ids), "Expected downstream entries"

    def test_get_data_for_channel_flow(self, hydro):
        cat = hydro.create_catalog()
        entry = cat[(cat["id"].str.startswith("CHAN_")) & (cat["variable"] == "flow")].iloc[0]
        df = hydro.get_data_for_catalog_entry(entry)
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert isinstance(df.index, pd.DatetimeIndex)

    def test_get_data_for_channel_stage(self, hydro):
        cat = hydro.create_catalog()
        entry = cat[(cat["id"].str.startswith("CHAN_")) & (cat["variable"] == "stage")].iloc[0]
        df = hydro.get_data_for_catalog_entry(entry)
        assert isinstance(df, pd.DataFrame)
        assert not df.empty

    def test_get_data_for_channel_avg_area(self, hydro):
        cat = hydro.create_catalog()
        avg_area = cat[(cat["variable"] == "area") & (~cat["id"].str.contains("UP|DOWN"))]
        if not avg_area.empty:
            df = hydro.get_data_for_catalog_entry(avg_area.iloc[0])
            assert not df.empty
            assert df.shape[1] == 1

    def test_get_data_for_reservoir_height(self, hydro):
        cat = hydro.create_catalog()
        res = cat[(cat["id"].str.startswith("RES_")) & (cat["variable"] == "height")]
        if not res.empty:
            df = hydro.get_data_for_catalog_entry(res.iloc[0])
            assert not df.empty

    def test_get_data_for_qext_flow(self, hydro):
        cat = hydro.create_catalog()
        qext = cat[cat["id"].str.startswith("QEXT_")]
        if not qext.empty:
            df = hydro.get_data_for_catalog_entry(qext.iloc[0])
            assert not df.empty

    def test_get_data_with_time_window(self, hydro):
        cat = hydro.create_catalog()
        entry = cat[(cat["id"].str.startswith("CHAN_")) & (cat["variable"] == "flow")].iloc[0]
        start, end = hydro.get_start_end_dates()
        tw = f"{start} - {end}"
        df = hydro.get_data_for_catalog_entry(entry, time_window=tw)
        assert isinstance(df, pd.DataFrame)
        assert not df.empty


# ---------------------------------------------------------------------------
# QualH5 catalog
# ---------------------------------------------------------------------------


class TestQualCatalog:
    def test_catalog_has_constituents(self, qual):
        cat = qual.create_catalog()
        assert not cat.empty
        assert "variable" in cat.columns

    def test_channel_entries_have_up_down(self, qual):
        cat = qual.create_catalog()
        chan_ids = cat.loc[cat["id"].str.startswith("CHAN_"), "id"]
        assert any("_UP" in i for i in chan_ids)
        assert any("_DOWN" in i for i in chan_ids)

    def test_get_data_for_channel_concentration(self, qual):
        cat = qual.create_catalog()
        entry = cat[cat["id"].str.contains("CHAN_") & cat["id"].str.contains("_UP")].iloc[0]
        df = qual.get_data_for_catalog_entry(entry)
        assert isinstance(df, pd.DataFrame)
        assert not df.empty

    def test_reservoir_entries_present(self, qual):
        cat = qual.create_catalog()
        res = cat[cat["id"].str.startswith("RES_")]
        assert not res.empty


# ---------------------------------------------------------------------------
# DSM2TidefileUIManager
# ---------------------------------------------------------------------------


class TestDSM2TidefileUIManager:
    @pytest.fixture(scope="class")
    def manager(self):
        from dsm2ui.dsm2ui import DSM2TidefileUIManager
        return DSM2TidefileUIManager([HYDRO_H5])

    @pytest.fixture(scope="class")
    def multi_manager(self):
        from dsm2ui.dsm2ui import DSM2TidefileUIManager
        return DSM2TidefileUIManager([HYDRO_H5, QUAL_H5])

    def test_catalog_dataframe_not_empty(self, manager):
        dfcat = manager.get_data_catalog()
        assert isinstance(dfcat, pd.DataFrame)
        assert not dfcat.empty

    def test_catalog_has_geoid_column(self, manager):
        dfcat = manager.get_data_catalog()
        assert "geoid" in dfcat.columns

    def test_geoid_extracted_from_id(self, manager):
        dfcat = manager.get_data_catalog()
        # geoid should be the second part of the id split by '_'
        sample = dfcat.iloc[0]
        expected_geoid = sample["id"].split("_")[1]
        assert sample["geoid"] == expected_geoid

    def test_time_range_set(self, manager):
        assert manager.time_range is not None
        assert len(manager.time_range) == 2
        assert manager.time_range[0] < manager.time_range[1]

    def test_get_data_for_time_range(self, manager):
        dfcat = manager.get_data_catalog()
        entry = dfcat[dfcat["id"].str.startswith("CHAN_") & (dfcat["variable"] == "flow")].iloc[0]
        ref = manager.get_data_reference(entry)
        df = ref.getData(time_range=manager.time_range)
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        unit = ref.get_attribute("unit", "")
        assert isinstance(unit, str)

    def test_table_column_width_map(self, manager):
        col_map = manager._get_table_column_width_map()
        assert "geoid" in col_map
        assert "variable" in col_map

    def test_table_filters(self, manager):
        filters = manager.get_table_filters()
        assert "geoid" in filters
        assert "variable" in filters

    def test_multi_tidefile_catalog_combines_both(self, multi_manager):
        dfcat = multi_manager.get_data_catalog()
        filenames = dfcat["filename"].unique()
        assert len(filenames) == 2

    def test_multi_tidefile_time_range_spans_both(self, multi_manager):
        tr = multi_manager.time_range
        assert tr[0] < tr[1]

    def test_create_curve(self, manager):
        import holoviews as hv
        from dsm2ui.dsm2ui import _TidefilePlotAction
        dfcat = manager.get_data_catalog()
        entry = dfcat[dfcat["id"].str.startswith("CHAN_") & (dfcat["variable"] == "flow")].iloc[0]
        ref = manager.get_data_reference(entry)
        df = ref.getData(time_range=manager.time_range)
        unit = ref.get_attribute("unit", "")
        crv = _TidefilePlotAction().create_curve(df, entry, unit)
        assert isinstance(crv, hv.Curve)

    def test_read_tidefile_hydro(self):
        from dsm2ui.dsm2ui import DSM2TidefileUIManager
        h5 = DSM2TidefileUIManager.read_tidefile(HYDRO_H5)
        assert isinstance(h5, HydroH5)

    def test_read_tidefile_qual(self):
        from dsm2ui.dsm2ui import DSM2TidefileUIManager
        h5 = DSM2TidefileUIManager.read_tidefile(QUAL_H5)
        # QualH5 detected via fallback
        assert isinstance(h5, (HydroH5, QualH5))
