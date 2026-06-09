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


# ---------------------------------------------------------------------------
# DSM2CombinedUIManager — regression tests for UP/DOWN catalog completeness
#
# Bug (fixed): normalize_ref() previously set station = geoid (e.g. "1") for
# every channel entry.  Because the catalog primary key is
# (source_num, station, variable), CHAN_1_UP and CHAN_1_DOWN produced the same
# pk tuple (0, "1", "stage").  The second entry was silently discarded.  The
# same collision also dropped all avg-area (CHAN_N, no UP/DOWN suffix) entries.
#
# Fix: station is now set to the full `id` string ("CHAN_1_UP", "CHAN_1_DOWN",
# "CHAN_1") so every entry is uniquely keyed.  A separate `geoid` attribute
# carries the numeric channel number for geo-matching.
# ---------------------------------------------------------------------------


class TestDSM2CombinedUIManager:
    @pytest.fixture(scope="class")
    def manager(self):
        from dsm2ui.dsm2ui import DSM2CombinedUIManager
        mgr = DSM2CombinedUIManager()
        mgr.add_source_files(HYDRO_H5)
        return mgr

    @pytest.fixture(scope="class")
    def raw_catalog(self):
        """The catalog produced directly by HydroH5 — ground truth entry count."""
        return HydroH5(HYDRO_H5).create_catalog()

    # -- completeness --------------------------------------------------------

    def test_catalog_total_matches_raw(self, manager, raw_catalog):
        """No entries must be silently dropped during normalization."""
        dfcat = manager.get_data_catalog()
        assert len(dfcat) == len(raw_catalog), (
            f"DSM2CombinedUIManager catalog has {len(dfcat)} entries but "
            f"HydroH5.create_catalog() returns {len(raw_catalog)}. "
            "Likely cause: primary-key collision in normalize_ref() is still dropping entries."
        )

    def test_channel_stage_has_both_up_and_down(self, manager):
        """CHAN_N_UP and CHAN_N_DOWN entries must both be present for stage."""
        dfcat = manager.get_data_catalog()
        stage = dfcat[dfcat["variable"] == "stage"]
        assert not stage.empty, "No stage entries found at all"
        up_ids = stage[stage["station"].str.endswith("_UP")]
        down_ids = stage[stage["station"].str.endswith("_DOWN")]
        assert not up_ids.empty, "Upstream stage entries missing from combined catalog"
        assert not down_ids.empty, (
            "Downstream stage entries missing from combined catalog — "
            "primary-key collision in normalize_ref() not fully fixed"
        )
        # UP and DOWN counts must match (one entry per channel per direction)
        assert len(up_ids) == len(down_ids), (
            f"UP stage count ({len(up_ids)}) != DOWN stage count ({len(down_ids)})"
        )

    def test_channel_flow_has_both_up_and_down(self, manager):
        """CHAN_N_UP and CHAN_N_DOWN entries must both be present for flow."""
        dfcat = manager.get_data_catalog()
        flow = dfcat[dfcat["variable"] == "flow"]
        assert not flow[flow["station"].str.endswith("_UP")].empty, "Upstream flow entries missing"
        assert not flow[flow["station"].str.endswith("_DOWN")].empty, (
            "Downstream flow entries missing from combined catalog"
        )

    def test_avg_area_entries_not_dropped(self, manager, raw_catalog):
        """Avg-area entries (CHAN_N, no UP/DOWN suffix) must not be dropped."""
        raw_avg = raw_catalog[
            raw_catalog["id"].str.match(r"^CHAN_\d+$")
        ]
        if raw_avg.empty:
            pytest.skip("No avg-area entries in raw catalog")
        dfcat = manager.get_data_catalog()
        mgr_avg = dfcat[dfcat["station"].str.match(r"^CHAN_\d+$")]
        assert len(mgr_avg) == len(raw_avg), (
            f"Expected {len(raw_avg)} avg-area entries, got {len(mgr_avg)}"
        )

    # -- data retrieval ------------------------------------------------------

    def test_can_retrieve_data_for_downstream_stage(self, manager):
        """Data retrieval must work for a downstream (CHAN_N_DOWN) stage entry."""
        dfcat = manager.get_data_catalog()
        down_stage = dfcat[
            (dfcat["variable"] == "stage") & dfcat["station"].str.endswith("_DOWN")
        ]
        assert not down_stage.empty, "No downstream stage entries to test retrieval"
        row = down_stage.iloc[0]
        ref = manager.get_data_reference(row)
        df = ref.getData(time_range=manager.time_range)
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert isinstance(df.index, pd.DatetimeIndex)

    def test_can_retrieve_data_for_upstream_stage(self, manager):
        """Data retrieval must work for an upstream (CHAN_N_UP) stage entry."""
        dfcat = manager.get_data_catalog()
        up_stage = dfcat[
            (dfcat["variable"] == "stage") & dfcat["station"].str.endswith("_UP")
        ]
        row = up_stage.iloc[0]
        ref = manager.get_data_reference(row)
        df = ref.getData(time_range=manager.time_range)
        assert isinstance(df, pd.DataFrame)
        assert not df.empty

    # -- station format ------------------------------------------------------

    def test_station_values_are_full_ids(self, manager):
        """station column must contain full id strings (e.g. CHAN_1_UP), not bare channel numbers."""
        dfcat = manager.get_data_catalog()
        chan = dfcat[dfcat["station"].str.startswith("CHAN_")]
        assert not chan.empty, "No CHAN_ entries in catalog"
        # No station should be a bare integer (old buggy behaviour)
        bare_int = chan["station"].str.match(r"^\d+$")
        assert not bare_int.any(), (
            "Some station values are bare channel numbers — normalize_ref fix not applied"
        )

    def test_geoid_column_contains_channel_numbers(self, manager):
        """geoid column must contain the numeric channel number extracted from id."""
        dfcat = manager.get_data_catalog()
        chan = dfcat[dfcat["station"].str.startswith("CHAN_")]
        assert "geoid" in dfcat.columns, "geoid attribute not stored on catalog entries"
        # geoid should be digits-only for channel entries
        assert chan["geoid"].dropna().str.match(r"^\d+$").all(), (
            "geoid values for CHAN_ entries must be numeric strings"
        )
