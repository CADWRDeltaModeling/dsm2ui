"""Tests for mixed-catalog DataReference loading.

Verifies the name-first lookup contract implemented across all dsm2ui manager
subclasses:

1. When a row comes from ``catalog.to_dataframe().reset_index()`` (normal Panel
   flow via ``DataUIManager._dfcat``), the ``'name'`` column is present and
   ``get_data_reference(row)`` resolves by it.

2. When ``'name'`` is absent from the row (legacy / stripped path), each
   manager falls back to its private key-reconstruction formula.

3. A heterogeneous catalog containing multiple DataReference subclasses
   (raw ``DataReference``, ``DSM2DSSDataReference``, ``CalibDataReference``)
   can have every row resolved by name through the base
   ``DataUIManager.get_data_reference()`` contract.

These tests do NOT call ``getData()`` — the loading path is covered by
existing per-manager tests.  The focus here is identity resolution.
"""

from __future__ import annotations

import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from dvue.catalog import (
    DataCatalog,
    DataReference,
    InMemoryDataReferenceReader,
    build_catalog_from_dataframe,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _row_with_name(catalog: DataCatalog, ref_name: str) -> pd.Series:
    """Return the catalog row as it appears in ``to_dataframe().reset_index()``."""
    df = catalog.to_dataframe().reset_index()
    match = df[df["name"] == ref_name]
    assert len(match) == 1, f"Expected exactly one row for {ref_name!r}"
    return match.iloc[0]


def _row_without_name(catalog: DataCatalog, ref_name: str) -> pd.Series:
    """Same as above but with the ``'name'`` column removed (fallback path)."""
    row = _row_with_name(catalog, ref_name)
    return row.drop("name")


# ---------------------------------------------------------------------------
# DSM2DataUIManager — name-first + fallback
# ---------------------------------------------------------------------------


def _make_output_channels_df():
    """Return a minimal output_channels GeoDataFrame for DSM2DataUIManager."""
    import geopandas as gpd
    from shapely.geometry import Point

    return gpd.GeoDataFrame(
        {
            "NAME": ["RSAC075", "RSAN007"],
            "CHAN_NO": [1, 2],
            "DISTANCE": [0.0, 0.5],
            "VARIABLE": ["EC", "EC"],
            "INTERVAL": ["15MIN", "15MIN"],
            "PERIOD_OP": ["INST", "INST"],
            "FILE": ["study.dss", "study.dss"],
        },
        geometry=[Point(0, 0), Point(1, 1)],
        crs="EPSG:4326",
    )


class TestDSM2DataUIManagerLookup:
    @pytest.fixture(autouse=True)
    def _manager(self):
        from dsm2ui.dsm2ui import DSM2DataUIManager

        self.mgr = DSM2DataUIManager(_make_output_channels_df())

    def test_name_first_lookup_returns_correct_ref(self):
        """Name-first path: row from to_dataframe().reset_index() resolves correctly."""
        cat = self.mgr.data_catalog
        for _, row in cat.to_dataframe().reset_index().iterrows():
            ref = self.mgr.get_data_reference(row)
            assert ref is not None
            assert ref.name == row["name"]

    def test_fallback_when_name_absent(self):
        """Fallback path: row without 'name' resolves via _ref_name reconstruction."""
        cat = self.mgr.data_catalog
        df = cat.to_dataframe().reset_index()
        row_with = df.iloc[0]
        row_without = row_with.drop("name")

        ref_via_name = self.mgr.get_data_reference(row_with)
        ref_via_fallback = self.mgr.get_data_reference(row_without)

        assert ref_via_name is ref_via_fallback

    def test_all_catalog_rows_resolve(self):
        """Every row in the catalog DataFrame resolves to a non-None reference."""
        cat = self.mgr.data_catalog
        for _, row in cat.to_dataframe().reset_index().iterrows():
            assert self.mgr.get_data_reference(row) is not None


# ---------------------------------------------------------------------------
# DSSDataUIManager — name-first + fallback (mocked DSS file)
# ---------------------------------------------------------------------------


def _make_dss_catalog_df():
    return pd.DataFrame([
        {"A": "AREA", "B": "RSAC075", "C": "EC", "D": "01JAN2020-31DEC2020",
         "E": "1HOUR", "F": "VER1", "T": "rts"},
        {"A": "AREA", "B": "RSAN007", "C": "EC", "D": "01JAN2020-31DEC2020",
         "E": "1HOUR", "F": "VER1", "T": "rts"},
    ])


def _mock_dssfile(catalog_df):
    mock = MagicMock()
    mock.read_catalog.return_value = catalog_df.copy()
    mock.close.return_value = None
    return mock


class TestDSSDataUIManagerLookup:
    @pytest.fixture(autouse=True)
    def _manager(self):
        mocks = {"study.dss": _mock_dssfile(_make_dss_catalog_df())}

        with patch("dsm2ui.dssui.dssui.dss.DSSFile", side_effect=lambda p: mocks[p]):
            from dsm2ui.dssui.dssui import DSSDataUIManager
            self.mgr = DSSDataUIManager("study.dss", filename_column="filename")

    def test_name_first_lookup_returns_correct_ref(self):
        cat = self.mgr.data_catalog
        for _, row in cat.to_dataframe().reset_index().iterrows():
            ref = self.mgr.get_data_reference(row)
            assert ref is not None
            assert ref.name == row["name"]

    def test_fallback_when_name_absent(self):
        cat = self.mgr.data_catalog
        row_with = cat.to_dataframe().reset_index().iloc[0]
        row_without = row_with.drop("name")

        assert self.mgr.get_data_reference(row_with) is self.mgr.get_data_reference(row_without)


# ---------------------------------------------------------------------------
# Heterogeneous catalog — multiple ref_types in one catalog
# ---------------------------------------------------------------------------


def _make_reader(df: pd.DataFrame) -> InMemoryDataReferenceReader:
    return InMemoryDataReferenceReader(df)


class TestHeterogeneousCatalog:
    """A single DataCatalog holding raw, DSM2DSSDataReference, and CalibDataReference
    entries.  All rows should resolve via name.
    """

    @pytest.fixture(autouse=True)
    def _catalog(self):
        from dsm2ui.dsm2ui import DSM2DSSDataReference
        from dsm2ui.calib.calibplotui import CalibDataReference

        dummy_df = pd.DataFrame({"value": [1.0, 2.0]})

        cat = DataCatalog()

        cat.add(DataReference(
            reader=_make_reader(dummy_df),
            name="raw_flow",
            ref_type="raw",
            variable="flow",
            unit="cfs",
        ))

        cat.add(DSM2DSSDataReference(
            reader=_make_reader(dummy_df),
            name="dss_ec_rsac075",
            variable="ec",
            station_name="RSAC075",
            source="study.dss",
        ))

        cat.add(CalibDataReference(
            reader=_make_reader(dummy_df),
            name="calib_rsac075_ec",
            source="",
            Name="RSAC075",
            vartype="ec",
        ))

        self.cat = cat

    def test_all_ref_types_present(self):
        df = self.cat.to_dataframe().reset_index()
        ref_types = set(df["ref_type"].tolist())
        assert "raw" in ref_types
        assert "dsm2_dss" in ref_types
        assert "calib" in ref_types

    def test_every_row_resolves_by_name(self):
        """Simulates the DataUIManager._dfcat → get_data_reference() flow."""
        df = self.cat.to_dataframe().reset_index()
        for _, row in df.iterrows():
            ref = self.cat.get(row["name"])
            assert ref is not None
            assert ref.name == row["name"]

    def test_every_row_resolves_via_base_manager_contract(self):
        """DataUIManager.get_data_reference() must work for any row from the catalog."""
        from dvue.dataui import DataUIManager

        class _TestMgr(DataUIManager):
            def __init__(self, catalog):
                # minimal init — skip Panel param setup by calling object.__init__
                object.__init__(self)
                self._catalog = catalog

            @property
            def data_catalog(self):
                return self._catalog

            def get_data_catalog(self):
                return self._catalog.to_dataframe().reset_index()

        mgr = _TestMgr(self.cat)
        df = self.cat.to_dataframe().reset_index()
        for _, row in df.iterrows():
            ref = mgr.get_data_reference(row)
            assert ref is not None
            assert ref.name == row["name"]

    def test_ref_type_attribute_accessible(self):
        """ref_type is stored as a DataReference attribute accessible on retrieval."""
        ref = self.cat.get("dss_ec_rsac075")
        assert ref.ref_type == "dsm2_dss"

        ref = self.cat.get("calib_rsac075_ec")
        assert ref.ref_type == "calib"

        ref = self.cat.get("raw_flow")
        assert ref.ref_type == "raw"
