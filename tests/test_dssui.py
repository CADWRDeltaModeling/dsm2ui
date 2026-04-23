"""Tests for DSSDataUIManager FILE_NUM / table-column consistency.

These tests mock ``pyhecdss.DSSFile`` so no real DSS files are needed.
"""

import pandas as pd
import pytest
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_dss_catalog_df(n_rows=2):
    """Return a DataFrame resembling ``pyhecdss.DSSFile.read_catalog()``."""
    rows = []
    for i in range(n_rows):
        rows.append(
            {
                "A": "AREA",
                "B": f"STA{i:03d}",
                "C": "EC",
                "D": "01JAN2020-31DEC2020",
                "E": "1HOUR",
                "F": "VER1",
                "T": "rts",
            }
        )
    return pd.DataFrame(rows)


def _mock_dssfile(catalog_df):
    """Return a MagicMock behaving like ``pyhecdss.DSSFile``."""
    mock = MagicMock()
    mock.read_catalog.return_value = catalog_df.copy()
    mock.get_pathnames.return_value = ["/AREA/STA000/EC//1HOUR/VER1/"]
    mock.close.return_value = None
    return mock


@pytest.fixture()
def _patch_dss():
    """Context manager that patches ``pyhecdss.DSSFile`` constructor.

    Yields a helper ``register(path, catalog_df)`` that maps file paths
    to mock DSSFile instances.
    """
    mocks = {}

    def _factory(path):
        return mocks[path]

    def register(path, catalog_df=None):
        if catalog_df is None:
            catalog_df = _make_dss_catalog_df()
        mocks[path] = _mock_dssfile(catalog_df)

    with patch("dsm2ui.dssui.dssui.dss.DSSFile", side_effect=_factory):
        yield register


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestDSSDataUIManagerSingleFile:
    def test_no_file_num_in_columns(self, _patch_dss):
        _patch_dss("file_a.dss")
        from dsm2ui.dssui.dssui import DSSDataUIManager

        mgr = DSSDataUIManager("file_a.dss", filename_column="filename")
        assert mgr.display_fileno is False
        assert "FILE_NUM" not in mgr.get_table_columns()

    def test_table_columns_subset_of_catalog(self, _patch_dss):
        _patch_dss("file_a.dss")
        from dsm2ui.dssui.dssui import DSSDataUIManager

        mgr = DSSDataUIManager("file_a.dss", filename_column="filename")
        df = mgr.get_data_catalog()
        missing = set(mgr.get_table_columns()) - set(df.columns)
        assert missing == set(), f"Columns {missing} not in catalog DataFrame"


class TestDSSDataUIManagerMultipleFiles:
    def test_file_num_in_columns(self, _patch_dss):
        # Same pathnames in both files — the real-world case
        _patch_dss("file_a.dss")
        _patch_dss("file_b.dss")
        from dsm2ui.dssui.dssui import DSSDataUIManager

        mgr = DSSDataUIManager("file_a.dss", "file_b.dss", filename_column="filename")
        assert mgr.display_fileno is True
        assert "FILE_NUM" in mgr.get_table_columns()

    def test_same_pathname_both_files_present(self, _patch_dss):
        """Same DSS pathname in two files must produce two separate catalog rows."""
        _patch_dss("file_a.dss")
        _patch_dss("file_b.dss")
        from dsm2ui.dssui.dssui import DSSDataUIManager

        mgr = DSSDataUIManager("file_a.dss", "file_b.dss", filename_column="filename")
        df = mgr.get_data_catalog()
        # 2 rows per file × 2 files = 4 total rows (no deduplication across files)
        assert len(df) == 4

    def test_file_num_in_catalog_df(self, _patch_dss):
        _patch_dss("file_a.dss")
        _patch_dss("file_b.dss")
        from dsm2ui.dssui.dssui import DSSDataUIManager

        mgr = DSSDataUIManager("file_a.dss", "file_b.dss", filename_column="filename")
        df = mgr.get_data_catalog()
        assert "FILE_NUM" in df.columns

    def test_table_columns_subset_of_catalog(self, _patch_dss):
        """Regression: all table columns must exist in the catalog DataFrame."""
        _patch_dss("file_a.dss")
        _patch_dss("file_b.dss")
        from dsm2ui.dssui.dssui import DSSDataUIManager

        mgr = DSSDataUIManager("file_a.dss", "file_b.dss", filename_column="filename")
        df = mgr.get_data_catalog()
        missing = set(mgr.get_table_columns()) - set(df.columns)
        assert missing == set(), f"Columns {missing} not in catalog DataFrame"

    def test_catalog_consistent_across_calls(self, _patch_dss):
        """Regression: second get_data_catalog() must still have FILE_NUM."""
        _patch_dss("file_a.dss")
        _patch_dss("file_b.dss")
        from dsm2ui.dssui.dssui import DSSDataUIManager

        mgr = DSSDataUIManager("file_a.dss", "file_b.dss", filename_column="filename")
        df1 = mgr.get_data_catalog()
        df2 = mgr.get_data_catalog()
        assert "FILE_NUM" in df1.columns
        assert "FILE_NUM" in df2.columns


class TestBuildPathname:
    def test_pathname_format(self, _patch_dss):
        _patch_dss("file_a.dss")
        from dsm2ui.dssui.dssui import DSSDataUIManager

        mgr = DSSDataUIManager("file_a.dss", filename_column="filename")
        row = pd.Series({"A": "AREA", "B": "STA001", "C": "EC", "E": "1HOUR", "F": "VER1"})
        assert mgr.build_pathname(row) == "/AREA/STA001/EC//1HOUR/VER1/"


# ---------------------------------------------------------------------------
# Tests — invalidate_all_caches wired through DSSDataUIManager
# ---------------------------------------------------------------------------


class TestDSSDataUIManagerCacheClear:
    def test_invalidate_all_caches_clears_refs(self, _patch_dss):
        _patch_dss("file_a.dss")
        from dsm2ui.dssui.dssui import DSSDataUIManager

        mgr = DSSDataUIManager("file_a.dss", filename_column="filename")
        # Warm cache on every ref
        for ref in mgr.data_catalog.list():
            ref.getData()
        assert any(ref._cached_data for ref in mgr.data_catalog.list())

        mgr.data_catalog.invalidate_all_caches()
        assert all(not ref._cached_data for ref in mgr.data_catalog.list())

    def test_invalidate_returns_catalog(self, _patch_dss):
        _patch_dss("file_a.dss")
        from dsm2ui.dssui.dssui import DSSDataUIManager

        mgr = DSSDataUIManager("file_a.dss", filename_column="filename")
        assert mgr.data_catalog.invalidate_all_caches() is mgr.data_catalog


# ---------------------------------------------------------------------------
# Tests — DSM2DSSReader standalone key (FILE attr uniquely identifies source)
# ---------------------------------------------------------------------------


class TestDSM2DSSReaderStandalone:
    def test_reader_uses_file_attribute(self):
        """DSM2DSSReader must forward FILE to locate the correct file."""
        from dsm2ui.dsm2ui import DSM2DSSReader
        from unittest.mock import patch, MagicMock

        reader = DSM2DSSReader()
        mock_df = pd.DataFrame({"value": [1.0, 2.0]})
        mock_df.attrs["unit"] = "cfs"
        mock_df.attrs["ptype"] = "inst-val"

        mock_ts = (mock_df, "cfs", "inst-val")
        with patch("dsm2ui.dsm2ui.dss.get_matching_ts", return_value=iter([mock_ts])) as mock_get:
            result = reader.load(FILE="study_a.dss", NAME="STA001", VARIABLE="FLOW")
            mock_get.assert_called_once()
            # Confirm FILE was forwarded (first positional arg)
            call_args = mock_get.call_args
            assert call_args[0][0] == "study_a.dss"

    def test_two_files_different_refs_via_build_catalog(self):
        """Entries for same NAME/VARIABLE in two different FILE values
        must both appear in the catalog (no silent deduplication)."""
        from dsm2ui.dsm2ui import DSM2DSSReader
        from dvue.catalog import build_catalog_from_dataframe, DataCatalog

        reader = DSM2DSSReader()
        rows = [
            {"FILE": "study_a.dss", "NAME": "STA001", "VARIABLE": "FLOW",
             "CHAN_NO": 1, "DISTANCE": 0.0, "INTERVAL": "15min", "PERIOD_OP": "INST"},
            {"FILE": "study_b.dss", "NAME": "STA001", "VARIABLE": "FLOW",
             "CHAN_NO": 1, "DISTANCE": 0.0, "INTERVAL": "15min", "PERIOD_OP": "INST"},
        ]
        df = pd.DataFrame(rows)

        def ref_name(row):
            return f'{row["FILE"]}::{row["NAME"]}/{row["VARIABLE"]}/{row["CHAN_NO"]}/{row["DISTANCE"]}'

        cat = build_catalog_from_dataframe(df, reader, ref_name)
        assert len(cat) == 2, "Both FILE variants must produce separate DataReferences"
