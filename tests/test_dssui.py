"""Tests for DSSDataUIManager source_num / table-column consistency.

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
        with patch("pyhecdss.DSSFile", side_effect=_factory):
            yield register


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestDSSDataUIManagerSingleFile:
    def test_no_source_num_in_columns(self, _patch_dss):
        _patch_dss("file_a.dss")
        from dsm2ui.dssui.dssui import DSSDataUIManager

        mgr = DSSDataUIManager("file_a.dss")
        df = mgr.get_data_catalog()
        assert "source_num" not in df.columns
        assert "source_num" not in mgr.get_table_columns()

    def test_table_columns_subset_of_catalog(self, _patch_dss):
        _patch_dss("file_a.dss")
        from dsm2ui.dssui.dssui import DSSDataUIManager

        mgr = DSSDataUIManager("file_a.dss")
        df = mgr.get_data_catalog()
        missing = set(mgr.get_table_columns()) - set(df.columns)
        assert missing == set(), f"Columns {missing} not in catalog DataFrame"


class TestDSSDataUIManagerMultipleFiles:
    def test_source_num_in_columns(self, _patch_dss):
        # Same pathnames in both files — the real-world case
        _patch_dss("file_a.dss")
        _patch_dss("file_b.dss")
        from dsm2ui.dssui.dssui import DSSDataUIManager

        mgr = DSSDataUIManager("file_a.dss", "file_b.dss")
        df = mgr.get_data_catalog()
        assert "source_num" in df.columns
        assert "source_num" in mgr.get_table_columns()

    def test_same_pathname_both_files_present(self, _patch_dss):
        """Same DSS pathname in two files must produce two separate catalog rows."""
        _patch_dss("file_a.dss")
        _patch_dss("file_b.dss")
        from dsm2ui.dssui.dssui import DSSDataUIManager

        mgr = DSSDataUIManager("file_a.dss", "file_b.dss")
        df = mgr.get_data_catalog()
        # 2 rows per file × 2 files = 4 total rows (no deduplication across files)
        assert len(df) == 4

    def test_source_num_in_catalog_df(self, _patch_dss):
        _patch_dss("file_a.dss")
        _patch_dss("file_b.dss")
        from dsm2ui.dssui.dssui import DSSDataUIManager

        mgr = DSSDataUIManager("file_a.dss", "file_b.dss")
        df = mgr.get_data_catalog()
        assert "source_num" in df.columns

    def test_table_columns_subset_of_catalog(self, _patch_dss):
        """Regression: all table columns must exist in the catalog DataFrame."""
        _patch_dss("file_a.dss")
        _patch_dss("file_b.dss")
        from dsm2ui.dssui.dssui import DSSDataUIManager

        mgr = DSSDataUIManager("file_a.dss", "file_b.dss")
        df = mgr.get_data_catalog()
        missing = set(mgr.get_table_columns()) - set(df.columns)
        assert missing == set(), f"Columns {missing} not in catalog DataFrame"

    def test_source_num_consistent_across_calls(self, _patch_dss):
        """Regression: second get_data_catalog() must still have source_num."""
        _patch_dss("file_a.dss")
        _patch_dss("file_b.dss")
        from dsm2ui.dssui.dssui import DSSDataUIManager

        mgr = DSSDataUIManager("file_a.dss", "file_b.dss")
        df1 = mgr.get_data_catalog()
        df2 = mgr.get_data_catalog()
        assert "source_num" in df1.columns
        assert "source_num" in df2.columns


class TestBuildPathname:
    def test_pathname_format(self, _patch_dss):
        _patch_dss("file_a.dss")
        from dsm2ui.dssui.dssui import DSSDataUIManager

        mgr = DSSDataUIManager("file_a.dss")
        row = pd.Series({"A": "AREA", "B": "STA001", "C": "EC", "E": "1HOUR", "F": "VER1"})
        assert mgr.build_pathname(row) == "/AREA/STA001/EC//1HOUR/VER1/"


# ---------------------------------------------------------------------------
# Tests — invalidate_all_caches wired through DSSDataUIManager
# ---------------------------------------------------------------------------


class TestDSSDataUIManagerCacheClear:
    def test_invalidate_all_caches_clears_refs(self, _patch_dss):
        _patch_dss("file_a.dss")
        from dsm2ui.dssui.dssui import DSSDataUIManager

        mgr = DSSDataUIManager("file_a.dss")
        # Warm cache on every ref
        for ref in mgr.data_catalog.list():
            ref.getData()
        assert any(ref._cached_data for ref in mgr.data_catalog.list())

        mgr.data_catalog.invalidate_all_caches()
        assert all(not ref._cached_data for ref in mgr.data_catalog.list())

    def test_invalidate_returns_catalog(self, _patch_dss):
        _patch_dss("file_a.dss")
        from dsm2ui.dssui.dssui import DSSDataUIManager

        mgr = DSSDataUIManager("file_a.dss")
        assert mgr.data_catalog.invalidate_all_caches() is mgr.data_catalog


# ---------------------------------------------------------------------------
# Tests — DSM2DSSReader standalone key (FILE attr uniquely identifies source)
# ---------------------------------------------------------------------------


class TestDSM2DSSReaderStandalone:
    def test_reader_uses_file_attribute(self):
        """DSM2DSSReader always reads from its constructed source path."""
        from dsm2ui.dsm2ui import DSM2DSSReader
        from unittest.mock import patch, MagicMock

        reader = DSM2DSSReader("study_a.dss")
        mock_df = pd.DataFrame({"value": [1.0, 2.0]}, index=pd.to_datetime(["2020-01-01", "2020-01-02"]))

        fh = MagicMock()
        fh.read_catalog.return_value = pd.DataFrame(
            [{"A": "AREA", "B": "STA001", "C": "FLOW", "D": "", "E": "1HOUR", "F": "VER1"}]
        )
        fh.get_pathnames.return_value = ["/AREA/STA001/FLOW//1HOUR/VER1/"]
        fh.read_rts.return_value = (mock_df, "cfs", "inst-val")

        with patch("pyhecdss.DSSFile", return_value=fh) as mock_dssfile:
            result = reader.load(FILE="study_b.dss", NAME="STA001", VARIABLE="FLOW")
            assert not result.empty
            # Unified reader uses self._source for handle resolution, not attributes["FILE"].
            mock_dssfile.assert_called_once_with("study_a.dss")

    def test_two_files_different_refs_via_build_catalog(self):
        """Entries for same NAME/VARIABLE in two different FILE values
        must both appear in the catalog (no silent deduplication).
        Uses reader=None (registry-based loading) since each file gets its
        own reader instance via ReaderRegistry at load time."""
        from dvue.catalog import build_catalog_from_dataframe, DataCatalog

        rows = [
            {"FILE": "study_a.dss", "NAME": "STA001", "VARIABLE": "FLOW",
             "CHAN_NO": 1, "DISTANCE": 0.0, "INTERVAL": "15min", "PERIOD_OP": "INST",
             "source": "study_a.dss"},
            {"FILE": "study_b.dss", "NAME": "STA001", "VARIABLE": "FLOW",
             "CHAN_NO": 1, "DISTANCE": 0.0, "INTERVAL": "15min", "PERIOD_OP": "INST",
             "source": "study_b.dss"},
        ]
        df = pd.DataFrame(rows)

        def ref_name(row):
            return f'{row["FILE"]}::{row["NAME"]}/{row["VARIABLE"]}/{row["CHAN_NO"]}/{row["DISTANCE"]}'

        cat = build_catalog_from_dataframe(df, None, ref_name)
        assert len(cat) == 2, "Both FILE variants must produce separate DataReferences"


class TestUnifiedDSSReaderScanModes:
    def test_dsm2_scan_filters_output_cparts(self):
        from dsm2ui.dsm2ui import DSM2DSSReader
        from unittest.mock import patch, MagicMock

        fh = MagicMock()
        fh.read_catalog.return_value = pd.DataFrame(
            [
                {"A": "AREA", "B": "STA001", "C": "FLOW", "D": "", "E": "1HOUR", "F": "VER1"},
                {"A": "AREA", "B": "STA001", "C": "AREA", "D": "", "E": "1HOUR", "F": "VER1"},
            ]
        )

        cm = MagicMock()
        cm.__enter__.return_value = fh
        cm.__exit__.return_value = False

        with patch("pyhecdss.DSSFile", return_value=cm):
            refs = DSM2DSSReader.scan("study.dss")

        assert len(refs) == 1
        assert refs[0]._attributes["VARIABLE"] == "FLOW"

    def test_generic_scan_keeps_all_cparts(self):
        from dsm2ui.dssui.dss_registry import DSSRegistryReader
        from unittest.mock import patch, MagicMock

        fh = MagicMock()
        fh.read_catalog.return_value = pd.DataFrame(
            [
                {"A": "AREA", "B": "STA001", "C": "FLOW", "D": "", "E": "1HOUR", "F": "VER1"},
                {"A": "AREA", "B": "STA001", "C": "AREA", "D": "", "E": "1HOUR", "F": "VER1"},
            ]
        )

        cm = MagicMock()
        cm.__enter__.return_value = fh
        cm.__exit__.return_value = False

        with patch("pyhecdss.DSSFile", return_value=cm):
            refs = DSSRegistryReader.scan("study.dss")

        assert len(refs) == 2


class TestDSSRegistryUIManagerSchema:
    """DSSRegistryUIManager owns explicit DSS A-F schema — verify it."""

    def _make_refs(self, path="hist.dss", extra_attrs=None):
        """Return scan-like DataReference list from a mock DSSFile."""
        from unittest.mock import MagicMock, patch
        from dsm2ui.dssui.dss_registry import DSSRegistryReader

        catalog_df = pd.DataFrame([
            {"A": "AREA", "B": "STA001", "C": "FLOW", "D": "01JAN2000-31DEC2000", "E": "1HOUR", "F": "VER1"},
            {"A": "AREA", "B": "STA002", "C": "EC",   "D": "01JAN2000-31DEC2000", "E": "1HOUR", "F": "VER1"},
        ])
        fh = MagicMock()
        fh.read_catalog.return_value = catalog_df
        fh.get_pathnames.return_value = []
        with patch("pyhecdss.DSSFile", return_value=fh):
            refs = DSSRegistryReader.scan(path)
        if extra_attrs:
            for ref in refs:
                for k, v in extra_attrs.items():
                    ref.set_attribute(k, v)
        return refs

    def _mgr_with_refs(self, refs):
        from dsm2ui.dssui.dss_registry import DSSRegistryUIManager
        mgr = DSSRegistryUIManager()
        for ref in refs:
            mgr._dvue_catalog.add(ref)
        return mgr

    def test_schema_required_columns_are_a_to_f(self):
        refs = self._make_refs()
        mgr = self._mgr_with_refs(refs)
        df = mgr.get_data_catalog()
        schema = mgr.get_table_schema(df)
        assert schema["required_columns"] == ["A", "B", "C", "D", "E", "F"]

    def test_schema_drop_if_all_null_is_true(self):
        refs = self._make_refs()
        mgr = self._mgr_with_refs(refs)
        df = mgr.get_data_catalog()
        schema = mgr.get_table_schema(df)
        assert schema["drop_if_all_null"] is True

    def test_schema_hidden_includes_ref_type_and_source(self):
        refs = self._make_refs()
        mgr = self._mgr_with_refs(refs)
        df = mgr.get_data_catalog()
        schema = mgr.get_table_schema(df)
        hidden = schema["hidden_by_default"]
        assert "ref_type" in hidden
        assert "source" in hidden

    def test_table_columns_contain_a_to_f(self):
        refs = self._make_refs()
        mgr = self._mgr_with_refs(refs)
        cols = mgr.get_table_columns()
        for col in ["A", "B", "C", "D", "E", "F"]:
            assert col in cols, f"Expected column {col!r} in table_columns"

    def test_all_null_optional_column_suppressed(self):
        """Optional columns that are entirely null/blank must be dropped."""
        refs = self._make_refs(extra_attrs={"station_id": None})
        mgr = self._mgr_with_refs(refs)
        cols = mgr.get_table_columns()
        assert "station_id" not in cols

    def test_undeclared_attribute_not_shown_even_if_populated(self):
        """Attributes not in the schema must NOT appear even when populated.

        DSSRegistryUIManager owns an explicit schema.  Only columns it
        declares (required or optional) are shown.  Arbitrary extra ref
        attributes (e.g. station_id) are invisible unless the schema
        explicitly lists them.
        """
        refs = self._make_refs(extra_attrs={"station_id": "cdec_001"})
        mgr = self._mgr_with_refs(refs)
        cols = mgr.get_table_columns()
        # station_id is not in the DSS schema — must stay hidden
        assert "station_id" not in cols
        # but the core A-F columns must still be present
        for col in ["A", "B", "C", "D", "E", "F"]:
            assert col in cols

    def test_table_filters_cover_a_b_c_e_f(self):
        refs = self._make_refs()
        mgr = self._mgr_with_refs(refs)
        filters = mgr.get_table_filters()
        for col in ["A", "B", "C", "E", "F"]:
            assert col in filters, f"Expected filter for {col!r}"

    def test_column_widths_explicit_for_a_b_c_e_f(self):
        refs = self._make_refs()
        mgr = self._mgr_with_refs(refs)
        widths = mgr.get_table_column_width_map()
        for col in ["A", "B", "C", "E", "F"]:
            assert col in widths, f"Expected width for {col!r}"
        # A-specific explicit width from schema
        assert widths["A"] == "14%"
        assert widths["B"] == "14%"


class TestUnifiedDSSReaderLoad:
    def test_regular_load_honors_time_window(self):
        from dsm2ui.dssui.dss_registry import DSSRegistryReader
        from unittest.mock import patch, MagicMock

        fh = MagicMock()
        fh.read_catalog.return_value = pd.DataFrame(
            [{"A": "AREA", "B": "STA001", "C": "FLOW", "D": "", "E": "1HOUR", "F": "VER1"}]
        )
        fh.get_pathnames.return_value = ["/AREA/STA001/FLOW//1HOUR/VER1/"]
        out_df = pd.DataFrame(
            {"FLOW": [1.0, 2.0]},
            index=pd.to_datetime(["2020-01-01", "2020-01-02"]),
        )
        fh.read_rts.return_value = (out_df, "CFS", "inst-val")

        with patch("pyhecdss.DSSFile", return_value=fh):
            reader = DSSRegistryReader("study.dss")
            df = reader.load(
                pathname="/AREA/STA001/FLOW//1HOUR/VER1/",
                time_range=("2020-01-01", "2020-01-10"),
            )

        assert not df.empty
        fh.read_rts.assert_called_once_with(
            "/AREA/STA001/FLOW//1HOUR/VER1/", "2020-01-01", "2020-01-10"
        )

    def test_irregular_load_uses_read_its(self):
        from dsm2ui.dssui.dss_registry import DSSRegistryReader
        from unittest.mock import patch, MagicMock

        fh = MagicMock()
        fh.read_catalog.return_value = pd.DataFrame(
            [{"A": "AREA", "B": "STA001", "C": "FLOW", "D": "", "E": "IR-DAY", "F": "VER1"}]
        )
        fh.get_pathnames.return_value = ["/AREA/STA001/FLOW//IR-DAY/VER1/"]
        out_df = pd.DataFrame(
            {"FLOW": [1.0]},
            index=pd.to_datetime(["2020-01-01"]),
        )
        fh.read_its.return_value = (out_df, "CFS", "inst-val")

        with patch("pyhecdss.DSSFile", return_value=fh):
            reader = DSSRegistryReader("study.dss")
            df = reader.load(pathname="/AREA/STA001/FLOW//IR-DAY/VER1/")

        assert not df.empty
        fh.read_its.assert_called_once_with(
            "/AREA/STA001/FLOW//IR-DAY/VER1/", "1753-01-01", "2200-12-31"
        )
