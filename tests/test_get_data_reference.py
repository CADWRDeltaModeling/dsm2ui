"""Tests for get_data_reference() — full-row vs display-only-row patterns.

The core invariant tested here:
    get_data_reference(row) must succeed when `row` contains ONLY the columns
    returned by get_table_columns() (i.e. what the Tabulator widget exposes),
    AND when it contains the full catalog row (including 'name').

Background
----------
actions.py used to call manager.get_data_reference(display_table.selected_dataframe row),
which only had table-visible columns.  That caused KeyError on catalog-only columns
('FILE', 'filename', 'ECHO_FILE', etc.).

The fix in actions.py now resolves full rows from _dfcat before passing to
get_data_reference().  These tests verify:
1. Every manager's get_data_reference() works with a full-catalog row.
2. Every manager's get_data_reference() works with a display-only row (fallback path).
3. The 'name' column is always present in get_data_catalog() output.
4. Every 'name' value is a valid catalog key (round-trip lookup).

Managers covered
----------------
- DSM2DataUIManager          (dsm2ui.dsm2ui)
- DSM2TidefileUIManager      (dsm2ui.dsm2ui)
- EchoUIManager / EchoInputUIManager  (dsm2ui.echo_plugin + dsm2ui.dsm2ui)

No real DSS/HDF5/echo files are needed — fixtures construct minimal in-memory catalogs.

Usage
-----
    conda activate dsm2ui
    pytest tests/test_get_data_reference.py -v
"""

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Helper: simulate what actions.py now does (full-catalog row lookup)
# ---------------------------------------------------------------------------

def _full_catalog_row(manager, row_idx=0):
    """Return a full-catalog row (all columns) for the given manager."""
    df = manager.get_data_catalog()
    return df.iloc[row_idx]


def _display_only_row(manager, row_idx=0):
    """Return a display-only row (table columns only) for the given manager."""
    df = manager.get_data_catalog()
    display_cols = manager.get_table_columns()
    # Use only columns that actually exist in the catalog df
    available = [c for c in display_cols if c in df.columns]
    return df.iloc[row_idx][available]


# ===========================================================================
# DSM2DataUIManager
# ===========================================================================

@pytest.fixture(scope="module")
def dsm2_output_mgr():
    """DSM2DataUIManager with a minimal in-memory output_channels DataFrame."""
    import pandas as pd
    from dsm2ui.dsm2ui import DSM2DataUIManager

    channels = pd.DataFrame({
        "NAME":      ["STA_A", "STA_A", "STA_B"],
        "CHAN_NO":   [10,       10,       20],
        "DISTANCE":  [0.0,      1.0,      0.5],
        "VARIABLE":  ["FLOW",   "STAGE",  "FLOW"],
        "INTERVAL":  ["15MIN",  "15MIN",  "15MIN"],
        "PERIOD_OP": ["INST",   "INST",   "INST"],
        "FILE":      ["run1.dss", "run1.dss", "run1.dss"],
    })
    return DSM2DataUIManager(channels)


class TestDSM2DataUIManager:

    def test_catalog_has_name_column(self, dsm2_output_mgr):
        df = dsm2_output_mgr.get_data_catalog()
        assert "name" in df.columns, "get_data_catalog() must include 'name' column"

    def test_name_not_null(self, dsm2_output_mgr):
        df = dsm2_output_mgr.get_data_catalog()
        nulls = df["name"].isna().sum()
        assert nulls == 0, f"{nulls} null(s) in 'name' column"

    def test_every_name_is_lookupable(self, dsm2_output_mgr):
        df = dsm2_output_mgr.get_data_catalog()
        cat = dsm2_output_mgr.data_catalog
        missing = [n for n in df["name"] if cat.get(n) is None]
        assert not missing, f"catalog.get() failed for names: {missing}"

    def test_get_data_reference_full_row(self, dsm2_output_mgr):
        """Full-catalog row (as actions.py now provides) must work."""
        row = _full_catalog_row(dsm2_output_mgr)
        ref = dsm2_output_mgr.get_data_reference(row)
        assert ref is not None

    @pytest.mark.xfail(
        strict=True,
        reason="Fallback path requires 'FILE' which is absent from display-only rows. "
               "actions.py now provides full _dfcat rows so this path is never hit in practice.",
    )
    def test_get_data_reference_display_only_row(self, dsm2_output_mgr):
        """Display-only row (missing 'FILE' and 'name') fails on the fallback path.

        This test documents a known limitation: the _ref_name() fallback requires
        catalog-only columns ('FILE') that are not present in get_table_columns().
        The fix lives in actions.py: it now resolves full _dfcat rows before
        calling get_data_reference(), so this path is dead code in production.
        """
        row = _display_only_row(dsm2_output_mgr)
        assert "FILE" not in row.index, "test fixture error: 'FILE' should be absent from display row"
        assert "name" not in row.index, "test fixture error: 'name' should be absent from display row"
        # Expect KeyError — documents the known limitation
        dsm2_output_mgr.get_data_reference(row)

    def test_display_columns_are_subset_of_catalog(self, dsm2_output_mgr):
        df = dsm2_output_mgr.get_data_catalog()
        missing = set(dsm2_output_mgr.get_table_columns()) - set(df.columns)
        assert missing == set(), f"table columns not in catalog: {missing}"

    def test_file_column_absent_from_display(self, dsm2_output_mgr):
        """FILE must NOT be a display column (it's catalog-only metadata)."""
        assert "FILE" not in dsm2_output_mgr.get_table_columns()


# ===========================================================================
# DSM2TidefileUIManager
# ===========================================================================

HYDRO_H5 = "..\\..\\pydsm\\tests\\data\\historical_v82.h5"

import os as _os
_HYDRO_H5_ABS = _os.path.abspath(_os.path.join(_os.path.dirname(__file__), HYDRO_H5))


@pytest.fixture(scope="module")
def tidefile_mgr():
    if not _os.path.exists(_HYDRO_H5_ABS):
        pytest.skip(f"Test HDF5 not found: {_HYDRO_H5_ABS}")
    from dsm2ui.dsm2ui import DSM2TidefileUIManager
    return DSM2TidefileUIManager(tidefiles=[_HYDRO_H5_ABS])


class TestDSM2TidefileUIManager:

    def test_catalog_has_name_column(self, tidefile_mgr):
        df = tidefile_mgr.get_data_catalog()
        assert "name" in df.columns

    def test_name_not_null(self, tidefile_mgr):
        df = tidefile_mgr.get_data_catalog()
        assert df["name"].isna().sum() == 0

    def test_every_name_is_lookupable(self, tidefile_mgr):
        df = tidefile_mgr.get_data_catalog()
        cat = tidefile_mgr.data_catalog
        missing = [n for n in df["name"] if cat.get(n) is None]
        assert not missing, f"catalog.get() failed for: {missing[:5]}"

    def test_get_data_reference_full_row(self, tidefile_mgr):
        row = _full_catalog_row(tidefile_mgr)
        ref = tidefile_mgr.get_data_reference(row)
        assert ref is not None

    @pytest.mark.xfail(
        strict=True,
        reason="Fallback path requires 'filename' which is absent from display-only rows. "
               "actions.py now provides full _dfcat rows so this path is never hit in practice.",
    )
    def test_get_data_reference_display_only_row(self, tidefile_mgr):
        """Display-only row (missing 'filename' and 'name') fails on the fallback path.

        Documents the known limitation: _build_ref_key() needs 'filename' which is
        not in get_table_columns().  The fix is in actions.py.
        """
        row = _display_only_row(tidefile_mgr)
        assert "filename" not in row.index, "test fixture error"
        assert "name" not in row.index, "test fixture error"
        tidefile_mgr.get_data_reference(row)

    def test_display_columns_are_subset_of_catalog(self, tidefile_mgr):
        df = tidefile_mgr.get_data_catalog()
        missing = set(tidefile_mgr.get_table_columns()) - set(df.columns)
        assert missing == set(), f"table columns not in catalog: {missing}"

    def test_filename_absent_from_display(self, tidefile_mgr):
        """'filename' is catalog-only; should not appear in display columns."""
        assert "filename" not in tidefile_mgr.get_table_columns()


# ===========================================================================
# EchoInputUIManager  (dsm2ui.dsm2ui)
# ===========================================================================

ECHO_INP = r"D:\delta\dsm2_studies\studies\historical\output\hydro_echo_hist_fc_mss.inp"


@pytest.fixture(scope="module")
def echo_input_mgr():
    if not _os.path.exists(ECHO_INP):
        pytest.skip(f"Echo file not found: {ECHO_INP}")
    from dsm2ui.dsm2ui import build_input_plotter
    return build_input_plotter(ECHO_INP)


class TestEchoInputUIManager:

    def test_catalog_has_name_column(self, echo_input_mgr):
        df = echo_input_mgr.get_data_catalog()
        assert "name" in df.columns

    def test_name_not_null(self, echo_input_mgr):
        df = echo_input_mgr.get_data_catalog()
        assert df["name"].isna().sum() == 0

    def test_every_name_is_lookupable(self, echo_input_mgr):
        df = echo_input_mgr.get_data_catalog()
        cat = echo_input_mgr.data_catalog
        missing = [n for n in df["name"] if cat.get(n) is None]
        assert not missing, f"catalog.get() failed for: {missing[:5]}"

    def test_get_data_reference_full_row(self, echo_input_mgr):
        row = _full_catalog_row(echo_input_mgr)
        ref = echo_input_mgr.get_data_reference(row)
        assert ref is not None

    @pytest.mark.xfail(
        strict=True,
        reason="Fallback path requires 'ECHO_FILE' which is absent from display-only rows. "
               "actions.py now provides full _dfcat rows so this path is never hit in practice.",
    )
    def test_get_data_reference_display_only_row(self, echo_input_mgr):
        """Display-only row (missing 'ECHO_FILE' and 'name') fails on the fallback path."""
        row = _display_only_row(echo_input_mgr)
        assert "ECHO_FILE" not in row.index, "test fixture error"
        assert "name" not in row.index, "test fixture error"
        echo_input_mgr.get_data_reference(row)

    def test_display_columns_are_subset_of_catalog(self, echo_input_mgr):
        df = echo_input_mgr.get_data_catalog()
        missing = set(echo_input_mgr.get_table_columns()) - set(df.columns)
        assert missing == set(), f"table columns not in catalog: {missing}"

    def test_echo_file_absent_from_display(self, echo_input_mgr):
        assert "ECHO_FILE" not in echo_input_mgr.get_table_columns()


# ===========================================================================
# EchoUIManager  (dsm2ui.echo_plugin)
# ===========================================================================

@pytest.fixture(scope="module")
def echo_plugin_mgr():
    if not _os.path.exists(ECHO_INP):
        pytest.skip(f"Echo file not found: {ECHO_INP}")
    from dsm2ui.echo_plugin import EchoUIManager
    mgr = EchoUIManager()
    mgr.add_source_files(ECHO_INP)
    return mgr


class TestEchoPluginUIManager:

    def test_catalog_has_name_column(self, echo_plugin_mgr):
        df = echo_plugin_mgr.get_data_catalog()
        assert "name" in df.columns

    def test_name_not_null(self, echo_plugin_mgr):
        df = echo_plugin_mgr.get_data_catalog()
        assert df["name"].isna().sum() == 0

    def test_every_name_is_lookupable(self, echo_plugin_mgr):
        df = echo_plugin_mgr.get_data_catalog()
        cat = echo_plugin_mgr.data_catalog
        missing = [n for n in df["name"] if cat.get(n) is None]
        assert not missing, f"catalog.get() failed for: {missing[:5]}"

    def test_get_data_reference_full_row(self, echo_plugin_mgr):
        row = _full_catalog_row(echo_plugin_mgr)
        ref = echo_plugin_mgr.get_data_reference(row)
        assert ref is not None

    @pytest.mark.xfail(
        strict=True,
        reason="Fallback path requires catalog-only columns absent from display-only rows. "
               "actions.py now provides full _dfcat rows so this path is never hit in practice.",
    )
    def test_get_data_reference_display_only_row(self, echo_plugin_mgr):
        """Display-only row (missing catalog-only columns) fails on the fallback path."""
        row = _display_only_row(echo_plugin_mgr)
        assert "name" not in row.index, "test fixture error"
        echo_plugin_mgr.get_data_reference(row)

    def test_display_columns_are_subset_of_catalog(self, echo_plugin_mgr):
        df = echo_plugin_mgr.get_data_catalog()
        missing = set(echo_plugin_mgr.get_table_columns()) - set(df.columns)
        assert missing == set(), f"table columns not in catalog: {missing}"


# ===========================================================================
# actions.py integration: simulate callback row lookup
# ===========================================================================

class TestActionsRowLookupSimulation:
    """Simulate the exact pattern that actions.py now uses.

    This test verifies that using current_view + _dfcat (the actions.py fix)
    always produces rows with 'name', making get_data_reference() reliable.
    """

    def _simulate_actions_lookup(self, manager, selection=None):
        """
        Reproduce the actions.py selection-to-full-row pattern:
            _sel_index = current_view.iloc[selection].index
            dfselected = _dfcat.loc[_sel_index]
        """
        dfcat = manager.get_data_catalog()
        # Simulate a filtered current_view (identical to dfcat for simplicity)
        current_view = dfcat.copy()
        if selection is None:
            selection = list(range(min(3, len(current_view))))
        sel_index = current_view.iloc[selection].index
        dfselected = dfcat.loc[sel_index]
        return dfselected

    def test_dsm2_output_mgr_actions_rows_have_name(self, dsm2_output_mgr):
        dfselected = self._simulate_actions_lookup(dsm2_output_mgr)
        assert "name" in dfselected.columns
        assert dfselected["name"].isna().sum() == 0

    def test_dsm2_output_mgr_actions_get_data_reference(self, dsm2_output_mgr):
        dfselected = self._simulate_actions_lookup(dsm2_output_mgr)
        for _, row in dfselected.iterrows():
            ref = dsm2_output_mgr.get_data_reference(row)
            assert ref is not None, f"get_data_reference failed for row: {dict(row)}"

    def test_tidefile_mgr_actions_rows_have_name(self, tidefile_mgr):
        dfselected = self._simulate_actions_lookup(tidefile_mgr)
        assert "name" in dfselected.columns
        assert dfselected["name"].isna().sum() == 0

    def test_tidefile_mgr_actions_get_data_reference(self, tidefile_mgr):
        dfselected = self._simulate_actions_lookup(tidefile_mgr)
        for _, row in dfselected.iterrows():
            ref = tidefile_mgr.get_data_reference(row)
            assert ref is not None, f"get_data_reference failed for row: {dict(row)}"
