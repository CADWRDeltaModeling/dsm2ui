"""Tests for DSM2TidefileUIManager — initial-load vs drag-and-drop load paths.

Run headless: no Panel server required.  The tests exercise catalog-building
and display-DataFrame logic only; no Panel widgets are created.

Usage::

    conda activate dsm2ui
    pytest tests/test_tidefile_ui_manager.py -v
"""
import os

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Shared fixture path
# ---------------------------------------------------------------------------

HYDRO_H5 = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "pydsm",
        "tests",
        "data",
        "historical_v82.h5",
    )
)

# Columns the Tabulator must render without NaN.
_REQUIRED_COLUMNS = ["geoid", "id", "variable", "unit", "source"]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def h5_path():
    if not os.path.exists(HYDRO_H5):
        pytest.skip(f"Test HDF5 fixture not found: {HYDRO_H5}")
    return HYDRO_H5


@pytest.fixture(scope="module")
def manager_initial(h5_path):
    """Manager pre-loaded via the __init__ (initial-load) path."""
    from dsm2ui.dsm2ui import DSM2TidefileUIManager

    return DSM2TidefileUIManager(tidefiles=[h5_path])


@pytest.fixture(scope="module")
def manager_dnd(h5_path):
    """Manager started empty, then populated via add_source_files (DnD path)."""
    from dsm2ui.dsm2ui import DSM2TidefileUIManager

    mgr = DSM2TidefileUIManager(tidefiles=[])
    mgr.add_source_files(h5_path)
    return mgr


# ---------------------------------------------------------------------------
# Parity between initial-load and DnD-load paths
# ---------------------------------------------------------------------------


class TestLoadPathParity:
    """Both loading routes must produce an equivalent display DataFrame."""

    def test_initial_not_empty(self, manager_initial):
        assert not manager_initial.get_data_catalog().empty

    def test_dnd_not_empty(self, manager_dnd):
        assert not manager_dnd.get_data_catalog().empty

    def test_same_row_count(self, manager_initial, manager_dnd):
        n_init = len(manager_initial.get_data_catalog())
        n_dnd = len(manager_dnd.get_data_catalog())
        assert n_init == n_dnd, f"row counts differ: initial={n_init}, dnd={n_dnd}"

    def test_both_have_required_columns(self, manager_initial, manager_dnd):
        df_init = manager_initial.get_data_catalog()
        df_dnd = manager_dnd.get_data_catalog()
        for col in _REQUIRED_COLUMNS:
            assert col in df_init.columns, f"initial-load path missing column: {col}"
            assert col in df_dnd.columns, f"dnd-load path missing column: {col}"

    def test_both_have_name_column(self, manager_initial, manager_dnd):
        assert "name" in manager_initial.get_data_catalog().columns
        assert "name" in manager_dnd.get_data_catalog().columns

    def test_same_name_values(self, manager_initial, manager_dnd):
        names_init = set(manager_initial.get_data_catalog()["name"])
        names_dnd = set(manager_dnd.get_data_catalog()["name"])
        only_init = names_init - names_dnd
        only_dnd = names_dnd - names_init
        assert not only_init and not only_dnd, (
            f"Name sets differ.  Only in initial: {list(only_init)[:5]!r}.  "
            f"Only in dnd: {list(only_dnd)[:5]!r}."
        )

    def test_same_variables(self, manager_initial, manager_dnd):
        vars_init = set(manager_initial.get_data_catalog()["variable"].unique())
        vars_dnd = set(manager_dnd.get_data_catalog()["variable"].unique())
        assert vars_init == vars_dnd, f"variable sets differ: {vars_init} vs {vars_dnd}"


# ---------------------------------------------------------------------------
# No NaN in key display columns (parametrised over column names)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("col", _REQUIRED_COLUMNS)
class TestNoNaNsInKeyColumns:
    """Each required column must be fully populated in both loading paths."""

    def test_initial_path_no_nans(self, manager_initial, col):
        df = manager_initial.get_data_catalog()
        nan_count = int(df[col].isna().sum())
        assert nan_count == 0, (
            f"initial-load path: {nan_count} NaN(s) in column '{col}'"
        )

    def test_dnd_path_no_nans(self, manager_dnd, col):
        df = manager_dnd.get_data_catalog()
        nan_count = int(df[col].isna().sum())
        assert nan_count == 0, (
            f"dnd-load path: {nan_count} NaN(s) in column '{col}'"
        )


# ---------------------------------------------------------------------------
# add_source_files idempotency and edge cases
# ---------------------------------------------------------------------------


class TestAddSourceFilesIdempotency:

    def test_duplicate_add_is_skipped(self, h5_path):
        from dsm2ui.dsm2ui import DSM2TidefileUIManager

        mgr = DSM2TidefileUIManager(tidefiles=[])
        result1 = mgr.add_source_files(h5_path)
        n_after_first = len(mgr.get_data_catalog())

        result2 = mgr.add_source_files(h5_path)
        n_after_second = len(mgr.get_data_catalog())

        assert result1, "first add should return a non-empty list"
        assert result2 == [], "second add of the same file should be skipped"
        assert n_after_first == n_after_second, "row count must not grow on duplicate add"

    def test_unsupported_extension_is_skipped(self, tmp_path):
        from dsm2ui.dsm2ui import DSM2TidefileUIManager

        bad = tmp_path / "not_an_hdf5.txt"
        bad.write_text("definitely not hdf5")
        mgr = DSM2TidefileUIManager(tidefiles=[])
        result = mgr.add_source_files(str(bad))
        assert result == [], "unsupported file extension should be skipped"
        assert mgr.get_data_catalog().empty, "catalog should remain empty"

    def test_nonexistent_file_is_skipped(self, tmp_path):
        from dsm2ui.dsm2ui import DSM2TidefileUIManager

        mgr = DSM2TidefileUIManager(tidefiles=[])
        result = mgr.add_source_files(str(tmp_path / "ghost.h5"))
        assert result == [], "non-existent file should be skipped"
        assert mgr.get_data_catalog().empty


# ---------------------------------------------------------------------------
# Catalog / DataReference integrity
# ---------------------------------------------------------------------------


class TestCatalogIntegrity:

    def test_ref_count_matches_display_rows_initial(self, manager_initial):
        n_refs = len(manager_initial.data_catalog)
        n_rows = len(manager_initial.get_data_catalog())
        assert n_refs == n_rows, (
            f"initial: catalog has {n_refs} refs but display df has {n_rows} rows"
        )

    def test_ref_count_matches_display_rows_dnd(self, manager_dnd):
        n_refs = len(manager_dnd.data_catalog)
        n_rows = len(manager_dnd.get_data_catalog())
        assert n_refs == n_rows, (
            f"dnd: catalog has {n_refs} refs but display df has {n_rows} rows"
        )

    def test_every_name_is_lookupable_initial(self, manager_initial):
        cat = manager_initial.data_catalog
        df = manager_initial.get_data_catalog()
        assert "name" in df.columns
        missing = [n for n in df["name"] if cat.get(n) is None]
        assert not missing, f"names not found in catalog: {missing[:5]}"

    def test_every_name_is_lookupable_dnd(self, manager_dnd):
        cat = manager_dnd.data_catalog
        df = manager_dnd.get_data_catalog()
        assert "name" in df.columns
        missing = [n for n in df["name"] if cat.get(n) is None]
        assert not missing, f"names not found in catalog: {missing[:5]}"

    def test_get_data_catalog_is_idempotent(self, manager_initial):
        """Calling get_data_catalog() twice returns same object when catalog unchanged."""
        df1 = manager_initial.get_data_catalog()
        df2 = manager_initial.get_data_catalog()
        assert df1 is df2, "expected same cached object on repeated calls"


# ---------------------------------------------------------------------------
# What the Tabulator actually receives — end-to-end display slice
# ---------------------------------------------------------------------------


class TestTabulatorSlice:
    """Verify the exact DataFrame slice that _refresh_table sends to the Tabulator."""

    def _table_slice(self, manager):
        """Simulate what _refresh_table does: reindex to get_table_columns()."""
        df = manager.get_data_catalog()
        cols = manager.get_table_columns()
        return df.reindex(columns=cols)

    def test_initial_tabulator_slice_no_nans(self, manager_initial):
        sliced = self._table_slice(manager_initial)
        for col in sliced.columns:
            nan_count = int(sliced[col].isna().sum())
            assert nan_count == 0, (
                f"initial Tabulator slice: {nan_count} NaN(s) in column '{col}'"
            )

    def test_dnd_tabulator_slice_no_nans(self, manager_dnd):
        sliced = self._table_slice(manager_dnd)
        for col in sliced.columns:
            nan_count = int(sliced[col].isna().sum())
            assert nan_count == 0, (
                f"dnd Tabulator slice: {nan_count} NaN(s) in column '{col}'"
            )

    def test_table_columns_same_for_both_paths(self, manager_initial, manager_dnd):
        cols_init = manager_initial.get_table_columns()
        cols_dnd = manager_dnd.get_table_columns()
        assert cols_init == cols_dnd, (
            f"get_table_columns() differs: initial={cols_init}, dnd={cols_dnd}"
        )

    def test_tabulator_slice_identical_content(self, manager_initial, manager_dnd):
        """Both paths must deliver the same data to the Tabulator."""
        s_init = self._table_slice(manager_initial).sort_values("id").reset_index(drop=True)
        s_dnd = self._table_slice(manager_dnd).sort_values("id").reset_index(drop=True)
        # Columns must be identical
        assert list(s_init.columns) == list(s_dnd.columns), (
            f"column mismatch: {list(s_init.columns)} vs {list(s_dnd.columns)}"
        )
        # Values must match
        mismatches = []
        for col in s_init.columns:
            if not s_init[col].equals(s_dnd[col]):
                diff = (~(s_init[col] == s_dnd[col])).sum()
                mismatches.append(f"{col}: {diff} rows differ")
        assert not mismatches, "Tabulator slice content differs:\n" + "\n".join(mismatches)

    def test_dnd_display_dfcat_columns_debug(self, manager_initial, manager_dnd):
        """Diagnostic: print column differences between both paths (always passes)."""
        df_init = manager_initial.get_data_catalog()
        df_dnd = manager_dnd.get_data_catalog()
        only_init = set(df_init.columns) - set(df_dnd.columns)
        only_dnd = set(df_dnd.columns) - set(df_init.columns)
        print(f"\ninitial _display_dfcat columns ({len(df_init.columns)}): {sorted(df_init.columns)}")
        print(f"dnd _display_dfcat columns ({len(df_dnd.columns)}): {sorted(df_dnd.columns)}")
        if only_init:
            print(f"ONLY in initial: {sorted(only_init)}")
        if only_dnd:
            print(f"ONLY in dnd: {sorted(only_dnd)}")
        # Print NaN counts for shared columns
        for col in sorted(set(df_init.columns) & set(df_dnd.columns)):
            n_init = int(df_init[col].isna().sum())
            n_dnd = int(df_dnd[col].isna().sum())
            if n_init or n_dnd:
                print(f"  NaN in '{col}': initial={n_init}, dnd={n_dnd}")
        # This test is diagnostic only — always passes
        assert True


# ---------------------------------------------------------------------------
# Production-file tests — real tidefile from the study directory
# ---------------------------------------------------------------------------

PROD_H5 = r"D:\delta\dsm2_studies\studies\historical\output\hist_fc_mss.h5"


@pytest.fixture(scope="module")
def prod_h5_path():
    if not os.path.exists(PROD_H5):
        pytest.skip(f"Production HDF5 not found: {PROD_H5}")
    return PROD_H5


@pytest.fixture(scope="module")
def prod_manager_initial(prod_h5_path):
    """Initial-load path against the production tidefile."""
    from dsm2ui.dsm2ui import DSM2TidefileUIManager

    return DSM2TidefileUIManager(tidefiles=[prod_h5_path])


@pytest.fixture(scope="module")
def prod_manager_dnd(prod_h5_path):
    """DnD path against the production tidefile."""
    from dsm2ui.dsm2ui import DSM2TidefileUIManager

    mgr = DSM2TidefileUIManager(tidefiles=[])
    mgr.add_source_files(prod_h5_path)
    return mgr


class TestProductionFile:
    """Parity and NaN checks against the real production tidefile."""

    def _table_slice(self, manager):
        df = manager.get_data_catalog()
        cols = manager.get_table_columns()
        return df.reindex(columns=cols)

    # --- basic non-empty checks ---

    def test_initial_not_empty(self, prod_manager_initial):
        assert not prod_manager_initial.get_data_catalog().empty

    def test_dnd_not_empty(self, prod_manager_dnd):
        assert not prod_manager_dnd.get_data_catalog().empty

    # --- row / column parity ---

    def test_same_row_count(self, prod_manager_initial, prod_manager_dnd):
        n_init = len(prod_manager_initial.get_data_catalog())
        n_dnd = len(prod_manager_dnd.get_data_catalog())
        assert n_init == n_dnd, f"row counts differ: initial={n_init}, dnd={n_dnd}"

    def test_same_columns(self, prod_manager_initial, prod_manager_dnd):
        cols_init = sorted(prod_manager_initial.get_data_catalog().columns)
        cols_dnd = sorted(prod_manager_dnd.get_data_catalog().columns)
        assert cols_init == cols_dnd, (
            f"column sets differ.\n  only initial: {sorted(set(cols_init)-set(cols_dnd))}\n"
            f"  only dnd:     {sorted(set(cols_dnd)-set(cols_init))}"
        )

    def test_table_columns_same(self, prod_manager_initial, prod_manager_dnd):
        tc_init = prod_manager_initial.get_table_columns()
        tc_dnd = prod_manager_dnd.get_table_columns()
        assert tc_init == tc_dnd, (
            f"get_table_columns() differs: initial={tc_init}, dnd={tc_dnd}"
        )

    # --- no NaN in Tabulator-visible columns ---

    @pytest.mark.parametrize("col", _REQUIRED_COLUMNS)
    def test_initial_tabulator_no_nans(self, prod_manager_initial, col):
        sliced = self._table_slice(prod_manager_initial)
        assert col in sliced.columns, f"column '{col}' missing from initial Tabulator slice"
        nan_count = int(sliced[col].isna().sum())
        assert nan_count == 0, f"initial: {nan_count} NaN(s) in Tabulator column '{col}'"

    @pytest.mark.parametrize("col", _REQUIRED_COLUMNS)
    def test_dnd_tabulator_no_nans(self, prod_manager_dnd, col):
        sliced = self._table_slice(prod_manager_dnd)
        assert col in sliced.columns, f"column '{col}' missing from dnd Tabulator slice"
        nan_count = int(sliced[col].isna().sum())
        assert nan_count == 0, f"dnd: {nan_count} NaN(s) in Tabulator column '{col}'"

    # --- diagnostic: always passes, prints everything ---

    def test_columns_debug(self, prod_manager_initial, prod_manager_dnd):
        """Print full column/NaN diagnostic for the production file (always passes)."""
        df_init = prod_manager_initial.get_data_catalog()
        df_dnd = prod_manager_dnd.get_data_catalog()
        only_init = set(df_init.columns) - set(df_dnd.columns)
        only_dnd = set(df_dnd.columns) - set(df_init.columns)
        print(f"\n[prod] initial columns ({len(df_init.columns)}): {sorted(df_init.columns)}")
        print(f"[prod] dnd     columns ({len(df_dnd.columns)}): {sorted(df_dnd.columns)}")
        if only_init:
            print(f"[prod] ONLY in initial: {sorted(only_init)}")
        if only_dnd:
            print(f"[prod] ONLY in dnd:     {sorted(only_dnd)}")
        tc_init = prod_manager_initial.get_table_columns()
        tc_dnd = prod_manager_dnd.get_table_columns()
        print(f"[prod] initial get_table_columns(): {tc_init}")
        print(f"[prod] dnd     get_table_columns(): {tc_dnd}")
        for col in sorted(set(df_init.columns) & set(df_dnd.columns)):
            n_init = int(df_init[col].isna().sum())
            n_dnd = int(df_dnd[col].isna().sum())
            if n_init or n_dnd:
                print(f"  NaN in '{col}': initial={n_init}, dnd={n_dnd}")
        # Show Tabulator-slice NaN counts
        s_init = self._table_slice(prod_manager_initial)
        s_dnd = self._table_slice(prod_manager_dnd)
        for col in s_init.columns:
            n_i = int(s_init[col].isna().sum())
            n_d = int(s_dnd[col].isna().sum()) if col in s_dnd.columns else "MISSING"
            if n_i or n_d:
                print(f"  Tabulator NaN in '{col}': initial={n_i}, dnd={n_d}")
        assert True
