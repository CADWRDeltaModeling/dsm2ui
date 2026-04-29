# Mixed DataReference Loading — Implementation Plan

## Goal

Ensure every `DataReference` subclass in dsm2ui can be loaded from a
heterogeneous (`mixed`) catalog by standardising on **catalog `name` as the
universal row-to-reference identity**, rather than requiring each manager to
reconstruct a private key from manager-specific visible columns.

The base dvue loading path (`TimeSeriesDataUIManager.get_data()` →
`get_data_reference(row).getData()`) already handles mixed reader types.  The
risk is concentrated in the six dsm2ui manager subclasses that override
`get_data_reference()`.

---

## Design Contract

**Source of truth for rows:** `DataUIManager._dfcat` is always populated from
`get_data_catalog()` which calls `catalog.to_dataframe().reset_index()`.  The
`reset_index()` call promotes the catalog name to a regular `"name"` column, so
**every row passed to `get_data_reference(row)` carries `name` in `row.index`
during normal Panel operation.**

**Lookup priority:**
1. If `"name"` is present in the row → use `catalog.get(row["name"])`
2. Fallback → reconstruct the private key (backward compatibility)

This means existing homogeneous catalogs keep working unchanged, and mixed
catalogs can be resolved without teaching every manager every other catalog
schema.

---

## Manager-by-Manager Checklist

### 1. `DSM2DataUIManager` — `dsm2ui/dsm2ui/dsm2ui.py`

| Task | Status |
|------|--------|
| `get_data_reference()` — add `name`-first lookup | ✅ done |
| Canonical attrs (`station_name`, lowercase `variable`) already added in `__init__` | ✅ existing |
| `_ref_name()` kept as stable key formula for construction and fallback | ✅ done |

**Required row columns for fallback:** `FILE`, `NAME`, `VARIABLE`, `CHAN_NO`, `DISTANCE`

---

### 2. `DSM2TidefileUIManager` — `dsm2ui/dsm2ui/dsm2ui.py`

| Task | Status |
|------|--------|
| `get_data_reference()` — add `name`-first lookup | ✅ done |
| Canonical `station_name` already added in `__init__` | ✅ existing |
| `_build_ref_key()` kept as stable fallback | ✅ done |

**Required row columns for fallback:** `filename`, `id`, `variable`

---

### 3. `DSSDataUIManager` — `dsm2ui/dsm2ui/dssui/dssui.py`

| Task | Status |
|------|--------|
| `get_data_reference()` — add `name`-first lookup | ✅ done |
| `build_ref_key()` kept as stable fallback | ✅ done |

**Required row columns for fallback:** `filename`, `A`, `B`, `C`, `E`, `F`

---

### 4. `DeltaCDNodesUIManager` — `dsm2ui/dsm2ui/deltacdui/deltacdui.py`

| Task | Status |
|------|--------|
| `get_data_reference()` — add `name`-first lookup | ✅ done |
| `_ref_name()` kept as stable fallback | ✅ done |

**Required row columns for fallback:** `source`, `node`, `variable`

---

### 5. `DeltaCDAreaUIManager` — `dsm2ui/dsm2ui/deltacdui/deltacduimgr.py`

| Task | Status |
|------|--------|
| `get_data_reference()` — add `name`-first lookup | ✅ done |
| `_build_dvue_catalog()` migrated to `build_catalog_from_dataframe()` | ✅ done |
| `_ref_name()` kept as stable fallback | ✅ done |

**Required row columns for fallback:** `source`, `area_id`, `variable`, `crop` (optional)

---

### 6. `CalibPlotUIManager` — `dsm2ui/dsm2ui/calib/calibplotui.py`

| Task | Status |
|------|--------|
| `get_data_reference()` — add `name`-first lookup | ✅ done |
| `_ref_name()` static method extracted from inline formula | ✅ done |
| Manual `_build_dvue_catalog()` loop kept (needs `cache=False` for `CalibDataReference`) | ✅ kept |

**Required row columns for fallback:** `Name`, `vartype`

---

## Tests Added

File: `dsm2ui/tests/test_mixed_catalog.py`

| Test | Coverage |
|------|----------|
| `test_name_lookup_preferred_for_dsm2dss` | DSM2DataUIManager uses name when present |
| `test_fallback_to_reconstructed_key_when_name_absent` | DSM2DataUIManager falls back correctly |
| `test_name_lookup_preferred_for_tidefile` | DSM2TidefileUIManager uses name when present |
| `test_name_lookup_preferred_for_dss` | DSSDataUIManager uses name when present |
| `test_name_lookup_preferred_for_deltacd_nodes` | DeltaCDNodesUIManager uses name when present |
| `test_name_lookup_preferred_for_deltacd_area` | DeltaCDAreaUIManager uses name when present |
| `test_name_lookup_preferred_for_calib` | CalibPlotUIManager uses name when present |
| `test_heterogeneous_catalog_all_rows_resolve` | All ref_types in one catalog resolve by name |

---

## Relevant Files

| File | Role |
|------|------|
| `dvue/dvue/dataui.py` | `DataUIManager._dfcat` source; `get_data_reference()` base contract |
| `dvue/dvue/tsdataui.py` | Mixed-catalog-friendly loading in `TimeSeriesDataUIManager.get_data()` |
| `dvue/dvue/catalog.py` | `DataReference`, `build_catalog_from_dataframe()`, lazy reader path |
| `dvue/dvue/math_reference.py` | `MathDataReference` for derived entries in mixed catalogs |
| `dsm2ui/dsm2ui/dsm2ui.py` | `DSM2DataUIManager`, `DSM2TidefileUIManager` |
| `dsm2ui/dsm2ui/dssui/dssui.py` | `DSSDataUIManager` |
| `dsm2ui/dsm2ui/deltacdui/deltacdui.py` | `DeltaCDNodesUIManager` |
| `dsm2ui/dsm2ui/deltacdui/deltacduimgr.py` | `DeltaCDAreaUIManager` |
| `dsm2ui/dsm2ui/calib/calibplotui.py` | `CalibPlotUIManager`, `CalibDataReference`, `CalibNullReader` |

---

## Decisions

- **Scope:** dsm2ui integration change only; dvue catalog semantics unchanged.
- **Identity strategy:** `name` as universal key; manager-specific key reconstruction as
  backward-compatible fallback only.
- **`cache=False` for CalibDataReference:** `build_catalog_from_dataframe()` hardcodes
  `cache=True`; CalibPlotUIManager keeps a manual loop to preserve `cache=False`.
- **Not included:** new user-facing mixed-catalog authoring UI, catalog serialization
  redesign, changes to plotting behavior.

---

## Future Considerations

1. If `build_catalog_from_dataframe()` is extended with a `cache` parameter in dvue,
   `CalibPlotUIManager._build_dvue_catalog()` can be simplified.
2. If mixed catalogs must be persisted, add an end-to-end YAML round-trip test and verify
   each subclass's reader FQCN is importable and its required attributes are all serialised.
3. If a future mixed catalog combines manager families in one table, define a minimal shared
   metadata contract early: `name`, `ref_type`, `source`, `station_name`, `variable`.
