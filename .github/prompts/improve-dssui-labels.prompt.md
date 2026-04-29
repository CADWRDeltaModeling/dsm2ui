---
description: "Improve axis labels, legend entries, and titles in dssui and calibplot"
agent: "agent"
---

Implement the label improvements for `dssui` and `calibplot` as designed. Full plan context is in this file.

## Background

Labels in the DSS file browser (`dssui`) and calibration plots (`calibplot`) are too generic:
- Legends show `"value"` instead of station IDs
- Y-axis labels are raw DSS ALL-CAPS strings instead of title-cased readable labels
- X-axis has no "Time" label in calibplot time series
- Scatter plot y-axis shows `"Model"` with no units

## Phase 1 — `dsm2ui/dssui/dssui.py`

### Root cause
`DSSDataUIManager.create_curve`, `append_to_title_map`, `create_title` are **dead code** — they override the manager class, but `TimeSeriesPlotAction.create_curve` is what the framework actually calls. Without a `name`/`station_name`/`station_id` column in the DSS catalog, the default falls back to `"value"`.

### Fix

1. Add module-level `_smart_title(s)` helper:
   - Title-case only if `s.isupper() and len(s) > 2`
   - So `EC`, `CFS`, `DO` stay as-is; `FLOW → Flow`, `STAGE → Stage`

2. Create `DSSTimeSeriesPlotAction(TimeSeriesPlotAction)` (import `TimeSeriesPlotAction` from `dvue.tsdataui`):
   - Override `render(df, refs_and_data, manager)`:
     - Pre-compute `self._varying = {"B": bool, "C": bool, "A": bool, "F": bool}` by checking if each DSS path-part column has >1 unique value across `df`
     - Also set `self._multi_file` for multi-file detection
     - Then call `super().render(df, refs_and_data, manager)`
   - Override `create_curve(data, row, unit, file_index="")`:
     - **Legend**: include only varying parts — B if stations vary, F if studies vary, C if variables vary; fallback to B-part for single curve; append `[file_index]` if multi-file
     - **Y-axis**: `_smart_title(row["C"]) + " (" + unit + ")"` (omit parens if no unit)
     - **X-axis**: `"Time"` (fixed)
     - Use `.redim(value=label)` for legend consistency in overlays
     - `.opts(xlabel="Time", ylabel=ylabel, responsive=True, active_tools=["wheel_zoom"], tools=["hover"])`
   - Override `append_to_title_map(title_map, group_key, row)`:
     - Accumulate sets of B, C, A, F values per group_key
   - Override `create_title(v)`:
     - Format: `"B1, B2 (C) [A/F]"` — title-case C part via `_smart_title`

3. Add `_make_plot_action(self)` override on `DSSDataUIManager` → `return DSSTimeSeriesPlotAction()`

4. Mark old dead methods on `DSSDataUIManager` with comment:
   `# dead — override DSSTimeSeriesPlotAction instead`
   Keep `is_irregular` (used in `DSSReader.load`).

## Phase 2 — `dsm2ui/calib/calibplot.py`

5. Add same `_smart_title(s)` helper at module level.

6. `tsplot()`: add `xlabel="Time"` to the returned `hv.Overlay.opts(...)`.

7. `build_inst_plot()` + `build_godin_plot()` (two occurrences, plus a third in `build_scatter_plots`):
   - Wrap `vartype.name` with `_smart_title()` in `y_axis_label = f"..."`.

8. `build_scatter_plots()`:
   - Change `ylabel="Model"` → `ylabel="Model " + unit_string`
   - (`unit_string` is already computed earlier in the same function via `get_units(...)`)

## Key code patterns
- `hv.Curve(data.iloc[:, [0]], label=label).redim(value=label)` — `.redim` is essential for legend
- DSS catalog columns available in `row`: `A`, `B`, `C`, `D`, `E`, `F`, `filename`
- `unit` comes from `data.attrs.get("unit", "")` (set by `DSSReader.load`)
- Override `render()` (not `callback`) to pre-compute `_varying` — `render` receives the full `df` of selected rows

## Decisions
- `_smart_title`: title-case only when `s.isupper() and len(s) > 2`
- `_varying` stored on action instance before `super().render()` call — safe for single-user Panel session
- Dead methods on manager: keep with comment, don't delete (to avoid breaking subclassers)

## Verification
1. dssui multi-study overlay → legend shows `"RSAC155 [hist_fc_mss]"`, not `"value"`
2. dssui single curve → legend shows B-part (station ID) as fallback
3. Y-axis shows `"Flow (cfs)"` not `"FLOW (cfs)"`
4. calibplotui time series → x-axis shows `"Time"`; y-axis title-cased
5. calibplotui scatter → y-axis `"Model (CFS)"`, x-axis `"Observed (CFS)"`
6. `pytest tests/`

Implement the label improvements for `dssui` and `calibplot` as designed. Full plan context is in this file.

## Background

Labels in the DSS file browser (`dssui`) and calibration plots (`calibplot`) are too generic:
- Legends show `"value"` instead of station IDs
- Y-axis labels are raw DSS ALL-CAPS strings instead of title-cased readable labels
- X-axis has no "Time" label in calibplot time series
- Scatter plot y-axis shows `"Model"` with no units

## Phase 1 — `dsm2ui/dssui/dssui.py`

### Root cause
`DSSDataUIManager.create_curve`, `append_to_title_map`, `create_title` are **dead code** — they override the manager class, but `TimeSeriesPlotAction.create_curve` is what the framework actually calls. Without a `name`/`station_name`/`station_id` column in the DSS catalog, the default falls back to `"value"`.

### Fix

1. Add module-level `_smart_title(s)` helper:
   - Title-case only if `s.isupper() and len(s) > 2`
   - So `EC`, `CFS`, `DO` stay as-is; `FLOW → Flow`, `STAGE → Stage`

2. Create `DSSTimeSeriesPlotAction(TimeSeriesPlotAction)` (import `TimeSeriesPlotAction` from `dvue.tsdataui`):
   - Override `callback(df_selected, manager, time_range, **kwargs)`:
     - Pre-compute `self._varying = {"B": bool, "C": bool, "A": bool, "F": bool}` by checking if each DSS path-part column has >1 unique value across `df_selected`
     - Then call `super().callback(...)`
   - Override `create_curve(data, row, unit, file_index="")`:
     - **Legend**: include only varying parts — B if stations vary, F if studies vary, C if variables vary; fallback to B-part for single curve; prepend `file_index` if multi-file
     - **Y-axis**: `_smart_title(row["C"]) + " (" + unit + ")"` (omit parens if no unit)
     - **X-axis**: `"Time"` (fixed)
     - Use `.redim(value=label)` for legend consistency in overlays
     - `.opts(xlabel="Time", ylabel=ylabel, responsive=True, active_tools=["wheel_zoom"], tools=["hover"])`
   - Override `append_to_title_map(title_map, group_key, row)`:
     - Accumulate sets of B, C, A, F values per group_key
   - Override `create_title(v)`:
     - Format: `"B1, B2 (C) [A/F]"` — title-case C part via `_smart_title`

3. Add `_make_plot_action(self)` override on `DSSDataUIManager` → `return DSSTimeSeriesPlotAction()`

4. Mark old dead methods on `DSSDataUIManager` with comment:
   `# NOTE: not called — override DSSTimeSeriesPlotAction instead`
   Keep `is_irregular` (used in `DSSReader.load`).

## Phase 2 — `dsm2ui/calib/calibplot.py`

5. Add same `_smart_title(s)` helper at module level.

6. `tsplot()` (line ~77): add `xlabel="Time"` to the returned `hv.Overlay.opts(...)`.

7. `build_inst_plot()` (line ~881) + `build_godin_plot()` (line ~947):
   - Wrap `vartype.name` with `_smart_title()` in `y_axis_label = f"..."`.

8. `build_scatter_plots()` (line ~1092):
   - Change `ylabel="Model"` → `ylabel="Model " + unit_string`
   - (`unit_string` is already computed earlier in the same function via `get_units(...)`)

## Key code patterns
- `hv.Curve(data.iloc[:, [0]], label=label).redim(value=label)` — `.redim` is essential for legend
- DSS catalog columns available in `row`: `A`, `B`, `C`, `D`, `E`, `F`, `filename`
- `unit` comes from `data.attrs.get("unit", "")` (set by `DSSReader.load`)

## Decisions made
- `_smart_title`: title-case only when `s.isupper() and len(s) > 2`
- `_varying` stored on action instance before `super().callback()` call — safe for single-user Panel session
- Dead methods on manager: keep with comment, don't delete (to avoid breaking subclassers)

## Verification
1. dssui multi-study overlay → legend shows `"RSAC155 [hist_fc_mss]"`, not `"value"`
2. dssui single curve → legend shows B-part (station ID) as fallback
3. Y-axis shows `"Flow (cfs)"` not `"FLOW (cfs)"`
4. calibplotui time series → x-axis shows `"Time"`; y-axis title-cased
5. calibplotui scatter → y-axis `"Model (CFS)"`, x-axis `"Observed (CFS)"`
6. `pytest tests/`