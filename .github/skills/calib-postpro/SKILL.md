---
name: calib-postpro
description: >
  Use when working on DSM2 calibration post-processing plots, the postpro JSON config,
  location CSV station files, expression stations (computed flows), PostProcessor /
  PostProCache internals, calibplot.py, postpro_dsm2.py, or the `dsm2ui calib postpro`
  CLI commands.
---

# DSM2 Calibration Post-Processing — Agent Skill

## What This System Does

`dsm2ui calib postpro` generates HTML/PNG comparison plots and metric tables for each
station in a location CSV, comparing DSM2 model output (HEC-DSS) against observed field
data (HEC-DSS). It is **separate** from the optimizer (`calib run` / `calib optimize`).

Full user guide: [station-math-expressions-plan.md](../../station-math-expressions-plan.md)  
Developer summary: [station-math-expressions.md](../station-math-expressions.md)

---

## Key Files

| File | Role |
|------|------|
| `dsm2ui/calib/postpro_dsm2.py` | `process_model_data()`, `process_observed_data()`, `make_plots()` — top-level orchestration |
| `dsm2ui/calib/calibplot.py` | `load_data_for_plotting()`, `build_calib_plot_template()`, `_compute_expression()`, `_is_expression()` |
| `dsm2ui/calib/expression_eval.py` | `parse_expression_tokens()`, `eval_expression()` — safe arithmetic on DSS series |
| `pydsm/analysis/postpro.py` | `Location`, `Study`, `VarType`, `PostProcessor`, `PostProCache` |

---

## Config JSON → Location CSV → DSS

The pipeline is wired by a **JSON config** (not YAML):

```
postpro_config.json
  location_files_dict[VARTYPE]  →  location CSV (one row = one station)
  observed_files_dict[VARTYPE]  →  observed HEC-DSS file
  study_files_dict[StudyName]   →  model HEC-DSS file
  timewindow_dict               →  named time windows
  process_vartype_dict          →  true = reprocess (clear cache)
```

Generate with: `dsm2ui calib postpro setup -s study/ -p postprocessing/ -o config.yml`

---

## Location CSV

Required columns: `dsm2_id`, `obs_station_id`, `station_name`, `subtract`, `time_window_exclusion_list`, `threshold_value`.  
Rows starting with `#` are skipped.

- `dsm2_id` → loaded as model DSS B-part (or evaluated as expression)
- `obs_station_id` → loaded as observed DSS B-part (or evaluated as expression)

---

## Expression Stations

If either `dsm2_id` or `obs_station_id` is **not a plain identifier**
(`^[a-zA-Z_][a-zA-Z0-9_]*$`), it is an arithmetic expression.

```csv
VCU,-VCU,...           # observed: negate VCU series
SDC-GES,SDC-GES,...    # both: SDC minus GES
RSAC128-RSAC123,SDC-GES,...  # model: RSAC128-RSAC123; observed: SDC-GES
```

Detection: `_is_expression(s)` in `calibplot.py`.  
Execution: `_compute_expression(p, expr, study, vartype)` in `calibplot.py`.  
Evaluator: `eval_expression(expr, series_map)` in `expression_eval.py`, uses
`dvue.math_reference._MATH_NAMESPACE` (numpy ufuncs + vtools filters; no builtins).

---

## PostProcessor / Cache

`PostProcessor(study, location, vartype)` reads DSS, resamples to 15-min, gap-fills,
runs Godin filter, computes tidal high/low, stores to `PostProCache`.

Cache key: `/{BPART}/{CPART}/{EPART}/` (uppercase). Backed by `diskcache`.

`load_data_for_plotting()` in `calibplot.py`:
1. Detects if `location.bpart` (observed) or `location.name` (model) is an expression
2. Expression path: try `load_processed()` → compute → `store_processed()` → `load_processed()`
3. Standard path: `load_processed()` → `process()` → `store_processed()`

---

## CLI Commands

```bash
# Process observed data (run once per observed DSS change)
dsm2ui calib postpro run observed postpro_config.yml

# Process model output (clears model cache by default)
dsm2ui calib postpro run model postpro_config.yml
dsm2ui calib postpro run model postpro_config.yml --skip-cached  # reuse cache

# Generate plots
dsm2ui calib postpro run plots postpro_config.yml
dsm2ui calib postpro run plots postpro_config.yml --workers 4  # parallel

# Interactive viewer
dsm2ui calib ui plot postpro_config.yml
```

`--workers` uses `ProcessPoolExecutor`. Do not exceed physical core count.

---

## Adding a New Expression Station

1. Confirm all component B-parts exist in both model and observed DSS.
2. Add a row to the location CSV: `dsm2_id=A-B`, `obs_station_id=A-B`, `station_name=...`
3. Re-run `model` and `plots` steps.
4. If one side differs: `dsm2_id=MODEL_EXPR`, `obs_station_id=OBS_EXPR`.

---

## Common Pitfalls

- **Cache stale after DSS change**: set `process_vartype_dict → VARTYPE: true` or delete
  the diskcache directory.
- **Expression token not found in DSS**: `_compute_expression` logs a warning and skips
  the station — check B-part names match exactly (case-insensitive in DSS but exact in CSV).
- **Godin filter warmup**: ensure the DSS data extends ≥2 months before the plot
  timewindow; insufficient warmup produces NaN in filtered output.
- **`pyhecdss` logging noise**: suppress with
  `logging.getLogger("pyhecdss").setLevel(logging.WARNING)` at startup.
