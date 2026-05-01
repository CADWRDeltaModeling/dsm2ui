# dsm2ui — Copilot Instructions

**dsm2ui** is a Python toolkit for interactive visualization, post-processing, and automated calibration of **DSM2** (Delta Simulation Model II) — a 1D hydrodynamic/water-quality model of the Sacramento–San Joaquin Delta operated by California DWR.

See [README.md](../README.md) for a package overview and [README-calibrator.md](../README-calibrator.md) for full calibration workflow docs.

---

## Environment & Install

```bash
conda env create -f environment.yml --name dsm2ui
conda activate dsm2ui
pip install -e .
```

- Conda channel `cadwr-dms` is required for `pyhecdss`, `vtools3`, `pydsm`, `dms-datastore`.
- Dynamic versioning via `setuptools_scm` — do not hand-edit `dsm2ui/_version.py`.

```bash
pytest tests/
```
## Running tests
All tests are in the `tests/` directory. Run with `pytest`:

# Terminal And Environment Rules

- Use Command Prompt (`cmd`) for terminal commands in this repository.
- Before any project command, run `conda activate dsm2ui` first.
- Do not run project commands before environment activation.
- If the environment is unavailable, stop and ask for the correct environment setup instead of continuing in a different shell.

## Standard Command Sequence

```bat
conda activate dsm2ui
<project command here>
```

## Examples

```bat
conda activate dsm2ui
pytest tests/
```

```bat
conda activate dsm2ui
python -m dsm2ui.cli calib --help
```

---

## Architecture

| Module | Role |
|--------|------|
| `dsm2ui/calib/` | **Core active area**: YAML-driven calibration variation runner and optimizer |
| `dsm2ui/dsm2ui.py` | Interactive map + time-series viewer for DSM2 tidefiles |
| `dsm2ui/dssui/` | Generic HEC-DSS file browser and plotter |
| `dsm2ui/deltacdui/` | DeltaCD crop model netCDF viewer |
| `dsm2ui/ptm/` | PTM particle tracking animator |
| `dsm2ui/dsm2gis/` | Embedded channel/node GeoJSON for visualization |
| `dsm2ui/cli.py` | Click entry point (`dsm2ui` command); lazy-loads UI modules |

---

## CLI

```bash
dsm2ui calib setup   [--output calib_config.yml]   # write template config
dsm2ui calib run     --config <yml>                # run variation + compute metrics
dsm2ui calib optimize --config <yml> [--dry-run] [--skip-init]  # gradient/DE optimizer
```

---

## Calib Subpackage — Key Files

| File | Purpose |
|------|---------|
| `calib_run.py` | `setup_variation()`, `run_study()`, `compute_ec_metric()`, metric registry, `load_dss_ts()`, `plot_from_yaml()` |
| `calib_optimize.py` | `optimize()`, `ObjectiveEvaluator`, `EvalResult`, `OptimizationResult`, parallel gradient, Nelder-Mead / L-BFGS-B |
| `calib_cli.py` | Click CLI (`calib run`, `calib optimize`, `calib setup`) |
| `calibrator_design.md` | Design decisions, workflow diagram, file structure |
| `postpro_dsm2.py` | `process_model_data()`, `process_observed_data()`, `make_plots()` — JSON-config driven postprocessing |
| `calibplot.py` | `load_data_for_plotting()`, `build_calib_plot_template()` — per-station plot construction |
| `expression_eval.py` | `parse_expression_tokens()`, `eval_expression()` — safe arithmetic on DSS station series |

---

## Postpro / Calibration Plot System

The `dsm2ui calib postpro run` workflow is distinct from the optimizer workflow. It uses `pydsm.analysis.postpro` and a JSON config (not YAML).

### Key objects (from `pydsm.analysis.postpro`)
- `Study(name, dssfile)` — a named HEC-DSS file (model or observed)
- `Location(name, bpart, description, time_window_exclusion_list, threshold_value)` — one station row from a location CSV
  - `name` = `dsm2_id` column (model B-part or expression)
  - `bpart` = `obs_station_id` column (observed B-part or expression)
- `VarType(name, units)` — e.g. `VarType("FLOW", "cfs")`
- `PostProcessor(study, location, vartype)` — reads DSS, resamples, gap-fills, caches
- `PostProCache` — `diskcache`-backed store keyed by `/{BPART}/{CPART}/{EPART}/`

### Expression stations in location CSVs
`obs_station_id` or `dsm2_id` may contain arithmetic expressions instead of plain station IDs. Detection rule: if the field is not a plain identifier (`^[a-zA-Z_][a-zA-Z0-9_]*$`) it is treated as an expression. Examples: `-VCU`, `SDC-GES`, `RSAC128-RSAC123`.

- `_is_expression(s)` in `calibplot.py` performs the classification
- `_compute_expression(p, expr, study, vartype)` loads each token station, evaluates the expression, and populates `p.df/gdf/high/low/amp` on the PostProcessor
- The expression evaluator uses `dvue.math_reference._MATH_NAMESPACE` (safe namespace: numpy ufuncs, vtools filters, no builtins)
- Full user guide: [station-math-expressions-plan.md](../station-math-expressions-plan.md)

### Postpro config JSON structure
```json
{
  "location_files_dict":   {"FLOW": "./location_info/calibration_flow_stations.csv", ...},
  "observed_files_dict":   {"FLOW": "./observed_data/cdec/flow_merged.dss", ...},
  "study_files_dict":      {"StudyName": "./model_output/run.dss"},
  "vartype_dict":          {"FLOW": "cfs", "EC": "uS/cm", "STAGE": "feet"},
  "process_vartype_dict":  {"FLOW": true, "EC": false},
  "timewindow_dict":       {"default_timewindow": "hydro_calibration", "hydro_calibration": "01OCT2010 - 01OCT2012"},
  "inst_plot_timewindow_dict": {"FLOW": "2011-09-01:2011-09-30", "EC": null}
}
```
Generate a config: `dsm2ui calib postpro setup -s study/ -p postprocessing/ -o postpro_config.yml`

### Location CSV columns
`dsm2_id`, `obs_station_id`, `station_name`, `subtract` (use `no`), `time_window_exclusion_list`, `threshold_value`. Rows starting with `#` are skipped. Both `dsm2_id` and `obs_station_id` accept arithmetic expressions.

### Parallel plot generation
```bash
dsm2ui calib postpro run plots postpro_config.yml --workers 4
```
Uses `ProcessPoolExecutor`; each worker is an independent process with its own webdriver. Do not use `--workers` > number of physical cores.

### Cache invalidation
Set `process_vartype_dict → VARTYPE: true` to reprocess and overwrite cache. The `model` step clears model cache by default unless `--skip-cached` is passed.

---

## Critical Conventions

### Text-based channel patching — never use pydsm parser for CHANNEL writes
`apply_channel_modifications()` edits `channel_std_delta_grid.inp` with direct regex text replacement, **not** via `pydsm.input.parser.write_input`. Reason: the parser reformats floats (e.g. `0.00` → `0.0`) which breaks Fortran EOF parsing of XSECT_LAYER rows in later DSM2 sections.

### local_input/ directory
Variation studies create a `local_input/` subdirectory and patch `DSM2INPUTDIR` to point there. All `.inp` grid files are copied; the channel file is the only one modified.

### Always re-run hydro when DISPERSION changes
Due to a DSM2 bug, `DISPERSION` changes require hydro to recompute the advection matrix. Set `run_steps: [hydro, qual]` in the variation config — never omit hydro when changing dispersion.

### Godin filter warmup
Always start the model run ≥2 months before the metrics `timewindow` start. The Godin tidal filter (30 hr + 24 hr cascaded cosine Lanczos from `vtools3`) needs warmup data; starting the timewindow before data is available produces NaN metrics.

### pyhecdss logging noise
`pyhecdss` has a bug in `_respond_to_istat_state` that calls `logging.debug(msg, (RuntimeWarning,))` — the extra tuple arg causes `TypeError: not all arguments converted` in Python's logging when `%` formatting is triggered. Suppress it at startup:
```python
logging.getLogger("pyhecdss").setLevel(logging.WARNING)
logging.getLogger("matplotlib").setLevel(logging.WARNING)
```

### Objective metric
The optimizer minimises $\sum_i w_i (m_i - t_i)^2$. The metric is set via `metrics.objective_metric` in the YAML. Supported values and ideal targets:

| Key | Target | Notes |
|-----|--------|-------|
| `slope` | 1.0 | Default; linregress(obs→model) |
| `rmse`, `nrmse`, `mse`, `nmse` | 0.0 | Error magnitude |
| `nse`, `kge` | 1.0 | Nash-Sutcliffe / Kling-Gupta efficiency |
| `bias`, `nbias`, `pbias`, `rsr` | 0.0 | Systematic error / RSR |

### Optimizer methods
- `lbfgsb` — gradient-based; N+1 parallel DSM2 runs per iteration (parallel via `ThreadPoolExecutor`)
- `neldermead` — gradient-free; **sequential** (1 DSM2 run per call) — very slow for many groups
- `diffevol` — differential evolution; parallel population evaluations

### Windows file locks
DSM2 output files (HDF5, DSS) may remain locked by the model process. `_clear_eval_dir()` retries up to 12 times with 3s delay before proceeding — do not lower these values on Windows.

---

## Config YAML Schema (quick reference)

```yaml
base_run:
  study_dir: /path/to/historical      # base study with validated output
  modifier: hist_fc_mss               # DSM2MODIFIER used in base run
  model_dss_pattern: "{modifier}_qual.dss"
  batch_file: DSM2_batch.bat

dsm2_bin_dir: D:/delta/DSM2-8.5.0-win64/bin  # override DSM2 binary dir

variation:
  name: my_variation                  # becomes DSM2MODIFIER + output dir name
  study_dir: /path/to/variations/my_variation
  run_steps: [hydro, qual]            # always include hydro when DISPERSION changes
  envvar_overrides:                   # optional: shrink run window for speed
    START_DATE: 01DEC2014
    END_DATE: 30SEP2017
  channel_modifications:
    - name: group_a                   # human label used in logs & history CSV
      param: DISPERSION               # or MANNING
      channels: [10, 11, 12]         # list of int IDs, or Python regex string
      value: 500.0                    # ft²/s (DISPERSION) or — (MANNING)

observed_ec_dss: /path/to/ec_cal.dss
ec_stations_csv: /path/to/calibration_ec_stations.csv

active_stations: [RSAC075, RSAN007]  # subset of CSV dsm2_id; null = all

metrics:
  timewindow: "01DEC2014 - 30SEP2017"
  objective_metric: slope             # see table above

station_weights:
  RSAN007: 2.0                        # unlisted stations default to 1.0

optimizer:
  method: lbfgsb                      # lbfgsb | neldermead | diffevol
  max_model_runs: 100
  max_iter: 20
  no_improve_patience: 5
  no_improve_tol: 0.005
  finite_diff_rel_step: 0.05          # lbfgsb only
  max_workers: 8
  bounds: [50, 5000]                  # ft²/s, applied to all groups
```

Full reference: [README-calibrator.md](../README-calibrator.md)

---

## Primary Data Conventions

- **Time series**: `pandas.DataFrame` with `DatetimeIndex` (PST, no DST)
- **EC units**: µS/cm (electrical conductivity) — do not mix with salinity ppt
- **Length/DISPERSION**: US customary feet (ft²/s for dispersion)
- **MANNING**: dimensionless (typically 0.025–0.045)
- **DSS path**: `/A-PART/B-PART(=station)/C-PART(=EC)/dates/E-PART/F-PART/`
- Do not mix tz-aware and tz-naive timestamps
