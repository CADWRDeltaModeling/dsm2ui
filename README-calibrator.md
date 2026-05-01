# DSM2 Calibrator

A YAML-driven workflow for running DSM2 calibration variations, comparing tidal-filtered EC metrics against observations, generating diagnostic plots, and optionally running a gradient-based parameter optimizer.

---

## Table of Contents

1. [Overview](#overview)
2. [Quick Start](#quick-start)
3. [Configuration File](#configuration-file)
4. [CLI Reference](#cli-reference)
5. [Output Files](#output-files)
6. [Diagnostic Plots](#diagnostic-plots)
7. [Metrics](#metrics)
8. [Optimizer](#optimizer)
9. [Developer Reference](#developer-reference)

---

## Overview

The calibrator automates the following workflow:

1. Copy a **base study** into a new **variation study directory**
2. Apply **channel parameter changes** (DISPERSION or MANNING) to the copied channel input file
3. Patch `config.inp` (modifier name, input directory, run dates)
4. Execute DSM2 (hydro + qual, or a subset of modules)
5. Compute **Godin-filtered EC regression slopes** (model vs observed) for selected stations
6. Write a results table, per-station CSV, and diagnostic PNG plots

Everything is controlled by a single YAML file (`calib_config.yml`). No manual editing of DSM2 input files is required after the initial setup.

---

## Quick Start

### 1. Generate a template config

```bash
dsm2ui calib setup --output calib_config.yml
```

### 2. Edit `calib_config.yml`

Set the paths to your base study, observed EC DSS file, station CSV, and define your channel modification groups.

### 3. Run the calibration variation

```bash
dsm2ui calib run --config calib_config.yml
```

### 4. Regenerate plots without re-running the model

```bash
dsm2ui calib run --config calib_config.yml --plot
```

### 5. Run the gradient-based optimizer

```bash
dsm2ui calib optimize --config calib_config.yml
```

---

## Configuration File

```yaml
base_run:
  # Directory of the pre-existing, already-completed base study.
  study_dir: /path/to/base/study
  # DSM2MODIFIER value used by the base study.
  modifier: base_modifier
  # DSS output filename pattern; {modifier} is substituted at runtime.
  model_dss_pattern: "{modifier}_qual.dss"
  # Batch file name relative to study_dir.
  batch_file: DSM2_batch.bat

# Path to the DSM2 binary directory.
# Required when the batch file's relative binary paths point to an
# incompatible DSM2 version.
dsm2_bin_dir: /path/to/DSM2-8.5.0-win64/bin

variation:
  # Name used as DSM2MODIFIER and as the output file prefix.
  name: my_variation
  # Directory to create for this variation run.
  study_dir: /path/to/variations/my_variation
  # DSM2 modules to run. Hydro must be included whenever DISPERSION changes.
  run_steps: [hydro, qual]

  # Optional: override ENVVAR values in config.inp for this variation only.
  # Useful for restricting the run period to the metrics window + warmup.
  envvar_overrides:
    START_DATE: 01OCT2014       # 2 months before metrics window (Godin warmup)
    QUAL_START_DATE: 02OCT2014
    END_DATE: 30SEP2017

  channel_modifications:
    # One entry per named group.  Later groups override earlier ones for
    # any overlapping channel IDs.
    - name: group_a
      param: DISPERSION           # MANNING or DISPERSION
      channels: [10, 11, 12]      # list of CHAN_NO integers, or a Python regex string
      value: 500.0                # ft²/s for DISPERSION; dimensionless for MANNING
    - name: group_b
      param: DISPERSION
      channels: [20, 21]
      value: 1000.0

# Observed EC DSS file (pyhecdss-readable).
observed_ec_dss: /path/to/observed_data/ec_cal.dss

# CSV mapping station names to DSM2 output B-parts, channel numbers, and distances.
ec_stations_csv: /path/to/location_info/calibration_ec_stations.csv

# Stations to include in metrics (DSM2 output B-part / dsm2_id column in CSV).
# Omit or set to null to use all stations in the CSV.
active_stations:
  - RSAC075
  - RSAC081
  - RSAN007

metrics:
  # Metrics evaluation window.  The model run starts 2 months earlier for
  # Godin filter warmup; those 2 months are excluded from the metrics computation.
  timewindow: "01DEC2014 - 30SEP2017"

# Station weights for the optimizer objective function.
# Stations not listed default to 1.0.
station_weights:
  RSAN007: 2.0
  RSAC081: 1.5
  RSAC075: 1.0

# Optimizer settings (used by: dsm2ui calib optimize).
# See the Optimizer section below for details.
optimizer:
  max_model_runs: 100
  max_iter: 20
  no_improve_patience: 5
  no_improve_tol: 0.005
  finite_diff_rel_step: 0.05
  max_workers: 8
  bounds: [50, 5000]
  scratch_dir: null   # auto: <var_study_dir>_optim_scratch/
  best_dir: null      # auto: <var_study_dir>_optim_best/
```

### Channel modifications: lists vs regex

`channels` accepts either:
- A YAML list of integer channel numbers: `[10, 11, 200]`
- A Python regex string matched against `CHAN_NO` as text: `"^4[0-9]$"`

Multiple groups are applied in order. If the same channel appears in two groups, the last group wins.

### `envvar_overrides`

Keys must exactly match the ENVVAR names in `config.inp`. Common uses are shortening the run period to only the metrics window (plus 2-month Godin warmup) and adjusting constituent start dates. The base study is never modified.

---

## CLI Reference

All commands are available under the `dsm2ui calib` group.

### `dsm2ui calib setup`

Write a template `calib_config.yml` to get started.

```
dsm2ui calib setup [--output calib_config.yml] [--force]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--output`, `-o` | `calib_config.yml` | Destination file path |
| `--force` | false | Overwrite if file already exists |

---

### `dsm2ui calib run`

Run a calibration variation: set up the study directory, execute the model, compute EC slope metrics, and (optionally) generate plots.

```
dsm2ui calib run --config calib_config.yml [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--config PATH` | Path to the YAML config file (default: `calib_config.yml`) |
| `--setup-only` | Create the variation directory and batch file, then stop |
| `--run-base` | Re-execute the base study before computing base slopes |
| `--metrics-only` | Recompute slopes from existing output without running the model |
| `--plot` | Generate per-station diagnostic PNGs from existing output |
| `--log-file PATH` | Override the model output log path (default: `<var_dir>/run.log`) |
| `--log-level LEVEL` | `DEBUG \| INFO \| WARNING \| ERROR` (default: `INFO`) |

**Typical workflow:**

```bash
# 1. Inspect the generated config (dates, binary paths, channel values)
dsm2ui calib run --config calib_config.yml --setup-only

# 2. Run the model
dsm2ui calib run --config calib_config.yml

# 3. Regenerate plots if needed (no model re-run)
dsm2ui calib run --config calib_config.yml --plot

# 4. Monitor while running
Get-Content <var_study_dir>/run.log -Tail 10 -Wait
```

---

### `dsm2ui calib optimize`

Run the gradient-based optimizer to minimize EC slope deviation from 1.0 across the active stations.

```
dsm2ui calib optimize --config calib_config.yml [--dry-run]
```

| Option | Description |
|--------|-------------|
| `--config PATH` | Path to the YAML config file |
| `--dry-run` | Evaluate the starting point only; skip the optimization loop |
| `--log-level LEVEL` | `DEBUG \| INFO \| WARNING \| ERROR` (default: `INFO`) |

See the [Optimizer](#optimizer) section for details.

---

### `dsm2ui calib checklist`

Run a DSM2 calibration checklist step (resample observed data, extract model output, or generate plots).

```
dsm2ui calib checklist PROCESS_NAME JSON_CONFIG_FILE
```

`PROCESS_NAME` is one of: `resample`, `extract`, `plot`.

---

### `dsm2ui calib postpro run`

Run a DSM2 post-processing step for calibration output.

```
dsm2ui calib postpro run PROCESS_NAME JSON_CONFIG_FILE [OPTIONS]
```

`PROCESS_NAME` is one of: `observed`, `model`, `plots`, `heatmaps`, `validation_bar_charts`, `copy_plot_files`.

| Option | Default | Description |
|--------|---------|-------------|
| `--dask / --no-dask` | `--no-dask` | Run post-processing steps with Dask |
| `--skip-cached` | false | Use the existing post-processing cache instead of clearing and recomputing (applies to `model` and `plots`; by default the cache is cleared on each run) |

```bash
dsm2ui calib postpro run observed calib_config.json
dsm2ui calib postpro run model calib_config.json         # clears cache, reprocesses all
dsm2ui calib postpro run model calib_config.json --skip-cached  # reuses existing cache
dsm2ui calib postpro run plots calib_config.json         # clears cache, regenerates all plots
dsm2ui calib postpro run plots calib_config.json --skip-cached  # reuses existing cache
```

---

### `dsm2ui calib postpro setup`

Auto-generate a calibration UI YAML config by introspecting real study folders and a postprocessing folder.

```
dsm2ui calib postpro setup --study DIR --postprocessing DIR --output FILE [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--study`, `-s` | *(required, repeatable)* | Study folder path (repeat for multiple studies) |
| `--postprocessing`, `-p` | *(required)* | Postprocessing folder (must contain `location_info/` and `observed_data/`) |
| `--output`, `-o` | *(required)* | Output YAML config file path |
| `--module`, `-m` | `hydro` | DSM2 module whose DSS output to reference (`hydro`, `qual`, `gtm`) |
| `--output-folder` | `./plots/` | Plot output folder written into the YAML |

```bash
dsm2ui calib postpro setup \
  -s studies/historical/ \
  -s studies/my_variation/ \
  -p postprocessing/ \
  -o calib_ui.yml
```

---

### `dsm2ui calib ui plot`

Launch the interactive calibration plot viewer from a YAML config.

```
dsm2ui calib ui plot CONFIG_FILE [--base_dir DIR]
```

| Option | Description |
|--------|-------------|
| `CONFIG_FILE` | YAML config produced by `calib postpro setup` |
| `--base_dir DIR` | Base directory for relative paths in the config (default: config file directory) |

```bash
dsm2ui calib ui plot calib_ui.yml
```

---

### `dsm2ui calib ui heatmap`

Show a geographic heatmap of calibration metrics.

```
dsm2ui calib ui heatmap SUMMARY_FILE STATION_LOCATION_FILE [--metric NMSE]
```

| Argument / Option | Description |
|-------------------|-------------|
| `SUMMARY_FILE` | CSV with columns: `DSM2 Run`, `Location`, metric columns (e.g. `NMSE`, `RMSE`) |
| `STATION_LOCATION_FILE` | CSV with columns: `dsm2_id`, `lat`, `lon`, `utm_easting`, `utm_northing` |
| `--metric` | Metric column name to visualize (default: `NMSE`) |

```bash
dsm2ui calib ui heatmap metrics_summary.csv station_locs.csv --metric NMSE
```

---

## Output Files

After a successful run, the variation study directory contains:

| Path | Description |
|------|-------------|
| `calib_config.yml` | Snapshot of the config used to create this run |
| `config.inp` | Patched copy of the base `config.inp` |
| `DSM2_batch_var.bat` | Filtered and patched batch file used to run the model |
| `run.log` | Full model stdout/stderr |
| `results.txt` | Human-readable EC slope comparison table |
| `slopes_<modifier>.csv` | Machine-readable slope table |
| `local_input/` | Modified channel file and copies of all other grid input files |
| `output/` | Model DSS output, HDF5 tidefile, echo files |
| `plots/<bpart>.png` | Per-station diagnostic figures |

---

## Diagnostic Plots

Each active station produces one PNG at `<var_study_dir>/plots/<bpart>.png`:

```
┌──────────────────────────────────────────────────┐
│  Time series (Godin-filtered)                    │
│  — Observed (black)                              │
│  — Base run (blue)                               │
│  — Variation (orange-red)                        │
├───────────────────────┬──────────────────────────┤
│  Scatter + regression │  Channel modifications   │
│  model vs observed    │  applied to this run     │
│  slope and R²         │  (group / param /        │
│  annotations with 1:1 │   channels / value)      │
└───────────────────────┴──────────────────────────┘
```

The scatter panel subsamples to ≤ 3 000 points for rendering speed; regression lines are computed on the full time series.

---

## Metrics

The primary metric is the **Godin-filtered EC linear regression slope** with the observed record as the independent variable:

$$\text{EC}_\text{model}(t) = \text{slope} \times \text{EC}_\text{observed}(t) + \text{intercept}$$

A slope of **1.0** indicates perfect proportional agreement. Values above 1.0 indicate the model over-predicts salinity intrusion; below 1.0 it under-predicts.

The results table reports:

| Column | Meaning |
|--------|---------|
| `slope_base` | Regression slope for the base run |
| `slope_<modifier>` | Regression slope for the variation |
| `r_sq_base` / `r_sq_<modifier>` | R² for each run |
| `delta_slope` | `slope_var − slope_base` |
| `pct_change_slope` | `100 × delta / |slope_base|` |

Rows are sorted by `|delta_slope|` descending so the stations most affected by the variation appear first.

---

## Optimizer

The optimizer uses **L-BFGS-B** (scipy) to find dispersion (or Manning) values that minimize the weighted sum of squared EC slope deviations from 1.0:

$$f(\mathbf{x}) = \sum_i w_i \left( \text{slope}_i(\mathbf{x}) - 1 \right)^2$$

Each call to $f(\mathbf{x})$ is a full DSM2 hydro + qual run.

### Gradient computation

Gradients are estimated by **forward finite differences**. For each channel group $i$:

$$\frac{\partial f}{\partial x_i} \approx \frac{f(\mathbf{x} + h_i \mathbf{e}_i) - f(\mathbf{x})}{h_i}, \qquad h_i = \max\!\bigl(\texttt{rel\_step} \times |x_i|,\ 10\bigr)$$

The $N$ perturbed evaluations (one per channel group) run in **parallel** up to `max_workers` concurrent DSM2 processes. Setting `max_workers` equal to or greater than the number of channel groups maximizes throughput.

### Optimizer directory layout

Eval directories are placed as siblings of the base study (same level as `studies/historical/`) so that relative paths in `config.inp` resolve correctly:

```
studies/
  historical/                       ← base study (untouched)
  my_variation_optim_eval_base/     ← base-point evaluation (reused each iteration)
  my_variation_optim_eval_p0/       ← perturbation for group 0
  my_variation_optim_eval_p1/       ← perturbation for group 1
  ...
  my_variation_optim_scratch/       ← metadata only (history CSV)
  my_variation_optim_best/          ← copy of the best result found so far
```

### Optimizer output

All results are written to `<var_study_dir>_optim_best/`:

| File | Description |
|------|-------------|
| `optim_history.csv` | Objective, parameter values, and slopes at every iteration |
| `results.txt` | Slope comparison table from the best run |
| `calib_config_optimized.yml` | Ready-to-use warm-start config for the next optimizer pass |
| `output/` | Model DSS and HDF5 from the best run |
| `plots/` | Per-station diagnostic PNGs from the best run |

### Warm-starting

After an initial optimization pass, run the next pass using the optimized config:

```bash
dsm2ui calib optimize --config <best_dir>/calib_config_optimized.yml
```

### Key optimizer YAML settings

| Key | Default | Description |
|-----|---------|-------------|
| `max_model_runs` | 100 | Hard budget: total model evaluations |
| `max_iter` | 20 | L-BFGS-B iteration limit |
| `no_improve_patience` | 5 | Stop if no improvement for this many gradient steps |
| `no_improve_tol` | 0.005 | Minimum improvement threshold |
| `finite_diff_rel_step` | 0.05 | Finite-difference step as fraction of current value |
| `max_workers` | 8 | Parallel DSM2 processes (safe to set ≥ number of groups) |
| `bounds` | [50, 5000] | Global [min, max] for all parameter groups (ft²/s) |
| `bounds_overrides` | — | Per-group bound overrides: `{group_name: [lo, hi]}` |

---

## Developer Reference

### File map

```
dsm2ui/calib/
    calib_cli.py          — Click command group (dsm2ui calib run/optimize/setup/checklist/postpro/ui)
    calib_run.py          — Core library: setup, run, metrics, plots
    calib_optimize.py     — Gradient-based optimizer
    calib_config_builder.py — Auto-build calib-ui YAML from study folders (dsm2ui calib postpro setup)
    calibplotui.py        — Interactive plot viewer (dsm2ui calib ui plot)
    geoheatmap.py         — Geographic metrics heatmap (dsm2ui calib ui heatmap)
    postpro_dsm2.py       — Post-processing steps (dsm2ui calib postpro run)
    checklist_dsm2.py     — Checklist steps (dsm2ui calib checklist)
    calib_config.yml      — Example/working configuration
    calibrator_design.md  — Internal design notes
```

The `calib` group is registered in `dsm2ui/cli.py` via:

```python
from dsm2ui.calib.calib_cli import calib
main.add_command(calib)
```

The standalone scripts `run_calib.py` and `run_optimize.py` remain as thin
wrappers around the same library functions for users who prefer direct invocation
without installing the package entry point.

---

### Key design decisions

#### Text-based channel file patching

`apply_channel_modifications()` edits the `CHANNEL` table in-place as raw text.
The `pydsm` parser/writer route is not used because it reformats all float
values, including the XSECT_LAYER table. DSM2's Fortran reader interprets an
abbreviated row as an EOF, causing an immediate fatal error. Text-based patching
changes only the target CHANNEL data lines and leaves all XSECT_LAYER lines
byte-for-byte identical.

#### `local_input/` directory for DSM2INPUTDIR

The `GRID` section of `hydro.inp` references files as `${DSM2INPUTDIR}/filename`.
DSM2 requires this `${ENVVAR}/filename` form — bare filenames and absolute
paths both fail. Rather than modifying `hydro.inp`, `setup_variation()` creates
a `local_input/` subdirectory in the variation study, copies all grid `.inp`
files there (including the modified channel file), and redirects `DSM2INPUTDIR`
in `config.inp` to point at it.

#### Eval directories at the base study level

The optimizer places all eval directories as siblings of `studies/historical/`
(i.e., directly under `studies/`). This ensures relative paths in `config.inp`
— such as paths to timeseries or boundary condition files — resolve identically
to a normal variation run, regardless of any deeper nesting of scratch
directories.

#### TEMPDIR auto-fix

If `config.inp` contains a `TEMPDIR` path that does not exist on the current
machine, DSM2 exits immediately. `_ensure_tempdir()` detects this and silently
redirects to the system temp directory on every `setup_variation()` call.

#### Concurrency safety

Multiple calibration runs pointing at different config files and study
directories are safe to run simultaneously. There is no `os.chdir()`, no shared
global state, and no shared temp files. Observed and base model DSS files are
opened read-only; HEC-DSS 6 supports concurrent readers without file locking.

---

### Public API (`calib_run.py`)

#### Data classes

| Class | Description |
|-------|-------------|
| `ChannelParamModification` | One named group: parameter type, channel list or regex, target value |
| `ECLocation` | Station pairing: `station_name`, `model_bpart`, `obs_bpart` |

#### Core functions

| Function | Description |
|----------|-------------|
| `load_yaml_config(path)` | Load and validate a `calib_config.yml` |
| `apply_channel_modifications(file, mods)` | Text-based in-place CHANNEL table edit |
| `setup_variation(base_dir, var_dir, ...)` | Create and configure the variation study directory |
| `run_study(batch, cwd, log_file)` | Execute a DSM2 batch file; stream output to log |
| `load_dss_ts(dss_file, b_part, c_part)` | Load a time series from a DSS file |
| `compute_ec_slopes(model_dss, obs_dss, locations, timewindow)` | Godin filter + linear regression |
| `compare_slopes(base_df, var_df, ...)` | Build delta-slope comparison DataFrame |
| `plot_station_results(...)` | Generate a per-station diagnostic PNG |
| `run_from_yaml(yaml_path, ...)` | End-to-end orchestration (YAML-driven) |
| `plot_from_yaml(yaml_path)` | Generate plots from existing output (no model run) |
| `read_ec_locations_csv(csv_path, ...)` | Build an `ECLocation` list from the station CSV |

---

### Dependencies

| Package | Role |
|---------|------|
| `pyhecdss` | Read/write HEC-DSS files |
| `vtools3` | Godin tidal filter |
| `pandas` | Time series and DataFrame operations |
| `scipy` | Linear regression and L-BFGS-B optimizer |
| `numpy` | Numerical array operations |
| `matplotlib` | Plot generation (required for `--plot` and `optimize`) |
| `pyyaml` | YAML config loading |

---

### Extension points

**Adding a new metric:** Add a function alongside `compute_ec_slopes()` returning a DataFrame with the same station index. Call it in `run_calibration_variation()` and thread it through `run_from_yaml()` and the CLI.

**Other constituents (temperature, turbidity, …):** `load_dss_ts()` accepts a `c_part` argument. Add a `constituent` key to the YAML and pass it through to the relevant functions.

**Batching multiple variations:** The current design is one variation per YAML file. To batch variations, either loop over YAML files in a driver script, or extend the schema with a `variations:` list and iterate in `run_from_yaml()`.

**MANNING calibration:** `ChannelParamModification` already supports `param: MANNING`. The workflow is identical; simply set `param: MANNING` and provide appropriate channel groups and values. Note that changes to Manning's n still require a full hydro + qual sequence.
