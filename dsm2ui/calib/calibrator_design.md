# calib_run.py — Design and Reference

## Purpose

`calib_run.py` automates the **DSM2 calibration variation workflow**: given a
baseline historical run, it creates a new study directory with modified channel
parameters (MANNING or DISPERSION), executes the model, and computes EC (electrical
conductivity) regression metrics comparing base vs variation.

The entire workflow is driven by a single YAML file (`calib_config.yml`), invoked
through the CLI command `dsm2ui calib run`.

---

## File Map

```
dsm2ui/calib/
    calib_run.py        — core library (all logic lives here)
    calib_cli.py        — Click commands (`dsm2ui calib ...`)
    calib_config.yml    — user configuration (one per variation)

<variation_study_dir>/
    config.inp          — patched copy from base run
    hydro.inp           — copied from base run (unmodified)
    qual_ec.inp / gtm.inp  — copied from base run (module-dependent)
    DSM2_batch_var.bat  — filtered/patched batch (replaces DSM2_batch.bat)
    calib_config.yml    — snapshot of the config used to create this run
    run.log             — full model stdout/stderr
    results.txt         — human-readable slope comparison table
    local_input/
        channel_std_delta_grid.inp   — modified channel file
        *.inp                        — all other grid files (copied)
    output/
        <modifier>_qual.dss or <modifier>_gtm.dss
        <modifier>.h5
        ...
    plots/
        RSAC075.png
        RSAN007.png
        ...
```

---

## Workflow

```
calib_config.yml
      │
      ▼
run_from_yaml()
      │
      ├─── setup_variation()
      │         ├── copy study files from base dir
      │         ├── copy channel .inp → local_input/
      │         ├── apply_channel_modifications()   ← text-based, in-place
      │         ├── copy all other grid .inp → local_input/
      │         ├── patch config.inp:
      │         │       DSM2MODIFIER
      │         │       DSM2INPUTDIR → local_input/
      │         │       TEMPDIR (auto-fix if missing)
      │         │       envvar_overrides (START_DATE, END_DATE, …)
      │         └── write DSM2_batch_var.bat
      │               (filtered modules, optional bin rewrite, selected REM lines uncommented)
      │
      ├─── run_study()              ← subprocess, streams to run.log
      │
      ├─── compute_ec_slopes()      ← base run
      │         └── load_dss_ts → _apply_timewindow → godin → linregress
      │
      ├─── compute_ec_slopes()      ← variation run
      │
      ├─── resolve model_dss_pattern
      │         └── explicit base_run.model_dss_pattern wins
      │             else GTM-only → {modifier}_gtm.dss, otherwise {modifier}_qual.dss
      │
      └─── compare_slopes()
                └── writes results.txt + slopes_<modifier>.csv
```

---

## Key Design Decisions

### 1. Text-based channel file patching

`apply_channel_modifications()` edits the `CHANNEL` table **in-place as raw
text**, replacing only the target column values on matched data rows.

**Why not use `pydsm.input.parser.write_input()`?**  
The `pydsm` writer reformats all float values (e.g. XSECT_LAYER elevation/width
columns) through Python's `float → str` conversion, which can change the
representation of values like `0.00` to `0.0`. DSM2's Fortran reader interprets
a shortened XSECT_LAYER row as an EOF, causing an immediate `[FATAL] IOSTAT=-3`
error. Text-based patching changes only the 20 CHANNEL data lines and leaves all
26 000+ XSECT_LAYER lines byte-for-byte identical.

### 2. `local_input/` directory for DSM2INPUTDIR

The `GRID` section of `hydro.inp` references files as `${DSM2INPUTDIR}/filename`.
DSM2's Fortran parser requires this `${ENVVAR}/filename` form — bare filenames
and absolute backslash paths both fail silently or fatally.

Rather than modifying `hydro.inp`, `setup_variation()` creates a `local_input/`
subdirectory, copies all grid `.inp` files there (including the modified channel
file), and redirects `DSM2INPUTDIR` in `config.inp` to point at it. The GRID
section remains untouched.

### 3. DSM2 v8.5 binary compatibility and run-step filtering

The channel file was updated (2026-04-07) to add a `DX` column required by
DSM2 v8.5. The pre-v8.5 binaries in `dsm2_studies/bin/` cannot parse this and
exit with `[FATAL] -3` on the XSECT header. The `dsm2_bin_dir` YAML key allows
specifying an alternate binary location (e.g. `D:/delta/DSM2-8.5.0-win64/bin`).
`_write_filtered_batch()` replaces relative `..\..\bin\<module>` references in
the batch file with absolute paths to the specified binaries, and now supports
both extensionless invocations and `.exe` invocations.

When `run_steps` is provided, `_write_filtered_batch()` keeps only selected
modules and drops unselected module lines. If selected lines are prefixed with
`REM`/`@REM` in the template batch file, they are automatically uncommented so
the selected steps actually execute.

### 4. `TEMPDIR` auto-fix

The base study `config.inp` has `TEMPDIR z:/temp` (a mapped network drive).
When this path doesn't exist, DSM2 exits immediately. `_ensure_tempdir()` checks
whether the configured path exists; if not, it silently redirects to `%TEMP%`.
This runs on every `setup_variation()` call so it's never a manual step.

### 5. `envvar_overrides` for run period control

Rather than editing the base study's `config.inp` dates globally, per-variation
overrides are applied after copying. This lets you shorten the variation run
(e.g. `01OCT2014–30SEP2017` instead of `01SEP2014–31DEC2024`) to save time
while leaving the base study untouched.

Godin filter warmup requires ~2 months before the metrics window starts, so a
2-month pre-period is standard (`START_DATE = metrics_start − 2 months`).

### 6. Dynamic model DSS pattern resolution

`run_from_yaml()` resolves `model_dss_pattern` with this precedence:

- Use `base_run.model_dss_pattern` when it is explicitly configured.
- If not configured and `run_steps` includes `gtm` and excludes `qual`, use
  `{modifier}_gtm.dss`.
- Otherwise use `{modifier}_qual.dss`.

This avoids GTM-only runs attempting to read QUAL output by default.

### 7. Concurrency safety

Multiple `dsm2ui calib run` processes pointing at different config files / study
directories are safe to run simultaneously:
- No `os.chdir()`, no shared global state, no shared temp files.
- Observed DSS and base qual DSS are opened read-only via `pyhecdss.get_ts` +
  `contextlib.closing`; HEC-DSS 6 supports concurrent readers without locking.
- Each variation writes exclusively to its own `study_dir` and `output/`.

### 8. Results stored with the study

Every artefact needed to reproduce or interpret a run is stored inside
`<variation_study_dir>/`:

| File | Content |
|------|---------|
| `calib_config.yml` | Exact YAML used to create this run |
| `run.log` | Full model stdout/stderr |
| `results.txt` | Human-readable slope comparison table |
| `slopes_<modifier>.csv` | Machine-readable slope table |
| `plots/<bpart>.png` | Per-station diagnostic figures |

---

## Public API

### Data classes

| Class | Purpose |
|-------|---------|
| `ChannelParamModification` | One named group: param + channel list/regex + value |
| `ECLocation` | Station pairing: `station_name`, `model_bpart`, `obs_bpart` |

### Core functions

| Function | Purpose |
|----------|---------|
| `apply_channel_modifications(file, mods)` | Text-based in-place CHANNEL table edit |
| `setup_variation(base, var, channel_src, mods, ...)` | Create variation study directory |
| `run_study(batch, cwd, log_file)` | Execute DSM2 batch, stream to log |
| `load_dss_ts(dss_file, b_part, c_part)` | Load time series from DSS |
| `compute_ec_slopes(model_dss, obs_dss, locations, timewindow)` | Godin + linregress |
| `compare_slopes(base_df, var_df, ...)` | Delta-slope comparison DataFrame |
| `plot_station_results(...)` | Generate per-station PNG figures |
| `run_calibration_variation(...)` | End-to-end orchestration (programmatic) |
| `run_from_yaml(yaml_path, ...)` | End-to-end orchestration (YAML-driven) |
| `plot_from_yaml(yaml_path)` | Generate plots from existing output (no model run) |
| `read_ec_locations_csv(csv_path, ...)` | Build `ECLocation` list from CSV |

### Private helpers (not part of public API)

| Function | Purpose |
|----------|---------|
| `_patch_dsm2_modifier(config_inp, modifier)` | Patch `DSM2MODIFIER` in config.inp |
| `_patch_dsm2inputdir(config_inp, new_dir)` | Redirect `DSM2INPUTDIR` |
| `_patch_envvars(config_inp, overrides)` | Patch arbitrary ENVVAR key→value pairs |
| `_ensure_tempdir(config_inp)` | Auto-fix missing TEMPDIR |
| `_write_filtered_batch(src, dst, steps, bin_dir)` | Filter modules + fix binary paths |
| `_resolve_model_dss_pattern(base_cfg, run_steps)` | Choose default model DSS pattern |
| `_apply_timewindow(df, timewindow)` | Slice DataFrame to time window string |
| `_copy_study_files(base, var)` | Copy `.inp/.bat/.json` from base dir |

---

## YAML Configuration Reference

```yaml
base_run:
  study_dir: d:/delta/dsm2_studies/studies/historical
  modifier: hist_fc_mss                 # DSM2MODIFIER of the existing base run
  # Optional. If omitted, defaults are:
  #   GTM-only run_steps: "{modifier}_gtm.dss"
  #   all other cases:    "{modifier}_qual.dss"
  # model_dss_pattern: "{modifier}_qual.dss"
  batch_file: DSM2_batch.bat

# Path to DSM2 binaries. Required when the batch file's relative
# ..\..\bin\hydro path points to an incompatible version.
dsm2_bin_dir: D:/delta/DSM2-8.5.0-win64/bin

variation:
  name: hist_fc_mss_disp_incconf       # becomes DSM2MODIFIER + output file prefix
  study_dir: d:/delta/dsm2_studies/studies/historical_disp_incconf
  run_steps: [hydro, qual]             # e.g. [hydro, gtm] for GTM-only studies

  # Override config.inp ENVVARs (optional — shorten the run period)
  envvar_overrides:
    START_DATE: 01OCT2014
    QUAL_START_DATE: 02OCT2014
    END_DATE: 30SEP2017

  channel_modifications:
    - name: pct_to_cll
      param: DISPERSION                # MANNING or DISPERSION
      channels: [291, 290, 436]        # list of CHAN_NO values, or a regex string
      value: 1500.0                    # ft²/s for DISPERSION, dimensionless for MANNING

observed_ec_dss: d:/delta/postprocessing/observed_data/ec_cal.dss
ec_stations_csv:  d:/delta/postprocessing/location_info/calibration_ec_stations.csv

active_stations:   # restrict to a subset; omit to use all rows in the CSV
  - RSAC075
  - RSAN007

metrics:
  timewindow: "01DEC2014 - 30SEP2017"  # Godin warmup excluded from metrics
```

**Notes:**
- `channels` can be a YAML list (`[291, 290]`) **or** a Python regex string
  (`"^29[0-9]$"`). Lists are internally normalised to an exact-match regex.
- Multiple `channel_modifications` groups are applied in order; later groups
  override earlier ones for overlapping channel IDs.
- `envvar_overrides` keys must exactly match the ENVVAR names in `config.inp`.
- `dsm2_bin_dir` is optional; omit it if the batch file's relative paths
  already point to the right binaries.

---

## CLI Reference (`dsm2ui calib run`)

```
dsm2ui calib run --config calib_config.yml [OPTIONS]
```

| Flag | Effect |
|------|--------|
| *(none)* | Full run: setup → model → metrics → results |
| `--setup-only` | Create `var_study_dir`, write batch — stop before running |
| `--metrics-only` | Skip setup/run; recompute slopes from existing DSS output |
| `--plot` | Generate PNGs from existing output — no model run |
| `--run-base` | Re-execute the base study's batch before computing base slopes |
| `--log-file PATH` | Override log path (default: `<var_study_dir>/run.log`) |
| `--log-level LEVEL` | `DEBUG \| INFO \| WARNING \| ERROR` (default: `INFO`) |

### Recommended workflow for a new variation

```cmd
cd /d d:\dev\dsm2ui
conda activate dsm2ui

rem 1. Verify setup (inspect config.inp dates, batch binary paths, channel values)
dsm2ui calib run --config dsm2ui\calib\calib_config.yml --setup-only

rem 2. Run the model
dsm2ui calib run --config dsm2ui\calib\calib_config.yml

rem 3. Regenerate plots without re-running
dsm2ui calib run --config dsm2ui\calib\calib_config.yml --plot

rem 4. Monitor while running
powershell -Command "Get-Content '<var_study_dir>\run.log' -Tail 10"
```

---

## Plot Layout

Each station produces one PNG at `<var_study_dir>/plots/<bpart>.png`:

```
┌──────────────────────────────────────────────────┐
│  Time series (Godin-filtered)                    │
│  — Observed (black)                              │
│  — Base (blue)                                   │
│  — Variation (orange-red)                        │
├───────────────────────┬──────────────────────────┤
│  Scatter + regression │  Channel modifications   │
│  model vs observed    │  table (group / param /  │
│  with slope + R²      │  channels / value)       │
│  annotations and 1:1  │                          │
└───────────────────────┴──────────────────────────┘
```

The scatter panel subsamples to ≤3 000 points for rendering speed; regression
lines are computed on the full dataset.

---

## Metrics

The primary metric is the **Godin-filtered EC linear regression slope**
(model as dependent variable, observed as independent variable):

$$\text{model\_EC} = \text{slope} \times \text{observed\_EC} + \text{intercept}$$

A slope of 1.0 indicates perfect proportional agreement. The comparison table
reports:

| Column | Meaning |
|--------|---------|
| `slope_base` | Regression slope for base run |
| `slope_<modifier>` | Regression slope for variation |
| `r_sq_base / r_sq_<modifier>` | R² for each run |
| `delta_slope` | `slope_var − slope_base` |
| `pct_change_slope` | `100 × delta / |slope_base|` |

Sorted by `|delta_slope|` descending — stations most influenced by the
variation appear first.

---

## Known Constraints and Gotchas

| Issue | Resolution |
|-------|-----------|
| DSM2 v8.5 added `DX` column to CHANNEL table | Pre-v8.5 binaries exit `[FATAL] -3`; use `dsm2_bin_dir` to point at v8.5 |
| `pydsm` writer reformats XSECT_LAYER floats | Never use `write_input()` on channel files; text-based patching only |
| DSM2 GRID section requires `${ENVVAR}/file` form | Bare paths and absolute backslash paths both fail; `local_input/` + `DSM2INPUTDIR` redirect is the only reliable approach |
| Batch templates may use `.exe` module names and/or `REM` comments | `_write_filtered_batch()` matches optional `.exe` and uncomments selected run_steps |
| GTM-only runs can read the wrong default DSS pattern | If `model_dss_pattern` is omitted, GTM-only defaults to `{modifier}_gtm.dss` |
| `TEMPDIR z:/temp` mapped drive may not exist | Auto-fixed by `_ensure_tempdir()` on every setup |
| Godin filter needs ~2-month warmup | Set `START_DATE` 2 months before metrics window start |
| DSS C-part for EC is `EC` (not `ELEC-COND`) | Hardcoded default in `load_dss_ts`; override with `c_part` arg if needed |
| `PERIOD_OP AVE` on tidal data aliases the 25-hr tidal cycle | Model output should always be `INST` at 15-min intervals for EC |

---

## Dependencies

| Package | Role |
|---------|------|
| `pyhecdss` | Read/write HEC-DSS files (cadwr-dms channel) |
| `vtools3` | Godin tidal filter (`vtools.functions.filter.godin`) |
| `pandas` | Time series and DataFrame operations |
| `scipy` | `linregress` for slope computation |
| `numpy` | Numerical array operations |
| `matplotlib` | Plot generation (optional; only needed for `--plot`) |
| `pyyaml` | YAML config loading |

---

## Extension Points

### Adding a new metric

Add a function alongside `compute_ec_slopes()` that accepts a model DSS path,
observed DSS path, and list of locations, and returns a DataFrame. Call it in
`run_calibration_variation()` and add its output to the returned dict. Wire it
through `run_from_yaml()` and the `dsm2ui.calib.calib_cli` command handlers.

### Supporting QUAL/GTM constituents other than EC

`load_dss_ts()` accepts a `c_part` argument. `compute_ec_slopes()` and
`plot_station_results()` pass it through. Adding a `constituent` key to the
YAML and threading it to these calls is straightforward.

### Multiple variations in one YAML

The current design is one variation per YAML file. To batch multiple variations,
loop over YAML files in a driver script or extend the YAML schema with a
`variations:` list and iterate in `run_from_yaml()`.

### Adding MANNING sensitivity

`ChannelParamModification` already supports `param: MANNING`. The workflow is
identical — just set `param: MANNING` and appropriate channel groups in the YAML.
Note: unlike DISPERSION changes, MANNING changes do not require re-running HYDRO
in theory, but DSM2 does not support hot-starting QUAL with changed friction, so
the full hydro+qual sequence is required regardless.
