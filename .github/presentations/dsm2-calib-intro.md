---
marp: true
theme: default
paginate: true
style: |
  section {
    font-size: 1.4rem;
  }
  section.lead h1 {
    font-size: 2.4rem;
  }
  pre, code {
    font-size: 0.8rem;
  }
---

<!-- _class: lead -->

# DSM2 Calibration Workflow
## `dsm2ui calib`

Automated parameter tuning for the Sacramento–San Joaquin Delta

---

**YAML-driven · CLI-first · Repeatable**

A toolkit for running DSM2 channel-parameter variations, computing tidal-filtered EC metrics, and optionally driving a gradient-based optimizer — all from a single config file.

> *Part of `dsm2ui` — the DWR Delta Simulation Model II user interface toolkit*

<!--
Audience: hydrologists/modelers familiar with DSM2 but new to the calibration toolkit.

Key message: this is NOT a manual workflow. Every step (file setup, model execution,
metrics, plots) is automated from one YAML file. Emphasize reproducibility — the YAML
snapshot is stored alongside every run artifact.

DSM2 must already be installed and a validated base study must exist before using calib.
-->

---

# The Problem

## Manual DSM2 calibration is error-prone and slow

| Pain point | Consequence |
|---|---|
| Edit `channel_std_delta_grid.inp` by hand | Risk of corrupting 26 000+ XSECT_LAYER rows |
| Copy study directory manually | Stale files, wrong modifier name |
| Run model → extract output → compute metrics | Hours per iteration |
| No history of what was tried | Calibration becomes unrepeatable |

<!--
The "26 000+ XSECT_LAYER" point is concrete — pydsm's writer reformats floats and can
change `0.00` → `0.0`, causing DSM2's Fortran reader to interpret a short row as EOF:
[FATAL] IOSTAT=-3. Text-based patching (regex on the 20 CHANNEL data lines only) avoids this.
-->

---

## What we need

- A **single source of truth** — one config file per run
- **Safe**, automated editing of channel parameters
- Automatic metrics + diagnostic plots after every run
- A way to **optimize** parameter values without manual iteration

---

# Architecture

## `dsm2ui/calib/` — Key components

```
dsm2ui/calib/
  calib_run.py         ← core library: setup, run, metrics, plots
  calib_optimize.py    ← gradient-based optimizer
  calib_cli.py         ← CLI entry points (dsm2ui calib …)
  postpro_dsm2.py      ← post-processing pipeline
  calibplotui.py       ← interactive plot viewer
  geoheatmap.py        ← geographic metrics heatmap
```

<!--
`local_input/` is the key isolation mechanism. DSM2INPUTDIR in config.inp is
redirected to this subdirectory, so only the channel file is modified.
All other grid .inp files are byte-for-byte copies of the base study.
-->

---

## Inputs & Outputs

```
calib_config.yml          ← single YAML controls everything
      │
      ▼
<variation_study_dir>/
  ├── local_input/          ← patched channel file + grid copies
  ├── output/               ← DSS + HDF5 tidefile
  ├── plots/                ← per-station PNGs
  ├── run.log
  ├── results.txt           ← EC slope comparison table
  └── calib_config.yml      ← snapshot of config used
```

<!--
The YAML snapshot stored alongside every run makes it possible to reproduce any variation
months later — no reliance on memory or scattered notes.
-->

---

# The Workflow

```
calib_config.yml
      │
      ├─ 1. setup_variation()
      │       ├── Copy study files from base dir
      │       ├── Patch channel .inp  ← text-based, regex only
      │       ├── Copy all other grid .inp → local_input/
      │       ├── Patch config.inp (modifier, DSM2INPUTDIR, dates)
      │       └── Write DSM2_batch_var.bat  (filtered modules + binary paths)
      │
      ├─ 2. run_study()         → subprocess, streams to run.log
      │
      ├─ 3. compute_ec_slopes() [base]    → load DSS → Godin → linregress
      ├─ 4. compute_ec_slopes() [variation]
      │
      └─ 5. compare_slopes()
              └── results.txt + slopes_<modifier>.csv + per-station PNGs
```

> ⚠ Start 2 months before the metrics window — the Godin filter needs warmup data.

<!--
Step 1 is where safety lives. Text-based patching means DSM2 sees the exact same bytes
for all non-CHANNEL rows. The DSM2INPUTDIR redirect makes local_input/ work without
touching hydro.inp.

linregress: observed as x, model as y. Slope of 1.0 is ideal.
Godin filter removes the tidal signal; we compare the subtidal (low-frequency) envelope.

The 2-month warmup rule is non-negotiable: the 30 hr + 24 hr cascaded Godin filter
needs leading data to produce valid output at the metrics window start date.
-->

---

# Configuration

## One YAML file drives everything

```yaml
base_run:
  study_dir: d:/delta/dsm2_studies/studies/historical
  modifier:  hist_fc_mss
  batch_file: DSM2_batch.bat
dsm2_bin_dir: D:/delta/DSM2-8.5.0-win64/bin   # optional: override binary path

variation:
  name: hist_v1_disp
  study_dir: d:/delta/dsm2_studies/studies/hist_v1_disp
  run_steps: [hydro, qual]       # always include hydro when DISPERSION changes
  envvar_overrides:
    START_DATE: 01OCT2014        # 2-month Godin warmup before metrics window
    END_DATE:   30SEP2017
  channel_modifications:
    - name: western_channels
      param: DISPERSION
      channels: [291, 290, 436]  # CHAN_NO list or Python regex string
      value: 1500.0              # ft²/s

observed_ec_dss: d:/delta/postprocessing/observed_data/ec_cal.dss
ec_stations_csv:  d:/delta/postprocessing/location_info/calibration_ec_stations.csv
active_stations: [RSAC075, RSAN007]
metrics:
  timewindow: "01DEC2014 - 30SEP2017"
```

<!--
The base study is NEVER modified. All changes go into a new variation directory.

channels: accepts a YAML list of integers OR a Python regex string (e.g. "^29[0-9]$").

envvar_overrides is the safest way to shorten the run window to save time.

Omitting hydro when changing DISPERSION is a common mistake — DSM2 has a bug where
the advection matrix is not recomputed without a fresh hydro run.
-->

---

# Metrics & Diagnostics

## Primary metric: Godin-filtered EC regression slope

$$\text{EC}_\text{model}(t) = \text{slope} \times \text{EC}_\text{observed}(t) + \text{intercept}$$

| Slope | Interpretation |
|---|---|
| **1.0** | Perfect proportional agreement ✓ |
| **> 1.0** | Model over-predicts salinity intrusion |
| **< 1.0** | Model under-predicts salinity intrusion |

Results: `slope_base`, `slope_var`, `r_sq`, `delta_slope`, `pct_change_slope`
*(sorted by |Δ slope| descending — most-affected stations first)*

<!--
The Godin filter is a 30 hr + 24 hr cascaded cosine Lanczos filter from vtools3.
It removes the dominant tidal signal (M2, K1, O1 constituents) and leaves the
low-frequency (subtidal) envelope.

Regression: observed as x, model as y. Slope > 1 → model response larger than observed.
-->

---

## Per-station diagnostic plot

```
┌──────────────────────────────────────────────────┐
│  Time series (Godin-filtered)                    │
│  — Observed  — Base run  — Variation             │
├───────────────────────┬──────────────────────────┤
│  Scatter + regression │  Channel modifications   │
│  slope & R²  +  1:1   │  applied to this run     │
└───────────────────────┴──────────────────────────┘
```

Regenerate plots without re-running the model:
```bash
dsm2ui calib run --config calib_config.yml --plot
```

<!--
The scatter subplot subsamples to ≤ 3 000 points for rendering speed;
regression lines are computed on the full time series.
-->

---

# The Optimizer

## Minimize weighted EC slope deviation from 1.0

$$f(\mathbf{x}) = \sum_i w_i \left( \text{slope}_i(\mathbf{x}) - 1 \right)^2$$

Each evaluation of $f(\mathbf{x})$ is a **full DSM2 run**.

### Gradient — forward finite differences

$$\frac{\partial f}{\partial x_j} \approx \frac{f(\mathbf{x} + h_j \mathbf{e}_j) - f(\mathbf{x})}{h_j}$$

$N$ perturbed runs (one per channel group) execute **in parallel** — set `max_workers ≥ N`.

<!--
L-BFGS-B is gradient-based and fast for smooth objectives like EC slope.
Nelder-Mead is also available but sequential — very slow for many groups.

"Warm-starting": the optimizer writes calib_config_optimized.yml in the best-result
directory. Point the next run at that file to continue from the best known point.

Windows file-lock: DSM2 output files may stay locked briefly after model exit.
The code retries 12× with 3s delay — do not lower these values on Windows.
-->

---

## Optimizer settings

| Key | Default | Role |
|---|---|---|
| `max_model_runs` | 100 | Hard evaluation budget |
| `max_workers` | 8 | Parallel DSM2 processes |
| `bounds` | [50, 5000] | ft²/s global bounds for all groups |
| `finite_diff_rel_step` | 0.05 | Step size as fraction of current value |
| `no_improve_patience` | 5 | Early stop if no improvement |

> After optimization, warm-start the next pass with `calib_config_optimized.yml`

---

# Quick Start

## Four commands to your first run

```bash
# 1. Generate a template config
dsm2ui calib setup --output calib_config.yml

# 2. Inspect setup — verify paths, dates, binary locations (no model run)
dsm2ui calib run --config calib_config.yml --setup-only

# 3. Full run: setup → model → metrics → plots
dsm2ui calib run --config calib_config.yml

# 4. Regenerate plots from existing output
dsm2ui calib run --config calib_config.yml --plot
```

```bash
# Optimize
dsm2ui calib optimize --config calib_config.yml

# Warm-start refinement from best result
dsm2ui calib optimize --config <best_dir>/calib_config_optimized.yml
```

<!--
--setup-only is the single most important flag to show first.
Lets users verify file paths, dates, and binary paths before a 30–60 min model run.

Monitor a running model:
  Get-Content <var_dir>/run.log -Tail 10 -Wait   (PowerShell)

Concurrent runs pointing at different configs are safe — no shared global state,
no os.chdir(), each run writes exclusively to its own study_dir.
-->

---

## Useful flags

| Flag | Effect |
|---|---|
| `--setup-only` | Create study dir + batch, don't run the model |
| `--metrics-only` | Recompute slopes from existing DSS output |
| `--plot` | Generate PNGs from existing output — no model run |
| `--dry-run` *(optimize)* | Evaluate starting point only |
| `--log-level DEBUG` | Verbose output for troubleshooting |

---

<!-- _class: lead -->

# Summary

| Capability | How |
|---|---|
| Safe channel parameter edits | Text-based patching — XSECT_LAYER rows untouched |
| Isolated variation studies | `local_input/` + `DSM2INPUTDIR` redirect |
| Tidal-filtered EC metrics | Godin filter → linregress slope vs observed |
| Diagnostic plots | Per-station time series + scatter (auto-generated) |
| Automated optimization | L-BFGS-B with parallel gradient evaluation |
| Reproducibility | YAML snapshot stored with every run artifact |

---

## Resources

| Resource | Where |
|---|---|
| Full docs | `README-calibrator.md` |
| Design notes | `dsm2ui/calib/calibrator_design.md` |
| Example config | `dsm2ui/calib/calib_config.yml` |
| CLI help | `dsm2ui calib --help` |

<!--
Closing: the goal is to make calibration iteration fast and trustworthy.
A modeler should be able to hand a YAML file to a colleague and reproduce the exact same
run without any additional explanation.

Anticipated questions:
- "Calibrate MANNING instead of DISPERSION?" → same YAML, change param:
- "Multiple parameters at once?" → yes, list multiple channel_modifications groups
- "Python version?" → see environment.yml (conda env: dsm2ui)
-->
