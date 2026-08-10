# Integrated Comparison Workflow: compare-dss, calib postpro, and diff

Three separate tools all touch on "how do two DSM2 studies compare?", each solving
a different part of the problem:

| Tool | Compares | Setup burden | Best for |
|---|---|---|---|
| [`pydsm compare-dss`](../pydsm/README-compare-dss.md) | Two DSS **output** files, numerically | None — just two file paths | Fast pass/fail check, scripting/CI |
| [`dsm2ui calib postpro`](README-postpro.md) | N model studies' **output**, optionally vs. observed | Low–medium (station CSV, config) | Rich plots/metrics, planning (baseline vs. alternative), calibration/validation |
| [`pydsm diff`](../pydsm/README-diff.md) | Two studies' **input** (echo files) | None — just two echo file paths | Explaining *why* outputs differ |

They are intentionally kept **separate, standalone tools** — none of them calls
another automatically. This document is the manual, step-by-step guide for
combining them.

---

## Decision guide

**"I have exactly two DSS output files and want a quick numeric check."**
→ [`pydsm compare-dss`](../pydsm/README-compare-dss.md). No config, no station
list — just point it at two files.

**"I have a baseline study and one or more alternatives (planning scenario), no
observed data yet, and I want to see time-series/scatter plots and metrics for
every station."**
→ [`dsm2ui calib postpro setup-compare`](README-postpro.md#quick-baseline-vs-alternative-comparison-no-observed-data),
then `run model` + `run plots`.

**"I have completed model run(s) and observed field data, and want full
calibration/validation plots (time series, scatter, KDE, tidal, metrics
tables)."**
→ [`dsm2ui calib postpro setup`](README-postpro.md) or
[`setup-from-datastore`](README-postpro.md), then the standard
`run observed` → `run model` → `run plots` pipeline.

**"I've found a station/timewindow where two studies' outputs diverge, and I want
to know what changed in the model setup that explains it."**
→ [`pydsm diff`](../pydsm/README-diff.md) on the two studies' Hydro echo files,
scoped with `-T`/`--timewindow` to the relevant tables and period.

---

## Worked example: baseline vs. alternative planning study

Scenario: you have a `baseline` DSM2 study and an `alternative` study (e.g. a
channel-dredging or gate-operation scenario) and want to understand what changed
and why.

### Step 1 — Quick sanity check (optional)

If you already have both studies' QUAL DSS output files handy and just want a fast
first look before setting up anything:

```bash
pydsm compare-dss baseline/output/baseline_qual.dss alternative/output/alternative_qual.dss \
  --cpart EC --time-window "01OCT2020 - 30SEP2022" \
  --threshold 0.05 --threshold-plots
```

This gives you a CSV of RMSE/NSE/bias per station and (with `--threshold-plots`)
HTML overlay plots for stations that exceed the threshold. Good enough to decide
whether it's worth setting up the richer comparison below.

### Step 2 — Rich baseline-vs-alternative comparison

```bash
dsm2ui calib postpro setup-compare \
    -s baseline/ \
    -s alternative/ \
    -o compare_config.yml \
    -m qual

dsm2ui calib postpro run model compare_config.yml
dsm2ui calib postpro run plots compare_config.yml --workers 4
```

- The **first** `-s` (`baseline`) is the reference for scatter plots and metrics.
- No `run observed` step — this config has no observed data (see
  [README-postpro.md](README-postpro.md#quick-baseline-vs-alternative-comparison-no-observed-data)).
- Review `./plots/` for time-series overlays, scatter plots (baseline vs.
  alternative), and the metrics summary CSV. Identify which stations and time
  periods show the largest divergence (regression slope far from 1.0, high RMSE,
  etc.).

### Step 3 — Explain the divergence with `pydsm diff`

Once you know *where* (which stations/variables) and *when* (which time window)
the two studies diverge most, scope `pydsm diff` to that period and the input
tables most likely to explain it (see the guidance table below):

```bash
pydsm diff baseline/output/hydro_echo.inp alternative/output/hydro_echo.inp \
  -T CHANNEL -T BOUNDARY_STAGE -T OPERATING_RULE \
  --timewindow "01JUL2021 0000 - 01SEP2021 0000" \
  --threshold 0.05 \
  --outdir diff_output/ \
  -o diff_report.txt
```

Read `diff_report.txt` (or the CSVs in `diff_output/`) for:
- **Static table changes** — e.g. a `CHANNEL` row with a different `DISPERSION` or
  `MANNING` value, an added/removed `GATE`, a changed `OPERATING_RULE`.
- **Time-series changes** — e.g. a `BOUNDARY_STAGE` entry with high RMSE between
  the two studies, pinpointing exactly which boundary condition changed and by
  how much.

This closes the loop: Step 2 told you *that* (and where) outputs diverge; Step 3
tells you *what in the model setup* caused it.

---

## Variable → input table guidance

When you don't already know which input tables to scope `pydsm diff` to, start
here (see [README-diff.md](../pydsm/README-diff.md) for the full table list):

| Output variable that differs | Input tables worth checking |
|---|---|
| `FLOW` | `CHANNEL` (geometry, Manning), `BOUNDARY_FLOW`, `GATE`/`INPUT_GATE`, `TRANSFER`/`INPUT_TRANSFER_FLOW` |
| `STAGE` | `CHANNEL`, `BOUNDARY_STAGE`, `RESERVOIR`/`RESERVOIR_CONNECTION`, `GATE` |
| `EC` | `CHANNEL` (DISPERSION), `BOUNDARY_STAGE`, `OPERATING_RULE`/`OPRULE_TIME_SERIES`, `SOURCE_FLOW` |

---

## This is a manual workflow by design

None of these tools automatically calls another:

- `compare-dss` and `calib postpro` do not trigger `pydsm diff` when a threshold is
  exceeded — you review the plots/metrics yourself and decide what to diff.
- `calib postpro`'s no-observed `setup-compare` mode does not compute a
  compare-dss-style metrics table against the baseline beyond what the existing
  scatter/metrics plots already show.

This keeps each tool simple, standalone, and independently testable. If your
workflow would benefit from tighter automation (e.g. auto-scoping `pydsm diff` to
only the tables affecting a flagged station), that would be a deliberate follow-on
enhancement — not something either tool does today.

## See also

- [../pydsm/README-compare-dss.md](../pydsm/README-compare-dss.md)
- [../pydsm/README-diff.md](../pydsm/README-diff.md)
- [README-postpro.md](README-postpro.md)
- [README-calibrator.md](README-calibrator.md) — for setting up the baseline and
  alternative *runs themselves* (channel parameter variations) before comparing
  their output here
