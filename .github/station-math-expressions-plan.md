# Station Math Expressions — Calibration Plot Configuration Guide

This guide explains how to configure computed (expression-based) stations in DSM2
calibration plots. It covers the location CSV format, the postpro config JSON, and
the rules for how expressions are interpreted at runtime.

---

## How Calibration Plots Work

The calibration plot pipeline (`dsm2ui calib postpro`) compares **model output** (from
HEC-DSS files produced by DSM2 QUAL or HYDRO) against **observed field data** (also
stored in HEC-DSS). The pipeline is driven by two configuration inputs:

1. **A postpro config JSON** — lists study DSS files, observed DSS files, location CSVs,
   time windows, and vartypes.
2. **Location CSV files** (one per vartype: FLOW, EC, STAGE) — list the stations to plot,
   with one row per DSM2 model station / observed station pairing.

The location CSV tells the pipeline:

- Which **model DSS B-part** to load (column `dsm2_id`)
- Which **observed DSS B-part** to load (column `obs_station_id`)
- The human-readable station name (`station_name`)
- Optional: data quality windows to exclude, a magnitude threshold for spike filtering

Some locations, the physical measurement we want to compare is **not** a single station reading but a **mathematical combination** of two or more stations. For example:

- **Victoria Canal** — the sensor measures flow in the wrong direction; we need to **negate** the observed signal (`-VCU`)
- **Cross-Delta Flow** — there is no single measurement; it must be **computed** as Sacramento River above Delta Cross Channel minus Sacramento below Georgiana Slough (`RSAC128 - RSAC123` from the model, `SDC - GES` from observations)

---

## The Postpro Config JSON

The postpro config JSON (`postpro_config.json`) wires everything together. Key sections:

```json
{
    "location_files_dict": {
        "EC":    "./location_info/calibration_ec_stations.csv",
        "FLOW":  "./location_info/calibration_flow_stations.csv",
        "STAGE": "./location_info/calibration_stage_stations.csv"
    },
    "observed_files_dict": {
        "EC":    "./observed_data/cdec/ec_merged.dss",
        "FLOW":  "./observed_data/cdec/flow_merged.dss",
        "STAGE": "./observed_data/cdec/stage_merged.dss"
    },
    "study_files_dict": {
        "DSM2v8_3":   "./model_output/run0_8_2_mann_disp.dss",
        "DSM2v8_2_1": "./model_output/hist_v82_19smcd.dss"
    },
    "vartype_dict": {
        "EC":    "uS/cm",
        "FLOW":  "cfs",
        "STAGE": "feet"
    },
    "timewindow_dict": {
        "default_timewindow":  "hydro_calibration",
        "hydro_calibration":   "01OCT2010 - 01OCT2012",
        "qual_calibration":    "01OCT2009 - 01OCT2017"
    },
    "inst_plot_timewindow_dict": {
        "FLOW":  "2011-09-01:2011-09-30",
        "EC":    null,
        "STAGE": "2011-09-01:2011-09-30"
    },
    "process_vartype_dict": {
        "EC":    false,
        "FLOW":  true,
        "STAGE": true
    }
}
```

- `location_files_dict` — maps each vartype to its station CSV. **This is where you point to custom CSVs** if you want different station sets per run.
- `observed_files_dict` — the observed HEC-DSS file for each vartype. Station B-parts in these files must match the `obs_station_id` values in the location CSV (or the identifiers used in `obs_station_id` expressions).
- `study_files_dict` — one entry per model study. B-parts must match `dsm2_id` values in the location CSV (or expression identifiers).
- `process_vartype_dict` — set `true` to reprocess (clear cache), `false` to use cached output.

Generate a fresh config with:
```bash
dsm2ui calib postpro setup -s study1/ -s study2/ -p postprocessing/ -o postpro_config.yml
```

---

## The Location CSV Format

Each row in a location CSV defines one station to include in calibration plots. Columns:

| Column | Required | Description |
|---|---|---|
| `dsm2_id` | yes | B-part name for the **model** DSS lookup (or arithmetic expression) |
| `obs_station_id` | yes | B-part name for the **observed** DSS lookup (or arithmetic expression) |
| `station_name` | yes | Human-readable label shown in plot headers |
| `subtract` | no | Legacy flag — leave `no` for all rows; use expressions instead |
| `time_window_exclusion_list` | no | Comma-separated `start_stop` pairs to blank out bad data |
| `threshold_value` | no | Magnitude threshold above/below which data is excluded (e.g. spikes) |

Lines starting with `#` are commented out — the station is skipped entirely.

**The `dsm2_id` column controls the plot header.** For most stations it is the plain
station code (e.g. `RSAC155`). For derived stations it is the expression string
(e.g. `SDC-GES`, `RSAC128-RSAC123`).

---

## Expressing Computed Stations

Some stations require arithmetic on the raw DSS time series before plotting. The
expression is written directly into `dsm2_id` or `obs_station_id`. No new columns are
needed.

**Rule:** if a field value is a plain identifier (only letters, digits, underscores,
starting with a letter) → used as a direct DSS B-part lookup. If it contains operators
(`-`, `+`, `*`, `/`) → evaluated as an arithmetic expression.

### Examples from `calibration_flow_stations.csv`

```csv
dsm2_id,obs_station_id,station_name,...
VCU,-VCU,VICTORIA CANAL NEAR BYRON,...
CHVCT000,-CHVCT000,VICTORIA CANAL NEAR BYRON,...
SDC-GES,SDC-GES,CROSS DELTA FLOW (RSAC128 - RSAC123),...
RSAC128-RSAC123,SDC-GES,CROSS DELTA FLOW (RSAC128 - RSAC123),...
```

| `dsm2_id` | `obs_station_id` | What happens |
|---|---|---|
| `VCU` | `-VCU` | Model: load `VCU` directly. Observed: negate the `VCU` series. |
| `CHVCT000` | `-CHVCT000` | Model: load `CHVCT000` directly. Observed: negate it. |
| `SDC-GES` | `SDC-GES` | Both model and observed: load `SDC` and `GES`, subtract. |
| `RSAC128-RSAC123` | `SDC-GES` | Model: `RSAC128 - RSAC123`. Observed: `SDC - GES`. |

### Supported operations

- **Negate**: `-STATION` — flips the sign (useful when sensor reports opposite direction)
- **Subtract**: `A-B` — net flow between two cross-sections
- **Add**: `A+B` — combined flow through two channels
- **Multiply/divide**: `A*0.5`, `A/B` — scale or ratio
- **Parentheses**: `(A+B)-C`
- Identifiers inside an expression are **B-part names in the same DSS file** as the station row

Standard Python arithmetic is supported. numpy functions (`abs`, `sqrt`, etc.) and vtools
filters (`godin`, `cosine_lanczos`) are also available inside expressions.

---

## Time-Window Exclusion

Use `time_window_exclusion_list` to blank out periods of bad observed data. Format is
`YYYY-MM-DD HH:MM:SS_YYYY-MM-DD HH:MM:SS`, comma-separated for multiple windows:

```csv
BDT,BDT,SAN JOAQUIN R AT BRANDT BRIDGE,no,"2012-06-07 08:15:00_2012-11-01 19:30:00,2013-05-17 00:45:00_2013-10-25 03:00:00",...
```

For expressions, exclusion windows apply to the **composite result**, not the individual
component stations. If a component station has its own bad period, add it as a separate
row in the CSV and put the exclusion window there.

---

## Threshold Filtering

`threshold_value` removes instantaneous spikes. Data points whose absolute value exceeds
the threshold are set to NaN before filtering and metric computation:

```csv
GES,GES,SACRAMENTO RIVER BELOW GEORGIANA SLOUGH,no,,10000,...
```

This is applied after the expression is evaluated, so for `SDC-GES` the threshold acts on
the net flow after subtraction.

---

## Disabling a Station

Prefix the `dsm2_id` with `#` to comment out a row without deleting it:

```csv
#GSS,GSS,GEORGIANA SLOUGH AT SACRAMENTO RIVER,...
```

This is useful to park stations that have no current data availability without losing the
row configuration.

---

## Caching Behaviour

All postprocessed time series (raw + Godin-filtered + tidal high/low) are stored in a
disk cache keyed by station B-part and vartype. Expression results are cached under the
composite key (e.g. `SDC-GES / FLOW`), so each component station is also individually
cached on first use.

**To force reprocessing**, either:
- Set `process_vartype_dict → FLOW: true` in the postpro config
- Or run with `--skip-cached` omitted (default behaviour clears the cache on each `model` step)

---

## Typical Workflow

```bash
# 1. Build or edit the postpro config
dsm2ui calib postpro setup -s study/ -p postprocessing/ -o postpro_config.yml

# 2. Process observed data (run once per observed DSS change)
dsm2ui calib postpro run observed postpro_config.yml

# 3. Process model output (clears model cache by default)
dsm2ui calib postpro run model postpro_config.yml

# 4. Generate plots (sequential)
dsm2ui calib postpro run plots postpro_config.yml

# 4b. Generate plots in parallel (faster for many stations)
dsm2ui calib postpro run plots postpro_config.yml --workers 4
```

To add a new computed station (e.g. `NEWCHAN-UPSTREAM`):
1. Confirm both `NEWCHAN` and `UPSTREAM` B-parts exist in your model and observed DSS files.
2. Add a row to the relevant location CSV: `dsm2_id=NEWCHAN-UPSTREAM`, `obs_station_id=NEWCHAN-UPSTREAM`, `station_name=My New Station`.
3. Re-run the `model` and `plots` steps.

| Column | Meaning |
|---|---|
| `dsm2_id` | The B-part name of the station in the **model** DSS file |
| `obs_station_id` | The B-part name of the station in the **observed** DSS file |

In some locations, the physical measurement we want to compare is **not** a single station reading but a **mathematical combination** of two or more stations. For example:

- **Victoria Canal** — the sensor measures flow in the wrong direction; we need to **negate** the observed signal (`-VCU`)
- **Cross-Delta Flow** — there is no single measurement; it must be **computed** as Sacramento River above Delta Cross Channel minus Sacramento below Georgiana Slough (`RSAC128 - RSAC123` from the model, `SDC - GES` from observations)

---

## Design: Expressions Live in Existing CSV Fields

No new columns are needed. The existing `obs_station_id` and `dsm2_id` columns already
hold the expressions — they always did. The convention is:

- If a field value is a **plain identifier** (letters, digits, underscores, starts with a
  letter) → used as a direct DSS B-part lookup
- If a field value **contains operators** (`-`, `+`, `*`, `/`, unary `-`, etc.) → treated
  as a Python arithmetic expression whose identifiers are B-part station names in the same
  DSS file

### Examples

| dsm2_id | obs_station_id | Observed | Model |
|---|---|---|---|
| `FPT` | `FPT` | load station `FPT` | load station `FPT` |
| `VCU` | `-VCU` | evaluate `-VCU` (negated) | load station `VCU` |
| `CHVCT000` | `-CHVCT000` | evaluate `-CHVCT000` (negated) | load station `CHVCT000` |
| `SDC-GES` | `SDC-GES` | evaluate `SDC - GES` | evaluate `SDC - GES` |
| `RSAC128-RSAC123` | `SDC-GES` | evaluate `SDC - GES` | evaluate `RSAC128 - RSAC123` |

---

## How the Expression Engine Works

When a field contains an expression, the pipeline:

1. **Detects** that the field is not a plain identifier (`_is_expression()` in `calibplot.py`)
2. **Parses identifiers** from the expression string (e.g. `SDC-GES` → tokens `SDC`, `GES`)
3. **Loads each token station** from the same DSS file through the full postprocessing
   pipeline (resampling, gap-filling) to get a raw time series
4. **Evaluates the expression** using those time series as variables — e.g. `SDC - GES`
   subtracts the two pandas Series
5. **Derives filtered outputs** from the composite series: Godin tidal filter → `gdf`;
   tidal high/low detection → `high`, `low`, `amp`
6. **Caches the result** under the composite station key, so repeated runs skip recomputation

The expression evaluator reuses the safe math namespace from the `dvue` library (already a
dependency), which includes numpy functions and vtools tidal filters. No Python builtins
are exposed.

---

## Plot Display

The plot header shows `location.name` (the `dsm2_id`), which is already the expression
string for derived stations:

- Simple station: `## Sacramento at Freeport (FPT / FLOW)`
- Negated: `## Victoria Canal (VCU / FLOW)` (model) and observed uses `-VCU` internally
- Subtraction: `## Cross Delta Flow (SDC-GES / FLOW)` or `## Cross Delta Flow (RSAC128-RSAC123 / FLOW)`

---

## Implementation Summary

| File | Change |
|---|---|
| `dsm2ui/calib/expression_eval.py` | New — `parse_expression_tokens()`, `eval_expression()` using dvue namespace |
| `dsm2ui/calib/calibplot.py` | `_is_expression()`, `_compute_expression()`, expression-aware `load_data_for_plotting()` |
| `pydsm/analysis/postpro.py` | No change to `Location` namedtuple |
| `calibration_flow_stations.csv` | No change — existing fields already encode expressions |

| 5 | Remove old subtract/invert logic (cleanup) | `pydsm/analysis/postpro.py`, `calibplot.py` |

Phases 0 and 1 can proceed in parallel. Phase 2 is independent of 1. Phase 3 depends on 1 and 2. Phases 4 and 5 depend on 3.

---

## Backward Compatibility

- Rows without `obs_expression` or `model_expression` columns behave exactly as before
- The `subtract` and `ratio` logic in `PostProcessor._read_ts()` is left as a fallback initially; it will be removed in phase 5 after all CSVs are migrated and tested
- The `invert_series` parameter in `load_processed()` is removed once phase 3 is confirmed working
