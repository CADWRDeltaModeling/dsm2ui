# DSM2 Calibration / Validation Plot Generation

This guide explains how to produce comparison plots and metric tables for DSM2 runs
against observed field data using the `dsm2ui calib postpro` command group.

> **Note:** This workflow is for generating plots and metrics from *completed* DSM2 runs.
> For the calibration optimizer (tuning DISPERSION/MANNING coefficients),
> see [README-calibrator.md](README-calibrator.md).

---

## What this produces

For each station in your location CSV, `calib postpro` generates:

- **Tidally-averaged time-series plots** — model vs observed, with shading for data gaps
- **Scatter/regression plots** — model vs observed with a linear fit and slope metric
- **KDE distribution plots** — side-by-side density comparison
- **Short instantaneous time-series** — zoomed-in tidal view for a representative month
- **Metric tables** — slope, RMSE, bias, NSE, etc. per station and study
- **Summary heatmap** — geo-colored map of metrics across all stations

All outputs are written as HTML (interactive) and PNG to your configured `output_folder`.

---

## Prerequisites

- `dsm2ui` installed and its conda environment active:

  ```bat
  conda activate dsm2ui
  ```

- One or more **completed** DSM2 runs. Each study folder must have an `output/` subdirectory
  containing the DSM2 echo `.inp` file and the output `.dss` file.

  ```
  studies/
    historical/
      output/
        hist_fc_mss_hydro_echo.inp   ← contains DSM2MODIFIER and dates
        hist_fc_mss_hydro.dss        ← model output
    scenario_a/
      output/
        scenario_a_hydro_echo.inp
        scenario_a_hydro.dss
  ```

- **Observed data** in one of two forms:
  - A **DMS Datastore** directory (preferred — see [Path A](#path-a--quickstart-with-dms-datastore))
  - Existing HEC-DSS observed files + location station CSVs
    (see [Path B](#path-b--manual-setup-with-a-postprocessing-folder))

---

## Workflow at a glance

| Step | Command | When to re-run |
|------|---------|----------------|
| 1. Generate config | `calib postpro setup-from-datastore` or `calib postpro setup` | Once; or when studies change |
| 2. Process observed data | `calib postpro run observed` | Once; or when observed DSS changes |
| 3. Process model output | `calib postpro run model` | After each new model run |
| 4. Generate plots | `calib postpro run plots` | After step 2 or 3 |
| 5. Interactive review | `calib ui plot` | Any time |

---

## Path A — Quickstart with DMS Datastore

This is the recommended path when your observed data lives in a DMS Datastore.
A single command extracts observed time series into a DSS file and builds the config.

### Step 1 — Generate config from the datastore

```bat
conda activate dsm2ui
dsm2ui calib postpro setup-from-datastore ^
    -s D:/delta/dsm2_studies/studies/historical ^
    -d D:/delta/dms_datastore ^
    -o postpro_config.yml ^
    --vartype EC
```

> **Share drive users:** pass the directory that *contains* `inventory_datasets_screened*.csv`
> and the `screened/` subdirectory — not `screened/` itself. For example:
> `-d "\\cnrastore-bdo\Modeling_Data\repo\continuous"`
>
> If `pyhecdss` raises an error on a long UNC path, map the share to a drive letter first:
> ```bat
> net use Z: \\cnrastore-bdo\Modeling_Data
> ```
> then use `-d Z:/repo/continuous`.
>
> **Performance tip:** The extraction step reads every matching station CSV from the
> network share. Run it once and keep the generated `ec_cal.dss` locally. Subsequent
> `run model` and `run plots` steps use only the local DSS and are fast.

**What this does:**

1. Reads the echo file in `studies/historical/output/` to find `DSM2MODIFIER` and the
   simulation `START_DATE` / `END_DATE`.
2. Locates the model DSS file (`{modifier}_qual.dss` by default).
3. Reads the DMS Datastore inventory and extracts all EC stations into `ec_cal.dss`
   next to `postpro_config.yml`.
4. Writes `postpro_config.yml` referencing the model DSS, the extracted `ec_cal.dss`,
   and the bundled default location station CSV.

> **B-part naming:** DSS B-parts in `ec_cal.dss` are the datastore `station_id` uppercased
> (with subloc appended when present, e.g. `ANH` + `upper` → `ANHUPPER`). The bundled
> default location CSVs already use matching `obs_station_id` values for the standard
> Delta EC network. If you add custom stations to the location CSV, use the same convention.

**Key options:**

| Option | Default | Description |
|--------|---------|-------------|
| `-s / --study DIR` | *(required, repeatable)* | Study folder (repeat for multiple runs) |
| `-d / --datastore DIR` | *(required)* | DMS Datastore directory — the folder that *contains* `inventory_datasets_screened*.csv` and the `screened/` subdirectory, **not** `screened/` itself |
| `-o / --output FILE` | *(required)* | Output YAML config path |
| `--vartype TYPE` | `EC` *(repeatable)* | Vartype(s): `EC`, `FLOW`, `STAGE` |
| `-m / --module` | `qual` | DSM2 module: `qual` or `gtm` for EC; `hydro` for FLOW/STAGE |
| `--dss-dir DIR` | same dir as `--output` | Where to write extracted observed DSS files |
| `--repo-level` | `screened` | Datastore level: `screened` or `raw` |
| `--location-file VARTYPE=PATH` | *(repeatable)* | Override a location CSV, e.g. `EC=/path/ec.csv`; when omitted bundled defaults are used |
| `--output-folder DIR` | `./plots/` | Folder where plots are saved |
| `--timewindow "A - B"` | auto from echo file | Override time window, e.g. `"01OCT2020 - 30SEP2022"` |

### EC vs FLOW/STAGE module flag

| Variable(s) | Module flag | DSS file used |
|---|---|---|
| EC | `-m qual` (default) or `-m gtm` | `{modifier}_qual.dss` or `{modifier}_gtm.dss` |
| FLOW, STAGE | `-m hydro` | `{modifier}_hydro.dss` |

QUAL/GTM output DSS files contain water quality constituents (EC). They do **not**
contain FLOW or STAGE — those only exist in the hydro DSS. If you need both EC and
FLOW/STAGE plots, generate **two separate configs**: one with `-m qual` for EC and
one with `-m hydro` for FLOW/STAGE.

### Multiple vartypes

Multiple vartypes are supported within a single module:

```bat
# EC and other water quality constituents from a qual run:
dsm2ui calib postpro setup-from-datastore ^
    -s D:/studies/historical ^
    -d D:/datastore ^
    -o postpro_ec.yml ^
    --vartype EC -m qual

# FLOW and STAGE from a hydro run:
dsm2ui calib postpro setup-from-datastore ^
    -s D:/studies/historical ^
    -d D:/datastore ^
    -o postpro_hydro.yml ^
    --vartype FLOW --vartype STAGE -m hydro
```

---

## Path B — Manual setup with a postprocessing folder

Use this path when you already have:
- A `location_info/` directory with hand-maintained station CSVs
- An `observed_data/` directory with pre-assembled observed DSS files

```
postprocessing/
    location_info/
        calibration_ec_stations.csv
        calibration_flow_stations.csv
        calibration_stage_stations.csv
    observed_data/
        ec_cal.dss
        flow_cal.dss
        stage_cal.dss
```

### Step 1 — Generate config from a postprocessing folder

```bat
dsm2ui calib postpro setup ^
    -s D:/studies/historical ^
    -p D:/postprocessing ^
    -o postpro_config.yml ^
    -m qual
```

**Options** (same as `setup-from-datastore` except `--datastore`):

| Option | Default | Description |
|--------|---------|-------------|
| `-s / --study DIR` | *(required, repeatable)* | Study folder |
| `-p / --postprocessing DIR` | — | Postprocessing folder with `location_info/` and `observed_data/` |
| `-o / --output FILE` | *(required)* | Output YAML config path |
| `-m / --module` | `hydro` | DSM2 module: `hydro`, `qual`, or `gtm` |
| `--output-folder DIR` | `./plots/` | Folder where plots are saved |
| `--timewindow "A - B"` | auto from echo file | Override time window |
| `--location-file VARTYPE=PATH` | *(repeatable)* | Override a location CSV, e.g. `EC=/path/ec.csv` |
| `--observed-file VARTYPE=PATH` | *(repeatable)* | Override an observed DSS path, e.g. `EC=/path/ec.dss` |

You can also mix approaches — use the postprocessing folder for location CSVs but
override individual observed files:

```bat
dsm2ui calib postpro setup ^
    -s D:/studies/historical ^
    -p D:/postprocessing ^
    -o postpro_config.yml ^
    --observed-file FLOW=D:/updated/flow_cal.dss
```

---

## Step 2 — Process observed data

Run once for each set of observed DSS files. The results are cached on disk so subsequent
model reruns can skip this step.

```bat
dsm2ui calib postpro run observed postpro_config.yml
```

This resamples to 15-minute intervals, applies gap-filling, runs the Godin tidal filter,
and stores results in a `diskcache` directory next to the config file.

---

## Step 3 — Process model output

Run after each new DSM2 run. By default this clears the model cache and reprocesses all
model DSS files.

```bat
dsm2ui calib postpro run model postpro_config.yml
```

To reuse an existing model cache (useful when only changing plot options):

```bat
dsm2ui calib postpro run model postpro_config.yml --skip-cached
```

---

## Step 4 — Generate plots

```bat
dsm2ui calib postpro run plots postpro_config.yml
```

This can be slow for large station lists. Use `--workers` to parallelize across CPU cores:

```bat
dsm2ui calib postpro run plots postpro_config.yml --workers 4
```

> **Rule of thumb:** Set `--workers` to half your physical core count. Exceeding physical
> cores can cause out-of-memory errors because each worker loads its own webdriver instance.

To reuse cached data and only regenerate plot files (skip reprocessing):

```bat
dsm2ui calib postpro run plots postpro_config.yml --workers 4 --skip-cached
```

---

## Step 5 — Interactive review

Launch a browser-based interactive viewer to browse all station plots, filter by vartype,
and inspect metrics:

```bat
dsm2ui calib ui plot postpro_config.yml
```

Useful options:

| Option | Description |
|--------|-------------|
| `--vartype EC` | Show only EC stations (repeat for multiple) |
| `--clear-cache` | Wipe the cache and reprocess from DSS before showing the UI |
| `--option write_html=false` | Override any `options_dict` entry without editing the YAML |

---

## Comparing multiple DSM2 runs

To overlay two or more model runs on every station plot, pass `-s` multiple times when
generating the config. Each study gets its own curve.

```bat
dsm2ui calib postpro setup-from-datastore ^
    -s D:/studies/historical ^
    -s D:/studies/scenario_a ^
    -d D:/datastore ^
    -o postpro_config.yml ^
    --vartype EC
```

The study folder name (e.g. `historical`, `scenario_a`) is used as the legend label.
Choose descriptive folder names.

Then run the full pipeline again — both studies are processed together:

```bat
dsm2ui calib postpro run observed postpro_config.yml
dsm2ui calib postpro run model postpro_config.yml
dsm2ui calib postpro run plots postpro_config.yml --workers 4
```

---

## The generated config file

`postpro_config.yml` is a YAML file that can be reviewed and edited after generation.
Key sections:

```yaml
options_dict:
  output_folder: ./plots/      # where HTML/PNG are written
  write_html: true
  write_graphics: true
  include_kde_plots: true
  zoom_inst_plot: true

location_files_dict:
  EC: null                     # null = use bundled default station CSV (~60 Delta EC stations)
  FLOW: null                   # null = use bundled default (~30 flow stations)
  STAGE: null                  # null = use bundled default (~20 stage stations)
                               # Replace null with a file path to use your own station list

observed_files_dict:
  EC: D:/postprocessing/ec_cal.dss
  FLOW: null
  STAGE: null

study_files_dict:
  historical: D:/studies/historical/output/hist_fc_mss_qual.dss
  scenario_a: D:/studies/scenario_a/output/scenario_a_qual.dss

vartype_timewindow_dict:
  EC: simulation_period        # which named timewindow to use for EC plots
  FLOW: null                   # null = skip this vartype
  STAGE: null

timewindow_dict:
  simulation_period: 01OCT2020 0000 - 30SEP2022 0000
  hydro_calibration: 01OCT2020 0000 - 30SEP2022 0000

inst_plot_timewindow_dict:
  EC: null                     # null = skip instantaneous plot for EC
  FLOW: 2022-08-01:2022-08-31  # one-month window for FLOW tidal plot
  STAGE: null
```

To disable a vartype entirely, set `vartype_timewindow_dict → VARTYPE: null`.

---

## Customizing the station list

### Bundled defaults vs custom location CSVs

When `location_files_dict` entries are `null`, the system uses the bundled station CSVs
shipped with `dsm2ui`. These cover the **standard DSM2 calibration/validation network**
(~60 EC stations, ~30 flow stations, ~20 stage stations). The `obs_station_id` values
in these CSVs already match the DSS B-parts written by `setup-from-datastore`.

**Use the bundled defaults when** your datastore contains the same station IDs as the
standard Delta network (CDEC/USGS/DWR stations like `MALUPPER`, `ANHUPPER`, `FPT`, etc.).
You do not need to generate any location CSVs.

**Generate custom location CSVs when** you have stations that are not in the bundled
defaults (e.g. a local project network or additional monitoring sites).

### Generating location CSVs from the datastore

Use the following two-step process to build a location CSV from your datastore and wire
it directly into the config:

```bat
# Step 1 — extract station metadata (obs_station_id, lat/lon) from the datastore.
# Passing only --stations reads the inventory CSV and nothing else — it does NOT
# read any time-series data files, so it is fast even over a network share.
dsm2ui datastore extract ec ^
    --repo \\cnrastore-bdo\Modeling_Data\repo\continuous ^
    --stations stations_ec.csv

# Step 2 — snap stations to DSM2 channels to assign dsm2_id
#   (bundled GeoJSON works for the standard Delta grid)
dsm2ui calib stations-csv ^
    stations_ec.csv ^
    dsm2ui/dsm2gis/dsm2_channels_centerlines_8_2.geojson ^
    location_ec.csv

# Step 3 — generate config using the custom location CSV
dsm2ui calib postpro setup-from-datastore ^
    -s D:/studies/historical ^
    -d \\cnrastore-bdo\Modeling_Data\repo\continuous ^
    -o postpro_config.yml ^
    --vartype EC ^
    --location-file EC=location_ec.csv
```

Step 2 writes `location_ec_unmatched.csv` for any stations that could not be snapped
within 100 ft of a channel. Use `--distance-tolerance 200` to cast a wider net.

Alternatively, if you already have `postpro_config.yml`, just edit `location_files_dict`
directly:

```yaml
location_files_dict:
  EC: D:/my_project/location_ec.csv
  FLOW: null      # keep bundled default for flow
  STAGE: null
```

### Location CSV format

The location CSV maps DSM2 output B-parts to observed station IDs:

```
dsm2_id,obs_station_id,station_name,subtract,time_window_exclusion_list,threshold_value
RSAC075,MALUPPER,Sacramento R at Mallard Island,NO,"2016-01-02 08:45:00_2016-01-16 19:45:00",
RSAN007,ANHUPPER,San Joaquin R at Antioch,NO,,
```

| Column | Required | Description |
|--------|----------|-------------|
| `dsm2_id` | Yes | DSM2 output B-part (or arithmetic expression) used to look up model data |
| `obs_station_id` | Yes | Observed DSS B-part (or arithmetic expression) |
| `station_name` | Yes | Human-readable station name for plot titles |
| `subtract` | Yes | `NO` (use `YES` only for legacy gate/diversion logic) |
| `time_window_exclusion_list` | No | Bad-data periods to mask. Each period is `START_TIMESTAMP_END_TIMESTAMP` (underscore-separated). Multiple periods are comma-separated. |
| `threshold_value` | No | Absolute value threshold above which data is treated as suspect |

Rows starting with `#` are skipped (useful for temporarily disabling a station).

### Excluding bad data windows

Add comma-separated time windows to the `time_window_exclusion_list` column using
`START_END` format with an underscore separator:

```
2016-01-02 08:45:00_2016-01-16 19:45:00,2017-08-20 08:15:00_2017-09-30 18:15:00
```

These windows are masked from both the plots and the metric calculations.

### Expression stations (computed flows)

Some stations represent arithmetic combinations of measurements — for example, a net
export flow or the difference between two gauges. Write the expression directly in
`dsm2_id` or `obs_station_id`:

| Column value | Meaning |
|---|---|
| `FPT` | Load station `FPT` directly from DSS |
| `-VCU` | Negate the `VCU` time series |
| `SDC-GES` | Subtract `GES` from `SDC` |
| `RSAC128-RSAC123` | Difference of two Sacramento River stations |

Standard Python arithmetic (`+`, `-`, `*`, `/`, parentheses, numeric literals) is
supported. Each identifier is looked up as a DSS B-part in the same DSS file.

Example row for a net flow station:

```
SDC-GES,SDC-GES,Sacramento R minus Georgiana Slough,NO,,
```

See [station-math-expressions-plan.md](station-math-expressions-plan.md) for full details.

---

## Advanced topics

### Time window control

The plot time window is read from the echo file `START_DATE` / `END_DATE` by default.
Override it at config generation time:

```bat
dsm2ui calib postpro setup-from-datastore ^
    -s D:/studies/historical ^
    -d D:/datastore ^
    -o postpro_config.yml ^
    --timewindow "01OCT2020 - 30SEP2022"
```

Or edit `timewindow_dict` in the YAML directly after generation.

### Godin filter warmup

The Godin tidal filter (cascaded 30 hr + 24 hr) needs at least **2 months** of data before
the start of the plot time window. Ensure your DSM2 run starts at least 2 months before the
`timewindow_dict` window starts — and that observed DSS data is available for the same
pre-window period.

If the warmup data is missing, tidally-averaged values will be `NaN` at the start of the
plot even though instantaneous data appears correct.

### Cache management

The post-processing cache is stored in a `diskcache` directory alongside the config file.
Cache entries are keyed by `/{BPART}/{CPART}/{EPART}/`.

- `run observed` always rewrites the observed cache.
- `run model` clears and rewrites the model cache by default.
- Add `--skip-cached` to reuse existing cached data for either step.
- To force a full recompute, delete the `diskcache/` directory or run without `--skip-cached`.

### Selecting vartypes at plot time

The interactive viewer and `run plots` can be restricted to a subset of vartypes:

```bat
dsm2ui calib ui plot postpro_config.yml --vartype EC
dsm2ui calib ui plot postpro_config.yml --vartype FLOW --vartype STAGE
```

### Options overrides

Override any `options_dict` key on the command line without editing the YAML:

```bat
dsm2ui calib ui plot postpro_config.yml ^
    --option write_html=false ^
    --option include_kde_plots=false
```

---

## Troubleshooting

### Plots are missing for some stations

Check the log output for `WARNING` messages about missing B-parts. The model DSS B-part
must exactly match the `dsm2_id` column (case-insensitive in DSS lookup, but must be
a valid B-part in the file). Browse your DSS output with:

```bat
dsm2ui ui dss D:/studies/historical/output/hist_fc_mss_qual.dss
```

### Tidally-averaged values are NaN at the start

Godin filter warmup is insufficient. Extend the DSM2 run start date (or the observed DSS
coverage) to be at least 2 months before the `timewindow_dict` window start.

### Cache is stale after observed data update

Delete or clear the cache, then re-run `run observed`:

```bat
dsm2ui calib postpro run observed postpro_config.yml
```

Or delete `diskcache/` in the config directory to start fresh.

### `pyhecdss` logging noise in terminal output

`pyhecdss` emits spurious `--- Logging error ---` messages at `DEBUG` level. These are
harmless. To suppress them, set log level to `INFO` (the default) or add to your code:

```python
import logging
logging.getLogger("pyhecdss").setLevel(logging.WARNING)
```

### Expression station not found

If a `dsm2_id` or `obs_station_id` expression contains a B-part that is absent from the
DSS file, `postpro` logs a warning and skips that station. Verify each component B-part
exists using `dsm2ui ui dss`.

### `FileNotFoundError` for the echo file

`setup` and `setup-from-datastore` look for a `{module}*echo*.inp` file in
`study_folder/output/`. If the run used a non-standard naming convention, copy or
rename the echo file to match that pattern, e.g. `qual_echo.inp`.

---

## Quick reference

```bat
conda activate dsm2ui

# ===========================================================================
# PATH A: Setup using the DMS Datastore (share drive)
# Note: -d must point to the parent of screened/, not screened/ itself.
# e.g. \\cnrastore-bdo\Modeling_Data\repo\continuous  (not ...\continuous\screened)
# ===========================================================================

# --- Generate config (EC from a qual run; bundled default station list) ---
dsm2ui calib postpro setup-from-datastore ^
    -s D:/studies/historical -s D:/studies/scenario_a ^
    -d "\\cnrastore-bdo\Modeling_Data\repo\continuous" ^
    -o postpro_config.yml ^
    --vartype EC -m qual

# --- Generate config with custom location CSV ---
#   Step 1: extract station metadata only (fast — reads inventory, not data files)
dsm2ui datastore extract ec ^
    --repo "\\cnrastore-bdo\Modeling_Data\repo\continuous" ^
    --stations stations_ec.csv
#   Step 2: snap stations to DSM2 channels to assign dsm2_id
dsm2ui calib stations-csv ^
    stations_ec.csv ^
    dsm2ui/dsm2gis/dsm2_channels_centerlines_8_2.geojson ^
    location_ec.csv
#   Step 3: generate config referencing the custom station list
dsm2ui calib postpro setup-from-datastore ^
    -s D:/studies/historical ^
    -d "\\cnrastore-bdo\Modeling_Data\repo\continuous" ^
    -o postpro_config.yml ^
    --vartype EC -m qual ^
    --location-file EC=location_ec.csv

# ===========================================================================
# PATH B: Setup using an existing postprocessing folder
# ===========================================================================

dsm2ui calib postpro setup ^
    -s D:/studies/historical -s D:/studies/scenario_a ^
    -p D:/postprocessing ^
    -o postpro_config.yml ^
    -m qual

# ===========================================================================
# Processing pipeline (same for both paths)
# ===========================================================================

# --- Process observed data (run once; re-run only when observed DSS changes) ---
dsm2ui calib postpro run observed postpro_config.yml

# --- Process model output (run after each new DSM2 run) ---
dsm2ui calib postpro run model postpro_config.yml

# --- Generate plots ---
dsm2ui calib postpro run plots postpro_config.yml --workers 4

# --- Review interactively ---
dsm2ui calib ui plot postpro_config.yml

# --- Rebuild plots faster without reprocessing ---
dsm2ui calib postpro run plots postpro_config.yml --workers 4 --skip-cached

# --- FLOW/STAGE from hydro run (separate config) ---
dsm2ui calib postpro setup-from-datastore ^
    -s D:/studies/historical ^
    -d "\\cnrastore-bdo\Modeling_Data\repo\continuous" ^
    -o postpro_hydro.yml ^
    --vartype FLOW --vartype STAGE -m hydro
dsm2ui calib postpro run observed postpro_hydro.yml
dsm2ui calib postpro run model   postpro_hydro.yml
dsm2ui calib postpro run plots   postpro_hydro.yml --workers 4
```
