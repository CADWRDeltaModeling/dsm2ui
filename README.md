# dsm2ui

Python user interface and analysis tools for DSM2 (Delta Simulation Model II).

## Overview

This package provides interactive UI components and analysis utilities for DSM2 hydro/water-quality modeling, including:

- **`dsm2ui.dsm2ui`** – Interactive map and time-series viewer for DSM2 output channels and tidefiles
- **`dsm2ui.dssui`** – Generic HEC-DSS file browser and plotter
- **`dsm2ui.calib.postpro_dsm2`** – Calibration plot generation and post-processing
- **`dsm2ui.deltacdui`** – DeltaCD (crop model) netCDF data UI
- **`dsm2ui.dsm2gis`** – GIS utilities for DSM2 channel geometry
- **`dsm2ui.calib.checklist_dsm2`** – DSM2 run checklist utilities
- **`dsm2ui.cli`** – Command-line interface (`dsm2ui` command)

## Installation

```bash
pip install git+https://github.com/CADWRDeltaModeling/dsm2ui
```

Or in development mode:

```bash
git clone https://github.com/CADWRDeltaModeling/dsm2ui
cd dsm2ui
pip install -e .
```

## Dependencies

- [`dvue`](https://github.com/CADWRDeltaModeling/dvue) – Data visualization UI framework
- `panel`, `holoviews`, `geoviews`, `hvplot` – Interactive visualization
- `pyhecdss` – HEC-DSS file I/O
- `vtools` – Time series utilities for California water resources
- `pydsm` – Python interface to DSM2 tidefiles

## Usage

### Python API

```python
from dsm2ui.dsm2ui import build_output_plotter
from dvue.dataui import DataUI

plotter = build_output_plotter("hydro_echo.inp", channel_shapefile="channels.geojson")
DataUI(plotter).create_view().show()
```

```python
from dsm2ui.dssui import DSSDataUIManager
from dvue.dataui import DataUI

manager = DSSDataUIManager("output.dss")
DataUI(manager).create_view().show()
```

See the [`examples/`](examples/) directory for complete scripts and notebooks.

---

## CLI Commands

After installation the `dsm2ui` command is available. Run `dsm2ui --help` to see all sub-commands.

---

### Interactive viewers — `ui`

#### `ui output`

Show an interactive map and time-series viewer for DSM2 output channel files. Reads DSM2 echo files containing an `OUTPUT_CHANNEL` table and displays output locations on a channel map alongside time-series plots.

| Argument / Option | Type | Description |
|---|---|---|
| `ECHO_FILES...` | paths (one or more) | DSM2 echo files; at least one must contain a `CHANNEL` table (hydro echo) |
| `--channel-shapefile FILE` | path | GeoJSON of channel centerlines for map display |

```bash
dsm2ui ui output hydro_echo.inp
dsm2ui ui output hydro_echo.inp qual_echo.inp --channel-shapefile channels.geojson
```

#### `ui tide`

Show an interactive map and time-series viewer for DSM2 HDF5 tidefile outputs.

| Argument / Option | Type | Description |
|---|---|---|
| `TIDEFILES...` | paths (one or more) | DSM2 HDF5 tidefile(s) |
| `--channel-file FILE` | path | GeoJSON of channel centerlines for map display |

```bash
dsm2ui ui tide hydro.h5
dsm2ui ui tide hydro.h5 qual.h5 --channel-file channels.geojson
```

#### `ui xsect`

Show a cross-section viewer for a DSM2 HDF5 tidefile.

| Argument | Type | Description |
|---|---|---|
| `TIDEFILE` | path | DSM2 HDF5 tidefile |

```bash
dsm2ui ui xsect hydro.h5
```

#### `ui map`

Show an interactive DSM2 network map. Use `--channel` for a channel map colored by Manning's n, dispersion, or length. Use `--node` for a node flow-split map. Both flags may be combined. At least one of `--channel` or `--node` is required.

| Argument / Option | Type | Default | Description |
|---|---|---|---|
| `HYDRO_ECHO` | path | — | DSM2 hydro echo file |
| `--channel FILE` | path | — | Flowline shapefile for channel map |
| `--node FILE` | path | — | Node shapefile for node flow-split map |
| `-c / --colored-by` | `MANNING\|DISPERSION\|LENGTH\|ALL` | `MANNING` | Attribute to color channels by |
| `-b / --base-file FILE` | path | — | Base hydro echo file for comparison overlay |

```bash
dsm2ui ui map hydro_echo.inp --channel flowlines.shp
dsm2ui ui map hydro_echo.inp --channel flowlines.shp -c DISPERSION
dsm2ui ui map hydro_echo.inp --channel flowlines.shp --node nodes.shp
dsm2ui ui map hydro_echo.inp --channel flowlines.shp -b base_echo.inp -c ALL
```

#### `dss-ui`

Generic HEC-DSS file browser and plotter. Optionally overlay station locations from a GeoJSON file.

| Argument / Option | Type | Default | Description |
|---|---|---|---|
| `DSSFILES...` | paths (one or more) | — | HEC-DSS file(s) to browse |
| `--location-file FILE` | path | — | GeoJSON with station locations (`lat`/`lon` columns) |
| `--location-id-column COL` | string | `station_id` | Column in location file matching DSS station IDs |
| `--station-id-column COL` | string | `B` | DSS pathname part used as station identifier |

```bash
dsm2ui dss-ui output.dss
dsm2ui dss-ui output.dss --location-file stations.geojson
```

---

### Calibration & post-processing — `calib`

#### `calib setup`

Write a template `calib_config.yml` to get started.

| Option | Default | Description |
|---|---|---|
| `-o / --output FILE` | `calib_config.yml` | Destination path for the template config |
| `--force` | off | Overwrite an existing file |

```bash
dsm2ui calib setup
dsm2ui calib setup --output my_run/calib_config.yml
```

#### `calib run`

Run a DSM2 calibration variation: set up the study directory, execute the model (hydro + qual), and compute EC slope metrics against observed data.

| Option | Default | Description |
|---|---|---|
| `--config FILE` | `calib_config.yml` | YAML configuration file |
| `--setup-only` | off | Create study directory and batch file, then stop |
| `--run-base` | off | Re-run the base study before computing metrics |
| `--metrics-only` | off | Recompute metrics from existing DSS output; skip model run |
| `--plot` | off | Generate per-station diagnostic PNGs from existing output |
| `--log-file FILE` | `<var_dir>/run.log` | Write model output to this log file |
| `--log-level LEVEL` | `INFO` | Logging verbosity (`DEBUG\|INFO\|WARNING\|ERROR`) |

```bash
dsm2ui calib run --config calib_config.yml
dsm2ui calib run --config calib_config.yml --metrics-only
dsm2ui calib run --config calib_config.yml --plot
```

#### `calib optimize`

Optimize DSM2 DISPERSION or MANNING values to minimise EC slope deviation from 1.0 using gradient-based (L-BFGS-B), Nelder-Mead, or differential evolution methods (configured in the YAML).

| Option | Default | Description |
|---|---|---|
| `--config FILE` | `calib_config.yml` | YAML configuration file |
| `--dry-run` | off | Evaluate starting point only; skip the optimization loop |
| `--skip-init` | off | Reuse existing `eval_base` output (e.g. after `--dry-run` or a crash) |
| `--log-level LEVEL` | `INFO` | Logging verbosity |

```bash
dsm2ui calib optimize --config calib_config.yml
dsm2ui calib optimize --config calib_config.yml --dry-run
dsm2ui calib optimize --config calib_config.yml --skip-init
```

#### `calib cascade`

Run a downstream-to-upstream cascading optimization sequence. Each stage frees only the channel groups closest to the target station(s) for that stage, freezing all other groups at the best values from the previous stage. Results are checkpointed after every stage; use `--resume` to continue an interrupted run.

| Option | Default | Description |
|---|---|---|
| `-c / --config FILE` | *(required)* | Cascade meta-config YAML (`calib_meta_*.yml`) |
| `--resume` | off | Skip completed stages found in `cascade_checkpoint.yml` |
| `--dry-run` | off | Evaluate starting point only in each stage; no optimization |
| `--skip-init` | off | Reuse existing `eval_base` DSS output for each stage |
| `--log-level LEVEL` | `INFO` | Logging verbosity |

```bash
dsm2ui calib cascade -c calib_meta_config.yml
dsm2ui calib cascade -c calib_meta_config.yml --resume
dsm2ui calib cascade -c calib_meta_config.yml --dry-run
```

#### `calib stations-csv`

Build `calibration_ec_stations.csv` from a datastore stations CSV by snapping each station to the nearest DSM2 channel. DSM2 output names (`dsm2_id`) are written in uppercase. Stations that cannot be snapped within the tolerance are written to a separate unmatched CSV for review.

| Argument / Option | Type | Default | Description |
|---|---|---|---|
| `STATIONS_CSV` | path | — | Enriched stations CSV from `datastore extract --stations` |
| `CENTERLINES_GEOJSON` | path | — | DSM2 channel centerlines GeoJSON (UTM Zone 10N) |
| `OUTPUT_CSV` | path | — | Output `calibration_ec_stations.csv` |
| `--distance-tolerance INT` | int | `100` | Max distance (ft) for a station to be considered matched |
| `--unmatched FILE` | path | `<output>_unmatched.csv` | CSV for stations that could not be snapped |

```bash
dsm2ui calib stations-csv stations_ec.csv \
    dsm2ui/dsm2gis/dsm2_channels_centerlines_8_2.geojson \
    calibration_ec_stations.csv

# Wider search radius
dsm2ui calib stations-csv stations_ec.csv channels.geojson cal_stations.csv \
    --distance-tolerance 200 --unmatched unmatched.csv
```

#### `calib checklist`

Run a DSM2 calibration checklist step driven by a JSON config.

| Argument | Description |
|---|---|
| `PROCESS_NAME` | Step to run: `resample`, `extract`, or `plot` |
| `JSON_CONFIG_FILE` | Path to the JSON config file |

```bash
dsm2ui calib checklist resample checklist_config.json
dsm2ui calib checklist extract checklist_config.json
dsm2ui calib checklist plot checklist_config.json
```

#### `calib postpro run`

Run a DSM2 post-processing step driven by a JSON config.

| Argument / Option | Description |
|---|---|
| `PROCESS_NAME` | Step: `observed`, `model`, `plots`, `heatmaps`, `validation_bar_charts`, `copy_plot_files` |
| `JSON_CONFIG_FILE` | Path to the JSON config file |
| `--skip-cached` | Use the existing post-processing cache instead of clearing and recomputing (applies to `model` and `plots` steps; by default the cache is cleared on each run) |
| `--workers N` | Number of parallel worker processes for the `plots` step (default: `1` = sequential). Each worker is a separate OS process with its own webdriver instance. Recommended: `4`–`8` for a typical workstation. |

```bash
dsm2ui calib postpro run observed calib_config.json
dsm2ui calib postpro run model calib_config.json         # clears cache, reprocesses all
dsm2ui calib postpro run model calib_config.json --skip-cached  # reuses existing cache
dsm2ui calib postpro run plots calib_config.json         # sequential (default)
dsm2ui calib postpro run plots calib_config.json --workers 4    # 4 parallel workers
dsm2ui calib postpro run plots calib_config.json --workers 4 --skip-cached  # parallel + reuse cache
dsm2ui calib postpro run heatmaps calib_config.json
```

#### `calib postpro setup`

Generate a calib-ui YAML config from study folders and an optional postprocessing directory.

| Option | Default | Description |
|---|---|---|
| `-s / --study DIR` | *(required, repeatable)* | Study folder path (repeat `-s` for multiple) |
| `-p / --postprocessing DIR` | — | Postprocessing folder (contains `location_info/` and `observed_data/`); if omitted, bundled defaults are used |
| `-o / --output FILE` | *(required)* | Output YAML config path |
| `-m / --module` | `hydro` | DSM2 module whose DSS output to reference (`hydro\|qual\|gtm`) |
| `--output-folder DIR` | `./plots/` | Plot output folder written into the YAML |
| `--timewindow "START - END"` | — | Override simulation time window (e.g. `"01OCT2020 - 30SEP2022"`) |
| `--location-file VARTYPE=PATH` | *(repeatable)* | Override a vartype location CSV (e.g. `EC=/path/ec.csv`) |
| `--observed-file VARTYPE=PATH` | *(repeatable)* | Override a vartype observed DSS path (e.g. `EC=/path/ec.dss`) |

```bash
dsm2ui calib postpro setup \
    -s study1/ -s study2/ \
    -p postprocessing/ \
    -o postpro_config.yml

dsm2ui calib postpro setup \
    -s study1/ \
    -o postpro_config.yml \
    --timewindow "01OCT2020 - 30SEP2022" \
    --observed-file EC=/data/ec_cal.dss
```

#### `calib postpro setup-from-datastore`

Generate a calib-ui YAML config by extracting observed data directly from a DMS Datastore. Extracts one DSS file per requested vartype, then builds a `postpro_config.yml` referencing those files. Bundled default station CSVs are used for location files — no `--postprocessing` folder needed.

| Option | Default | Description |
|---|---|---|
| `-s / --study DIR` | *(required, repeatable)* | Study folder path (repeat `-s` for multiple) |
| `-d / --datastore DIR` | *(required)* | DMS Datastore directory (must contain `inventory_datasets_*.csv`) |
| `-o / --output FILE` | *(required)* | Output YAML config path |
| `--dss-dir DIR` | same dir as `--output` | Directory for extracted observed DSS files |
| `-m / --module` | `qual` | DSM2 module whose DSS output to reference (`hydro\|qual\|gtm`) |
| `--vartype TYPE` | `EC` *(repeatable)* | Vartype(s) to extract (e.g. `--vartype EC --vartype FLOW`) |
| `--repo-level` | `screened` | Datastore repository level (`screened\|raw`) |
| `--output-folder DIR` | `./plots/` | Plot output folder written into the YAML |
| `--timewindow "START - END"` | — | Override simulation time window |

```bash
dsm2ui calib postpro setup-from-datastore \
    -s D:/delta/dsm2_studies/studies/historical \
    -d D:/delta/dms_datastore \
    -o postpro_config.yml \
    --vartype EC

# Multiple vartypes with time window override
dsm2ui calib postpro setup-from-datastore \
    -s study/ \
    -d /data/datastore \
    -o postpro_config.yml \
    --vartype EC --vartype FLOW \
    --timewindow "01OCT2020 - 30SEP2022"
```

#### `calib ui plot`

Launch the interactive calibration plot viewer driven by a postpro YAML config.

| Argument / Option | Default | Description |
|---|---|---|
| `CONFIG_FILE` | *(required)* | Path to the postpro YAML config |
| `--base_dir DIR` | — | Override base directory for relative paths in the config |
| `--clear-cache` | off | Clear all post-processing caches before launching |
| `--vartype TYPE` | *(repeatable)* | Restrict active vartypes (e.g. `--vartype EC`) |
| `--option KEY=VALUE` | *(repeatable)* | Override an `options_dict` entry (e.g. `--option write_html=false`) |

```bash
dsm2ui calib ui plot postpro_config.yml
dsm2ui calib ui plot postpro_config.yml --vartype EC --clear-cache
dsm2ui calib ui plot postpro_config.yml --option write_html=false
```

#### `calib ui heatmap`

Show a geographic heatmap of calibration metrics.

| Argument / Option | Default | Description |
|---|---|---|
| `SUMMARY_FILE` | *(required)* | CSV with per-station metric values |
| `STATION_LOCATION_FILE` | *(required)* | CSV/GeoJSON with station locations |
| `--metric NAME` | `NMSE` | Metric column name to visualize |

```bash
dsm2ui calib ui heatmap metrics_summary.csv station_locs.csv
dsm2ui calib ui heatmap metrics_summary.csv station_locs.csv --metric slope
```

#### Building `calibration_ec_stations.csv` from scratch

The calibration station CSV (`ec_stations_csv` in the config) maps DSM2 output names to observed data station IDs. To generate it from a DMS Datastore:

```bash
# Step 1 — extract station metadata from the datastore
dsm2ui datastore extract ec \
    --repo y:/repo/continuous \
    --stations stations_ec.csv

# Step 2 — snap stations to DSM2 channels and assemble the calibration CSV
dsm2ui calib stations-csv \
    stations_ec.csv \
    dsm2ui/dsm2gis/dsm2_channels_centerlines_8_2.geojson \
    calibration_ec_stations.csv
```

Stations that cannot be snapped within the tolerance (default 100 ft) are written to
`calibration_ec_stations_unmatched.csv` for review. Use `--distance-tolerance 200` to
cast a wider net. The output CSV has blank defaults for manual fields (`note`, `subtract`,
`time_window_exclusion_list`, `Calibration Period`, etc.) which should be filled in before use.

#### Computed (expression) stations in the station CSV

Some calibration stations represent a mathematical combination of measurements rather than a single sensor — for example, a negated flow or the difference between two upstream/downstream gauges. These are supported by writing an arithmetic expression directly into the `obs_station_id` or `dsm2_id` column of the station CSV:

| Column value | Behaviour |
|---|---|
| `FPT` (plain identifier) | Load station `FPT` directly from the DSS file |
| `-VCU` | Negate the `VCU` time series |
| `SDC-GES` | Subtract the `GES` time series from `SDC` |
| `RSAC128-RSAC123` | Subtract `RSAC123` from `RSAC128` |

Standard Python arithmetic (`+`, `-`, `*`, `/`, parentheses, numeric constants) is supported. Each identifier in the expression is looked up as a B-part station name in the same DSS file.

See [station-math-expressions-plan.md](station-math-expressions-plan.md) for full details and examples.

---

### Channel geometry

#### `mann-disp`

Apply group-based Manning's n and dispersion values to a DSM2 channels input file.

| Argument | Description |
|---|---|
| `CHAN_TO_GROUP` | CSV mapping channel numbers to group names |
| `GROUP_MANN_DISP` | CSV mapping group names to Manning and dispersion values |
| `CHANNELS_IN` | Input DSM2 channels `.inp` file |
| `CHANNELS_OUT` | Output DSM2 channels `.inp` file (modified) |

```bash
dsm2ui mann-disp chan_to_group.csv group_mann_disp.csv \
    channel_std_delta_grid.inp channel_std_delta_grid_out.inp
```

---

### Station mapping — `station-map`

Convert between real-world lat/lon station locations and DSM2 channel-output positions (`CHAN_NO` + `DISTANCE`), and vice versa.

The bundled centerlines file for DSM2 v8.2 is:
`dsm2ui/dsm2gis/dsm2_channels_centerlines_8_2.geojson`

#### `station-map to-dsm2`

Snap lat/lon stations to DSM2 channels. Output columns are `NAME` (uppercased station ID), `CHAN_NO`, `DISTANCE` — ready to paste into an `OUTPUT_CHANNEL` DSM2 input section. Stations that cannot be snapped within the tolerance are written to a separate CSV.

| Argument / Option | Type | Default | Description |
|---|---|---|---|
| `STATIONS_CSV` | path | — | Station CSV with `station_id`, `lat`, `lon` columns |
| `CENTERLINES_GEOJSON` | path | — | DSM2 channel centerlines GeoJSON (UTM Zone 10N) |
| `OUTPUT_CSV` | path | — | Output CSV with `NAME`, `CHAN_NO`, `DISTANCE` |
| `--distance-tolerance INT` | int | `100` | Max distance (ft) from centerline to consider a match |
| `--unmatched FILE` | path | `<output>_unmatched.csv` | CSV for stations that could not be snapped |

```bash
dsm2ui station-map to-dsm2 stations.csv channels_centerlines.geojson output_locs.csv

# Wider tolerance; explicit unmatched output
dsm2ui station-map to-dsm2 stations.csv channels_centerlines.geojson output_locs.csv \
    --distance-tolerance 200 --unmatched unmatched.csv
```

#### `station-map from-dsm2`

Geolocate DSM2 `OUTPUT_CHANNEL` stations from an echo file. Reads `CHANNEL` and `OUTPUT_CHANNEL` tables, interpolates each station's position along its channel centerline, and writes a GeoJSON with `NAME`, `CHAN_NO`, `DISTANCE`, and point geometry (EPSG:26910).

| Argument | Description |
|---|---|
| `ECHO_FILE` | DSM2 echo/input file containing `CHANNEL` and `OUTPUT_CHANNEL` tables |
| `CENTERLINES_GEOJSON` | DSM2 channel centerlines GeoJSON |
| `OUTPUT_GEOJSON` | Output GeoJSON of geolocated stations |

```bash
dsm2ui station-map from-dsm2 hydro_echo.inp channels_centerlines.geojson stations.geojson
```

---

### Datastore export — `datastore`

Export time series from a [DMS Datastore](https://github.com/CADWRDeltaModeling/dms_datastore) to DSS and/or extract station metadata to CSV.

#### `datastore extract`

| Argument / Option | Type | Default | Description |
|---|---|---|---|
| `PARAM` | choice | *(required)* | Parameter: `elev`, `predictions`, `flow`, `temp`, `do`, `ec`, `ssc`, `turbidity`, `ph`, `velocity`, `cla` |
| `--repo DIR` | path | *(required)* | DMS Datastore directory (must contain `inventory_datasets_*.csv`) |
| `--output FILE` | path | — | DSS file to write extracted time series to |
| `--stations FILE` | path | — | Write station metadata CSV (`station_id`, `station_name`, `agency`, `lat`, `lon`, `utm_easting`, `utm_northing`) |
| `--repo-level` | choice | `screened` | Datastore repository level |
| `--unit-name NAME` | string | — | Override the unit name written to the DSS file |

At least one of `--output` or `--stations` must be provided. The `--stations` output is the required input to `calib stations-csv` and `station-map to-dsm2`.

```bash
# Station metadata CSV only
dsm2ui datastore extract ec --repo /data/datastore --stations stations_ec.csv

# DSS time series only
dsm2ui datastore extract ec --repo /data/datastore --output ec.dss

# Both
dsm2ui datastore extract ec --repo /data/datastore --output ec.dss --stations stations_ec.csv
```

---

### DeltaCD (crop model) — `dcd`

#### `dcd ui`

Full DeltaCD netCDF data viewer.

| Argument / Option | Default | Description |
|---|---|---|
| `NC_FILES...` | *(required)* | DeltaCD netCDF file(s) |
| `--geojson_file FILE` | — | GeoJSON with area geometries |

```bash
dsm2ui dcd ui deltacd.nc
dsm2ui dcd ui deltacd.nc --geojson_file delta_subareas.geojson
```

#### `dcd nodes`

DeltaCD nodes viewer (for netCDF files using `node` as the station dimension).

| Argument / Option | Default | Description |
|---|---|---|
| `NC_FILES...` | *(required)* | DeltaCD netCDF file(s) |
| `--nodes_file FILE` | — | GeoJSON with node geometries |

```bash
dsm2ui dcd nodes deltacd.nc
dsm2ui dcd nodes deltacd.nc --nodes_file dsm2_nodes.geojson
```

#### `dcd map`

Geographic map of DeltaCD node diversions.

| Option | Description |
|---|---|
| `--ncfile FILE` | DeltaCD netCDF file |
| `--nodes_file FILE` | GeoJSON with node geometries |

```bash
dsm2ui dcd map --ncfile deltacd.nc --nodes_file dsm2_nodes.geojson
```

---

### PTM

#### `ptm-animate`

Animate PTM particle tracks on an interactive map.

| Argument | Description |
|---|---|
| `PTM_FILE` | PTM output HDF5 file |
| `HYDRO_FILE` | DSM2 hydro tidefile (HDF5) |
| `FLOWLINES_SHP` | Flowline shapefile for the channel network |

```bash
dsm2ui ptm-animate ptm_output.h5 hydro.h5 flowlines.shp
```

## Related Repositories

- [pydelmod](https://github.com/CADWRDeltaModeling/pydelmod) – Delta Modeling utilities
- [dvue](https://github.com/CADWRDeltaModeling/dvue) – Data visualization UI framework
- [dsm2-calsim-analysis](https://github.com/CADWRDeltaModeling/dsm2-calsim-analysis) – DSM2/CalSim analysis tools

## License

MIT
