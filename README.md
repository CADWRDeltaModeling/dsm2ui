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

### Interactive viewers

| Command | Arguments | Description |
|---|---|---|
| `output-ui` | `ECHO_FILES...` `[--channel-shapefile FILE]` | Interactive map + time-series viewer for DSM2 output files |
| `tide-ui` | `TIDEFILES...` `[--channel-file FILE]` | Interactive map + time-series viewer for DSM2 HDF5 tidefiles |
| `xsect-ui` | `TIDEFILE` | Cross-section viewer for a DSM2 tidefile |
| `dss-ui` | *(args per dvue)* | Generic HEC-DSS file browser and plotter |
| `calib-ui` | `CONFIG_FILE` `[--base_dir DIR]` | Calibration plot viewer driven by a JSON config |

```bash
dsm2ui output-ui hydro_echo.inp --channel-shapefile channels.geojson
dsm2ui tide-ui hydro.tidefile qual.tidefile --channel-file channels.geojson
dsm2ui xsect-ui hydro.tidefile
dsm2ui calib-ui calib_config.json
```

### Channel mapping

| Command | Arguments | Description |
|---|---|---|
| `channel-map` | `FLOWLINE_SHP HYDRO_ECHO` `[-c MANNING\|DISPERSION\|LENGTH\|ALL]` `[-b BASE_FILE]` | Map channels colored by Manning, dispersion, or length |
| `node-map` | `NODE_SHP HYDRO_ECHO` | Node map showing flow splits |

```bash
dsm2ui channel-map flowlines.shp hydro_echo.inp -c MANNING
dsm2ui node-map nodes.shp hydro_echo.inp
```

### Calibration & post-processing

| Command | Arguments | Description |
|---|---|---|
| `postpro` | `PROCESS_NAME CONFIG_JSON` `[--dask]` | Run a DSM2 calibration post-processing step (`observed`, `model`, `plots`, `heatmaps`, `validation_bar_charts`, `copy_plot_files`) |
| `checklist` | `PROCESS_NAME CONFIG_JSON` | Run a DSM2 checklist step (`resample`, `extract`, `plot`) |
| `geo-heatmap` | `SUMMARY_FILE STATION_LOC_FILE` `[--metric NMSE]` | Geographic heatmap of calibration metrics (e.g. NMSE) |

```bash
dsm2ui postpro plots calib_config.json
dsm2ui checklist plot checklist_config.json
dsm2ui geo-heatmap metrics_summary.csv station_locs.csv --metric NMSE
```

### Channel geometry

| Command | Arguments | Description |
|---|---|---|
| `mann-disp` | `CHAN_TO_GROUP GROUP_MANN_DISP CHANNELS_IN CHANNELS_OUT` | Apply Manning/dispersion values to DSM2 channel input tables |
| `chan-orient` | *(see `--help`)* | Generate a channel orientation file from geometry |
| `geolocate` | *(see `--help`)* | Geolocate DSM2 output locations using channel centerlines |
| `stations-out` | `STATIONS_CSV CENTERLINES_GEOJSON OUTPUT_FILE` `[--distance-tolerance INT]` | Build a DSM2 output-locations file from a station lat/lon CSV |

```bash
dsm2ui mann-disp chan_groups.csv group_mann_disp.csv channels.inp channels_out.inp
dsm2ui stations-out stations.csv channels.geojson output_locs.txt
```

### Datastore export

| Command | Arguments | Description |
|---|---|---|
| `ds2dss` | `DATASTORE_DIR DSSFILE PARAM` `[--repo-level screened]` `[--unit-name NAME]` | Export a parameter from a DMS Datastore to a DSS file |
| `ds2stations` | `DATASTORE_DIR STATIONFILE PARAM` | Write station IDs and lat/lon to a CSV from a Datastore |

Valid `PARAM` values: `elev`, `predictions`, `flow`, `temp`, `do`, `ec`, `ssc`, `turbidity`, `ph`, `velocity`, `cla`

```bash
dsm2ui ds2dss /data/datastore output.dss ec
dsm2ui ds2stations /data/datastore stations.csv ec
```

### DeltaCD (crop model)

| Command | Arguments | Description |
|---|---|---|
| `dcd-ui` | `NC_FILES...` `[--geojson-file FILE]` | Full DeltaCD netCDF data viewer |
| `dcd-nodes` | `NC_FILES...` `[--nodes-file FILE]` | DeltaCD nodes viewer |
| `dcd-map` | *(see `--help`)* | Geographic map of DeltaCD data |

```bash
dsm2ui dcd-ui deltacd.nc --geojson-file delta.geojson
dsm2ui dcd-nodes deltacd.nc
```

### PTM

| Command | Arguments | Description |
|---|---|---|
| `ptm-animate` | `PTM_FILE HYDRO_FILE FLOWLINES_SHP` | Animate PTM particle tracks |

```bash
dsm2ui ptm-animate ptm_output.h5 hydro.tidefile flowlines.shp
```

## Related Repositories

- [pydelmod](https://github.com/CADWRDeltaModeling/pydelmod) – Delta Modeling utilities
- [dvue](https://github.com/CADWRDeltaModeling/dvue) – Data visualization UI framework
- [dsm2-calsim-analysis](https://github.com/CADWRDeltaModeling/dsm2-calsim-analysis) – DSM2/CalSim analysis tools

## License

MIT
