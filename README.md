# dsm2ui

Python user interface and analysis tools for DSM2 (Delta Simulation Model II).

## Overview

This package provides interactive UI components and analysis utilities for DSM2 hydro/water-quality modeling, including:

- **`dsm2ui.dsm2ui`** – Interactive map and time-series viewer for DSM2 output channels and tidefiles
- **`dsm2ui.echo_plugin`** – Combined input+output viewer from a DSM2 echo `.inp` file (registry-based)
- **`dsm2ui.input_viewer`** – Dashboard-style channel/node map of a hydro echo file with channel properties, cross sections, boundary-condition and source-flow time series, output locations, and hydraulic structures (devices and operating rules)
- **`dsm2ui.dssui`** – Generic HEC-DSS file browser and plotter
- **`dsm2ui.dssui.dss_registry`** – Registry-based DSS browser (drag-and-drop, dvue plugin API)
- **`dsm2ui.calib.postpro_dsm2`** – Calibration plot generation and post-processing
- **`dsm2ui.deltacdui`** – DeltaCD (crop model) netCDF data UI
- **`dsm2ui.dsm2gis`** – GIS utilities for DSM2 channel geometry
- **`dsm2ui.calib.checklist_dsm2`** – DSM2 run checklist utilities
- **`dsm2ui.cli`** – Command-line interface (`dsm2ui` command)

Detailed guides:

- [README-postpro.md](README-postpro.md) – `calib postpro` calibration/validation
  plot generation, including the no-observed baseline-vs-alternative `setup-compare`
  workflow.
- [README-calibrator.md](README-calibrator.md) – `calib run` / `calib optimize`
  YAML-driven variation runner and DISPERSION/MANNING optimizer.
- [README-integrated-comparison.md](README-integrated-comparison.md) – how to
  combine `pydsm compare-dss`, `dsm2ui calib postpro`, and `pydsm diff` into one
  "what changed, and why" workflow.

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

## DVue Plugin Registration

When installed, **dsm2ui registers automatically as a dvue plugin** via setuptools entry points. This means:

- All dsm2ui readers (HDF5 tidefiles, DSS output, echo files) are auto-discovered by `dvue ui`
- No need to pass `--plugin dsm2ui.dsm2ui` on the command line
- File extensions (`.h5`, `.dss`, `.inp`) are immediately available for drag-and-drop

Example:
```bash
# Readers auto-discovered, no --plugin needed
dvue ui run.h5 hist_qual.dss

# Force per-file reader when multiple readers share .dss
dvue ui dsm2_dss:hist_fc_mss_hydro.dss dss:other_generic.dss

# List all available plugins and readers
dvue list-plugins

# Diagnose plugin/registry issues
dvue diagnose
```

See [dvue plugin entry points documentation](https://github.com/CADWRDeltaModeling/dvue/blob/main/.github/plugin-entry-points.md) for details.

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

#### `ui echo`

Combined **input + output viewer** loaded from one or more DSM2 echo `.inp` files.  Presents boundary-condition input rows (all `BOUNDARY_FLOW`, `BOUNDARY_STAGE`, `SOURCE_FLOW`, `INPUT_GATE`, … tables) and `OUTPUT_CHANNEL` rows in a **single unified catalog**.  Station locations are auto-loaded from the bundled DSM2 channel centre-line GeoJSON.

Drop additional `.inp` files onto the running window to merge more runs into the same catalog.

| Argument / Option | Type | Default | Description |
|---|---|---|---|
| `ECHO_INP...` | paths (zero or more) | — | DSM2 echo `.inp` file(s) |
| `--channel-file FILE` | path | bundled centerlines GeoJSON | Override the default channel geometry |
| `--port PORT` | int | `5006` | Port for the Panel server |
| `--desktop` | flag | off | Open in a native OS window instead of a browser tab |

```bash
# Quickest start — auto-loads bundled geometry
dsm2ui ui echo output/run_hydro_echo.inp

# Load hydro + qual echoes together
dsm2ui ui echo output/run_hydro_echo.inp output/run_qual_echo.inp

# Open in a desktop window
dsm2ui ui echo output/run_hydro_echo.inp --desktop

# Use a custom channel geometry
dsm2ui ui echo output/run_hydro_echo.inp --channel-file my_channels.geojson
```

You can also launch from the generic `dvue ui` entry point. Since dsm2ui registers as a dvue plugin, readers are auto-discovered:

```bash
# With auto-discovery (no --plugin needed)
dvue ui output/run_hydro_echo.inp --desktop

# Force echo reader explicitly for this file (optional)
dvue ui dsm2_echo_inp:output/run_hydro_echo.inp --desktop

# Or explicitly load the plugin (optional, for development)
dvue ui --plugin dsm2ui.echo_plugin output/run_hydro_echo.inp --desktop
```

---

#### `ui dss`

Generic **HEC-DSS file browser** built on the dvue registry pattern.  Enumerates every DSS path in the file and presents them as a browseable catalog.  Optionally loads station geometry for map display.

Drop additional `.dss` files onto the running window to merge them into the same catalog.

| Argument / Option | Type | Default | Description |
|---|---|---|---|
| `DSS_FILE...` | paths (zero or more) | — | HEC-DSS file(s) to browse |
| `--geo-file FILE` | path | — | GeoJSON / CSV / shapefile with station geometry |
| `--geo-id-column COL` | string | `station_id` | Column in the geo file matching DSS B-part station names |
| `--port PORT` | int | `5006` | Port for the Panel server |
| `--desktop` | flag | off | Open in a native OS window |

```bash
# Browse a DSS file (table only, no map)
dsm2ui ui dss output/hist_qual.dss

# Multiple files merged into one catalog
dsm2ui ui dss base.dss variation.dss

# With station geometry for map display
dsm2ui ui dss output.dss --geo-file stations.geojson --geo-id-column STATION_ID

# Desktop window
dsm2ui ui dss output.dss --desktop
```

Alternatively, launch via the generic `dvue ui` entry point. Since dsm2ui registers as a dvue plugin, the reader is auto-discovered:

```bash
# With auto-discovery (no --plugin needed)
dvue ui output.dss

# Force generic DSS reader for this file
dvue ui dss:output.dss

# Force DSM2-filtered DSS reader for this file
dvue ui dsm2_dss:output.dss

# Or explicitly load the plugin (optional, for development)
dvue ui --plugin dsm2ui.dssui.dss_registry output.dss
```

---

#### `ui map`

Show an interactive DSM2 network map colored by Manning's n, dispersion, or length. One or more echo files may be supplied; multiple files each get their own map panel stacked vertically.

All shapefile options default to the bundled DSM2 v8.2 GeoJSONs (`dsm2ui/dsm2gis/`) so no paths are required for a quick look.

| Argument / Option | Type | Default | Description |
|---|---|---|---|
| `HYDRO_ECHO_FILES...` | paths | *(required)* | One or more DSM2 hydro echo files |
| `--channel FILE` | path | bundled centerlines GeoJSON | Channel centerline file for the channel map |
| `--node FILE` | path | bundled node GeoJSON | Node GeoJSON — adds a node flow-split map (single-file mode only) |
| `-c / --colored-by` | `MANNING\|DISPERSION\|LENGTH\|ALL` | `MANNING` | Attribute to color channels by |
| `-b / --base-file FILE` | path | — | Base hydro echo file; map shows differences, hover shows value and base value |

```bash
# Quickest start — uses bundled GeoJSONs automatically
dsm2ui ui map hydro_echo.inp

# Custom GeoJSON; compare two files side-by-side
dsm2ui ui map hydro_echo.inp --channel flowlines.geojson -c DISPERSION
dsm2ui ui map file_a.inp file_b.inp

# Difference map vs a base run
dsm2ui ui map hydro_echo.inp -b base_echo.inp -c ALL

# Also show the node flow-split map
dsm2ui ui map hydro_echo.inp --node nodes.geojson
```

> **Note:** `dsm2ui dss-ui` has been superseded by [`dsm2ui ui dss`](#ui-dss), which uses the
> dvue registry pattern and supports drag-and-drop file loading.  The old command is still
> functional but may be removed in a future release.

---

### Input viewer — `input_viewer`

A dashboard-style, web-based **map of the DSM2 channel network** driven by a hydro echo file. Channels are drawn as lines with their up/down nodes, positive channel direction (upnode to downnode) is denoted with arrow, and boundary-condition (BC) nodes, source flows, reservoir locations and connections, output locations, and hydraulic structures (gates) are marked.

Right-click menus open pop-up windows with channel **cross sections**, BC and source-flow **time series**, reservoir information, and the **devices and operating rules** of a gate.

Under **Tools**, users can **search** a channel or node by number and **color channels by** Length, Manning's n, Dispersion or DX with a choice of color maps (see [Map and layers](#map-and-layers) and [Search](#search)). 

Georeferencing defaults to the bundled DSM2 v9.3 GeoJSONs (`dsm2ui/dsm2gis/`), so no shapefile paths are required.


| Argument / Option | Type | Default | Description |
|---|---|---|---|
| `FILES...` | path (one or two) | *(required)* | One hydro echo `.inp` file to view, or two (**base** then **scenario**) to also get a **Compare** tab next to **Map** |
| `--input FILE` | path | — | Same as giving one `FILE` |
| `--base FILE` / `--scenario FILE` | path | — | Same as giving two `FILES`; the Map tab and the base layers of the Compare tab show the base file |
| `--compare BOOL` | bool | `False` | Optional; two `FILES` or `--scenario` already switch on the Compare tab |
| `--channel FILE` | path | bundled centerlines GeoJSON | Channel centerline GeoJSON (feature `id` = channel number) |
| `--node FILE` | path | bundled node GeoJSON | Node GeoJSON (feature `id` = node number) |
| `--port PORT` | int | `0` (random) | Port for the web server |

```bash
# Quickest start — uses the bundled GeoJSONs
dsm2ui input_viewer output/hydro_echo_hist_fc_mss.inp

# Custom geometry and a fixed port
dsm2ui input_viewer output/hydro_echo.inp --channel my_channels.geojson --node my_nodes.geojson --port 5006

# Compare a scenario against a base run (base first, then scenario)
dsm2ui input_viewer output/hydro_echo_base.inp output/hydro_echo_scenario.inp
```

If the connection to the server is lost (server stopped, network or tunnel dropped, idle proxy timeout), the page shows a red "Connection to the server was lost" notification; relaunch if needed and reload the page.

**Echo-file tables used**

| Table | Used for |
|---|---|
| `CHANNEL` | Channel length, Manning's n, dispersion, up/down nodes, DX; joined to the GeoJSON by channel number |
| `XSECT_LAYER` | Cross-section data (`DIST`, `ELEV`, `AREA`, `WIDTH`, `WET_PERIM`) |
| `BOUNDARY_FLOW`, `BOUNDARY_STAGE` | Boundary nodes and their DSS `FILE` / `PATH` |
| `SOURCE_FLOW` | Source-flow nodes and their DSS `FILE` / `PATH` (a `FILE` of `constant` uses the `PATH` value as a fixed flow) |
| `SOURCE_FLOW_RESERVOIR` | Source flows of reservoirs (`RES_NAME`, `SIGN`, `FILE` / `PATH`), opened from the reservoir icon |
| `RESERVOIR_CONNECTION` | Nodes each reservoir is connected to (together with the `TO_NODE` of reservoir gates) |
| `RESERVOIR`, `RESERVOIR_VOL`, `RESERVOIR_IC` | Reservoirs on the map and in the reservoir window: surface area and bottom elevation, the elevation–area table, and the initial stage |
| `OUTPUT_CHANNEL` | Output locations (channel number and distance from the upnode) |
| `GATE` | Gate locations (`FROM_OBJ` / `FROM_IDENTIFIER` / `TO_NODE`) |
| `GATE_WEIR_DEVICE`, `GATE_PIPE_DEVICE` | Weir and pipe devices of each gate, matched by `GATE_NAME` |
| `OPERATING_RULE`, `OPRULE_EXPRESSION`, `OPRULE_TIME_SERIES` | Operating rules of each gate and of each boundary / source flow (`ext_flow`), the named expressions they use, and the DSS series they read |
| `ENVVAR` | `START_DATE` / `END_DATE` (model run period) |

#### **Map tab**

#### Map and panels

- **Navigate:** left-drag to pan, mouse wheel to zoom about the cursor. The buttons at the top right zoom in, zoom out, return to the full network (home), zoom to the selected channel (locate), and show/hide the Layers panel. Zooming to a location keeps it clear of any open window.
- **Layers panel**: a checkbox switches each layer on and off, and the chevron collapses the box.
  - **Channels:** the channel lines.
  - **Channel Nodes:** a circle at each channel ends.
  - **Channel Direction:** an arrow pointing from upnode to downnode.
  - **Hydraulic Structures:** for each gate in the `GATE` table. A gate whose `FROM_OBJ` is `channel` is drawn beside its `TO_NODE` on that channel; a `reservoir` gate is drawn on its `TO_NODE`.
  - **Reservoirs:** drawn from the reservoir GeoJSONs in `dsm2gis/` and limited to the reservoirs defined in the echo file's `RESERVOIR` table.
  - **Reservoir Connections:** dashed lines from a reservoir to the channel nodes it connects to.
  - **Boundary Nodes:** location for boundary-condition nodes.
  - **Source Flow (DICU):** location for DICU source flows.
  - **Source Flow (other):** location for outflow, and reservoir source flow.
  - **Output Locations:** `OUTPUT_CHANNEL` stations along their channels.
- **Tools panel** (sidebar, below Layers):
    - **Search Channel/Node**: users can search a channel or node by number (the result is highlighted on the map and the map zooms to it)
        1.  Choose **Channel** or **Node**.
        2. Type the number and press Enter (or click the magnifier).
        3. The map zooms to the result and highlights it (channels also open the information card; nodes get a red ring).

    > **Note:**
     A number that does not exist shows a "not found" message. Editing or deleting the number, or switching Channel/Node, clears the previous result. Only nodes that appear in the `CHANNEL` table can be found.

    - **Color channels by**: paints the channels by a CHANNEL-table property: Length, Manning's n, Dispersion or DX, using the **Color map** you choose. Values fall into eight equal-width classes between the 2nd and 98th percentile (the end classes reach the minimum and maximum) so a few extreme channels do not wash out the rest. The legend lists the value range of each colour; choose **None** to restore the plain colour. Selection highlighting still works while a colouring is active.
- **Status:** a live latitude/longitude readout (NAD 83) is shown at the bottom left.
- **Hover:** 
    - channels show channel number and up→down nodes, nodes show the node number, and BC nodes show the node number and their BC inputs (e.g. `sac (flow)`). 
    - Nodes with source flows also list how many DICU and other sources they have. 
    - Output-location pins show the station name(s), channel, and distance, e.g. `ANH — channel 51 @ 11,789 ft`.
- **Base map:** Esri World Topo Map (requires internet access).

#### Channel information

Left-click a channel to highlight it (bold line with a glow) and open the **Channel Information** card: channel number, length, Manning's n, dispersion, upnode, downnode, and DX. Click the **✕** to close it.

#### Cross sections — right-click a channel → **View channel info**

Opens a **Cross Sections** window for the channel:

- A list of the channel's cross sections (`XS @ DIST = 0.61`); `DIST` is the fraction of channel length from the upnode. Click one to select it, or use **Previous / Next**.
- Location, distances to the upnode and downnode, reach length, and source for the selected section, plus ranges of elevation, area, wetted perimeter, and width.
- Elevation-vs-width, area-vs-elevation, and wetted-perimeter-vs-elevation charts.
- **Show Table** reveals the underlying `XSECT_LAYER` rows; **Export CSV** saves them.
- On the map, each cross section is a short line across the channel (the selected one in blue) and the channel is highlighted. **Zoom to Location** and **All XS Locations** frame them to the left of the window.

The window can be dragged by its title; the title and channel summary stay pinned while you scroll.

#### Boundary-condition data — right-click a BC node (or a channel ending at one) → **View BC data**

**Boundary Condition** window with the DSS time series named by the `FILE` and `PATH` columns of the `BOUNDARY_FLOW` / `BOUNDARY_STAGE` row:

- Name, type, node, sign, fill-in, units, interval, data period, model run period, min/mean/max, and missing-value count
- if a node has several BC inputs, a drop-down selects between them.
- The chart shows the **full record** in the DSS file. Choose a display mode:

| Mode | Plot |
|---|---|
| **Original** (default) | Line plot of the original data |
| **Daily mean** | Line plot of daily means |
| **Tidally filtered** | Line plot after `cosine_lanczos(data, '40h')` (needs regular sub-daily data) |
| **Monthly mean** | Bar plot of monthly means |

- **Full record** and **Model run period** buttons set the visible time window (the run period comes from `START_DATE`/`END_DATE`); pan and zoom work along the time axis.
- **Export CSV** saves the series as currently displayed.

The BC node is ringed on the map while its window is open. The window can be dragged by its title, like the cross-section window.

#### Source flow data — right-click a source-flow dot → **View source flow data**

Source-flow nodes come from the `SOURCE_FLOW` table and are marked with small grey dots (see the Layers box).

The **Source Flow** window is the same as the BC window — same display modes, buttons, CSV export, and file resolution — and reads the `FILE` / `PATH` of each `SOURCE_FLOW` row. 

> **Note**:
Because a node usually has several sources (DICU diversion, drain and seepage plus any others), a drop-down lists them as `name (DICU)` / `name (Other)`. The header also shows the sign and the DICU/Other group.

Source flows of a reservoir (`SOURCE_FLOW_RESERVOIR`) belong to the reservoir, not to a node: they are not drawn on node circles and are not counted in the node hover totals. A reservoir that has source flows shows the same dots as a node, at the corner of its icon: a light grey dot for DICU flows (e.g. the Clifton Court BBID flows) with a smaller dark grey one for other flows (e.g. the SWP flow). Each follows the **Reservoirs** layer and its own **Source Flow (DICU)** / **Source Flow (other)** layer. Right-click the reservoir icon and choose **View source flow data** to open the same window titled `SOURCE FLOW – RESERVOIR <name>`, with a **Reservoir** field in the header (e.g. the Clifton Court BBID and SWP flows).


> ***Operating rules on boundary and source flows*** 
If an operating rule sets the flow's value (`SET ext_flow(name=<NAME>) …`, where `<NAME>` is the `NAME` of the `SOURCE_FLOW` / `BOUNDARY_*` row), an *Operating Rules* section appears under the input details of the BC and Source Flow windows.
It uses the same rule blocks as the Hydraulic Structure window (see below):
> - a badge for the trigger, the trigger written in words (e.g. "stage in channel 185 at 0 ft ≤ 2.0")
> - the value it sets (e.g. "0 cfs" or the series times −1)
> - an expandable "Rule text", and a chart for each time series a rule reads. 

#### Hydraulic structures — right-click a gate icon → **View hydraulic structure**

The **Hydraulic Structure** window shows the gate name, its channel or reservoir, and the node, followed by two tabs. The map zooms to the gate, which is highlighted with a red glow and outline (like a selected channel) until the window is closed, and the window can be dragged by its title. If several gates share a location, a drop-down selects which one is shown.

- **Devices tab** lists one table of its weir devices (`GATE_WEIR_DEVICE`: device, number of devices, width, elevation, height, flow coefficients, default operation) and one of its pipe devices (`GATE_PIPE_DEVICE`: device, number of devices, radius, elevation, flow coefficients, default operation). Rows are matched to the gate by `GATE_NAME`.
<br>
- **Operating rules tab** summarises the rules in `OPERATING_RULE` that act on the gate (matched by `gate=` in the rule's `ACTION`), grouped by device and by the quantity they control (`gate_op`, `gate_coef`, `gate_install`, `gate_elev`, `gate_width`, `gate_height`, `gate_nduplicate`). The `from_node` and `to_node` twins of a rule are merged into one row. 

    1.Each row has a badge: 
    - **Trigger: True** (trigger `TRUE`: the gate follows a time series or is set to a value from the start of the run)
    - **Trigger: Schedule** (date triggers such as `DATETIME < 20SEP2018 12:00` / `>=`, shown as a table of "Before" / "From" dates with the value set and any ramp)
    - **Trigger: Condition** (any other trigger, written in words, e.g. "stage in channel 185 at 0 ft ≤ 2.0", with the named expressions from `OPRULE_EXPRESSION` it uses listed underneath)
    - **Not summarised** (a rule the viewer cannot interpret, shown as its raw text).

    2. "Rule text" expands the exact `ACTION` / `TRIGGER` as specified in echo file.

    3.  `OPRULE_TIME_SERIES`: directly under each rule there is one chart per time series the rule reads (in its value or its trigger, including through named expressions). Each chart shows the DSS file and path (or constant value), fill-in, minimum / mean / maximum and record period, and a step chart that opens on the model run period. It can be zoomed and panned with the chart toolbar (pan, box zoom, wheel zoom, reset; hover for date and value), and the **Model run period** and **Full record** buttons jump between the two views. Rules that set `ext_flow` are not tied to a gate and are not shown in this window (they appear in the BC and Source Flow windows, above).

#### Reservoirs — right-click a reservoir icon → **View reservoir**

Opens a **Reservoir** window for the reservoir. The map zooms to the reservoir and its connection lines, and the reservoir is ringed while the window is open. For a reservoir with `SOURCE_FLOW_RESERVOIR` rows the menu also offers **View source flow data** (see above).

- **Properties:** 
    - surface area (`AREA`, in million sq ft, also shown in acres)
    - bottom elevation (`BOT_ELEV`)
    - initial stage (`RESERVOIR_IC`)
    - the size of the elevation–area table (points, elevation and area range, and the volume at the top of the table).
- **Connections:** a table of the nodes the reservoir touches, either through a connection (`COEF_IN` / `COEF_OUT`) or through a gate.
- **Elevation vs area:** the `RESERVOIR_VOL` curve (area in acres on the x axis, elevation in ft on the y axis), with dashed lines at the bottom elevation and the initial stage. Hover a point for its elevation, area and volume; the chart toolbar pans and zooms. **Show Table** lists the points with a volume column derived from the area curve (trapezoidal rule from the lowest elevation, as DSM2 does), and **Export CSV** saves them.

#### **Compare tab**

With two echo files, the **Compare** tab uses the same layout as the Map tab.

- **Base Layers** (Channels, Channel Nodes, Channel Direction) show channel, node, and channel direction from the base echo file, with the same hover tooltips as the Map tab.
- **Diff Layers** draw in red what differs between the scenario and the base; each has a "Changed ..." checkbox.
- **Right-click** a red item and choose its **View ... diff** to open a pop-up with the base and scenario values side by side, the differing values highlighted.
- **Diff Summary** (button at the top of the sidebar) opens a window with one table per category, listing at a high level which items changed and in what way.

The base file is always the standard: "new" means not in the base file and "removed" means not in the scenario file. Changed and removed items are drawn at their base location. 

>**Notes:**
    1. New items have no base location, so they are not drawn and appear only in the **Diff Summary**. 
    2. A channel or node of the base echo file that has no geometry in the GeoJSON cannot be drawn; a warning card naming it appears in the bottom-right corner of the Map tab at launch until you close it.

#### Channels and nodes

- **Layers names:** **Changed Channels** and **Changed Nodes**
    1. **Changed Channels** show up when it is removed or when any of these changed: length, Manning's n, dispersion, upnode, downnode or DX (`CHANNEL`); cross-section locations, elevation, area, width or wetted perimeter (`XSECT_LAYER`); the initial condition (`CHANNEL_IC`).
    2. **Changed Nodes** show up when it is removed, or when a channel that started (upnode) or ended (downnode) there in the base no longer does because the channel was removed or its upnode / downnode moved.
- **View channel diff:** right-click on `changed channels` on the map to view detailed channel differences, including:
    1. channel properties
    2. the cross-section locations that were added, removed or changed (for a location in both files, the base and scenario elevation, area, width and wetted perimeter of every layer that differs)
    3. the initial condition.

#### Reservoirs

- **Layers:** **Changed Reservoirs** and **Changed Res. Connections**
    1. **Changed Reservoirs** show up when it is removed or when any of these changed: 
        1. connections, i.e. a node added or removed or its `COEF_IN` / `COEF_OUT` (`RESERVOIR_CONNECTION`); 
        2. surface area or bottom elevation (`RESERVOIR`); 
        3. the elevation vs area table (`RESERVOIR_VOL`); 
        4. the initial stage (`RESERVOIR_IC`).
    2. **Changed Res. Connections** show up when that connection changed or was removed (all lines of a removed reservoir turn red); a connection added in the scenario has no base line so it's not shown up in map but in **Diff Summary** table
- **View reservoir diff:** right click on `Changed Reservoirs` to view detailed reservoir differences, including: the properties, the connections (per node) and the differing points of the elevation vs area table.

#### Gates

- **Layer:** **Changed Gates**
    1. **Changed Gates** show up when it is removed or when any of these changed:
        1. `FROM_OBJ`, `FROM_IDENTIFIER` or `TO_NODE` (`GATE`);
        2. a weir or pipe device was added, removed or had a property changed (`GATE_WEIR_DEVICE`, `GATE_PIPE_DEVICE`);
        3. an operating rule that acts on the gate (matched by `gate=` in its `ACTION`) was added or removed, or its `ACTION` or `TRIGGER` changed (`OPERATING_RULE`);
        4. an `OPRULE_EXPRESSION` used by the gate's rule triggers changed;
        5. the **values** of an `OPRULE_TIME_SERIES` the gate's rules read changed; a series whose file name or path changed but whose data are identical is not a difference.
- **View gate diff:** right-click on `Changed Gates` to view detailed gate differences in a window with two tabs. It opens on the first tab that has differences:
    1. **Gate structure** tab: the location and the changed devices, every property base next to scenario.
    2. **Operating rules** tab: the changed rules (`ACTION` and `TRIGGER`), the changed expressions and, for each time series whose values differ, a chart overlaying the base and scenario series.

#### Boundary and source flows

- **Layers:** **Changed Boundary Nodes** and **Changed Source Flow**
    1. **Changed Boundary Nodes** show up at the base node of a boundary flow or boundary stage row (`BOUNDARY_FLOW`, `BOUNDARY_STAGE`) that is changed or removed.
    2. **Changed Source Flow** show up for a source flow row, DICU or other, that is changed or removed: at its base node (`SOURCE_FLOW`) or on its base reservoir (`SOURCE_FLOW_RESERVOIR`).
> **Notes**: changes are tracked by `NAME` within their table, and a row is changed when any of these changed:
    1. `NODE` (`RES_NAME` for `SOURCE_FLOW_RESERVOIR`);
    2. `SIGN`;
    3. `FILLIN`;
    4. the **values** of its time series; a renamed file or path with identical data is not a difference.
- **View boundary diff / View source flow diff:** right-click on `Changed Boundary Nodes` or `Changed Source Flow` to view the changed and removed rows at that node or reservoir, including:
    1. the base and scenario node (or reservoir), `SIGN` and `FILLIN`
    2. when the values differ, a chart overlaying the base and scenario time series
    3. for a removed row, the base row (node or reservoir, `SIGN`, `FILLIN`, file and path) next to empty scenario cells.

#### Output locations

- **Layer:** **Changed Output Locations**
    1. **Changed Output Locations** show up at the base position of a name that is removed or whose location changed. 
>**Notes**: `OUTPUT_CHANNEL` rows are matched by `NAME`, ignoring case; for a name in both files only the set of `CHAN_NO` and `DISTANCE` values decides whether it changed. The variable, interval, period operation and file are ignored.
- **Hover:** there is no right-click window. Hovering a pin shows, for each name, its base and its scenario location (channel and distance, or "removed").

#### Diff Summary

The **Diff Summary** button at the top of the sidebar opens a window with one table per category, listing at a high level which items differ and in what way. Categories without differences are left out.

> **New items are not shown on the map.** They have no base location, so they appear only in the Diff Summary (the "New" row of each table). Changed and removed items are drawn on the map and also listed here.

1. **Channels:** new or removed channel, and which properties changed (length, Manning's n, dispersion, upnode / downnode, DX, cross-section, initial condition).
2. **Reservoirs:** new or removed reservoir, and what changed (connections, area / bottom elevation, elevation vs area, initial condition).
3. **Gates:** new or removed gate, and location or device changes.
4. **Operating Rules:** rule changes of gates (rule added or removed, `ACTION`, `TRIGGER`, expression, time series).
5. **Boundary Flow, Boundary Stage, Source Flow, Source Flow Reservoir:** one table per input type, with the rows Node changed (or RES_NAME changed), SIGN changed, FILLIN changed, Data changed, New and Removed.
6. **Output Locations:** the rows Location changed, New and Removed.
7. **Scalars:** the `SCALAR` table is compared in the Diff Summary only; nothing is drawn on the map. Scalars are matched by `NAME`, and the table lists every changed, new and removed scalar with its base and scenario value.

#### **Error handling and warnings**

A problem with one input is reported where it occurs in the interative map, and the rest of the viewer keeps working.

**Startup and echo-file content**

- **Bad command line:** a missing file, more than two files, or only one file for a comparison stops the command with a usage message before the server starts.
- **Missing optional table:** a table that is not in the echo file (for example `OUTPUT_CHANNEL`, `GATE`, `BOUNDARY_*`, `SOURCE_FLOW*`, `XSECT_LAYER`, `RESERVOIR*`) is treated as empty, so that layer just has no items. The `CHANNEL` table is required; without it the viewer does not start.
- **Channels and nodes without geometry:** those in the echo file but not in the GeoJSON files are not drawn, and a warning card lists them at launch. Those in the GeoJSON files but not in the echo file are drawn in gray with a hover message and are listed in the same card.
- **Elements that cannot be placed:** a gate whose channel is not in the network or whose node is not an end of that channel, an output location on a channel without geometry, and a boundary or source flow at a node without geometry are left off the map, without a message.

**Time series windows and charts** (BC, source flow and operating-rule series)

- **DSS file not found:** a red message such as "Could not read the time series: DSS file not found: <name>" would show up to replace the chart. The rest of the window, such as the name, node, sign and operating rules, is still shown, and the other charts are not affected.
- **DSS path not in the file:** the same message, "path not found in DSS file: <path>" would show up to replace the chart.
- **Constant series without a run period:** message "constant input, but the echo file has no run period" would show up.
- **Empty series:** a rule chart for a series with no values (or only the -901 / -902 missing flags) says "The series has no data."
- **No model run period** (`START_DATE` / `END_DATE` missing from `ENVVAR`): the run period shows as n/a, the **Model run period** button is disabled and the chart opens on the full record.
- **Tidal filter not possible** (irregular or too short data): the original series is shown with a note.
- **Rules:** a series a rule refers to but does not define shows "Series <name> is not defined."; a rule the viewer cannot interpret is shown as raw text under **Not summarised**.
- **Search:** a number that does not exist shows a "not found" message.

**Compare tab**

- **Same definition, same file:** a time series with the same file (under the same folder with same file name), path and fill-in in both echo files that resolves to the same DSS file is treated as unchanged without reading it. The series is read and compared only when the actual file names and pathes differ.
- **File missing, same definition:** if both echo files give the same file, path and fill-in but the DSS file cannot be found (for either echo file), the series is treated as unchanged, with no warning message in the Compare tab. The Map tab's BC and source flow windows do show "DSS file not found" for the same case.
- **Data unreadable, different definition:** the series is listed as changed, and its chart is replaced by "values could not be compared (<reason>)".
- **Items without a map location:** changed items that cannot be placed on the map (see above) are still listed in the Diff Summary.

**Connection** 
- **Lost connection to server**: a red "Connection to the server was lost" notification appears; relaunch the command if needed and reload the page.

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
| `STATIONS_CSV` | path | — | Enriched stations CSV from `datastore extract --station-metadata-csv` |
| `CENTERLINES_GEOJSON` | path | — | DSM2 channel centerlines GeoJSON (UTM Zone 10N) |
| `OUTPUT_CSV` | path | — | Output `calibration_ec_stations.csv` |
| `--distance-tolerance INT` | int | `1000` | Max distance (ft) for a station to be considered matched |
| `--unmatched FILE` | path | `<output>_unmatched.csv` | CSV for stations that could not be snapped |

```bash
dsm2ui calib stations-csv stations_ec.csv \
    dsm2ui/dsm2gis/dsm2_channels_centerlines_9_3.geojson \
    calibration_ec_stations.csv

# Wider search radius
dsm2ui calib stations-csv stations_ec.csv channels.geojson cal_stations.csv \
    --distance-tolerance 2000 --unmatched unmatched.csv
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

#### `calib postpro setup-compare`

Generate a postpro config for a baseline-vs-alternative comparison with **no observed
data** — for planning studies where you just want to compare two or more completed
DSM2 runs against each other. The first `-s` study is the reference for scatter plots
and metrics. Skip `run observed` for configs generated this way; go straight to
`run model` then `run plots`. See
[README-postpro.md § Quick baseline-vs-alternative comparison](README-postpro.md#quick-baseline-vs-alternative-comparison-no-observed-data).

| Option | Default | Description |
|---|---|---|
| `-s / --study DIR` | *(required, repeatable)* | Study folder path; first `-s` is the baseline/reference |
| `-o / --output FILE` | *(required)* | Output YAML config path |
| `-m / --module` | `hydro` | DSM2 module whose DSS output to reference (`hydro\|qual\|gtm`) |
| `--output-folder DIR` | `./plots/` | Plot output folder written into the YAML |
| `--timewindow "START - END"` | — | Override simulation time window |
| `--location-file VARTYPE=PATH` | *(repeatable)* | Override a vartype location CSV |

```bash
dsm2ui calib postpro setup-compare \
    -s D:/delta/dsm2_studies/studies/baseline \
    -s D:/delta/dsm2_studies/studies/alternative \
    -o compare_config.yml \
    -m qual
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
    --station-metadata-csv stations_ec.csv

# Step 2 — snap stations to DSM2 channels and assemble the calibration CSV
dsm2ui calib stations-csv \
    stations_ec.csv \
    dsm2ui/dsm2gis/dsm2_channels_centerlines_9_3.geojson \
    calibration_ec_stations.csv
```

Stations that cannot be snapped within the tolerance (default 1000 ft) are written to
`calibration_ec_stations_unmatched.csv` for review. Use `--distance-tolerance 2000` to
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

Both subcommands default to the bundled DSM2 v9.3 centerlines when `--centerlines` is omitted:
`dsm2ui/dsm2gis/dsm2_channels_centerlines_9_3.geojson`

#### `station-map to-dsm2`

Snap lat/lon stations to DSM2 channels. Output columns are `NAME` (uppercased station ID), `CHAN_NO`, `DISTANCE` — ready to paste into an `OUTPUT_CHANNEL` DSM2 input section. Stations that cannot be snapped within the tolerance are written to a separate CSV.

| Argument / Option | Type | Default | Description |
|---|---|---|---|
| `STATIONS_CSV` | path | — | Station CSV with `station_id`, `lat`, `lon` columns |
| `OUTPUT_CSV` | path | — | Output CSV with `NAME`, `CHAN_NO`, `DISTANCE` |
| `--centerlines FILE` | path | bundled centerlines GeoJSON | DSM2 channel centerlines GeoJSON (UTM Zone 10N) |
| `--distance-tolerance INT` | int | `1000` | Max distance (ft) from centerline to consider a match |
| `--unmatched FILE` | path | `<output>_unmatched.csv` | CSV for stations that could not be snapped |

```bash
# Quickest form — uses bundled centerlines automatically
dsm2ui station-map to-dsm2 stations.csv output_locs.csv

# Custom centerlines; wider tolerance
dsm2ui station-map to-dsm2 stations.csv output_locs.csv \
    --centerlines my_channels.geojson --distance-tolerance 2000 --unmatched unmatched.csv
```

#### `station-map to-inp`

Snap lat/lon stations to DSM2 channels and write a ready-to-use `OUTPUT_CHANNEL` table section
(wrapped in the `OUTPUT_CHANNEL ... END` syntax DSM2 expects) directly to a `.inp` file. Unlike
`to-dsm2`, no manual reformatting is needed — the output already has `VARIABLE`, `INTERVAL`,
`PERIOD_OP` and `FILE` columns filled in.

| Argument / Option | Type | Default | Description |
|---|---|---|---|
| `STATIONS_CSV` | path | — | Station CSV with `station_id`, `lat`, `lon` columns |
| `OUTPUT_INP` | path | — | `.inp` file to write the `OUTPUT_CHANNEL` section to |
| `--centerlines FILE` | path | bundled centerlines GeoJSON | DSM2 channel centerlines GeoJSON (UTM Zone 10N) |
| `--variable TEXT` | string | `flow`, `stage` | DSM2 output variable to write; repeat for multiple, e.g. `--variable flow --variable ec` |
| `--interval TEXT` | string | `15MIN` | DSM2 output interval, e.g. `15MIN`, `1HOUR` |
| `--period-op TEXT` | string | `inst` | DSM2 period operation: `inst` or `ave` |
| `--dss-file TEXT` | string | `${OUTPUTDSS}` | Output DSS path or ENVVAR written to the `FILE` column |
| `--distance-tolerance INT` | int | `100` | Max distance (ft) from centerline to consider a match |
| `--append` / `--overwrite` | flag | `--overwrite` | Append to `OUTPUT_INP` instead of overwriting it |

```bash
# Quickest form — flow + stage at 15MIN, uses bundled centerlines automatically
dsm2ui station-map to-inp stations.csv output_channel.inp

# EC only, hourly, custom centerlines and DSS output path
dsm2ui station-map to-inp stations.csv output_channel.inp \
    --variable ec --interval 1HOUR --dss-file '${QUALOUTDSSFILE}' \
    --centerlines my_channels.geojson
```

#### `station-map from-dsm2`

Geolocate DSM2 `OUTPUT_CHANNEL` stations from an echo file. Reads `CHANNEL` and `OUTPUT_CHANNEL` tables, interpolates each station's position along its channel centerline, and writes a GeoJSON with `NAME`, `CHAN_NO`, `DISTANCE`, and point geometry (EPSG:26910).

| Argument / Option | Type | Default | Description |
|---|---|---|---|
| `ECHO_FILE` | path | — | DSM2 echo/input file containing `CHANNEL` and `OUTPUT_CHANNEL` tables |
| `OUTPUT_GEOJSON` | path | — | Output GeoJSON of geolocated stations |
| `--centerlines FILE` | path | bundled centerlines GeoJSON | DSM2 channel centerlines GeoJSON |

```bash
# Quickest form — uses bundled centerlines automatically
dsm2ui station-map from-dsm2 hydro_echo.inp stations.geojson

# Custom centerlines
dsm2ui station-map from-dsm2 hydro_echo.inp stations.geojson --centerlines my_channels.geojson
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
| `--station-metadata-csv FILE` | path | — | Write station metadata CSV (`station_id`, `station_name`, `agency`, `lat`, `lon`, `utm_easting`, `utm_northing`) — this is an **output** path, not a station filter |
| `--station-id ID` | string | — | Restrict extraction to this station_id (case-insensitive); repeat for multiple stations |
| `--repo-level` | choice | `screened` | Datastore repository level |
| `--unit-name NAME` | string | — | Override the unit name written to the DSS file |

At least one of `--output` or `--csv` or `--station-metadata-csv` must be provided. The `--station-metadata-csv` output is the required input to `calib stations-csv` and `station-map to-dsm2`.

```bash
# Station metadata CSV only
dsm2ui datastore extract ec --repo /data/datastore --station-metadata-csv stations_ec.csv

# DSS time series only
dsm2ui datastore extract ec --repo /data/datastore --output ec.dss

# Both
dsm2ui datastore extract ec --repo /data/datastore --output ec.dss --station-metadata-csv stations_ec.csv

# Restrict to specific stations
dsm2ui datastore extract elev --repo /data/datastore --output elev.dss --station-id srv
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
