@echo off
:: ============================================================================
:: animate_examples.bat  —  dsm2ui animate command reference / examples
:: ============================================================================
::
:: PURPOSE
::   Illustrates all major features of the "dsm2ui animate" command family,
::   including hydro, qual, flow, and export-corrected sub-commands, as well
::   as the datastore preparation utilities.
::
:: HOW TO USE
::   This file is NOT meant to be run directly (GOTO :EOF exits immediately).
::   Copy individual commands into your terminal after activating the env:
::
::       conda activate dsm2ui
::       <paste command>
::
:: COMMANDS COVERED
::   dsm2ui datastore make-clip-polygon  — build a spatial filter polygon
::   dsm2ui datastore extract            — pull observations from DMS repo
::   dsm2ui datastore average-sublocs    — collapse multi-subloc stations
::   dsm2ui animate export-corrected     — pre-compute IDW-corrected QUAL H5
::   dsm2ui animate qual                 — animate EC / constituent on map
::   dsm2ui animate hydro                — animate flow / stage / velocity
::   dsm2ui animate flow                 — standalone flow arrows + bars
::
:: TRANSFORMS (available for both hydro and qual)
::   none             raw timestep data (default)
::   daily            daily mean
::   daily-min        daily minimum
::   daily-max        daily maximum
::   rolling-24h      24-hour centred rolling mean
::   rolling-14d      14-day centred rolling mean
::   rolling-14d-daily  14-day rolling mean then daily mean
::   godin            Godin tidal filter (~35-hour low-pass)
::   godin-daily      Godin filter then daily mean
::   godin-daily-min  Godin filter then daily minimum
::   godin-daily-max  Godin filter then daily maximum
::
:: COMMON OPTIONS (all animate sub-commands)
::   --shapefile PATH           override bundled channel centrelines
::   --channel-id-column NAME   column in shapefile with integer channel IDs
::                              (auto-detected: tries 'id', 'channel_nu', 'CHAN_NO')
::   --simplify METRES          geometry simplification tolerance (0 = off; default 50)
::   --colormap NAME            any curated colormap (default: turbo)
::   --vmin / --vmax FLOAT      fixed colour-scale bounds (default: data min/max)
::   --size FLOAT               line-width in pixels (default: 3.0)
::   --title TEXT               custom map title
::   --port N                   Bokeh server port (0 = auto; default: 0)
::   --desktop                  open in a native pywebview window instead of browser
::   --log-level debug|info|warning|error
::
:: ============================================================================
GOTO :EOF


:: ============================================================================
:: PATH VARIABLES — adjust these to match your installation
:: ============================================================================

:: --- Historical v8.2.1 study ------------------------------------------------
SET "V821_EC_H5=D:\delta\dsm2_v821\studies\historical\output\hist_v821_202312_EC.h5"
SET "V821_HYDRO_H5=D:\delta\dsm2_v821\studies\historical\output\hist_v821_202312.h5"
SET "V821_ECHO_INP=D:\delta\dsm2_v821\studies\historical\output\hydro_echo_hist_v821_202312.inp"
SET "V821_CL=D:\delta\maps\dsm2_8_2_1_shapefiles\dsm2_v8_2_1_historical_centerline_chan_norest.shp"
SET "V821_EC_IDW_H5=D:\delta\hist_v821_202312_EC_idw.h5"

:: --- Historical FC/MSS study (2026-04 grid) ----------------------------------
SET "FC_DIR=D:\delta\dsm2_input_2026-04-16_historical_update\dsm2_studies\studies\historical\output"
SET "FC_EC_H5=%FC_DIR%\hist_fc_mss_qual_EC.h5"
SET "FC_HYDRO_H5=%FC_DIR%\hist_fc_mss.h5"
SET "FC_ECHO_INP=%FC_DIR%\hydro_echo_hist_fc_mss.inp"
SET "FC_CL=D:\delta\dsm2_grid_2026-04-16_historical_shapefiles\shapefiles\i12_DSM2_Grid_V2020-04-16_Hist_channels_centerlines.shp"
SET "FC_EC_IDW_H5=D:\delta\hist_fc_mss_qual_EC_idw.h5"

:: --- Observations -----------------------------------------------------------
SET "EC_OBS_RAW_CSV=D:\delta\ec_obs.csv"
SET "EC_OBS_AVG_CSV=D:\delta\ec_obs_avg.csv"
SET "EC_STATIONS_CSV=D:\delta\ec_obs_stations.csv"
SET "DELTA_CLIP_GEOJSON=D:\delta\delta_clip.geojson"
SET "EC_REPO=Y:\repo\continuous"

:: --- Flow overlay config ----------------------------------------------------
::   Schema: scale_mode, reference_flow, reference_arrow_length_m, arrow_width_m,
::           bar_width_m, bar_max_height_m, arrows:[{channel, position, label}],
::           bars:[{node, label, channels:[...]}]
SET "FLOW_CONFIG=D:\dev\dsm2ui\tests\flow_config.yaml"
:: --- Velocity overlay config ------------------------------------------------
::   Schema: variable: velocity, scale_mode: linear, reference_velocity (ft/s),
::           flow_vmin/flow_vmax (ft/s), arrows:[{channel, position, label}]
SET "VELOCITY_CONFIG=D:\dev\dsm2ui\tests\velocity_config.yaml"
:: --- Stage overlay config ---------------------------------------------------
::   Schema: bar_width_m, bar_max_height_m, reference_stage_range_ft,
::           show_labels, bars:[{channel, position, label, mean_stage_ft}]
SET "STAGE_CONFIG=D:\dev\dsm2ui\tests\stage_config.yaml"
:: --- Saved UI configs (YAML written by the browser UI's "Save config" button)
SET "FC_ANIM_CFG=%FC_DIR%\hist_fc_mss_qual_EC_animate.yml"
SET "FC_ANIM_OBS_CFG=%FC_DIR%\hist_fc_mss_qual_EC_animate_obs.yml"
SET "DUAL_FLOW_CFG=D:\delta\dsm2_dual_flow_animate.yml"

:: --- Planning study (network share) -----------------------------------------
SET "PLAN_SHARE=\\cnrastore-bdo\Delta_Mod\Share\DCP_ITP\HABRT_2026"
SET "PLAN_EC_OFF=%PLAN_SHARE%\Azure_Outputs_hdf5\2043_CC50_DCPOff_h5\output\2043_CC50_DCPOff_h5_EC_03AUG1999.h5"
SET "PLAN_EC_ON=%PLAN_SHARE%\Azure_Outputs_hdf5\2043_CC50_DCPOn_check_h5\output\2043_CC50_DCPOn_check_h5_EC_03AUG1999.h5"
SET "PLAN_CL=%PLAN_SHARE%\dsm2_8_2_1_shapefiles\dsm2_v8_2_1_historical_centerline_chan_norest.shp"


:: ============================================================================
:: SECTION 0: Datastore — build spatial clip polygon (run once)
:: ============================================================================
:: Creates a buffered polygon around the DSM2 channel network (GeoJSON).
:: Pass it to "datastore extract --clip" to keep only Delta-area stations.
:: Default buffer = 5000 m; use --buffer to change.

dsm2ui datastore make-clip-polygon %DELTA_CLIP_GEOJSON%

:: Custom 2 km buffer:
dsm2ui datastore make-clip-polygon %DELTA_CLIP_GEOJSON% --buffer 2000


:: ============================================================================
:: SECTION 1: Datastore — extract EC observations from DMS repo (run once)
:: ============================================================================
:: Writes a wide-format CSV (rows=timestamps, columns=station IDs).
:: A companion stations CSV ({stem}_stations.csv) is generated automatically.
:: --start / --end accept ISO (2014-10-01) or DSM2 military (01OCT2014) dates.
:: --clip restricts to stations inside the Delta polygon.
:: --merge extends an existing CSV (only fetches missing head/tail windows).

:: Full extraction, all Delta-area EC stations, Oct 2009 – Sep 2025:
dsm2ui datastore extract ec --repo %EC_REPO% --csv %EC_OBS_RAW_CSV% --clip %DELTA_CLIP_GEOJSON% --start 01OCT2009 --end 30SEP2025

:: Incremental update (append new data without re-downloading existing range):
dsm2ui datastore extract ec --repo %EC_REPO% --csv %EC_OBS_RAW_CSV% --clip %DELTA_CLIP_GEOJSON% --start 01OCT2009 --end 30SEP2025 --merge


:: ============================================================================
:: SECTION 2: Datastore — average multi-subloc stations (run once, fast)
:: ============================================================================
:: Collapses stations like ANH@upper / ANH@lower into a single ANH column
:: (NaN-safe mean).  The output CSV is used as --observations-csv input for
:: IDW/OI correction, which expects one column per station.

dsm2ui datastore average-sublocs %EC_OBS_RAW_CSV% --output %EC_OBS_AVG_CSV%

:: Without --output, writes to {input_stem}_avg.csv in the same directory:
dsm2ui datastore average-sublocs %EC_OBS_RAW_CSV%


:: ============================================================================
:: SECTION 3: export-corrected — pre-compute IDW-corrected QUAL HDF5
:: ============================================================================
:: Writes a new HDF5 with the same dataset layout as the raw QUAL file, but
:: with IDW-bias-corrected concentrations baked in.  The output can then be
:: compared with the raw model at full animation speed (no per-frame correction
:: overhead) using the standard two-file comparison workflow in Section 8.
::
:: Options:
::   --constituent   constituent to correct (default: ec)
::   --observations-csv  time-indexed CSV of observations (station IDs as columns)
::   --stations-csv  CSV with station_id and lat/lon or x/y columns
::   --echo-inp      DSM2 echo .inp fallback when H5 has no /input/channel table
::   --centerlines-file  GeoJSON/shapefile for station snapping (bundled default)
::   --start / --end  restrict time window written to the output H5
::   --idw-power     IDW distance exponent (default 2.0; higher = more local)
::   --max-obs-age   max age of obs relative to a model timestep (default "2h")
::   --chunk-size    timesteps per write chunk — controls memory use (default 1000)

:: v8.2.1 historical run, full calibration period:
dsm2ui animate export-corrected "%V821_EC_H5%" ^
    --output "%V821_EC_IDW_H5%" ^
    --observations-csv "%EC_OBS_AVG_CSV%" ^
    --stations-csv "%EC_STATIONS_CSV%" ^
    --echo-inp "%V821_ECHO_INP%" ^
    --start 01OCT2009 --end 30SEP2024 ^
    --idw-power 2.5 --max-obs-age 25h ^
    --centerlines-file "%V821_CL%"

:: FC/MSS historical update run:
dsm2ui animate export-corrected "%FC_EC_H5%" ^
    --output "%FC_EC_IDW_H5%" ^
    --observations-csv "%EC_OBS_AVG_CSV%" ^
    --stations-csv "%EC_STATIONS_CSV%" ^
    --echo-inp "%FC_ECHO_INP%" ^
    --start 01OCT2014 --end 30SEP2024 ^
    --idw-power 2.5 --max-obs-age 25h ^
    --centerlines-file "%FC_CL%"


:: ============================================================================
:: SECTION 4: animate qual — basic single file
:: ============================================================================
:: Minimum required: one QUAL HDF5 and a constituent name.
:: Opens a browser tab on a random port with the animated map.

dsm2ui animate qual "%V821_EC_H5%" --constituent ec

:: Specify a fixed port (useful for bookmarking or firewall rules):
dsm2ui animate qual "%V821_EC_H5%" --constituent ec --port 5007

:: Open in a native desktop window (requires: pip install pywebview):
dsm2ui animate qual "%V821_EC_H5%" --constituent ec --desktop

:: Different constituent (e.g. chloride):
dsm2ui animate qual "%V821_EC_H5%" --constituent cl

:: Custom colour scale and colormap:
dsm2ui animate qual "%V821_EC_H5%" --constituent ec --vmin 0 --vmax 5000 --colormap viridis

:: Custom title and larger line width:
dsm2ui animate qual "%V821_EC_H5%" --constituent ec --title "v821 EC Calibration" --size 5

:: Override the bundled centrelines shapefile (e.g. for a different grid version):
dsm2ui animate qual "%V821_EC_H5%" --constituent ec --shapefile "%V821_CL%"

:: Verbose logging (useful for debugging reader issues):
dsm2ui animate qual "%V821_EC_H5%" --constituent ec --log-level info


:: ============================================================================
:: SECTION 5: animate qual — time-domain transforms
:: ============================================================================
:: --transform applies a time-domain filter or resample before animation.
:: --resample-freq / --resample-agg stack an additional resample on top.

:: Godin tidal filter (~35-hour low-pass, removes tidal variability):
dsm2ui animate qual "%FC_EC_H5%" --constituent ec --transform godin

:: Godin then daily mean (good for inter-annual comparisons):
dsm2ui animate qual "%FC_EC_H5%" --constituent ec --transform godin-daily

:: Daily mean only (faster than Godin, no tidal filter):
dsm2ui animate qual "%FC_EC_H5%" --constituent ec --transform daily

:: 14-day rolling mean then daily mean (long-term trend):
dsm2ui animate qual "%FC_EC_H5%" --constituent ec --transform rolling-14d-daily

:: Additional resample stacked on top of a primary transform
:: (e.g. Godin filter then 6-hour means):
dsm2ui animate qual "%FC_EC_H5%" --constituent ec --transform godin --resample-freq 6h --resample-agg mean


:: ============================================================================
:: SECTION 6: animate qual — X2 isohaline overlay (single file only)
:: ============================================================================
:: X2 = upstream distance (km from Golden Gate) of the 2-PSU isohaline.
:: --x2-threshold sets the EC value in µS/cm that defines the isohaline.
:: 2 PSU ≈ 2700 µS/cm; 1 PSU ≈ 1350 µS/cm.

dsm2ui animate qual "%FC_EC_H5%" --constituent ec --x2-threshold 2700

:: Lower threshold (1 PSU isohaline):
dsm2ui animate qual "%FC_EC_H5%" --constituent ec --x2-threshold 1350


:: ============================================================================
:: SECTION 7: animate qual — live IDW observation correction (per-frame)
:: ============================================================================
:: Applies network inverse-distance weighting to bias-correct the model at
:: every animation frame.  Requires pre-extracted observation and station CSVs
:: (Sections 1–2).  Slower than the pre-computed workflow (Section 8).
::
:: Options:
::   --idw-power         IDW exponent (default 2.0; higher = tighter to obs)
::   --max-obs-age       discard obs older than this relative to model step
::   --centerlines-file  channel centrelines for station snapping
::   --echo-inp          fallback CHANNEL table when H5 lacks /input/channel

:: v8.2.1 with default IDW settings:
dsm2ui animate qual "%V821_EC_H5%" --constituent ec ^
    --observations-csv "%EC_OBS_AVG_CSV%" ^
    --stations-csv "%EC_STATIONS_CSV%" ^
    --echo-inp "%V821_ECHO_INP%"

:: FC/MSS with custom IDW power and max-obs-age, explicit centrelines:
dsm2ui animate qual "%FC_EC_H5%" --constituent ec ^
    --observations-csv "%EC_OBS_AVG_CSV%" ^
    --stations-csv "%EC_STATIONS_CSV%" ^
    --echo-inp "%FC_ECHO_INP%" ^
    --idw-power 1.5 --max-obs-age 25h ^
    --centerlines-file "%FC_CL%"


:: ============================================================================
:: SECTION 8: animate qual — compare model vs IDW-corrected side by side
:: ============================================================================
:: --compare-correction shows two panels: raw model | IDW-corrected.
:: Requires the same obs/stations options as Section 7.

dsm2ui animate qual "%V821_EC_H5%" --constituent ec ^
    --observations-csv "%EC_OBS_AVG_CSV%" ^
    --stations-csv "%EC_STATIONS_CSV%" ^
    --echo-inp "%V821_ECHO_INP%" ^
    --compare-correction

dsm2ui animate qual "%FC_EC_H5%" --constituent ec ^
    --observations-csv "%EC_OBS_AVG_CSV%" ^
    --stations-csv "%EC_STATIONS_CSV%" ^
    --echo-inp "%FC_ECHO_INP%" ^
    --idw-power 1.5 --max-obs-age 25h ^
    --centerlines-file "%FC_CL%" ^
    --compare-correction


:: ============================================================================
:: SECTION 9: animate qual — OI (optimal interpolation) correction
:: ============================================================================
:: --correction-method oi uses optimal interpolation instead of IDW.
:: OI parameters:
::   --oi-sigma-obs      observation error std-dev in µS/cm (default 10.0)
::   --oi-kernel         exponential (symmetric) or channel_direction (flow-aware)
::   --oi-resistance     against-flow cost multiplier for channel_direction (>= 1)

dsm2ui animate qual "%FC_EC_H5%" --constituent ec ^
    --observations-csv "%EC_OBS_AVG_CSV%" ^
    --stations-csv "%EC_STATIONS_CSV%" ^
    --echo-inp "%FC_ECHO_INP%" ^
    --correction-method oi ^
    --oi-sigma-obs 15 ^
    --oi-kernel channel_direction ^
    --oi-resistance 3.0


:: ============================================================================
:: SECTION 10: animate qual — pre-corrected H5 comparison (fastest workflow)
:: ============================================================================
:: After running export-corrected (Section 3), pass the raw and corrected H5
:: files together to get a side-by-side or diff map with no per-frame overhead.
:: Transforms (godin, daily, etc.) and diff mode all work at full speed.

:: Side-by-side: raw model (A) vs IDW-corrected (B):
dsm2ui animate qual "%V821_EC_H5%" "%V821_EC_IDW_H5%" --constituent ec

:: Godin-daily side-by-side (good for seasonal patterns):
dsm2ui animate qual "%V821_EC_H5%" "%V821_EC_IDW_H5%" --constituent ec --transform godin-daily

:: Difference map (A − B, i.e. model error relative to corrected):
dsm2ui animate qual "%V821_EC_H5%" "%V821_EC_IDW_H5%" --constituent ec --transform godin-daily --diff

:: Cross-study comparison (v821 raw vs FC/MSS raw):
dsm2ui animate qual "%V821_EC_H5%" "%FC_EC_H5%" --constituent ec


:: ============================================================================
:: SECTION 11: animate qual — two-file side-by-side with flow overlay
:: ============================================================================
:: When comparing two QUAL files with a flow overlay, pass --hydro-h5 twice
:: (once for each QUAL file, in matching order).

dsm2ui animate qual "%FC_EC_H5%" "%V821_EC_H5%" --constituent ec ^
    --flow-config "%FLOW_CONFIG%" ^
    --hydro-h5 "%FC_HYDRO_H5%" ^
    --hydro-h5 "%V821_HYDRO_H5%"


:: ============================================================================
:: SECTION 12: animate qual — flow arrows/bars overlay (single file)
:: ============================================================================
:: Overlays animated flow arrows and junction split-bars on the EC colour map.
:: Requires --flow-config (arrow/bar spec) and --hydro-h5 (flow data source).
:: The flow overlay mirrors the active transform (e.g. godin filter).
::
:: --flow-config     flow (cfs) arrows + junction bars  (FlowLayerSpec)
:: --velocity-config velocity (ft/s) arrows only         (mutually exclusive with --flow-config)
:: --nodes-file      override bundled nodes GeoJSON for junction bars

dsm2ui animate qual "%FC_EC_H5%" --constituent ec ^
    --flow-config "%FLOW_CONFIG%" ^
    --hydro-h5 "%FC_HYDRO_H5%"

:: Velocity arrows — use the dedicated --velocity-config flag:
dsm2ui animate qual "%FC_EC_H5%" --constituent ec ^
    --velocity-config "%VELOCITY_CONFIG%" ^
    --hydro-h5 "%FC_HYDRO_H5%"


:: ============================================================================
:: SECTION 13: animate qual — velocity arrows using velocity_config.yaml
:: ============================================================================
:: Use --velocity-config to overlay velocity (ft/s) arrows.  This is a
:: dedicated flag — mutually exclusive with --flow-config — that automatically
:: reads velocity from the HYDRO HDF5 without any extra --flow-variable flag.
::
:: velocity_config.yaml schema: scale_mode: linear, reference_velocity (ft/s),
:: reference_arrow_length_m, arrow_width_m, arrows:[{channel, position, label}].
:: Arrow length is directly proportional to velocity (ft/s), making tidal
:: reversal and channel-to-channel magnitude easy to compare at a glance.

:: Raw EC colours + linear velocity arrows:
dsm2ui animate qual "%FC_EC_H5%" --constituent ec ^
    --velocity-config "%VELOCITY_CONFIG%" ^
    --hydro-h5 "%FC_HYDRO_H5%"

:: Godin-daily EC + sub-tidal velocity arrows:
dsm2ui animate qual "%FC_EC_H5%" --constituent ec --transform godin-daily ^
    --velocity-config "%VELOCITY_CONFIG%" ^
    --hydro-h5 "%FC_HYDRO_H5%"

:: Velocity arrows + stage deviation bars:
dsm2ui animate qual "%FC_EC_H5%" --constituent ec ^
    --velocity-config "%VELOCITY_CONFIG%" ^
    --hydro-h5 "%FC_HYDRO_H5%" ^
    --stage-config "%STAGE_CONFIG%"


:: ============================================================================
:: SECTION 14: animate qual — flow + stage overlay (busiest single-file view)
:: ============================================================================
:: The most information-dense single-file animation: EC channel colours,
:: animated flow arrows at key channels, junction split-bars at nodes, AND
:: stage deviation bars at selected channels — all driven from one HYDRO HDF5.
::
:: Both overlays share the same --hydro-h5 file and both mirror the active
:: transform (e.g. godin applies to EC colours, flow arrows, and stage bars).
::
:: STAGE CONFIG YAML schema (stage_config.yaml):
::   bar_width_m: 150                    # bar width in EPSG:3857 metres
::   bar_max_height_m: 600               # max bar height for reference deviation
::   reference_stage_range_ft: 3.0       # stage deviation (ft) that maps to max bar height
::   show_range_box: true                # semi-transparent band showing ±reference_stage_range_ft
::   show_labels: false                  # channel labels below reference tick (default: hidden)
::   bars:
::     - channel: 17                     # DSM2 channel number
::       position: 0.5                   # fractional position (0=upstream, 1=downstream)
::       label: "Sacramento at Freeport"
::       mean_stage_ft: 5.2             # user-supplied reference (mean) stage in feet
::     - channel: 166
::       position: 0.5
::       label: "San Joaquin near Vernalis"
::       mean_stage_ft: 3.1
::
:: mean_stage_ft is pre-computed by the user (e.g. temporal mean of
:: /hydro/data/channel stage over the calibration period from the HDF5).
:: Bars above the reference line are rendered blue; below are rendered red.
::
:: NOTE: --stage-config requires --hydro-h5.  For two-file QUAL comparison,
::       pass --hydro-h5 twice (once per file); see Section 15 for examples.

:: EC colours + flow arrows/bars + stage deviation bars — fullest single-file view:
dsm2ui animate qual "%FC_EC_H5%" --constituent ec ^
    --flow-config "%FLOW_CONFIG%" --hydro-h5 "%FC_HYDRO_H5%" ^
    --stage-config "%STAGE_CONFIG%"

:: Same with Godin-daily transform (EC, arrows, and stage bars all follow it):
dsm2ui animate qual "%FC_EC_H5%" --constituent ec --transform godin-daily ^
    --flow-config "%FLOW_CONFIG%" --hydro-h5 "%FC_HYDRO_H5%" ^
    --stage-config "%STAGE_CONFIG%"

:: Velocity arrows instead of flow arrows — use --velocity-config:
dsm2ui animate qual "%FC_EC_H5%" --constituent ec ^
    --velocity-config "%VELOCITY_CONFIG%" --hydro-h5 "%FC_HYDRO_H5%" ^
    --stage-config "%STAGE_CONFIG%"

:: Everything plus X2 isohaline overlay (single-file only):
dsm2ui animate qual "%FC_EC_H5%" --constituent ec --x2-threshold 2700 ^
    --flow-config "%FLOW_CONFIG%" --hydro-h5 "%FC_HYDRO_H5%" ^
    --stage-config "%STAGE_CONFIG%"


:: ============================================================================
:: SECTION 15: animate qual — two-file comparison with flow overlay + orientation
:: ============================================================================
:: Two QUAL files displayed side-by-side with a synchronised time slider.
:: Each panel gets its own HYDRO HDF5 for the flow arrows/bars overlay.
:: Pass --hydro-h5 twice (in the same order as the QUAL files) for independent
:: flow data per panel; pass it once to share the same HYDRO across both panels.
::
:: LAYOUT ORIENTATION — Horizontal (default) vs Vertical two-panel split:
::   Toggle the orientation selector in the sidebar, then click "Save config"
::   to persist.  Restore the saved orientation via --config.  There is no
::   direct CLI flag — layout orientation is a UI control only.
::
::   YAML key:  layout_orientation: Horizontal   (panels left | right, default)
::              layout_orientation: Vertical      (panels top / bottom)
::
:: Stage bars are supported with two QUAL files — pass --stage-config and --hydro-h5.
:: The same StageLayerSpec is applied to both panels, each reading from its own HYDRO H5.

:: Side-by-side with independent flow overlay per panel (most complete two-file view):
dsm2ui animate qual "%FC_EC_H5%" "%V821_EC_H5%" --constituent ec ^
    --flow-config "%FLOW_CONFIG%" ^
    --hydro-h5 "%FC_HYDRO_H5%" ^
    --hydro-h5 "%V821_HYDRO_H5%"

:: Side-by-side with independent flow and stage overlay per panel (most complete two-file view):
dsm2ui animate qual "%FC_EC_H5%" "%V821_EC_H5%" --constituent ec ^
    --flow-config "%FLOW_CONFIG%" ^
    --stage-config "%STAGE_CONFIG%" ^
    --hydro-h5 "%FC_HYDRO_H5%" ^
    --hydro-h5 "%V821_HYDRO_H5%"

:: Godin-daily transform — EC colours and flow arrows both filtered, per panel:
dsm2ui animate qual "%FC_EC_H5%" "%V821_EC_H5%" --constituent ec --transform godin-daily ^
    --flow-config "%FLOW_CONFIG%" ^
    --hydro-h5 "%FC_HYDRO_H5%" ^
    --hydro-h5 "%V821_HYDRO_H5%"

:: One shared HYDRO drives both panels' flow overlay (pass --hydro-h5 once):
dsm2ui animate qual "%FC_EC_H5%" "%V821_EC_H5%" --constituent ec ^
    --flow-config "%FLOW_CONFIG%" ^
    --hydro-h5 "%FC_HYDRO_H5%"

:: Diff map (A − B, single combined panel) with flow overlay:
dsm2ui animate qual "%FC_EC_H5%" "%V821_EC_H5%" --constituent ec --diff ^
    --flow-config "%FLOW_CONFIG%" ^
    --hydro-h5 "%FC_HYDRO_H5%" ^
    --hydro-h5 "%V821_HYDRO_H5%"

:: Restore a saved config that has Vertical (stacked) layout — panels top/bottom:
::   (YAML contains: layout_orientation: Vertical)
dsm2ui animate qual --config D:\delta\dsm2_qual_dual_vertical.yml


:: ============================================================================
:: SECTION 16: animate qual — load from saved YAML config
:: ============================================================================
:: The browser UI has a "Save config" button that writes a YAML capturing all
:: active settings (files, transform, colormap, contours, flow overlay, etc.).
:: Pass it via --config to restore the exact UI state without any other flags.

dsm2ui animate qual --config "%FC_ANIM_CFG%"

:: Config saved from a compare-correction session (obs/correction params
:: are persisted in the YAML under the "correction" key):
dsm2ui animate qual --config "%FC_ANIM_OBS_CFG%"

:: Dual-file + flow overlay config:
dsm2ui animate qual --config "%DUAL_FLOW_CFG%"


:: ============================================================================
:: SECTION 17: animate hydro — basic single file
:: ============================================================================
:: --variable  flow (default) | stage | depth | velocity
::   flow     — channel flow (cfs)
::   stage    — water-surface ELEVATION above datum (ft NAVD88)
::              = depth + channel_bottom from /hydro/geometry/channel_bottom
::              (DSM2 issue #164: the HDF5 "channel stage" dataset stores depth;
::               --variable stage applies the channel-bottom correction)
::   depth    — raw water depth above channel bottom (ft, direct from HDF5)
::   velocity — depth-averaged velocity (cfs / area, ft/s)
:: --location  both (default, averages up+down) | upstream | downstream

:: Flow (default):
dsm2ui animate hydro "%FC_HYDRO_H5%"

:: Stage (water-surface elevation, corrected to ft NAVD88):
dsm2ui animate hydro "%FC_HYDRO_H5%" --variable stage

:: Depth (raw depth above channel bottom, uncorrected):
dsm2ui animate hydro "%FC_HYDRO_H5%" --variable depth

:: Velocity at the upstream end of each channel:
dsm2ui animate hydro "%FC_HYDRO_H5%" --variable velocity --location upstream

:: Custom colour scale (stage, NAVD88 feet — ~-5 to +15 ft range for the Delta):
dsm2ui animate hydro "%FC_HYDRO_H5%" --variable stage --vmin -5 --vmax 15 --colormap coolwarm


:: ============================================================================
:: SECTION 18: animate hydro — transforms
:: ============================================================================

:: Godin-filtered flow:
dsm2ui animate hydro "%FC_HYDRO_H5%" --variable flow --transform godin

:: Daily mean flow:
dsm2ui animate hydro "%FC_HYDRO_H5%" --variable flow --transform daily

:: Godin then daily mean stage:
dsm2ui animate hydro "%FC_HYDRO_H5%" --variable stage --transform godin-daily


:: ============================================================================
:: SECTION 19: animate hydro — two-file comparison and diff
:: ============================================================================
:: Side-by-side (default with 2 files):
dsm2ui animate hydro "%FC_HYDRO_H5%" "%V821_HYDRO_H5%" --variable flow

:: Difference map (A − B):
dsm2ui animate hydro "%FC_HYDRO_H5%" "%V821_HYDRO_H5%" --variable flow --diff

:: Godin-daily diff (long-term mean flow difference):
dsm2ui animate hydro "%FC_HYDRO_H5%" "%V821_HYDRO_H5%" --variable flow --diff --transform godin-daily

:: Load from saved config:
dsm2ui animate hydro --config D:\delta\dsm2_hydro_animate.yml


:: ============================================================================
:: SECTION 20: animate hydro — flow arrows/bars overlay
:: ============================================================================
:: Coloured channel map plus animated arrow/bar overlay from the same H5.
:: --flow-config    YAML for flow (cfs) arrows (FlowLayerSpec schema)
:: --velocity-config YAML for velocity (ft/s) arrows (auto-sets variable=velocity)
::
:: The overlay is only supported with a single HYDRO file; use
:: animate flow for a tile-background-only version.

:: Coloured flow map + arrows/bars:
dsm2ui animate hydro "%FC_HYDRO_H5%" --variable flow --flow-config "%FLOW_CONFIG%"

:: Stage map + flow arrows (arrows from same H5, but colouring is stage):
dsm2ui animate hydro "%FC_HYDRO_H5%" --variable stage --flow-config "%FLOW_CONFIG%"

:: Godin-filtered colours and arrows:
dsm2ui animate hydro "%FC_HYDRO_H5%" --variable flow --transform godin --flow-config "%FLOW_CONFIG%"

:: Velocity arrows — use --velocity-config (no --flow-variable needed):
dsm2ui animate hydro "%FC_HYDRO_H5%" --variable flow --velocity-config "%VELOCITY_CONFIG%"


:: ============================================================================
:: SECTION 21: animate hydro — velocity arrows using velocity_config.yaml
:: ============================================================================
:: Use --velocity-config for velocity (ft/s) arrows — mutually exclusive with
:: --flow-config.  The channel colour map (--variable) is independent of the
:: velocity overlay and can be any of flow | stage | velocity.

:: Flow colour map + linear velocity arrows:
dsm2ui animate hydro "%FC_HYDRO_H5%" --variable flow ^
    --velocity-config "%VELOCITY_CONFIG%"

:: Stage colour map + velocity arrows:
dsm2ui animate hydro "%FC_HYDRO_H5%" --variable stage ^
    --velocity-config "%VELOCITY_CONFIG%"

:: Godin-daily stage + sub-tidal velocity arrows:
dsm2ui animate hydro "%FC_HYDRO_H5%" --variable stage --transform godin-daily ^
    --velocity-config "%VELOCITY_CONFIG%"


:: ============================================================================
:: SECTION 22: animate hydro — stage deviation bars
:: ============================================================================
:: Overlays per-channel stage deviation bars (blue = above mean, red = below).
:: mean_stage_ft in stage_config.yaml is the user-supplied reference level.
:: reference_stage_range_ft sets the deviation (ft) that fills bar_max_height_m.
::
:: NOTE: --stage-config is only supported with a single HYDRO file.

:: Compute mean stage for each bar in the stage_config.yaml and write it back to the YAML (overwrites mean_stage_ft values):
dsm2ui animate compute-stage-means "%FC_HYDRO_H5%" --stage-config "%STAGE_CONFIG%"
:: Stage colour map + stage deviation bars (dual-view of the stage signal):
dsm2ui animate hydro "%FC_HYDRO_H5%" --variable stage ^
    --stage-config "%STAGE_CONFIG%"

:: Flow colour map + stage deviation bars:
dsm2ui animate hydro "%FC_HYDRO_H5%" --variable flow ^
    --stage-config "%STAGE_CONFIG%"

:: Godin-daily stage + stage deviation bars (sub-tidal signal):
dsm2ui animate hydro "%FC_HYDRO_H5%" --variable stage --transform godin-daily ^
    --stage-config "%STAGE_CONFIG%"

:: Maximum information density — flow colours + velocity arrows + stage deviation bars:
dsm2ui animate hydro "%FC_HYDRO_H5%" --variable flow ^
    --velocity-config "%VELOCITY_CONFIG%" ^
    --stage-config "%STAGE_CONFIG%"


:: ============================================================================
:: SECTION 23: animate flow — standalone flow arrows/bars (no channel colours)
:: ============================================================================
:: Renders only the flow overlay on a tile map background, with no channel
:: colour coding.  Useful for examining flow routing at junctions without the
:: EC/stage signal cluttering the view.
::
:: --flow-config is required.
:: --shapefile is used only for bounding box and tangent computation.
::
:: Flow config YAML schema:
::   scale_mode: linear | sqrt            # arrow scaling law
::   reference_flow: 10000                # cfs value that sets arrow scale
::   reference_arrow_length_m: 500        # metres at reference_flow
::   arrow_width_m: 150
::   bar_width_m: 200
::   bar_max_height_m: 600
::   arrows:
::     - channel: 17                      # DSM2 channel number
::       position: 0.5                    # fractional distance from upstream
::       label: "Sacramento R"
::   bars:
::     - node: 329                        # DSM2 node number
::       label: "Confluence"
::       channels: [10, 11, 12]           # channels meeting at this node

dsm2ui animate flow "%FC_HYDRO_H5%" --flow-config "%FLOW_CONFIG%"

:: Custom port and title:
dsm2ui animate flow "%FC_HYDRO_H5%" --flow-config "%FLOW_CONFIG%" --port 5008 --title "Delta Flow Routing"


:: ============================================================================
:: SECTION 24: animate qual — planning study from network share
:: ============================================================================
:: Uses UNC paths for H5 files on a shared network drive.
:: --shapefile overrides the bundled centrelines (needed when the planning grid
:: differs from the historical grid bundled in dsm2ui).
:: --constituent ec_jer targets the San Joaquin EC constituent.

dsm2ui animate qual "%PLAN_EC_OFF%" "%PLAN_EC_ON%" ^
    --constituent ec_jer ^
    --shapefile "%PLAN_CL%"
