# Network-Aware EC Correction for DSM2 Animation

> **Feature scope:** `pydsm/analysis/network_correction.py` (core algorithm)
> · `dsm2ui/animate.py` (SlicingReader wrapper + factory)
> · `dsm2ui/animate_cli.py` (CLI options)
>
> **Status:** Implemented and tested (47 tests in pydsm, 79 tests in dsm2ui).

---

## 1. Problem statement

DSM2 QUAL/GTM tidefiles contain model EC (electrical conductivity) at every
channel-end for every time step.  The model has systematic spatial biases:
boundary-condition errors, unresolved mixing, and parameter uncertainty all
shift the EC field in patterned ways.  Sparse observations (typically 10–20
CDEC/DWR monitoring stations) can correct this bias in real time, but naively
applying a single offset ignores the network topology and the spatial structure
of model errors.

The goal is an **additive correction**:

```
corrected(x, t) = model(x, t) + correction(x, t)
```

where `correction` is derived from the residuals `obs_i(t) - model(x_i, t)` at
available observation stations, spread to all other channel-ends via a
network-aware interpolation scheme that:

- Respects the **1D channel network topology** (not Euclidean distance).
- Handles **missing observations** gracefully (adapts on the fly, no code
  changes).
- Applies **before** any downstream transform (Godin filter, rolling average),
  so filtered time series are corrected at source.

---

## 2. Theoretical background

### 2.1 IDW (Inverse Distance Weighting)

The simplest approach.  For each channel-end `x`, the correction is a weighted
sum of available station residuals:

```
correction(x) = Σ w_i · r_i  /  Σ w_i
```

where `w_i = d(i → x)^(−power)` and `d` is the **directed** shortest-path
network distance (UPNODE → DOWNNODE, approximately tidally-filtered flow
direction).  Channel-ends that are upstream of all active observations receive
zero correction because they are unreachable in the directed graph.

**Key property:** with a single observation, the correction is uniform
everywhere (the weight cancels in numerator/denominator).  Two observations
with different residuals produce a blended correction whose gradient is
controlled by `power`.

### 2.2 Optimal Interpolation (OI)

Meteorology's classical data assimilation method.  Finds the minimum-variance
linear unbiased estimate given a background error covariance **B** and
observation error covariance **R**:

```
correction = B_{x,obs} · (B_obs + R)^{−1} · residuals
```

The critical advantage over IDW: the `(B_obs + R)^{−1}` term
**de-weights redundant observations**.  If two monitoring stations are 500 ft
apart they carry nearly the same signal; OI down-weights them jointly, whereas
IDW would apply their corrections independently and over-correct.

OI uses an **undirected** shortest-path graph (corrections spread in both
directions) with a symmetric exponential correlation kernel:

```
B(x, x') = σ_b² · exp(−d_undirected(x, x') / L_c)
```

The `B_obs` and `B_x_all` matrices are pre-computed at construction.  At each
time step, only the sub-matrices for *available* stations are extracted and a
small linear system (`n_obs × n_obs`, typically ≤ 20) is solved.

### 2.3 Why not SUPG / fdaPDE?

fdaPDE's SUPG (Streamline Upwind Petrov-Galerkin) formulation for
advection-dominated transport gives physics-consistent anisotropic
interpolation on a 2D FEM mesh, but:

- DSM2 is a **1D network**, not a 2D domain.
- SUPG requires the velocity field `β(x)` at each point and a triangulated
  mesh — neither is easily available.
- Per-step PDE solves are O(n_channels^1.5) vs the OI solve's O(n_obs^3) ≈ 3 375
  for 15 stations.

SUPG would be an improvement for *post-processing* over a fixed time window
when all data are available.  For live animation it is impractical.

### 2.4 Channel direction kernel (experimental)

An alternative OI kernel that applies a cost multiplier `resistance ≥ 1` to
path segments traversed against the UPNODE→DOWNNODE flow direction:

```
d_sym = sqrt(d_fwd · d_rev)   (geometric-mean symmetrisation)
corr  = exp(−d_sym / L_c)
```

**Limitation (documented):** for purely aligned paths of equal total length, `d_sym = L · sqrt(resistance)` regardless of direction — upstream and downstream neighbours at the same distance receive identical correlation.  The asymmetry only appears for *mixed*-direction paths.  True directional asymmetry requires per-channel `β̄` from the hydro tidefile (Green's function approach — future work).

### 2.5 Green's function approach (future work)

For the 1D advection-diffusion operator `L = −D ∂_xx + β ∂_x`, the covariance
function derived from the stochastic PDE `Lu = W` is asymmetric:

```
B(x, x') ∝ exp(−β(x−x') / (2D)) · exp(−|x−x'| / L_0)
```

where `L_0 = 2D/|β|` and `β̄` is the Godin-filtered (tidally-averaged) velocity
per channel.  This naturally gives stronger correlation downstream (where the
salt signal propagates) and weaker correlation upstream.

The Péclet number `Pe = |β̄| · L / (2D)` quantifies how much asymmetry matters:
- Pe << 1 (most Delta channels): symmetric OI is an accurate approximation.
- Pe >> 1 (main channels during high outflow): asymmetry matters near X2.

**Required change for implementation:** read Godin-filtered velocity per channel
from the hydro tidefile (available via `HydroH5.get_channel_flow` / area).

---

## 3. Architecture

```
pydsm/analysis/network_correction.py
│
├── NetworkCorrector (ABC)
│   └── correct(model_series, observations) → pd.Series   [abstract]
│
├── NetworkIDWCorrector(NetworkCorrector)
│   ├── Directed graph (UPNODE → DOWNNODE, LENGTH weight)
│   ├── Pre-computes: _weights[ce_key][station_id] = d^(−power) | inf
│   └── correct(): weighted sum, missing obs drop automatically
│
├── NetworkOICorrector(NetworkCorrector)
│   ├── Undirected graph (G.to_undirected())
│   ├── Pre-computes: B_obs (n_obs × n_obs), B_x_all (n_ce × n_obs)
│   └── correct(): np.linalg.solve once per step, apply to all channel-ends
│
├── Kernel factories
│   ├── exponential_kernel(length_scale=None)
│   │   corr_fn(total_length_ft) → float
│   └── channel_direction_kernel(length_scale=None, resistance=3.0)
│       corr_fn(segments: list[(chan_no, signed_length_ft)]) → float
│
├── extract_channel_end_values(qual_h5, constituent, time_window)
│   → pd.DataFrame (time × "{chan_no}-upstream/downstream")
│
└── snap_stations_to_channel_ends(stations_gdf, centerlines_gdf, channels_df)
    → pd.DataFrame (station_id index; chan_no, location, node_id, distance_fraction)

dsm2ui/animate.py
│
├── CorrectedQualH5ConcentrationReader(SlicingReader)
│   ├── Wraps QualH5ConcentrationReader (inner reader)
│   ├── Reads both upstream/downstream channel-ends before averaging
│   ├── Applies corrector.correct() at each get_slice / get_slice_range call
│   ├── get_slice_range() called by TransformedSlicingReader → all transforms
│   │   see already-corrected data
│   └── rebuild_corrector(new_corrector): live swap without rebuilding reader
│
├── _make_correction_card(mgr, reader, channels_df, ...)
│   → Panel Card with method selector, IDW/OI params, Apply button
│
└── animate_qual_corrected(h5file, observations_csv, stations_csv, ...)
    ├── Builds CorrectedQualH5ConcentrationReader
    ├── Calls animate_qual factory wiring (GeoAnimatorManager)
    ├── Appends correction card to mgr._controls
    ├── Sets mgr._animate_meta["correction"] for YAML save
    └── Patches mgr.collect_state() to emit the correction section
```

### Transform pipeline (correction order)

```
CorrectedQualH5ConcentrationReader.get_slice_range()   ← correction here
  └─ TransformedSlicingReader (Godin / rolling avg / daily mean)
       └─ BufferedSlicingReader (200-step HDF5 chunk cache)
```

The correction is the **innermost** layer.  All downstream transforms see
corrected data.

---

## 4. Channel table loading

`CorrectedQualH5ConcentrationReader._load_channels(h5file, echo_inp_file)`:

1. Try `/input/channel` in the H5 file (qual-format files — rare).
2. Try `/hydro/input/channel` in the H5 file (hydro H5 has the CHANNEL table).
3. Fall back to `pydsm.input.parser.parse(echo_inp_file)["CHANNEL"]`.

The qual H5 test fixtures do **not** contain the CHANNEL table; the hydro H5
or the echo `.inp` file must be supplied.

---

## 5. API quick-start

### 5.1 Standalone (pydsm)

```python
from pydsm.output.hydroh5 import HydroH5
from pydsm.output.qualh5 import QualH5
from pydsm.analysis.network_correction import (
    extract_channel_end_values,
    snap_stations_to_channel_ends,
    NetworkIDWCorrector,
    NetworkOICorrector,
    exponential_kernel,
)
import geopandas as gpd
import pandas as pd

# 1. Load model data
qual = QualH5("historical_v82_ec.h5")
hydro = HydroH5("historical_v82.h5")
model_df = extract_channel_end_values(qual, "ec")

# 2. Snap stations
channels_df = hydro.get_channels()
stations_gdf = read_stations("cdec_ec_stations.csv")   # pydsm.viz.dsm2gis
centerlines = gpd.read_file("channel_centerlines.geojson")
snapped = snap_stations_to_channel_ends(stations_gdf, centerlines, channels_df)

# 3a. IDW corrector (directed, UPNODE→DOWNNODE)
idw = NetworkIDWCorrector(channels_df, snapped, power=2)

# 3b. OI corrector (undirected, exponential kernel)
oi = NetworkOICorrector(channels_df, snapped, sigma_obs=10.0)

# 4. Apply at each time step
obs = pd.Series({"RSAC075": 820.0, "RSAN007": float("nan")})  # NaN = missing
corrected_row = oi.correct(model_df.iloc[0], obs)
```

### 5.2 Animation (dsm2ui)

```python
import panel as pn
pn.extension()

from dsm2ui.animate import animate_qual_corrected
from pydsm.analysis.network_correction import NetworkOICorrector, ...

# Optional: pre-build OI corrector and pass it in
# (otherwise IDW is built internally from --idw-power)
oi = NetworkOICorrector(channels_df, snapped, sigma_obs=15.0)

mgr = animate_qual_corrected(
    "ec.h5",
    observations_csv="obs.csv",
    stations_csv="stations.csv",
    echo_inp_file="hydro_echo.inp",   # for CHANNEL table
    corrector=oi,                     # None → uses IDW
)
mgr.servable()
```

---

## 6. CLI reference

```
dsm2ui animate qual FILE.h5 [OPTIONS]

Correction options (require --observations-csv):
  --observations-csv PATH       Time-indexed EC observations CSV
                                (columns = station IDs, NaN = missing)
  --stations-csv PATH           CSV with station_id + lat/lon or x/y  [required]
  --centerlines-file PATH       GeoJSON for station snapping (default: bundled)
  --echo-inp PATH               Fallback CHANNEL table source (.inp echo file)
  --max-obs-age TEXT            Max time gap for matching obs [default: 2h]
  --correction-method [idw|oi]  Algorithm [default: idw]
  --idw-power FLOAT             IDW distance exponent [default: 2.0]
  --oi-sigma-obs FLOAT          OI observation error σ in µS/cm [default: 10.0]
  --oi-kernel [exponential|channel_direction]  OI kernel [default: exponential]
  --oi-resistance FLOAT         Against-flow penalty for channel_direction [default: 3.0]
```

**IDW example:**
```bash
dsm2ui animate qual historical_v82_ec.h5 \
  --observations-csv cdec_ec_2024.csv \
  --stations-csv ec_stations.csv \
  --echo-inp hydro_echo_historical.inp \
  --transform godin
```

**OI example:**
```bash
dsm2ui animate qual historical_v82_ec.h5 \
  --observations-csv cdec_ec_2024.csv \
  --stations-csv ec_stations.csv \
  --echo-inp hydro_echo_historical.inp \
  --correction-method oi \
  --oi-sigma-obs 15.0 \
  --transform godin
```

---

## 7. YAML config schema (correction section)

```yaml
correction:
  enabled: true
  observations_csv: /path/to/obs.csv
  stations_csv: /path/to/stations.csv
  centerlines_file: null          # null → bundled centrelines
  echo_inp_file: /path/to/echo.inp
  max_obs_age: 2h
  method: oi                      # idw | oi
  idw:
    power: 2.0
  oi:
    sigma_obs: 15.0
    kernel: exponential           # exponential | channel_direction
    resistance: 3.0               # only used by channel_direction kernel
    length_scale: null            # null → auto from DISPERSION column
```

Load a saved config:
```bash
dsm2ui animate qual --config my_session.yml
```

All correction settings are restored from the YAML, including method and OI
parameters.  The UI correction card is pre-populated with the loaded values.

---

## 8. UI — Observation Correction card

The card is appended to the controls panel when `animate_qual_corrected` is
used.  It contains:

| Widget | Purpose |
|---|---|
| Observations CSV | Path; pre-filled from CLI or config |
| Stations CSV | Path; pre-filled from CLI or config |
| Method (IDW / OI) | Toggles visible parameter section |
| IDW: power slider | Shown when IDW is selected |
| OI: σ_obs, kernel, resistance | Shown when OI is selected |
| **↺ Apply** button | Rebuilds corrector, swaps into reader, clears buffer, re-renders |
| Status line | ⏳ / ✓ / ✗ |

Changing the method and clicking Apply also updates `_animate_meta["correction"]`
so that **Save Config captures the new state**.

---

## 9. Key design decisions

| Decision | Rationale |
|---|---|
| IDW uses directed graph | Hard proxy for "corrections don't spread upstream"; sufficient for most EC patterns |
| OI uses undirected graph | Symmetric kernel requires symmetric distances; OI advantage (de-weighting redundant stations) doesn't depend on directionality |
| Correction before transforms | `get_slice_range()` is the innermost reader; all downstream transforms (Godin, rolling) see corrected data |
| Pre-compute distance matrix / B matrices | Distance matrix and covariance matrices computed once at init; per-step `correct()` is O(n_obs × n_ch) for IDW and O(n_obs³ + n_obs × n_ch) for OI |
| `max_obs_age` guard | Nearest-neighbour match in time; if gap > threshold, return all-NaN → zero correction |
| `corrector=None` backwards-compatible | All existing `animate_qual_corrected` callers without explicit `corrector` get IDW as before |
| Symmetric exponential kernel SPD | Exponential is positive-definite on any metric space; guarantees `(B_obs + R)` is invertible |
| `channel_direction_kernel` asymmetry limitation | Geometric-mean symmetrisation cannot distinguish upstream vs downstream at equal path length; documented in docstring |

---

## 10. Files modified

| File | Change |
|---|---|
| `pydsm/analysis/network_correction.py` | NEW: `NetworkCorrector` ABC, `exponential_kernel`, `channel_direction_kernel`, `NetworkOICorrector`, `_normalise_channels`, `_auto_length_scale`; MODIFIED: `NetworkIDWCorrector` now inherits from ABC |
| `dsm2ui/animate.py` | NEW: `CorrectedQualH5ConcentrationReader.rebuild_corrector()`, `_make_correction_card()`; MODIFIED: `CorrectedQualH5ConcentrationReader.__init__` (corrector param + skip-snap), `animate_qual_corrected` (card + meta + collect_state patch) |
| `dsm2ui/animate_cli.py` | NEW: `--correction-method`, `--oi-sigma-obs`, `--oi-kernel`, `--oi-resistance` options; MODIFIED: config YAML loading (reads `correction` section), `build()` function (constructs OI corrector when method=oi) |
| `pydsm/tests/test_network_correction.py` | NEW: `TestKernels` (7 tests), `TestNetworkOICorrector` (14 tests); MODIFIED: import line |
| `dsm2ui/tests/test_animate.py` | NEW: `TestCorrectedQualH5ConcentrationReader` (12 tests), `TestCorrectedQualH5CLI` (5 tests); MODIFIED: skip markers, `HYDRO_INP` fixture path |

---

## 11. Test coverage

```
pydsm/tests/test_network_correction.py  47 passed
  TestSingleObservation           5  IDW directed graph, exact match, upstream boundary
  TestTwoObservations             4  IDW blending, distance-weighted
  TestMissingObservationAdapts    3  NaN handling, partial missing
  TestAllObservationsMissing      3  All-NaN → model unchanged
  TestExactNodeCorrection         3  IDW exact-node guard
  TestPowerParameter              2  IDW power, max_distance
  TestIntegrationRealData         7  Real H5 fixtures
  TestKernels                     7  exponential_kernel, channel_direction_kernel
  TestNetworkOICorrector          13 B_obs SPD, OI corrections, redundancy de-weighting

dsm2ui/tests/test_animate.py  79 passed
  TestCorrectedQualH5ConcentrationReader  12  _load_channels, slices, all-NaN guard
  TestCorrectedQualH5CLI                   5  CLI branching, UsageError validation
  (+ 62 pre-existing tests for other readers)
```

---

## 12. Observations CSV format

```
datetime,RSAC075,RSAN007,MAL,ANH
1990-01-05 00:00,820.0,,450.0,1200.0
1990-01-05 01:00,825.0,312.0,,1195.0
```

- Index column: `datetime` (parsed as DatetimeIndex)
- Other columns: station IDs matching `station_id` in the stations CSV
- Empty / NaN cells → station absent for that step
- Time resolution should match or be finer than the tidefile time step (typically 1h)

---

## 13. Stations CSV format

```
station_id,lat,lon
RSAC075,38.0327,-121.5648
RSAN007,37.9022,-121.3497
```

Or with UTM coordinates (EPSG:26910):

```
station_id,x,y
RSAC075,641234.0,4211234.0
```

Loaded by `pydsm.viz.dsm2gis.read_stations()` which handles both formats.

---

## 14. Future work

| Item | Description |
|---|---|
| Green's function kernel | Asymmetric OI kernel using per-channel Godin-filtered `β̄` from the hydro tidefile. True directional correction without the limitation of `channel_direction_kernel`. |
| Reservoir nodes | Extend `NetworkOICorrector` and `NetworkIDWCorrector` to include reservoir nodes (currently channels only). |
| `max_obs_age` fallback | Temporal interpolation of missing observations (currently falls back to zero correction; could use last-valid-value or linear interpolation within a gap threshold). |
| Write-back to HDF5 | Optionally write corrected concentrations back to a new HDF5 file for use in downstream tools. |
| OI with time correlation | Extend to 4DVar-style analysis using time correlation of background errors across tidal cycles. |
| Auto `sigma_obs` from sensor metadata | Read instrument accuracy from a station metadata CSV to set per-station observation error. |
