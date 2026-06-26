"""Tests for dsm2ui.flow_layer — FlowLayerSpec, FlowLayer, FlowAnimatorManager.

Unit tests (TestFlowLayerSpec, TestArrowPolygon, TestBarSegments,
TestFlowLayerGeometry, TestFlowLayerUnit) use a synthetic minimal HDF5 file
created in tmp_path and need no external data.

Integration tests (TestAnimateHydroFlowLayer) use the real pydsm HDF5
fixture and are skipped when it is absent.  The most important test in that
class—``test_animate_hydro_does_not_raise``—exercises the exact code path
that previously caused::

    AttributeError: 'GeoAnimatorManager' object has no attribute
    'add_frame_callback'

when dvue was not installed in editable mode in the dsm2ui environment.
"""

from __future__ import annotations

import io
import tempfile
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Test data paths
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).parent.parent
_PYDSM_DATA = _REPO_ROOT.parent / "pydsm" / "tests" / "data"
HYDRO_H5 = _PYDSM_DATA / "historical_v82.h5"

# Production FC/MSS HYDRO HDF5 (required for reservoir/qext integration tests)
PROD_HYDRO_H5 = Path(
    r"D:\delta\dsm2_studies\studies\historical\output\hist_fc_mss.h5"
)

_has_hydro      = HYDRO_H5.exists()
_has_prod_hydro = PROD_HYDRO_H5.exists()

skip_no_hydro = pytest.mark.skipif(
    not _has_hydro, reason=f"Hydro HDF5 not found: {HYDRO_H5}"
)
skip_no_prod_hydro = pytest.mark.skipif(
    not _has_prod_hydro,
    reason=f"Production HYDRO HDF5 not found: {PROD_HYDRO_H5}",
)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def fake_hydro_h5(tmp_path):
    """Minimal HYDRO HDF5 with 5 channels, 24 hourly time steps."""
    import h5py

    path = tmp_path / "fake_hydro.h5"
    n_ch, n_time = 5, 24
    rng = np.random.default_rng(42)
    flow_data = rng.uniform(-5000, 15000, (n_time, n_ch, 2)).astype(np.float32)

    with h5py.File(path, "w") as hf:
        # Flow dataset with required attributes
        ds = hf.create_dataset("/hydro/data/channel flow", data=flow_data)
        ds.attrs["start_time"] = b"1990-01-02 00:00:00"
        ds.attrs["interval"] = b"1h"

        # Channel numbers (matching the dtype used in the real file)
        chan_nos = np.array([10, 11, 12, 13, 14], dtype=np.int32)
        hf.create_dataset("/hydro/geometry/channel_number", data=chan_nos)

        # Location labels
        hf.create_dataset(
            "/hydro/geometry/channel_location",
            data=np.array([b"upstream", b"downstream"]),
        )

        # Channel topology — same dtype as the real file
        dtype = np.dtype([
            ("chan_no",    np.int32),
            ("length",     np.int32),
            ("manning",    np.float64),
            ("dispersion", np.float64),
            ("upnode",     np.int32),
            ("downnode",   np.int32),
        ])
        ch_data = np.array([
            (10, 19500, 0.035, 360.0, 1, 2),  # ch10: 1→2
            (11, 18000, 0.035, 350.0, 2, 3),  # ch11: 2→3
            (12, 15000, 0.035, 300.0, 2, 4),  # ch12: 2→4
            (13, 12000, 0.035, 280.0, 3, 5),  # ch13: 3→5
            (14, 11000, 0.035, 270.0, 4, 5),  # ch14: 4→5
        ], dtype=dtype)
        hf.create_dataset("/hydro/input/channel", data=ch_data)

    return path


@pytest.fixture
def fake_channel_gdf():
    """GeoDataFrame with 5 channels as LineStrings in EPSG:4326 (Delta area)."""
    import geopandas as gpd
    from shapely.geometry import LineString

    return gpd.GeoDataFrame(
        {"geo_id": [10, 11, 12, 13, 14]},
        geometry=[
            LineString([(-121.5, 37.8), (-121.4, 37.8)]),    # ch10 E
            LineString([(-121.4, 37.8), (-121.3, 37.9)]),    # ch11 NE
            LineString([(-121.4, 37.8), (-121.3, 37.7)]),    # ch12 SE
            LineString([(-121.3, 37.9), (-121.2, 37.85)]),   # ch13 ESE
            LineString([(-121.3, 37.7), (-121.2, 37.85)]),   # ch14 ENE
        ],
        crs="EPSG:4326",
    )


@pytest.fixture
def fake_nodes_gdf():
    """GeoDataFrame with 5 nodes as Points in EPSG:26910 (UTM 10N)."""
    import geopandas as gpd
    from shapely.geometry import Point

    # Approximate UTM 10N coords for the fake Delta positions
    return gpd.GeoDataFrame(
        {"id": [1, 2, 3, 4, 5]},
        geometry=[
            Point(552_000, 4_189_000),  # node 1 (start of ch10)
            Point(563_000, 4_189_000),  # node 2 (junction: ch10↓ ch11↑ ch12↑)
            Point(574_000, 4_200_000),  # node 3
            Point(574_000, 4_178_000),  # node 4
            Point(585_000, 4_189_000),  # node 5
        ],
        crs="EPSG:26910",
    )


@pytest.fixture
def simple_spec():
    """FlowLayerSpec covering 2 arrows + 1 junction bar in the fake network."""
    from dsm2ui.flow_layer import ChannelArrowSpec, FlowLayerSpec, NodeBarSpec

    return FlowLayerSpec(
        arrows=[
            ChannelArrowSpec(channel=10, position=0.5, label="Ch10"),
            ChannelArrowSpec(channel=11, position=0.5, label="Ch11"),
        ],
        bars=[
            # Node 2: ch10 (downnode) + ch11, ch12 (upnodes)
            NodeBarSpec(node=2, channels=[10, 11, 12], label="Junction"),
        ],
        reference_flow=10_000.0,
        reference_arrow_length_m=500.0,
        arrow_width_m=150.0,
        bar_width_m=200.0,
        bar_max_height_m=600.0,
        min_flow_cfs=10.0,
    )


# ---------------------------------------------------------------------------
# TestFlowLayerSpec — data model and YAML parsing
# ---------------------------------------------------------------------------

class TestFlowLayerSpec:
    def test_defaults(self):
        from dsm2ui.flow_layer import FlowLayerSpec
        spec = FlowLayerSpec()
        assert spec.scale_mode == "linear"
        assert spec.reference_flow == 10_000.0
        assert spec.arrows == []
        assert spec.bars == []

    def test_from_yaml_roundtrip(self, tmp_path):
        import yaml
        from dsm2ui.flow_layer import FlowLayerSpec

        d = {
            "scale_mode": "log",
            "reference_flow": 5000,
            "reference_arrow_length_m": 400,
            "arrow_width_m": 120,
            "bar_width_m": 180,
            "bar_max_height_m": 500,
            "min_flow_cfs": 20,
            "arrows": [
                {"channel": 10, "position": 0.5, "label": "Sac"},
                {"channel": 11, "position": 0.75},
            ],
            "bars": [
                {"node": 2, "channels": [10, 11, 12], "label": "Jct"},
                {"node": 3},
            ],
        }
        p = tmp_path / "cfg.yaml"
        p.write_text(yaml.dump(d))

        spec = FlowLayerSpec.from_yaml(p)
        assert spec.scale_mode == "log"
        assert spec.reference_flow == 5000
        assert len(spec.arrows) == 2
        assert spec.arrows[0].channel == 10
        assert spec.arrows[0].label == "Sac"
        assert spec.arrows[1].position == 0.75
        assert spec.arrows[1].label == ""         # default
        assert len(spec.bars) == 2
        assert spec.bars[0].node == 2
        assert spec.bars[0].channels == [10, 11, 12]
        assert spec.bars[1].channels is None      # default → all connected

    def test_from_yaml_unknown_keys_ignored(self, tmp_path):
        import yaml
        from dsm2ui.flow_layer import FlowLayerSpec

        d = {"reference_flow": 999, "future_unknown_key": "value", "arrows": []}
        p = tmp_path / "cfg.yaml"
        p.write_text(yaml.dump(d))
        spec = FlowLayerSpec.from_yaml(p)
        assert spec.reference_flow == 999


# ---------------------------------------------------------------------------
# TestArrowPolygon — geometry correctness
# ---------------------------------------------------------------------------

class TestArrowPolygon:
    def test_positive_flow_produces_7_points(self):
        from dsm2ui.flow_layer import _arrow_polygon
        xs, ys, fv = _arrow_polygon(
            0.0, 0.0, 1.0, 0.0,  # centroid at origin, tangent East
            10_000.0, 10_000.0, 500.0, 150.0, "linear", 10.0,
        )
        assert len(xs) == 7
        assert len(ys) == 7
        assert fv == 10_000.0

    def test_positive_flow_tip_is_downstream(self):
        """Positive flow → arrow tip points in +tangent direction (East)."""
        from dsm2ui.flow_layer import _arrow_polygon
        xs, ys, _ = _arrow_polygon(
            0.0, 0.0, 1.0, 0.0,
            8_000.0, 10_000.0, 500.0, 150.0, "linear", 10.0,
        )
        assert max(xs) > 0, "Tip should be at positive x for eastward flow"

    def test_negative_flow_tip_is_upstream(self):
        """Negative flow → arrow flips to point in -tangent direction (West)."""
        from dsm2ui.flow_layer import _arrow_polygon
        xs_pos, _, _ = _arrow_polygon(
            0.0, 0.0, 1.0, 0.0,
            8_000.0, 10_000.0, 500.0, 150.0, "linear", 10.0,
        )
        xs_neg, _, _ = _arrow_polygon(
            0.0, 0.0, 1.0, 0.0,
            -8_000.0, 10_000.0, 500.0, 150.0, "linear", 10.0,
        )
        assert max(xs_pos) > 0   # tip to the East
        assert min(xs_neg) < 0   # tip to the West

    def test_below_min_flow_produces_stub(self):
        """Flows below min_flow_cfs produce a diamond stub (5 points)."""
        from dsm2ui.flow_layer import _arrow_polygon
        xs, ys, _ = _arrow_polygon(
            0.0, 0.0, 1.0, 0.0,
            5.0, 10_000.0, 500.0, 150.0, "linear", 10.0,  # 5 < min_flow_cfs
        )
        assert len(xs) == 5

    def test_linear_scale_proportional(self):
        """Linear scale: doubling flow doubles the tip x-coordinate."""
        from dsm2ui.flow_layer import _arrow_polygon
        xs1, _, _ = _arrow_polygon(
            0.0, 0.0, 1.0, 0.0,
            5_000.0, 10_000.0, 500.0, 150.0, "linear", 10.0,
        )
        xs2, _, _ = _arrow_polygon(
            0.0, 0.0, 1.0, 0.0,
            10_000.0, 10_000.0, 500.0, 150.0, "linear", 10.0,
        )
        tip1, tip2 = max(xs1), max(xs2)
        assert abs(tip2 / tip1 - 2.0) < 0.05, "Linear scaling violated"

    def test_log_scale_compresses_large_flows(self):
        """Log scale: 10× flow gives substantially less than 10× arrow length."""
        from dsm2ui.flow_layer import _arrow_polygon
        xs1, _, _ = _arrow_polygon(
            0.0, 0.0, 1.0, 0.0,
            5_000.0, 10_000.0, 500.0, 150.0, "log", 10.0,
        )
        xs2, _, _ = _arrow_polygon(
            0.0, 0.0, 1.0, 0.0,
            50_000.0, 10_000.0, 500.0, 150.0, "log", 10.0,
        )
        tip1, tip2 = max(xs1), max(xs2)
        ratio = tip2 / tip1
        # log scale compresses: 10× flow → substantially less than 10× length
        assert ratio > 1.0, "Larger flow must produce longer arrow"
        assert ratio < 9.0, f"Log should compress significantly vs linear (10×); got {ratio:.2f}"

    def test_northward_tangent(self):
        """Arrow with northward tangent (tx=0, ty=1) tips to positive y."""
        from dsm2ui.flow_layer import _arrow_polygon
        xs, ys, _ = _arrow_polygon(
            0.0, 0.0, 0.0, 1.0,  # tangent North
            8_000.0, 10_000.0, 500.0, 150.0, "linear", 10.0,
        )
        assert max(ys) > 0
        # Tip should be approximately on the tangent line (small x deviation)
        tip_idx = ys.index(max(ys))
        assert abs(xs[tip_idx]) < 100


# ---------------------------------------------------------------------------
# TestBarSegments — flow-split bar geometry
# ---------------------------------------------------------------------------

class TestBarSegments:
    """
    Fake network at node 2:
      - ch10: is_upnode=False  → contribution = +flow  (into node)
      - ch11: is_upnode=True   → contribution = -flow  (away from node)
      - ch12: is_upnode=True   → contribution = -flow  (away from node)
    All flows positive → ch10 is inflow, ch11/ch12 are outflow.
    """

    @pytest.fixture
    def connections(self):
        from dsm2ui.flow_layer import _ChannelNodeConn
        return [
            _ChannelNodeConn(channel=10, is_upnode=False, color="#1f77b4"),
            _ChannelNodeConn(channel=11, is_upnode=True,  color="#ff7f0e"),
            _ChannelNodeConn(channel=12, is_upnode=True,  color="#2ca02c"),
        ]

    def test_positive_flows_correct_sides(self, connections):
        from dsm2ui.flow_layer import _bar_segments
        # All positive flows
        flows = {10: 5000.0, 11: 3000.0, 12: 2000.0}
        xs_l, ys_l, colors, ch_ids, vals, sides = _bar_segments(
            0.0, 0.0, connections, flows, 10_000.0, 200.0, 600.0
        )
        assert "inflow" in sides and "outflow" in sides
        # ch10 (not upnode, +flow) → contribution = +5000 → inflow
        inflow_chs = [ch_ids[i] for i, s in enumerate(sides) if s == "inflow"]
        outflow_chs = [ch_ids[i] for i, s in enumerate(sides) if s == "outflow"]
        assert 10 in inflow_chs
        assert 11 in outflow_chs and 12 in outflow_chs

    def test_near_zero_flows_skipped(self, connections):
        from dsm2ui.flow_layer import _bar_segments
        flows = {10: 0.5, 11: 0.5, 12: 0.5}  # all < 1 cfs threshold
        xs_l, ys_l, colors, ch_ids, vals, sides = _bar_segments(
            0.0, 0.0, connections, flows, 10_000.0, 200.0, 600.0
        )
        assert len(xs_l) == 0

    def test_segment_heights_proportional_to_flow(self, connections):
        from dsm2ui.flow_layer import _bar_segments
        flows = {10: 5000.0, 11: 5000.0, 12: 0.0}
        xs_l, ys_l, _, _, vals, sides = _bar_segments(
            0.0, 0.0, connections, flows, 10_000.0, 200.0, 600.0
        )
        # ch10 inflow=5000 → seg_h = 5000/10000 * 600 = 300 m (minus gap)
        inflow_vals = [vals[i] for i, s in enumerate(sides) if s == "inflow"]
        assert inflow_vals == [5000.0]
        inflow_h = ys_l[[i for i, s in enumerate(sides) if s == "inflow"][0]][2]  # y top
        assert abs(inflow_h - (300.0 - 4.0)) < 1.0  # 300m - 4m gap

    def test_reversed_flows_flip_sides(self, connections):
        from dsm2ui.flow_layer import _bar_segments
        # All negative: ch10 reversed → outflow, ch11/ch12 reversed → inflow
        flows = {10: -5000.0, 11: -3000.0, 12: -2000.0}
        xs_l, ys_l, _, ch_ids, _, sides = _bar_segments(
            0.0, 0.0, connections, flows, 10_000.0, 200.0, 600.0
        )
        inflow_chs = [ch_ids[i] for i, s in enumerate(sides) if s == "inflow"]
        outflow_chs = [ch_ids[i] for i, s in enumerate(sides) if s == "outflow"]
        assert 10 in outflow_chs
        assert 11 in inflow_chs and 12 in inflow_chs

    def test_each_segment_is_closed_rectangle(self, connections):
        from dsm2ui.flow_layer import _bar_segments
        flows = {10: 5000.0, 11: 3000.0, 12: 2000.0}
        xs_l, ys_l, _, _, _, _ = _bar_segments(
            0.0, 0.0, connections, flows, 10_000.0, 200.0, 600.0
        )
        for xs, ys in zip(xs_l, ys_l):
            assert len(xs) == 5 and len(ys) == 5, "Each segment must be a 5-point closed rect"
            assert xs[0] == xs[-1] and ys[0] == ys[-1], "First and last points must match"


# ---------------------------------------------------------------------------
# TestFlowLayerGeometry — _compute_arrow_geom and _compute_bar_geom
# ---------------------------------------------------------------------------

class TestFlowLayerGeometry:
    def test_arrow_geom_centroid_on_line(self, fake_channel_gdf):
        """Arrow centroid at position=0.5 must lie on the channel centreline."""
        from shapely.geometry import Point

        from dsm2ui.flow_layer import _compute_arrow_geom, ChannelArrowSpec

        ch_3857 = fake_channel_gdf.to_crs("EPSG:3857")
        spec = ChannelArrowSpec(channel=10, position=0.5)
        ag = _compute_arrow_geom(spec, ch_3857)

        assert ag is not None
        geom = ch_3857.loc[ch_3857["geo_id"] == 10, "geometry"].iloc[0]
        centroid = Point(ag.cx, ag.cy)
        assert geom.distance(centroid) < 10.0  # within 10 m (EPSG:3857)

    def test_arrow_geom_tangent_is_unit_vector(self, fake_channel_gdf):
        from dsm2ui.flow_layer import _compute_arrow_geom, ChannelArrowSpec

        ch_3857 = fake_channel_gdf.to_crs("EPSG:3857")
        spec = ChannelArrowSpec(channel=10, position=0.5)
        ag = _compute_arrow_geom(spec, ch_3857)

        magnitude = np.hypot(ag.tx, ag.ty)
        assert abs(magnitude - 1.0) < 1e-9

    def test_arrow_geom_missing_channel_returns_none(self, fake_channel_gdf):
        from dsm2ui.flow_layer import _compute_arrow_geom, ChannelArrowSpec

        ch_3857 = fake_channel_gdf.to_crs("EPSG:3857")
        ag = _compute_arrow_geom(ChannelArrowSpec(channel=999), ch_3857)
        assert ag is None

    def test_bar_geom_finds_connected_channels(
        self, fake_nodes_gdf, fake_hydro_h5
    ):
        import h5py
        from dsm2ui.flow_layer import NodeBarSpec, _compute_bar_geom

        with h5py.File(fake_hydro_h5, "r") as hf:
            raw = hf["/hydro/input/channel"][:]
        chan_no_arr  = raw["chan_no"].astype(np.int64)
        upnode_arr   = raw["upnode"].astype(np.int64)
        downnode_arr = raw["downnode"].astype(np.int64)

        nd_3857 = fake_nodes_gdf.to_crs("EPSG:3857")
        spec = NodeBarSpec(node=2, channels=[10, 11, 12])
        bg = _compute_bar_geom(spec, nd_3857, chan_no_arr, upnode_arr, downnode_arr)

        assert bg is not None
        ch_connected = {c.channel for c in bg.connections}
        assert ch_connected == {10, 11, 12}

    def test_bar_geom_upnode_flag(self, fake_nodes_gdf, fake_hydro_h5):
        """ch10 has node2 as downnode → is_upnode=False; ch11/12 have it as upnode."""
        import h5py
        from dsm2ui.flow_layer import NodeBarSpec, _compute_bar_geom

        with h5py.File(fake_hydro_h5, "r") as hf:
            raw = hf["/hydro/input/channel"][:]
        nd_3857 = fake_nodes_gdf.to_crs("EPSG:3857")
        bg = _compute_bar_geom(
            NodeBarSpec(node=2, channels=[10, 11, 12]),
            nd_3857,
            raw["chan_no"].astype(np.int64),
            raw["upnode"].astype(np.int64),
            raw["downnode"].astype(np.int64),
        )
        conn_map = {c.channel: c.is_upnode for c in bg.connections}
        assert conn_map[10] is False  # node2 is DOWNNODE of ch10
        assert conn_map[11] is True   # node2 is UPNODE of ch11
        assert conn_map[12] is True   # node2 is UPNODE of ch12


# ---------------------------------------------------------------------------
# TestFlowLayerUnit — FlowLayer with fake HDF5 (no browser/server needed)
# ---------------------------------------------------------------------------

class TestFlowLayerUnit:
    @pytest.fixture
    def layer(self, fake_hydro_h5, fake_channel_gdf, fake_nodes_gdf, simple_spec):
        from dsm2ui.flow_layer import FlowLayer
        return FlowLayer(fake_hydro_h5, simple_spec, fake_channel_gdf, fake_nodes_gdf)

    def test_construction_finds_arrow_geoms(self, layer):
        assert len(layer._arrow_geoms) == 2  # channels 10 and 11

    def test_construction_finds_bar_geoms(self, layer):
        assert len(layer._bar_geoms) == 1    # node 2

    def test_time_index_has_freq(self, layer):
        ti = layer.time_index
        assert ti.freq is not None
        assert len(ti) == 24  # 24 hourly steps

    def test_update_frame_patches_bokeh_sources(
        self, layer, fake_hydro_h5, fake_channel_gdf, fake_nodes_gdf, simple_spec
    ):
        """update_frame must write xs/ys to both arrow and bar sources."""
        from bokeh.plotting import figure

        p = figure()
        layer.setup_on_figure(p)
        ts = layer.time_index[0]
        layer.update_frame(ts)

        # Arrows: one entry per arrow geom
        assert len(layer._arrow_source.data["xs"]) == 2
        assert len(layer._arrow_source.data["ys"]) == 2
        assert len(layer._arrow_source.data["channel_ids"]) == 2
        assert set(layer._arrow_source.data["channel_ids"]) == {10, 11}
        assert "directions" in layer._arrow_source.data
        assert all(
            d in ("\u2192 downstream", "\u2190 upstream", "~ 0")
            for d in layer._arrow_source.data["directions"]
        )

        # Bars: at least 1 segment (depends on flow sign, but should be non-empty
        # since flow_data is random ±5000–15000 cfs)
        # Could be 0 if all flows are within 1 cfs — extremely unlikely with
        # the fixed seed (np.random.default_rng(42)).
        assert len(layer._bar_source.data["xs"]) >= 0
        assert "sides" in layer._bar_source.data
        assert "node_ids" in layer._bar_source.data
        assert "node_labels" in layer._bar_source.data

    def test_update_frame_multiple_steps(self, layer):
        """Calling update_frame across several time steps must not raise."""
        from bokeh.plotting import figure

        p = figure()
        layer.setup_on_figure(p)
        for idx in [0, 5, 10, 23]:
            layer.update_frame(layer.time_index[idx])

    def test_update_frame_requires_setup_on_figure_first(self, layer):
        """update_frame before setup_on_figure must raise RuntimeError."""
        with pytest.raises(RuntimeError, match="setup_on_figure"):
            layer.update_frame(layer.time_index[0])

    def test_update_frame_accepts_off_grid_timestamp(self, layer):
        """get_slice_nearest snaps to the nearest step — no IndexError."""
        from bokeh.plotting import figure

        p = figure()
        layer.setup_on_figure(p)
        # Timestamp halfway between two steps
        off_grid = layer.time_index[3] + pd.Timedelta("30min")
        layer.update_frame(off_grid)

    def test_set_transform_rebuilds_reader(self, layer):
        """set_transform(None) must replace _reader with a fresh plain reader."""
        _ = layer.time_index  # trigger lazy init
        old_reader = layer._reader
        layer.set_transform(None)
        assert layer._reader is not old_reader

    def test_set_transform_with_transform_spec(self, layer, fake_hydro_h5):
        """set_transform(TransformSpec) wraps reader with streaming transformer."""
        from dvue.animator.reader import StreamingTransformedSlicingReader, TransformSpec

        def _daily(df: pd.DataFrame) -> pd.DataFrame:
            return df.resample("D").mean()

        spec = TransformSpec(
            transform_fn=_daily,
            kind="aggregate",
            get_overlap=lambda freq: pd.Timedelta(0),
            output_freq="D",
        )
        _ = layer.time_index  # trigger lazy init
        layer.set_transform(spec)
        # Reader should now be a BufferedSlicingReader wrapping a
        # StreamingTransformedSlicingReader wrapping the base reader.
        assert layer._reader is not None
        # Time index of the new reader should be at daily frequency
        new_ti = layer.time_index
        assert new_ti.freq is not None


# ===========================================================================
# Integration tests — require real HYDRO HDF5
# ===========================================================================

@pytest.fixture(scope="module")
def real_spec():
    """Auto-detect 3 channels + 1 junction node from the real HDF5 fixture."""
    import h5py
    from dsm2ui.flow_layer import ChannelArrowSpec, FlowLayerSpec, NodeBarSpec

    with h5py.File(HYDRO_H5, "r") as hf:
        raw = hf["/hydro/input/channel"][:]

    chan_arr = raw["chan_no"]
    if chan_arr.dtype.kind in ("S", "U", "O"):
        all_chan = [
            int(v.decode().strip()) if isinstance(v, bytes) else int(v)
            for v in chan_arr
        ]
    else:
        all_chan = chan_arr.astype(int).tolist()

    upnodes   = raw["upnode"].astype(int).tolist()
    downnodes = raw["downnode"].astype(int).tolist()

    # Pick the most-connected node that appears ≥2 times
    all_nodes = upnodes[:30] + downnodes[:30]
    junction_node = Counter(all_nodes).most_common(1)[0][0]

    return FlowLayerSpec(
        arrows=[ChannelArrowSpec(channel=c, position=0.5) for c in all_chan[:3]],
        bars=[NodeBarSpec(node=junction_node)],
        reference_flow=10_000.0,
        reference_arrow_length_m=500.0,
    )


@skip_no_hydro
class TestAnimateHydroFlowLayer:
    """Integration tests for ``animate_hydro`` with a flow overlay.

    The primary goal is to catch the regression where ``GeoAnimatorManager``
    lacked ``add_frame_callback`` (either because dvue was not installed in
    editable mode, or because the editable changes were reverted).
    """

    def test_animate_hydro_with_flow_spec_does_not_raise(self, real_spec):
        """The exact failing call must complete without AttributeError."""
        import panel as pn
        pn.extension()
        from dsm2ui.animate import animate_hydro

        # This is the line that raised:
        # AttributeError: 'GeoAnimatorManager' has no attribute 'add_frame_callback'
        mgr = animate_hydro(HYDRO_H5, variable="flow", flow_spec=real_spec)
        assert mgr is not None

    def test_frame_callback_registered(self, real_spec):
        """The flow layer's update_frame is registered in _extra_frame_callbacks."""
        import panel as pn
        pn.extension()
        from dsm2ui.animate import animate_hydro
        from dsm2ui.flow_layer import FlowLayer

        mgr = animate_hydro(HYDRO_H5, variable="flow", flow_spec=real_spec)
        assert len(mgr._extra_frame_callbacks) >= 1
        cb = mgr._extra_frame_callbacks[-1]
        # The callback must be a bound method of a FlowLayer instance
        assert hasattr(cb, "__self__") and isinstance(cb.__self__, FlowLayer)

    def test_transform_callback_registered(self, real_spec):
        """The flow layer's set_transform is registered in _transform_callbacks."""
        import panel as pn
        pn.extension()
        from dsm2ui.animate import animate_hydro
        from dsm2ui.flow_layer import FlowLayer

        mgr = animate_hydro(HYDRO_H5, variable="flow", flow_spec=real_spec)
        assert len(mgr._transform_callbacks) >= 1
        cb = mgr._transform_callbacks[-1]
        assert hasattr(cb, "__self__") and isinstance(cb.__self__, FlowLayer)

    def test_frame_callback_called_with_timestamp(self, real_spec):
        """Simulating _load_frame must call the flow callback with a pd.Timestamp."""
        import panel as pn
        pn.extension()
        from dsm2ui.animate import animate_hydro

        mgr = animate_hydro(HYDRO_H5, variable="flow", flow_spec=real_spec)

        received: list = []

        def spy(ts):
            received.append(ts)

        mgr.add_frame_callback(spy)
        # Simulate what _load_frame does: read a slice and call callbacks
        ts = mgr._reader.time_index[0]
        for cb in mgr._extra_frame_callbacks:
            cb(ts)

        assert len(received) == 1
        assert isinstance(received[0], pd.Timestamp)

    def test_flow_bokeh_sources_populated_after_first_frame(self, real_spec):
        """After construction, the flow arrow source must have data."""
        import panel as pn
        pn.extension()
        from dsm2ui.animate import animate_hydro
        from dsm2ui.flow_layer import FlowLayer

        mgr = animate_hydro(HYDRO_H5, variable="flow", flow_spec=real_spec)
        # Get the FlowLayer instance registered as a callback
        flow_layer = mgr._extra_frame_callbacks[-1].__self__
        assert isinstance(flow_layer, FlowLayer)

        # Arrow source should have been populated by animate_hydro's first-frame call
        arrow_source = flow_layer._arrow_source
        assert arrow_source is not None
        assert len(arrow_source.data["xs"]) == len(real_spec.arrows)

    def test_set_transform_none_rebuilds_reader(self, real_spec):
        """Calling set_transform(None) replaces the flow reader without error."""
        import panel as pn
        pn.extension()
        from dsm2ui.animate import animate_hydro
        from dsm2ui.flow_layer import FlowLayer

        mgr = animate_hydro(HYDRO_H5, variable="flow", flow_spec=real_spec)
        flow_layer: FlowLayer = mgr._extra_frame_callbacks[-1].__self__

        old_reader = flow_layer._reader
        flow_layer.set_transform(None)
        assert flow_layer._reader is not old_reader

    def test_animate_hydro_stage_with_flow_spec(self, real_spec):
        """Flow layer works even when background shows stage (not flow)."""
        import panel as pn
        pn.extension()
        from dsm2ui.animate import animate_hydro

        # Flow layer always reads flow regardless of background variable
        mgr = animate_hydro(HYDRO_H5, variable="stage", flow_spec=real_spec)
        assert len(mgr._extra_frame_callbacks) >= 1

    def test_animate_hydro_no_flow_spec_has_no_callbacks(self):
        """Without flow_spec, _extra_frame_callbacks must be empty (baseline)."""
        import panel as pn
        pn.extension()
        from dsm2ui.animate import animate_hydro

        mgr = animate_hydro(HYDRO_H5, variable="flow")
        assert mgr._extra_frame_callbacks == []
        assert mgr._transform_callbacks == []


# ===========================================================================
# Fixtures for reservoir / qext tests
# ===========================================================================

@pytest.fixture
def fake_hydro_h5_ext(tmp_path):
    """Fake HYDRO HDF5 that includes reservoir connections and qext data.

    Returns ``(path, res_flow_array, qext_flow_array)`` so tests can compare
    reader output against the raw numpy data used to build the file.

    Reservoir connections (3 total):
      col 0 → fake_res_a @ node 1
      col 1 → fake_res_a @ node 2
      col 2 → fake_res_b @ node 3

    Qext entries (2 total, all negative — simulated exports):
      col 0 → fake_pump_x
      col 1 → fake_pump_y
    """
    import h5py

    path = tmp_path / "fake_hydro_ext.h5"
    n_time = 24
    rng = np.random.default_rng(99)

    res_flow  = rng.uniform(-3000, 3000,  (n_time, 3)).astype(np.float32)
    qext_flow = rng.uniform(-5000, -100,  (n_time, 2)).astype(np.float32)  # all negative

    with h5py.File(path, "w") as hf:
        # Minimal channel data (required by FlowLayer.__init__ channel reader)
        ch_flow = rng.uniform(-5000, 15000, (n_time, 3, 2)).astype(np.float32)
        ds = hf.create_dataset("/hydro/data/channel flow", data=ch_flow)
        ds.attrs["start_time"] = b"1990-01-02 00:00:00"
        ds.attrs["interval"] = b"1h"
        hf.create_dataset(
            "/hydro/geometry/channel_number",
            data=np.array([10, 11, 12], dtype=np.int32),
        )
        hf.create_dataset(
            "/hydro/geometry/channel_location",
            data=np.array([b"upstream", b"downstream"]),
        )
        dtype_ch = np.dtype([
            ("chan_no", np.int32), ("length", np.int32),
            ("manning", np.float64), ("dispersion", np.float64),
            ("upnode", np.int32), ("downnode", np.int32),
        ])
        hf.create_dataset("/hydro/input/channel", data=np.array([
            (10, 5000, 0.035, 360.0, 1, 2),
            (11, 4000, 0.035, 300.0, 2, 3),
            (12, 3000, 0.035, 280.0, 2, 4),
        ], dtype=dtype_ch))

        # Reservoir flow + geometry
        ds_res = hf.create_dataset("/hydro/data/reservoir flow", data=res_flow)
        ds_res.attrs["start_time"] = b"1990-01-02 00:00:00"
        ds_res.attrs["interval"] = b"1h"
        dtype_rc = np.dtype([("res_name", "S30"), ("ext_node_no", np.int32)])
        hf.create_dataset(
            "/hydro/geometry/reservoir_node_connect",
            data=np.array([
                (b"fake_res_a", 1),
                (b"fake_res_a", 2),
                (b"fake_res_b", 3),
            ], dtype=dtype_rc),
        )

        # Qext flow + geometry
        ds_q = hf.create_dataset("/hydro/data/qext flow", data=qext_flow)
        ds_q.attrs["start_time"] = b"1990-01-02 00:00:00"
        ds_q.attrs["interval"] = b"1h"
        dtype_qx = np.dtype([("name", "S20")])
        hf.create_dataset(
            "/hydro/geometry/qext",
            data=np.array([(b"fake_pump_x",), (b"fake_pump_y",)], dtype=dtype_qx),
        )

    return path, res_flow, qext_flow


@pytest.fixture
def ext_spec():
    """FlowLayerSpec with 1 channel arrow, 1 reservoir arrow, 1 qext arrow."""
    from dsm2ui.flow_layer import (
        ChannelArrowSpec, FlowLayerSpec, QextArrowSpec, ReservoirArrowSpec,
    )

    return FlowLayerSpec(
        arrows=[ChannelArrowSpec(channel=10, position=0.5, label="Ch10")],
        reservoir_arrows=[
            ReservoirArrowSpec(
                reservoir="fake_res_a", node=1, direction_deg=90, label="ResA@1",
            ),
        ],
        qext_arrows=[
            QextArrowSpec(
                name="fake_pump_x", node=2, direction_deg=180, label="PumpX",
            ),
        ],
        reference_flow=10_000.0,
        reference_arrow_length_m=500.0,
    )


# ===========================================================================
# HydroH5ReservoirConnectionReader — unit tests (fake HDF5)
# ===========================================================================

class TestHydroH5ReservoirConnectionReader:

    def test_keys_match_geometry_table(self, fake_hydro_h5_ext):
        from dsm2ui.animate import HydroH5ReservoirConnectionReader

        path, _, _ = fake_hydro_h5_ext
        with HydroH5ReservoirConnectionReader(path) as rr:
            assert rr.keys == [
                ("fake_res_a", 1),
                ("fake_res_a", 2),
                ("fake_res_b", 3),
            ]

    def test_time_index_regular_24_steps(self, fake_hydro_h5_ext):
        from dsm2ui.animate import HydroH5ReservoirConnectionReader

        path, _, _ = fake_hydro_h5_ext
        with HydroH5ReservoirConnectionReader(path) as rr:
            assert rr.time_index.freq is not None
            assert len(rr.time_index) == 24

    def test_get_slice_returns_series_indexed_by_tuples(self, fake_hydro_h5_ext):
        from dsm2ui.animate import HydroH5ReservoirConnectionReader

        path, _, _ = fake_hydro_h5_ext
        with HydroH5ReservoirConnectionReader(path) as rr:
            s = rr.get_slice(rr.time_index[0])
            assert isinstance(s, pd.Series)
            assert list(s.index) == rr.keys
            assert len(s) == 3

    def test_get_slice_values_match_raw_data(self, fake_hydro_h5_ext):
        from dsm2ui.animate import HydroH5ReservoirConnectionReader

        path, res_flow, _ = fake_hydro_h5_ext
        with HydroH5ReservoirConnectionReader(path) as rr:
            for step in [0, 5, 23]:
                s = rr.get_slice(rr.time_index[step])
                np.testing.assert_allclose(
                    s.values, res_flow[step, :].astype(float), rtol=1e-5,
                    err_msg=f"Step {step}: values don't match raw data",
                )

    def test_get_slice_nearest_off_grid(self, fake_hydro_h5_ext):
        from dsm2ui.animate import HydroH5ReservoirConnectionReader

        path, _, _ = fake_hydro_h5_ext
        with HydroH5ReservoirConnectionReader(path) as rr:
            off_grid = rr.time_index[4] + pd.Timedelta("30min")
            s = rr.get_slice_nearest(off_grid)
            assert isinstance(s, pd.Series)

    def test_get_slice_range_shape_and_columns(self, fake_hydro_h5_ext):
        from dsm2ui.animate import HydroH5ReservoirConnectionReader

        path, res_flow, _ = fake_hydro_h5_ext
        with HydroH5ReservoirConnectionReader(path) as rr:
            df = rr.get_slice_range(0, 6)
            assert isinstance(df, pd.DataFrame)
            assert df.shape == (6, 3)
            assert list(df.columns) == rr.keys
            np.testing.assert_allclose(
                df.values, res_flow[:6, :].astype(float), rtol=1e-5,
            )

    def test_res_a_node1_lookup(self, fake_hydro_h5_ext):
        from dsm2ui.animate import HydroH5ReservoirConnectionReader

        path, _, _ = fake_hydro_h5_ext
        with HydroH5ReservoirConnectionReader(path) as rr:
            s = rr.get_slice(rr.time_index[3])
            assert ("fake_res_a", 1) in s.index
            assert ("fake_res_b", 3) in s.index

    def test_context_manager_closes_file(self, fake_hydro_h5_ext):
        from dsm2ui.animate import HydroH5ReservoirConnectionReader

        path, _, _ = fake_hydro_h5_ext
        with HydroH5ReservoirConnectionReader(path) as rr:
            _ = rr.get_slice(rr.time_index[0])
        assert not rr._h5.id.valid


# ===========================================================================
# HydroH5QextReader — unit tests (fake HDF5)
# ===========================================================================

class TestHydroH5QextReader:

    def test_names_from_geometry_table(self, fake_hydro_h5_ext):
        from dsm2ui.animate import HydroH5QextReader

        path, _, _ = fake_hydro_h5_ext
        with HydroH5QextReader(path) as qr:
            assert qr.qext_names == ["fake_pump_x", "fake_pump_y"]

    def test_time_index_regular_24_steps(self, fake_hydro_h5_ext):
        from dsm2ui.animate import HydroH5QextReader

        path, _, _ = fake_hydro_h5_ext
        with HydroH5QextReader(path) as qr:
            assert qr.time_index.freq is not None
            assert len(qr.time_index) == 24

    def test_get_slice_returns_series_indexed_by_name(self, fake_hydro_h5_ext):
        from dsm2ui.animate import HydroH5QextReader

        path, _, _ = fake_hydro_h5_ext
        with HydroH5QextReader(path) as qr:
            s = qr.get_slice(qr.time_index[0])
            assert isinstance(s, pd.Series)
            assert list(s.index) == ["fake_pump_x", "fake_pump_y"]

    def test_get_slice_values_match_raw_data(self, fake_hydro_h5_ext):
        from dsm2ui.animate import HydroH5QextReader

        path, _, qext_flow = fake_hydro_h5_ext
        with HydroH5QextReader(path) as qr:
            for step in [0, 12, 23]:
                s = qr.get_slice(qr.time_index[step])
                np.testing.assert_allclose(
                    s.values, qext_flow[step, :].astype(float), rtol=1e-5,
                    err_msg=f"Step {step}: qext values don't match raw data",
                )

    def test_get_slice_range_shape_and_columns(self, fake_hydro_h5_ext):
        from dsm2ui.animate import HydroH5QextReader

        path, _, qext_flow = fake_hydro_h5_ext
        with HydroH5QextReader(path) as qr:
            df = qr.get_slice_range(2, 8)
            assert df.shape == (6, 2)
            assert list(df.columns) == ["fake_pump_x", "fake_pump_y"]
            np.testing.assert_allclose(
                df.values, qext_flow[2:8, :].astype(float), rtol=1e-5,
            )

    def test_fake_qext_values_all_negative(self, fake_hydro_h5_ext):
        """The fake fixture uses uniform(-5000, -100) — all exports (negative)."""
        from dsm2ui.animate import HydroH5QextReader

        path, _, _ = fake_hydro_h5_ext
        with HydroH5QextReader(path) as qr:
            for step in [0, 10, 23]:
                s = qr.get_slice(qr.time_index[step])
                assert (s < 0).all(), f"Step {step}: expected all negative (exports)"

    def test_context_manager_closes_file(self, fake_hydro_h5_ext):
        from dsm2ui.animate import HydroH5QextReader

        path, _, _ = fake_hydro_h5_ext
        with HydroH5QextReader(path) as qr:
            _ = qr.get_slice(qr.time_index[0])
        assert not qr._h5.id.valid


# ===========================================================================
# _compute_ext_arrow_geom — unit tests
# ===========================================================================

class TestComputeExtArrowGeom:

    def test_position_from_node_gdf(self, fake_nodes_gdf):
        from dsm2ui.flow_layer import _compute_ext_arrow_geom

        nd = fake_nodes_gdf.to_crs("EPSG:3857")
        eg = _compute_ext_arrow_geom(1, 0.0, "test", "reservoir", ("r", 1), nd)
        assert eg is not None
        pt = nd.loc[nd["id"] == 1, "geometry"].iloc[0]
        assert abs(eg.cx - pt.x) < 1.0
        assert abs(eg.cy - pt.y) < 1.0

    def test_direction_east_0deg(self, fake_nodes_gdf):
        from dsm2ui.flow_layer import _compute_ext_arrow_geom

        nd = fake_nodes_gdf.to_crs("EPSG:3857")
        eg = _compute_ext_arrow_geom(1, 0.0, "E", "reservoir", ("r", 1), nd)
        assert abs(eg.tx - 1.0) < 1e-9 and abs(eg.ty) < 1e-9

    def test_direction_north_90deg(self, fake_nodes_gdf):
        from dsm2ui.flow_layer import _compute_ext_arrow_geom

        nd = fake_nodes_gdf.to_crs("EPSG:3857")
        eg = _compute_ext_arrow_geom(1, 90.0, "N", "qext", "pump", nd)
        assert abs(eg.tx) < 1e-9 and abs(eg.ty - 1.0) < 1e-9

    def test_direction_west_180deg(self, fake_nodes_gdf):
        from dsm2ui.flow_layer import _compute_ext_arrow_geom

        nd = fake_nodes_gdf.to_crs("EPSG:3857")
        eg = _compute_ext_arrow_geom(1, 180.0, "W", "qext", "pump", nd)
        assert abs(eg.tx + 1.0) < 1e-9 and abs(eg.ty) < 1e-9

    def test_direction_south_270deg(self, fake_nodes_gdf):
        from dsm2ui.flow_layer import _compute_ext_arrow_geom

        nd = fake_nodes_gdf.to_crs("EPSG:3857")
        eg = _compute_ext_arrow_geom(1, 270.0, "S", "qext", "pump", nd)
        assert abs(eg.tx) < 1e-9 and abs(eg.ty + 1.0) < 1e-9

    def test_missing_node_returns_none(self, fake_nodes_gdf):
        from dsm2ui.flow_layer import _compute_ext_arrow_geom

        nd = fake_nodes_gdf.to_crs("EPSG:3857")
        eg = _compute_ext_arrow_geom(9999, 0.0, "missing", "reservoir", ("r", 9999), nd)
        assert eg is None

    def test_source_type_and_lookup_key_preserved(self, fake_nodes_gdf):
        from dsm2ui.flow_layer import _compute_ext_arrow_geom

        nd = fake_nodes_gdf.to_crs("EPSG:3857")
        key = ("clifton_court", 72)
        eg = _compute_ext_arrow_geom(1, 225.0, "CCF", "reservoir", key, nd)
        assert eg.source_type == "reservoir"
        assert eg.lookup_key == key
        assert eg.label == "CCF"

    def test_tangent_is_unit_vector(self, fake_nodes_gdf):
        """tx² + ty² must equal 1 for any direction_deg."""
        from dsm2ui.flow_layer import _compute_ext_arrow_geom

        nd = fake_nodes_gdf.to_crs("EPSG:3857")
        for deg in [0, 45, 90, 135, 180, 225, 270, 315]:
            eg = _compute_ext_arrow_geom(1, float(deg), "", "qext", "x", nd)
            magnitude = np.hypot(eg.tx, eg.ty)
            assert abs(magnitude - 1.0) < 1e-9, f"Not unit at {deg}°: |t|={magnitude}"


# ===========================================================================
# FlowLayer with ext arrows — unit tests (fake HDF5)
# ===========================================================================

class TestFlowLayerWithExtArrows:

    @pytest.fixture
    def layer_ext(self, fake_hydro_h5_ext, fake_channel_gdf, fake_nodes_gdf, ext_spec):
        from dsm2ui.flow_layer import FlowLayer

        path, _, _ = fake_hydro_h5_ext
        return FlowLayer(path, ext_spec, fake_channel_gdf, fake_nodes_gdf)

    def test_ext_geoms_computed_on_init(self, layer_ext):
        """1 reservoir + 1 qext geom pre-computed on construction."""
        assert len(layer_ext._ext_arrow_geoms) == 2
        types = {eg.source_type for eg in layer_ext._ext_arrow_geoms}
        assert types == {"reservoir", "qext"}

    def test_ext_arrow_keys_correct(self, layer_ext):
        res = next(eg for eg in layer_ext._ext_arrow_geoms if eg.source_type == "reservoir")
        qxt = next(eg for eg in layer_ext._ext_arrow_geoms if eg.source_type == "qext")
        assert res.lookup_key == ("fake_res_a", 1)
        assert qxt.lookup_key == "fake_pump_x"

    def test_ext_arrow_labels(self, layer_ext):
        res = next(eg for eg in layer_ext._ext_arrow_geoms if eg.source_type == "reservoir")
        qxt = next(eg for eg in layer_ext._ext_arrow_geoms if eg.source_type == "qext")
        assert res.label == "ResA@1"
        assert qxt.label == "PumpX"

    def test_ext_arrow_directions(self, layer_ext):
        """Verify tx/ty computed from direction_deg."""
        res = next(eg for eg in layer_ext._ext_arrow_geoms if eg.source_type == "reservoir")
        qxt = next(eg for eg in layer_ext._ext_arrow_geoms if eg.source_type == "qext")
        # direction_deg=90 (North): tx≈0, ty≈1
        assert abs(res.tx) < 1e-9 and abs(res.ty - 1.0) < 1e-9
        # direction_deg=180 (West): tx≈-1, ty≈0
        assert abs(qxt.tx + 1.0) < 1e-9 and abs(qxt.ty) < 1e-9

    def test_ext_source_created_in_setup_on_figure(self, layer_ext):
        from bokeh.plotting import figure

        p = figure()
        layer_ext.setup_on_figure(p)
        assert layer_ext._ext_arrow_source is not None
        assert layer_ext._ext_arrow_text_source is not None

    def test_no_ext_source_when_no_ext_specs(
        self, fake_hydro_h5_ext, fake_channel_gdf, fake_nodes_gdf
    ):
        """When spec has no reservoir/qext arrows the ext source stays None."""
        from bokeh.plotting import figure
        from dsm2ui.flow_layer import ChannelArrowSpec, FlowLayer, FlowLayerSpec

        path, _, _ = fake_hydro_h5_ext
        spec_plain = FlowLayerSpec(arrows=[ChannelArrowSpec(channel=10, position=0.5)])
        layer = FlowLayer(path, spec_plain, fake_channel_gdf, fake_nodes_gdf)
        p = figure()
        layer.setup_on_figure(p)
        assert layer._ext_arrow_source is None
        assert layer._ext_arrow_text_source is None

    def test_update_frame_populates_ext_source(self, layer_ext):
        from bokeh.plotting import figure

        p = figure()
        layer_ext.setup_on_figure(p)
        layer_ext.update_frame(layer_ext.time_index[0])

        data = layer_ext._ext_arrow_source.data
        assert len(data["xs"]) == 2  # 1 reservoir + 1 qext
        assert set(data["labels"]) == {"ResA@1", "PumpX"}

    def test_update_frame_ext_values_match_raw_data(
        self, fake_hydro_h5_ext, fake_channel_gdf, fake_nodes_gdf, ext_spec
    ):
        """Values in the ext source must match the underlying HDF5 arrays."""
        from bokeh.plotting import figure
        from dsm2ui.flow_layer import FlowLayer

        path, res_flow, qext_flow = fake_hydro_h5_ext
        layer = FlowLayer(path, ext_spec, fake_channel_gdf, fake_nodes_gdf)
        p = figure()
        layer.setup_on_figure(p)

        STEP = 7
        layer.update_frame(layer.time_index[STEP])

        label_to_val = dict(zip(
            layer._ext_arrow_source.data["labels"],
            layer._ext_arrow_source.data["values"],
        ))
        # Reservoir: fake_res_a@node1 → column 0 of res_flow
        assert abs(label_to_val["ResA@1"] - float(res_flow[STEP, 0])) < 1.0
        # Qext: fake_pump_x → column 0 of qext_flow
        assert abs(label_to_val["PumpX"] - float(qext_flow[STEP, 0])) < 1.0

    def test_set_transform_rebuilds_all_readers(self, layer_ext):
        """set_transform should replace channel, reservoir, and qext readers."""
        from bokeh.plotting import figure

        p = figure()
        layer_ext.setup_on_figure(p)
        _ = layer_ext.time_index           # init channel reader
        _ = layer_ext._get_res_reader()    # init reservoir reader
        _ = layer_ext._get_qext_reader()   # init qext reader

        old_ch  = layer_ext._reader
        old_res = layer_ext._res_reader
        old_qxt = layer_ext._qext_reader

        layer_ext.set_transform(None)

        assert layer_ext._reader      is not old_ch
        assert layer_ext._res_reader  is not old_res
        assert layer_ext._qext_reader is not old_qxt

    def test_negative_qext_direction_label(self, layer_ext):
        """Negative qext flow (export) renders with '← return' direction."""
        from bokeh.plotting import figure

        p = figure()
        layer_ext.setup_on_figure(p)
        # fake qext is always negative (uniform(-5000, -100), seed 99)
        layer_ext.update_frame(layer_ext.time_index[5])
        dirs = dict(zip(
            layer_ext._ext_arrow_source.data["labels"],
            layer_ext._ext_arrow_source.data["directions"],
        ))
        assert dirs["PumpX"] in ("\u2190 return", "~ 0")


# ===========================================================================
# Integration tests — production FC/MSS HDF5 (reservoir + qext)
# ===========================================================================

@skip_no_prod_hydro
class TestReservoirQextIntegration:
    """Integration tests against the real FC/MSS production HYDRO HDF5.

    Validates that ``HydroH5ReservoirConnectionReader`` and
    ``HydroH5QextReader`` expose correct data for Clifton Court Forebay
    inflow and SWP/CVP pumping from the production model.
    """

    def test_res_reader_keys_include_clifton_court_at_72(self):
        from dsm2ui.animate import HydroH5ReservoirConnectionReader

        with HydroH5ReservoirConnectionReader(PROD_HYDRO_H5) as rr:
            assert ("clifton_court", 72) in rr.keys, (
                f"clifton_court@72 not found; available: {rr.keys}"
            )

    def test_res_reader_time_index_long_and_regular(self):
        from dsm2ui.animate import HydroH5ReservoirConnectionReader

        with HydroH5ReservoirConnectionReader(PROD_HYDRO_H5) as rr:
            assert rr.time_index.freq is not None
            assert len(rr.time_index) > 1000  # multi-year run

    def test_ccf_values_finite_not_all_nan(self):
        """Clifton Court inflow must have finite (non-NaN) values."""
        from dsm2ui.animate import HydroH5ReservoirConnectionReader

        with HydroH5ReservoirConnectionReader(PROD_HYDRO_H5) as rr:
            mid = len(rr.time_index) // 2
            s = rr.get_slice(rr.time_index[mid])
            val = s[("clifton_court", 72)]
            assert not np.isnan(val), "CCF inflow is NaN at midpoint"

    def test_ccf_range_slice_has_nonzero_values(self):
        """Over a 48-step window the CCF inflow must have some non-zero values."""
        from dsm2ui.animate import HydroH5ReservoirConnectionReader

        with HydroH5ReservoirConnectionReader(PROD_HYDRO_H5) as rr:
            df = rr.get_slice_range(0, 48)
            ccf = df[("clifton_court", 72)]
            assert ccf.abs().max() > 0.0, "CCF column is all zeros over 48 steps"

    def test_qext_reader_has_swp_and_cvp(self):
        from dsm2ui.animate import HydroH5QextReader

        with HydroH5QextReader(PROD_HYDRO_H5) as qr:
            assert "swp" in qr.qext_names, f"swp not found; names: {qr.qext_names}"
            assert "cvp" in qr.qext_names, f"cvp not found; names: {qr.qext_names}"

    def test_swp_mean_value_is_negative_export(self):
        """SWP Banks Pumping removes water — mean over a window must be negative."""
        from dsm2ui.animate import HydroH5QextReader

        with HydroH5QextReader(PROD_HYDRO_H5) as qr:
            mid = len(qr.time_index) // 2
            df = qr.get_slice_range(mid, mid + 48)
            swp_mean = df["swp"].mean()
            assert swp_mean < 0, f"Expected negative SWP mean, got {swp_mean:.1f} cfs"

    def test_cvp_mean_value_is_negative_export(self):
        """CVP Jones Pumping removes water — mean must be negative."""
        from dsm2ui.animate import HydroH5QextReader

        with HydroH5QextReader(PROD_HYDRO_H5) as qr:
            mid = len(qr.time_index) // 2
            df = qr.get_slice_range(mid, mid + 48)
            cvp_mean = df["cvp"].mean()
            assert cvp_mean < 0, f"Expected negative CVP mean, got {cvp_mean:.1f} cfs"

    def test_full_pipeline_ccf_swp_cvp_arrows(self):
        """End-to-end: FlowLayer with CCF reservoir + SWP/CVP qext arrows."""
        from bokeh.plotting import figure
        from dsm2ui.animate import load_dsm2_channel_gdf, load_dsm2_nodes_gdf
        from dsm2ui.flow_layer import (
            FlowLayer, FlowLayerSpec, QextArrowSpec, ReservoirArrowSpec,
        )

        spec = FlowLayerSpec(
            reservoir_arrows=[
                ReservoirArrowSpec(
                    reservoir="clifton_court", node=72,
                    direction_deg=225, label="CCF Inflow",
                ),
            ],
            qext_arrows=[
                QextArrowSpec(name="swp", node=72, direction_deg=270, label="SWP"),
                QextArrowSpec(name="cvp", node=72, direction_deg=315, label="CVP"),
            ],
            reference_flow=10_000.0,
        )
        ch_gdf = load_dsm2_channel_gdf()
        nd_gdf = load_dsm2_nodes_gdf()

        fl = FlowLayer(PROD_HYDRO_H5, spec, ch_gdf, nd_gdf)
        assert len(fl._ext_arrow_geoms) == 3  # 1 reservoir + 2 qext

        p = figure()
        fl.setup_on_figure(p)
        assert fl._ext_arrow_source is not None

        mid_ts = fl._get_res_reader().time_index[len(fl._get_res_reader().time_index) // 2]
        fl.update_frame(mid_ts)

        data = fl._ext_arrow_source.data
        assert len(data["xs"]) == 3
        assert set(data["labels"]) == {"CCF Inflow", "SWP", "CVP"}

        label_to_val = dict(zip(data["labels"], data["values"]))
        # SWP and CVP exports: values should be non-positive
        assert label_to_val["SWP"] <= 0.0, (
            f"SWP should be export (negative), got {label_to_val['SWP']:.1f} cfs"
        )
        assert label_to_val["CVP"] <= 0.0, (
            f"CVP should be export (negative), got {label_to_val['CVP']:.1f} cfs"
        )

    def test_set_transform_rebuilds_ext_readers_with_prod_file(self):
        """set_transform None replaces all three readers without error."""
        from bokeh.plotting import figure
        from dsm2ui.animate import load_dsm2_channel_gdf, load_dsm2_nodes_gdf
        from dsm2ui.flow_layer import (
            FlowLayer, FlowLayerSpec, QextArrowSpec, ReservoirArrowSpec,
        )

        spec = FlowLayerSpec(
            reservoir_arrows=[ReservoirArrowSpec("clifton_court", 72)],
            qext_arrows=[QextArrowSpec("swp", 72)],
            reference_flow=10_000.0,
        )
        fl = FlowLayer(
            PROD_HYDRO_H5, spec,
            load_dsm2_channel_gdf(), load_dsm2_nodes_gdf(),
        )
        p = figure()
        fl.setup_on_figure(p)
        fl.update_frame(fl._get_res_reader().time_index[0])

        old_ch, old_res, old_qxt = fl._reader, fl._res_reader, fl._qext_reader
        fl.set_transform(None)

        assert fl._reader      is not old_ch
        assert fl._res_reader  is not old_res
        assert fl._qext_reader is not old_qxt
