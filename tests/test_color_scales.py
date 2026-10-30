import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString

from dsm2ui.input_viewer import _channel_color_scales


def _chans(dx):
    n = len(dx)
    return gpd.GeoDataFrame({
        "LENGTH": [100.0 * (i + 1) for i in range(n)], "MANNING": [0.03] * n, "DISPERSION": [360.0] * n, "DX": dx,
        "geometry": [LineString([(0, 0), (1, i)]) for i in range(n)],
    })


def test_one_legend_row_and_one_color_when_every_channel_has_the_same_dx():
    colors, legend = _channel_color_scales(_chans([5000.0] * 6))["dx"]["viridis"]
    assert len(set(colors)) == 1
    assert legend.count("border:1px solid #bbb") == 1 and "5,000" in legend and "same value" in legend
    other, _ = _channel_color_scales(_chans([5000.0] * 6))["length"]["viridis"]
    assert len(set(other)) > 1  # properties that vary still use the classes


def test_unavailable_values_are_gray_and_do_not_add_a_class_when_the_rest_is_constant():
    colors, legend = _channel_color_scales(_chans([5000.0, float("nan"), 5000.0]))["dx"]["viridis"]
    assert colors[0] == colors[2] != colors[1] and legend.count("border:1px solid #bbb") == 1


def test_varying_dx_still_has_eight_classes():
    _, legend = _channel_color_scales(_chans([500.0, 1000.0, 2000.0, 5000.0] * 5))["dx"]["viridis"]
    assert legend.count("border:1px solid #bbb") == 8
