import pandas as pd

from dsm2ui.input_viewer import _COLOR_BY, _COLORMAPS, _N_CLASSES, _channel_color_scales


def _chans():
    return pd.DataFrame({
        "LENGTH": [1000, 2000, 3000, 100000], "MANNING": [0.03] * 4,
        "DISPERSION": [10, 20, 30, 40], "DX": [5000, 5000, 5000, 5000],
    })


def test_one_color_per_channel_and_all_properties():
    scales = _channel_color_scales(_chans())
    assert set(scales) == {key for key, *_ in _COLOR_BY.values()}
    for by_map in scales.values():
        assert set(by_map) == {ck for ck, _ in _COLORMAPS.values()}
        for colors, legend in by_map.values():
            assert len(colors) == 4 and legend.count("<span style='width:22px") == _N_CLASSES


def test_outlier_is_clipped_and_constant_column_is_uniform():
    scales = _channel_color_scales(_chans())
    assert scales["manning"]["viridis"][0] == [scales["manning"]["viridis"][0][0]] * 4
    colors = scales["length"]["viridis"][0]
    assert colors[0] != colors[3]
