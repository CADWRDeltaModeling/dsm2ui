import logging

import pandas as pd

from dsm2ui.input_diff import diff_channels
from dsm2ui.input_viewer import _add_dx


def _tables(deltax=None, dx_column=None):
    channel = pd.DataFrame({
        "CHAN_NO": [1, 2, 3], "LENGTH": [100.0, 200.0, 300.0], "MANNING": [0.03] * 3, "DISPERSION": [360.0] * 3,
        "UPNODE": [1, 2, 3], "DOWNNODE": [2, 3, 4],
    })
    if dx_column is not None:
        channel["DX"] = dx_column
    tables = {"CHANNEL": channel}
    if deltax is not None:
        tables["SCALAR"] = pd.DataFrame({"NAME": ["theta", "deltax"], "VALUE": ["0.6", deltax]})
    return tables


def test_existing_dx_column_is_kept():
    tables = _tables(deltax="5000", dx_column=[10.0, 20.0, 30.0])
    _add_dx(tables, "unused.inp")
    assert tables["CHANNEL"]["DX"].tolist() == [10.0, 20.0, 30.0]


def test_numeric_deltax_applies_to_every_channel():
    tables = _tables(deltax="5000")
    _add_dx(tables, "unused.inp")
    assert tables["CHANNEL"]["DX"].tolist() == [5000.0] * 3


def test_deltax_file_is_read_from_the_study_directory(tmp_path):
    out = tmp_path / "study" / "output"
    out.mkdir(parents=True)
    (tmp_path / "study" / "variable_dx.csv").write_text("1,5000.0\r\n3,750.5\r\n2,2000\r\n")
    tables = _tables(deltax="variable_dx.csv")
    _add_dx(tables, str(out / "hydro_echo.inp"))
    assert tables["CHANNEL"]["DX"].tolist() == [5000.0, 2000.0, 750.5]


def test_deltax_file_may_have_a_header_and_miss_channels(tmp_path):
    out = tmp_path / "study" / "output"
    out.mkdir(parents=True)
    (tmp_path / "study" / "variable_dx.csv").write_text("channel,dx\n1,100\n3,300\n")
    tables = _tables(deltax="variable_dx.csv")
    _add_dx(tables, str(out / "hydro_echo.inp"))
    dx = tables["CHANNEL"]["DX"].tolist()
    assert dx[0] == 100.0 and dx[2] == 300.0 and pd.isna(dx[1])


def test_dx_is_nan_when_it_cannot_be_found(tmp_path, caplog):
    for deltax in (None, "missing.csv"):
        tables = _tables(deltax=deltax)
        with caplog.at_level(logging.WARNING):
            _add_dx(tables, str(tmp_path / "hydro_echo.inp"))
        assert tables["CHANNEL"]["DX"].isna().all()
    assert "missing.csv" in caplog.text


def test_channel_diff_treats_unavailable_dx_as_unchanged_and_changed_dx_as_a_difference():
    base, scen = _tables(deltax="5000"), _tables(deltax="5000")
    _add_dx(base, "unused.inp")
    _add_dx(scen, "unused.inp")
    assert diff_channels(base, scen).channels == {}
    same_nan = _tables(), _tables()
    for t in same_nan:
        _add_dx(t, "unused.inp")
    assert diff_channels(*same_nan).channels == {}
    changed = _tables(deltax="2500")
    _add_dx(changed, "unused.inp")
    diff = diff_channels(base, changed)
    assert sorted(diff.channels) == [1, 2, 3] and diff.summary()["channels"] == [("DX", [1, 2, 3])]
    assert diff.detail(1)["fields"][-1]["base"] == "5,000.00 ft"
