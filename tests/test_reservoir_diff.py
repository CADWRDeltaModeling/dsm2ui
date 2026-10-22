import copy

import pandas as pd

from dsm2ui.reservoir_diff import diff_reservoirs


def _tables():
    return {
        "RESERVOIR": pd.DataFrame({"NAME": ["alpha", "beta"], "AREA": [10.0, 20.0], "BOT_ELEV": [-5.0, -6.0]}),
        "RESERVOIR_VOL": pd.DataFrame({
            "RES_NAME": ["alpha", "alpha", "beta", "beta"], "ELEV": [-5.0, 5.0, -6.0, 6.0], "AREA": [0.0, 100.0, 0.0, 200.0],
        }),
        "RESERVOIR_IC": pd.DataFrame({"RES_NAME": ["alpha", "beta"], "STAGE": [1.0, 2.0]}),
        "RESERVOIR_CONNECTION": pd.DataFrame({
            "RES_NAME": ["alpha", "alpha", "beta"], "NODE": [1, 2, 3], "COEF_IN": [100.0, 100.0, 50.0],
            "COEF_OUT": [100.0, 100.0, 50.0],
        }),
    }


def _scenario(edit):
    tables = copy.deepcopy(_tables())
    edit(tables)
    return diff_reservoirs(_tables(), tables)


def test_identical_files_have_no_differences():
    diff = diff_reservoirs(_tables(), _tables())
    assert diff.reservoirs == {} and diff.summary() == {"reservoirs": []}


def test_new_and_removed_reservoirs():
    def edit(t):
        t["RESERVOIR"] = pd.concat(
            [t["RESERVOIR"][t["RESERVOIR"]["NAME"] != "beta"],
             pd.DataFrame({"NAME": ["gamma"], "AREA": [1.0], "BOT_ELEV": [0.0]})], ignore_index=True,
        )
    diff = _scenario(edit)
    assert diff.kinds == {"beta": ["Removed reservoir"], "gamma": ["New reservoir"]}
    assert diff.detail("beta")["status"] == "removed" and diff.detail("gamma")["status"] == "new"


def test_connection_changes_are_reported_per_node():
    def edit(t):
        t["RESERVOIR_CONNECTION"].loc[0, "COEF_IN"] = 150.0  # coefficient
        t["RESERVOIR_CONNECTION"].loc[1, "NODE"] = 9  # node 2 removed, node 9 added
    diff = _scenario(edit)
    assert diff.kinds == {"alpha": ["Reservoir connections"]}
    changed = {c["node"] for c in diff.detail("alpha")["connections"] if c["changed"]}
    assert changed == {1, 2, 9}


def test_area_bottom_elevation_curve_and_initial_condition():
    def edit(t):
        t["RESERVOIR"].loc[0, "AREA"] = 11.0
        t["RESERVOIR"].loc[0, "BOT_ELEV"] = -4.0
        t["RESERVOIR_VOL"].loc[3, "AREA"] = 250.0
        t["RESERVOIR_IC"].loc[0, "STAGE"] = 3.0
    diff = _scenario(edit)
    assert diff.kinds == {
        "alpha": ["Area / bottom elevation", "Area / bottom elevation", "Initial condition"],
        "beta": ["Elevation vs area"],
    }
    assert dict(diff.summary()["reservoirs"]) == {
        "Area / bottom elevation": ["alpha"], "Elevation vs area": ["beta"], "Initial condition": ["alpha"],
    }
    rows = diff.detail("beta")["curve"]["rows"]
    assert rows == [{"point": 2, "base": (6.0, 200.0), "scenario": (6.0, 250.0)}]


def test_curve_with_a_different_number_of_points():
    def edit(t):
        t["RESERVOIR_VOL"] = t["RESERVOIR_VOL"][t["RESERVOIR_VOL"].index != 1].reset_index(drop=True)
    detail = _scenario(edit).detail("alpha")
    assert detail["curve"]["counts"] == (2, 1)
    assert detail["curve"]["rows"] == [{"point": 2, "base": (5.0, 100.0), "scenario": None}]
