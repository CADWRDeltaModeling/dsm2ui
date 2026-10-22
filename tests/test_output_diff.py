import copy

import pandas as pd

from dsm2ui.output_diff import diff_outputs


def _tables():
    return {"OUTPUT_CHANNEL": pd.DataFrame({
        "NAME": ["rsac075", "rsac075", "rsan007", "mtz", "mtz"],
        "CHAN_NO": [10, 10, 20, 30, 31],
        "DISTANCE": ["5000", "5000", "length", "100", "0"],
        "VARIABLE": ["stage", "flow", "ec", "ec", "ec"],
        "INTERVAL": ["15min"] * 5,
    })}


def _diff(edit):
    scenario = copy.deepcopy(_tables())
    edit(scenario["OUTPUT_CHANNEL"])
    return diff_outputs(_tables(), scenario)


def test_identical_files_have_no_differences():
    diff = diff_outputs(_tables(), _tables())
    assert diff.entries == [] and diff.summary() == {}


def test_only_channel_and_distance_decide_a_change():
    def edit(t):
        t.loc[:, "VARIABLE"] = "ec"  # variable, interval and the like are ignored
        t.loc[:, "INTERVAL"] = "1hour"
        t.loc[0, "DISTANCE"] = "5000.0"  # same number
        t.loc[2, "CHAN_NO"] = 21
        t.loc[3, "DISTANCE"] = "150"
    diff = _diff(edit)
    assert {e["name"]: e["status"] for e in diff.entries} == {"rsan007": "changed", "mtz": "changed"}
    mtz = [e for e in diff.entries if e["name"] == "mtz"][0]
    assert mtz["base"] == [(30, 100.0), (31, 0.0)] and mtz["scenario"] == [(30, 150.0), (31, 0.0)]


def test_new_and_removed_names_and_the_summary():
    def edit(t):
        t.loc[t["NAME"] == "rsan007", "CHAN_NO"] = 21
        t.drop(t[t["NAME"] == "mtz"].index, inplace=True)
        t.loc[len(t)] = ["Brand_New", 40, "100", "ec", "15min"]
    diff = _diff(edit)
    assert diff.summary() == {"output_locations": [
        ("Location changed", ["rsan007"]), ("New", ["Brand_New"]), ("Removed", ["mtz"]),
    ]}
    assert [e["name"] for e in diff.drawn()] == ["mtz", "rsan007"]  # new locations are not drawn


def test_names_match_regardless_of_case_and_length_is_its_own_distance():
    def edit(t):
        t["NAME"] = t["NAME"].str.upper()
        t.loc[2, "DISTANCE"] = "15000"  # 'length' is not equal to a number
    diff = _diff(edit)
    assert [(e["name"], e["status"]) for e in diff.entries] == [("rsan007", "changed")]
