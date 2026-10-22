import copy

import pandas as pd

from dsm2ui.input_diff import diff_channels


def _tables():
    return {
        "CHANNEL": pd.DataFrame({
            "CHAN_NO": [1, 2, 3], "LENGTH": [1000, 2000, 3000], "MANNING": [0.03] * 3,
            "DISPERSION": [360.0] * 3, "UPNODE": [1, 2, 3], "DOWNNODE": [2, 3, 4], "DX": [5000.0] * 3,
        }),
        "XSECT_LAYER": pd.DataFrame({
            "CHAN_NO": [1, 1, 2, 2], "DIST": [0.5, 0.5, 0.5, 0.5], "ELEV": [-5.0, 5.0, -5.0, 5.0],
            "AREA": [0.0, 100.0, 0.0, 100.0], "WIDTH": [0.0, 20.0, 0.0, 20.0], "WET_PERIM": [0.0, 21.0, 0.0, 21.0],
        }),
        "CHANNEL_IC": pd.DataFrame({
            "CHAN_NO": [1, 1, 2, 2, 3, 3], "DISTANCE": ["0", "length"] * 3,
            "STAGE": [1.0] * 6, "FLOW": [10.0] * 6,
        }),
    }


def _scenario(edit):
    tables = copy.deepcopy(_tables())
    edit(tables)
    return diff_channels(_tables(), tables)


def test_identical_files_have_no_differences():
    diff = diff_channels(_tables(), _tables())
    assert diff.channels == {} and diff.nodes == {}


def test_channel_field_changes_are_reported_per_channel():
    def edit(t):
        t["CHANNEL"].loc[0, "MANNING"] = 0.05
        t["CHANNEL"].loc[1, "DX"] = 4000.0
    diff = _scenario(edit)
    assert set(diff.channels) == {1, 2}
    assert diff.channels[1][0].startswith("Manning's n")
    assert diff.nodes == {}


def test_swapped_up_and_down_node_flags_both_nodes():
    def edit(t):
        t["CHANNEL"].loc[0, ["UPNODE", "DOWNNODE"]] = [2, 1]
    diff = _scenario(edit)
    assert set(diff.channels) == {1}
    assert set(diff.nodes) == {1, 2}


def test_cross_section_changes():
    def edit(t):
        t["XSECT_LAYER"].loc[1, "AREA"] = 150.0
        t["XSECT_LAYER"].loc[2:3, "DIST"] = 0.6
    diff = _scenario(edit)
    assert diff.channels[1] == ["Cross-section area changed"]
    assert diff.channels[2] == ["Cross-section locations changed"]


def test_initial_condition_changes():
    def edit(t):
        t["CHANNEL_IC"].loc[1, "STAGE"] = 2.0
        t["CHANNEL_IC"].loc[2, "FLOW"] = 99.0
    diff = _scenario(edit)
    assert diff.channels[1] == ["Initial stage changed"]
    assert diff.channels[2] == ["Initial flow changed"]


def test_new_channel_and_new_node():
    def edit(t):
        row = {"CHAN_NO": 4, "LENGTH": 500, "MANNING": 0.03, "DISPERSION": 360.0, "UPNODE": 3, "DOWNNODE": 9, "DX": 5000.0}
        t["CHANNEL"] = pd.concat([t["CHANNEL"], pd.DataFrame([row])], ignore_index=True)
    diff = _scenario(edit)
    assert diff.channels == {4: ["New channel"]}
    assert diff.nodes == {9: ["New node"]}  # node 3 only gained channel 4, so it is not flagged


def test_removed_channel_and_node():
    def edit(t):
        t["CHANNEL"] = t["CHANNEL"][t["CHANNEL"]["CHAN_NO"] != 3].reset_index(drop=True)
    diff = _scenario(edit)
    assert diff.channels == {3: ["Channel removed"]}
    assert diff.nodes[4] == ["Node removed"]
    assert diff.nodes[3] == ["Channel 3 removed"]


def test_detail_of_a_changed_channel():
    def edit(t):
        t["CHANNEL"].loc[0, "MANNING"] = 0.05
        t["XSECT_LAYER"].loc[1, "AREA"] = 150.0
        t["XSECT_LAYER"].loc[2:3, "DIST"] = 0.6
        t["CHANNEL_IC"].loc[1, "FLOW"] = 99.0
    tables = copy.deepcopy(_tables())
    edit(tables)
    diff = diff_channels(_tables(), tables)

    one = diff.detail(1)
    assert one["status"] == "changed"
    assert [f["label"] for f in one["fields"] if f["changed"]] == ["Manning's n"]
    assert one["xsections"] == [{
        "dist": 0.5, "status": "changed", "note": "area", "n_layers": 2,
        "layers": [{
            "layer": 2,
            "values": {"ELEV": (5.0, 5.0), "AREA": (100.0, 150.0), "WIDTH": (20.0, 20.0), "WET_PERIM": (21.0, 21.0)},
            "diff": ["AREA"],
        }],
    }]
    assert [ic["changed"] for ic in one["initial"]] == [False, True]

    two = diff.detail(2)
    assert [(x["dist"], x["status"]) for x in two["xsections"]] == [(0.5, "removed"), (0.6, "added")]
    assert two["xs_counts"] == (1, 1)


def test_detail_of_new_and_removed_channels_ignores_the_missing_side():
    removed = copy.deepcopy(_tables())
    removed["CHANNEL"] = removed["CHANNEL"][removed["CHANNEL"]["CHAN_NO"] != 1].reset_index(drop=True)
    detail = diff_channels(_tables(), removed).detail(1)
    assert detail["status"] == "removed"
    assert all(f["scenario"] is None and f["changed"] for f in detail["fields"])
    assert detail["xs_counts"] == (1, 0)
    assert all(ic["scenario"] is None for ic in detail["initial"])
    assert diff_channels(removed, _tables()).detail(1)["status"] == "new"


def test_summary_groups_channels_by_what_changed():
    def edit(t):
        t["CHANNEL"].loc[0, "MANNING"] = 0.05
        t["CHANNEL"].loc[1, ["UPNODE", "DOWNNODE"]] = [3, 2]
        t["XSECT_LAYER"].loc[1, "AREA"] = 150.0
        t["CHANNEL_IC"].loc[1, "FLOW"] = 99.0
    summary = _scenario(edit).summary()
    assert dict(summary["channels"]) == {
        "Manning's n": [1], "Upnode / downnode": [2], "Cross-section": [1], "Initial condition": [1],
    }
    assert "nodes" not in summary


def test_summary_of_identical_files_is_empty():
    assert diff_channels(_tables(), _tables()).summary() == {"channels": []}


def test_unchanged_channel_next_to_a_changed_node_is_not_a_changed_channel():
    def edit(t):
        t["CHANNEL"].loc[0, "DOWNNODE"] = 5  # channel 1 moves off node 2; channel 2 starts at node 2
    diff = _scenario(edit)
    assert set(diff.channels) == {1}
    assert 2 in diff.nodes
    assert dict(diff.summary()["channels"]) == {"Upnode / downnode": [1]}


def test_summary_lists_new_channel():
    def edit(t):
        row = {"CHAN_NO": 4, "LENGTH": 500, "MANNING": 0.03, "DISPERSION": 360.0, "UPNODE": 3, "DOWNNODE": 9, "DX": 5000.0}
        t["CHANNEL"] = pd.concat([t["CHANNEL"], pd.DataFrame([row])], ignore_index=True)
    assert dict(_scenario(edit).summary()["channels"]) == {"New channel": [4]}
