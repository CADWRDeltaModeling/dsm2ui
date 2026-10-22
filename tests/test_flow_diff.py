import copy

import pandas as pd

from dsm2ui.flow_diff import diff_flows


def _row(name, where, path="/A/B/C//1DAY/F/", file="a.dss"):
    return {"NAME": name, "where": where, "FILE": file, "PATH": path, "SIGN": 1, "FILLIN": "last"}


def _tables():
    bf = [_row("sjr", 4), _row("sac", 330)]
    bs = [_row("mtz", 361)]
    sf = [_row("dicu_a", 10), _row("dicu_b", 11)]
    sr = [_row("dicu_c", "clifton_court")]

    def frame(rows, column):
        df = pd.DataFrame(rows).rename(columns={"where": column})
        return df[["NAME", column, "SIGN", "FILLIN", "FILE", "PATH"]]

    return {
        "BOUNDARY_FLOW": frame(bf, "NODE"), "BOUNDARY_STAGE": frame(bs, "NODE"),
        "SOURCE_FLOW": frame(sf, "NODE"), "SOURCE_FLOW_RESERVOIR": frame(sr, "RES_NAME"),
    }


def _diff(edit, differs=lambda b, s: (b["path"] != s["path"] or b["file"] != s["file"], None)):
    scenario = copy.deepcopy(_tables())
    edit(scenario)
    return diff_flows(_tables(), scenario, differs)


def _by_name(diff):
    return {e["name"]: e for e in diff.entries}


def test_identical_files_have_no_differences():
    assert diff_flows(_tables(), _tables(), lambda b, s: (False, None)).entries == []


def test_node_change_is_changed_even_when_data_are_the_same():
    def edit(t):
        t["BOUNDARY_STAGE"].loc[0, "NODE"] = 359
        t["SOURCE_FLOW_RESERVOIR"].loc[0, "RES_NAME"] = "other_res"
    entries = _by_name(_diff(edit, lambda b, s: (False, None)))
    assert (entries["mtz"]["status"], entries["mtz"]["node_changed"], entries["mtz"]["values_changed"]) == ("changed", True, False)
    assert entries["dicu_c"]["column"] == "RES_NAME" and entries["dicu_c"]["node_changed"]


def test_data_change_is_changed_and_a_renamed_path_with_the_same_data_is_not():
    def edit(t):
        t["SOURCE_FLOW"].loc[0, "PATH"] = "/A/B/OTHER//1DAY/F/"
        t["SOURCE_FLOW"].loc[1, "FILE"] = "renamed.dss"
    differs = lambda b, s: (b["name"] == "dicu_a", None)  # only dicu_a has other values
    entries = _by_name(_diff(edit, differs))
    assert list(entries) == ["dicu_a"] and entries["dicu_a"]["values_changed"] and not entries["dicu_a"]["node_changed"]


def test_new_and_removed_rows():
    def edit(t):
        t["BOUNDARY_FLOW"] = pd.concat(
            [t["BOUNDARY_FLOW"][t["BOUNDARY_FLOW"]["NAME"] != "sac"],
             pd.DataFrame([{"NAME": "yolo", "NODE": 7, "SIGN": 1, "FILLIN": "last", "FILE": "a.dss", "PATH": "/p/"}])],
            ignore_index=True,
        )
    diff = _diff(edit, lambda b, s: (False, None))
    assert {e["name"]: e["status"] for e in diff.entries} == {"sac": "removed", "yolo": "new"}
    assert diff.summary() == {"boundary_flow": [("New", ["yolo"]), ("Removed", ["sac"])]}


def test_summary_lists_each_table_and_kind():
    def edit(t):
        t["BOUNDARY_STAGE"].loc[0, "NODE"] = 359
        t["SOURCE_FLOW"].loc[0, "PATH"] = "/x/"
        t["SOURCE_FLOW_RESERVOIR"].loc[0, "RES_NAME"] = "other_res"
        t["SOURCE_FLOW_RESERVOIR"].loc[0, "PATH"] = "/x/"
    assert _diff(edit).summary() == {
        "boundary_stage": [("Node changed", ["mtz"])],
        "source_flow": [("Data changed", ["dicu_a"])],
        "source_flow_reservoir": [("RES_NAME changed", ["dicu_c"]), ("Data changed", ["dicu_c"])],
    }


def test_drawn_at_uses_the_base_location_and_skips_new_rows():
    def edit(t):
        t["BOUNDARY_STAGE"].loc[0, "NODE"] = 359
        t["SOURCE_FLOW_RESERVOIR"].loc[0, "RES_NAME"] = "other_res"
        t["BOUNDARY_FLOW"] = pd.concat(
            [t["BOUNDARY_FLOW"],
             pd.DataFrame([{"NAME": "yolo", "NODE": 361, "SIGN": 1, "FILLIN": "last", "FILE": "a.dss", "PATH": "/p/"}])],
            ignore_index=True,
        )
    diff = _diff(edit, lambda b, s: (False, None))
    assert [e["name"] for e in diff.drawn_at("bc", 361)] == ["mtz"]
    assert diff.drawn_at("bc", 359) == []
    assert [e["name"] for e in diff.drawn_at("ressrc", "Clifton_Court")] == ["dicu_c"]


def test_removed_rows_are_drawn_but_new_rows_are_not():
    def edit(t):
        t["SOURCE_FLOW"] = t["SOURCE_FLOW"][t["SOURCE_FLOW"]["NAME"] != "dicu_a"]
        t["SOURCE_FLOW_RESERVOIR"] = t["SOURCE_FLOW_RESERVOIR"].iloc[0:0]
        t["BOUNDARY_FLOW"] = pd.concat(
            [t["BOUNDARY_FLOW"][t["BOUNDARY_FLOW"]["NAME"] != "sjr"],
             pd.DataFrame([{"NAME": "yolo", "NODE": 4, "SIGN": 1, "FILLIN": "last", "FILE": "a.dss", "PATH": "/p/"}])],
            ignore_index=True,
        )
    diff = _diff(edit, lambda b, s: (False, None))
    assert [e["name"] for e in diff.drawn_at("src", 10)] == ["dicu_a"]
    assert [e["name"] for e in diff.drawn_at("ressrc", "clifton_court")] == ["dicu_c"]
    assert [e["name"] for e in diff.drawn_at("bc", 4)] == ["sjr"]  # removed sjr, not the new yolo at the same node


def test_sign_and_fillin_changes_are_changed_even_with_identical_data():
    def edit(t):
        t["SOURCE_FLOW"].loc[0, "SIGN"] = -1
        t["SOURCE_FLOW"].loc[1, "FILLIN"] = "linear"
        t["BOUNDARY_STAGE"].loc[0, "FILLIN"] = "LAST "  # same value, different case and spacing
    diff = _diff(edit, lambda b, s: (False, None))
    entries = _by_name(diff)
    assert list(entries) == ["dicu_a", "dicu_b"]
    assert (entries["dicu_a"]["sign_changed"], entries["dicu_a"]["fillin_changed"]) == (True, False)
    assert (entries["dicu_b"]["sign_changed"], entries["dicu_b"]["fillin_changed"]) == (False, True)
    assert diff.summary() == {"source_flow": [("SIGN changed", ["dicu_a"]), ("FILLIN changed", ["dicu_b"])]}
    assert entries["dicu_a"]["base"]["sign"] == "1" and _by_name(diff)["dicu_a"]["scenario"]["sign"] == "-1"
