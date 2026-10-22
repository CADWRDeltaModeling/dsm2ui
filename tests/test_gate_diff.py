import copy

import pandas as pd

from dsm2ui.gate_diff import diff_gates


def _tables():
    return {
        "GATE": pd.DataFrame({
            "NAME": ["g1", "g2"], "FROM_OBJ": ["channel", "reservoir"], "FROM_IDENTIFIER": ["10", "res_a"], "TO_NODE": [5, 6],
        }),
        "GATE_WEIR_DEVICE": pd.DataFrame({
            "GATE_NAME": ["g1", "g1", "g2"], "DEVICE": ["w1", "w2", "w1"], "NDUPLICATE": [1.0, 1.0, 2.0],
            "WIDTH": [10.0, 20.0, 30.0], "DEFAULT_OP": ["gate_open", "gate_open", "gate_close"],
        }),
        "GATE_PIPE_DEVICE": pd.DataFrame({
            "GATE_NAME": ["g1"], "DEVICE": ["p1"], "NDUPLICATE": [1.0], "RADIUS": [2.0], "DEFAULT_OP": ["gate_open"],
        }),
    }


def _scenario(edit):
    tables = copy.deepcopy(_tables())
    edit(tables)
    return diff_gates(_tables(), tables)


def test_identical_files_have_no_differences():
    diff = diff_gates(_tables(), _tables())
    assert diff.gates == {} and diff.summary() == {"gates": [], "oprules": []}


def test_new_and_removed_gates():
    def edit(t):
        t["GATE"] = pd.concat(
            [t["GATE"][t["GATE"]["NAME"] != "g2"],
             pd.DataFrame({"NAME": ["g3"], "FROM_OBJ": ["channel"], "FROM_IDENTIFIER": ["11"], "TO_NODE": [7]})],
            ignore_index=True,
        )
    diff = _scenario(edit)
    assert diff.kinds == {"g2": ["Removed gate"], "g3": ["New gate"]}
    assert diff.detail("g3")["status"] == "new" and diff.detail("g2")["status"] == "removed"


def test_location_changes():
    def edit(t):
        t["GATE"].loc[0, "FROM_IDENTIFIER"] = "11"
        t["GATE"].loc[1, ["FROM_OBJ", "TO_NODE"]] = ["channel", 9]
    diff = _scenario(edit)
    assert diff.kinds == {"g1": ["FROM_IDENTIFIER"], "g2": ["FROM_OBJ", "TO_NODE"]}
    assert [f["label"] for f in diff.detail("g2")["fields"] if f["changed"]] == ["FROM_OBJ", "TO_NODE"]


def test_device_changes_are_listed_per_device():
    def edit(t):
        t["GATE_WEIR_DEVICE"].loc[0, "WIDTH"] = 12.0  # w1 changed
        t["GATE_WEIR_DEVICE"] = t["GATE_WEIR_DEVICE"][t["GATE_WEIR_DEVICE"].index != 1].reset_index(drop=True)  # w2 removed
        t["GATE_PIPE_DEVICE"].loc[0, "DEFAULT_OP"] = "gate_close"
    diff = _scenario(edit)
    assert diff.kinds == {"g1": ["Weir devices", "Weir devices", "Pipe devices"]}
    weirs = {d["device"]: d["status"] for d in diff.detail("g1")["devices"]["weir"]}
    assert weirs == {"w1": "changed", "w2": "removed"}
    width = [c for c in diff.detail("g1")["devices"]["weir"][0]["columns"] if c["label"] == "WIDTH"][0]
    assert (width["base"], width["scenario"], width["changed"]) == ("10", "12", True)
    assert dict(diff.summary()["gates"]) == {"Weir devices": ["g1"], "Pipe devices": ["g1"]}


def test_devices_of_a_removed_gate_are_ignored_on_the_missing_side():
    def edit(t):
        t["GATE"] = t["GATE"][t["GATE"]["NAME"] != "g1"].reset_index(drop=True)  # device rows stay behind
    detail = _scenario(edit).detail("g1")
    assert detail["status"] == "removed"
    assert all(d["status"] == "removed" for kind in ("weir", "pipe") for d in detail["devices"][kind])


def _rule_tables():
    t = _tables()
    t["OPERATING_RULE"] = pd.DataFrame({
        "NAME": ["r_open", "r_close"],
        "ACTION": ["SET gate_op(gate=g1, device=w1, direction=to_node) TO ts(name=sched)", "SET gate_op(gate=g2, device=w1) TO CLOSE"],
        "TRIGGER": ["mscs_calc > 1", "TRUE"],
    })
    t["OPRULE_EXPRESSION"] = pd.DataFrame({"NAME": ["mscs_calc", "unused"], "DEFINITION": ["ts(name=flow) + 1", "2"]})
    t["OPRULE_TIME_SERIES"] = pd.DataFrame({
        "NAME": ["sched", "flow", "other"], "FILE": ["a.dss", "a.dss", "a.dss"], "PATH": ["/A/B/C//E/F/"] * 3, "FILLIN": ["last"] * 3,
    })
    return t


def _rule_scenario(edit, series_differs=None):
    tables = copy.deepcopy(_rule_tables())
    edit(tables)
    return diff_gates(_rule_tables(), tables, series_differs)


def test_identical_rules_have_no_differences():
    assert diff_gates(_rule_tables(), _rule_tables()).gates == {}


def test_rule_action_and_trigger_changes_are_independent_and_whitespace_insensitive():
    def edit(t):
        t["OPERATING_RULE"].loc[0, "TRIGGER"] = "mscs_calc  >  1"  # only whitespace
        t["OPERATING_RULE"].loc[1, "ACTION"] = "SET gate_op(gate=g2, device=w1) TO OPEN"
        t["OPERATING_RULE"].loc[1, "TRIGGER"] = "FALSE"
    diff = _rule_scenario(edit)
    assert diff.kinds == {"g2": ["Rule ACTION", "Rule TRIGGER"]}
    rule = diff.rule_detail("g2")["rules"][0]
    assert (rule["name"], rule["action_changed"], rule["trigger_changed"]) == ("r_close", True, True)


def test_added_and_removed_rules_highlight_the_gate():
    def edit(t):
        t["OPERATING_RULE"] = pd.concat([
            t["OPERATING_RULE"][t["OPERATING_RULE"]["NAME"] != "r_close"],
            pd.DataFrame({"NAME": ["r_new"], "ACTION": ["SET gate_op(gate=g1, device=w2) TO OPEN"], "TRIGGER": ["TRUE"]}),
        ], ignore_index=True)
    diff = _rule_scenario(edit)
    assert diff.kinds == {"g1": ["Rule added"], "g2": ["Rule removed"]}


def test_expression_change_highlights_only_gates_whose_rules_use_it():
    def edit(t):
        t["OPRULE_EXPRESSION"].loc[0, "DEFINITION"] = "ts(name=flow) + 2"
        t["OPRULE_EXPRESSION"].loc[1, "DEFINITION"] = "3"  # not used by any gate rule
    diff = _rule_scenario(edit)
    assert diff.kinds == {"g1": ["OPRULE_EXPRESSION"]}
    assert [e["name"] for e in diff.rule_detail("g1")["expressions"]] == ["mscs_calc"]


def test_time_series_is_listed_only_when_its_values_differ():
    def edit(t):
        t["OPRULE_TIME_SERIES"].loc[0, "FILE"] = "b.dss"  # sched: renamed file
        t["OPRULE_TIME_SERIES"].loc[2, "FILE"] = "b.dss"  # other: not used by any rule
    assert _rule_scenario(edit, lambda b, s: (False, None)).gates == {}  # renamed file, identical data
    diff = _rule_scenario(edit, lambda b, s: (b["name"] == "sched", None))
    assert diff.kinds == {"g1": ["OPRULE_TIME_SERIES"]}
    entry = diff.rule_detail("g1")["series"][0]
    assert (entry["name"], entry["base"]["file"], entry["scenario"]["file"]) == ("sched", "a.dss", "b.dss")


def test_series_comparison_runs_once_per_series():
    calls = []

    def differs(b, s):
        calls.append(b["name"])
        return True, None

    def edit(t):
        t["OPERATING_RULE"].loc[1, "ACTION"] = "SET gate_op(gate=g2, device=w1) TO ts(name=sched)"
        t["OPERATING_RULE"].loc[1, "TRIGGER"] = "ts(name=sched) > 0"
    _rule_scenario(edit, differs)
    assert sorted(calls) == ["flow", "sched"]  # sched is read by g1 and g2 but compared once


def test_value_comparer_ignores_missing_flags_and_names(tmp_path):
    import numpy as np
    from dsm2ui.input_compare import _clean_series, series_values_differ

    idx = pd.date_range("2020-01-01", periods=4, freq="D")
    a = pd.DataFrame({"v": [1.0, 2.0, -901.0, 4.0]}, index=idx)
    b = pd.DataFrame({"w": [1.0, 2.0, np.nan, 4.0]}, index=idx)
    assert not series_values_differ(_clean_series(a), _clean_series(b))
    b.iloc[3, 0] = 4.5
    assert series_values_differ(_clean_series(a), _clean_series(b))


def test_rule_changes_have_their_own_summary_and_detail():
    def edit(t):
        t["GATE_WEIR_DEVICE"].loc[0, "WIDTH"] = 12.0  # g1 device change
        t["OPERATING_RULE"].loc[1, "TRIGGER"] = "FALSE"  # g2 rule change
        t["OPERATING_RULE"].loc[0, "TRIGGER"] = "FALSE"  # g1 rule change
    diff = _rule_scenario(edit)
    assert diff.summary() == {"gates": [("Weir devices", ["g1"])], "oprules": [("Rule TRIGGER", ["g1", "g2"])]}
    assert (diff.has_gate_diff("g1"), diff.has_rule_diff("g1")) == (True, True)
    assert (diff.has_gate_diff("g2"), diff.has_rule_diff("g2")) == (False, True)
    assert "rules" not in diff.detail("g1") and diff.rule_detail("g2")["name"] == "g2"
