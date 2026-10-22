import copy

import pandas as pd

from dsm2ui.scalar_diff import diff_scalars


def _tables():
    return {"SCALAR": pd.DataFrame({
        "NAME": ["theta", "display_intvl", "title", "maxiter", "temp_dir"],
        "VALUE": ["0.6", "1day", "Hydro run", "20", "c:/temp"],
    })}


def _diff(edit):
    scenario = copy.deepcopy(_tables())
    edit(scenario["SCALAR"])
    return diff_scalars(_tables(), scenario)


def test_identical_files_have_no_differences():
    diff = diff_scalars(_tables(), _tables())
    assert diff.entries == [] and diff.summary() == {}


def test_changed_scalars_show_base_and_scenario_values():
    def edit(t):
        t.loc[1, "VALUE"] = "3day"
        t.loc[3, "VALUE"] = "25"
    diff = _diff(edit)
    assert [(e["name"], e["status"], e["base"], e["scenario"]) for e in diff.entries] == [
        ("display_intvl", "changed", "1day", "3day"), ("maxiter", "changed", "20", "25"),
    ]
    assert diff.summary() == {"scalars": diff.entries}


def test_numbers_compare_as_numbers_and_text_ignores_case_and_padding():
    def edit(t):
        t.loc[0, "VALUE"] = "0.60"
        t.loc[3, "VALUE"] = "20.0"
        t.loc[1, "VALUE"] = " 1DAY "
    assert _diff(edit).entries == []


def test_new_and_removed_scalars_and_names_match_ignoring_case():
    def edit(t):
        t["NAME"] = t["NAME"].str.upper()
        t.drop(t[t["NAME"] == "TITLE"].index, inplace=True)
        t.loc[max(t.index) + 1] = ["gravity", "32.2"]
    diff = _diff(edit)
    assert [(e["name"], e["status"], e["base"], e["scenario"]) for e in diff.entries] == [
        ("gravity", "new", None, "32.2"), ("title", "removed", "Hydro run", None),
    ]
