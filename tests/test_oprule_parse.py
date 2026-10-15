from dsm2ui.oprule_parse import describe, parse_action, parse_rules, parse_trigger, summarise_gate, value_text

EXPRESSIONS = {
    "mscs_velclose": "chan_vel(channel=512,dist=5750) < -0.1",
    "mscs_calc": "ts(name=mscs_op) < 0",
}


def _rule(name, action, trigger):
    return {"NAME": name, "ACTION": action, "TRIGGER": trigger}


def test_parse_action_while_gives_two_targets():
    targets = parse_action(
        "SET gate_op(gate=dcc,device=gates,direction=from_node) TO ts(name=dcc_op)/2 WHILE "
        "SET gate_op(gate=dcc,device=gates,direction=to_node) TO ts(name=dcc_op)/2"
    )
    assert [t["direction"] for t in targets] == ["from_node", "to_node"]
    assert targets[0]["value"]["kind"] == "series"
    assert targets[0]["value"]["scale"].strip() == "\u00f7 2"


def test_parse_action_ramp_and_lowercase_keywords():
    (t,) = parse_action("SET gate_coef(gate=g, device=weir, direction=from_node) to 0.4 RAMP 120MIN")
    assert t["ramp"] == 120 and t["value"]["number"] == 0.4


def test_parse_action_top_level_then_is_not_understood():
    assert parse_action("SET gate_op(gate=g,device=d) TO 1 THEN SET gate_op(gate=g,device=d) TO 0") is None


def test_parse_trigger_kinds():
    assert parse_trigger("TRUE", EXPRESSIONS)["kind"] == "startup"
    date = parse_trigger("DATETIME >= 20SEP2018 12:00", EXPRESSIONS)
    assert date["kind"] == "date" and date["label"] == "20 Sep 2018 12:00"
    assert parse_trigger("ts(name=fb_install) >= 1.0", EXPRESSIONS)["kind"] == "threshold"
    cond = parse_trigger("mscs_velclose AND mscs_calc", EXPRESSIONS)
    assert cond["kind"] == "condition"
    assert {d["name"] for d in cond["defs"]} == {"mscs_velclose", "mscs_calc"}
    assert cond["series"] == ["mscs_op"]


def test_describe_rewrites_model_variables():
    assert describe("chan_stage(channel=185, dist=0) <= 2.0") == "stage in channel 185 at 0 ft \u2264 2.0"


def test_twin_directions_merge_and_date_rules_form_a_schedule():
    rules = parse_rules([
        _rule("a_frm", "SET gate_coef(gate=Weir1,device=weir,direction=from_node) TO 0.0 RAMP 120MIN", "DATETIME < 20SEP2018 12:00"),
        _rule("a_to", "SET gate_coef(gate=Weir1,device=weir,direction=to_node) TO 0.0 RAMP 120MIN", "DATETIME < 20SEP2018 12:00"),
        _rule("b_frm", "SET gate_coef(gate=Weir1,device=weir,direction=from_node) TO 0.4 RAMP 120MIN", "DATETIME >= 20SEP2018 12:00"),
        _rule("other", "SET gate_op(gate=Other,device=weir,direction=from_node) TO 1", "TRUE"),
    ], EXPRESSIONS)
    view = summarise_gate(rules, "weir1")
    assert view["n_rules"] == 3
    (device,) = view["devices"]
    (control,) = device["controls"]
    (schedule,) = control["items"]
    assert schedule["kind"] == "schedule"
    assert [r["directions"] for r in schedule["rows"]] == [["from_node", "to_node"], ["from_node"]]
    assert value_text("gate_coef", schedule["rows"][0]["value"], 120) == "0, ramped over 120 min"


def test_gate_without_rules_has_empty_view():
    assert summarise_gate([], "nothing") == {"devices": [], "series": [], "n_rules": 0}


def test_unsummarised_rule_is_kept_as_raw():
    rules = parse_rules([_rule("r", "SET gate_op(gate=G,device=d) TO 1 THEN SET gate_op(gate=G,device=d) TO 0", "TRUE")], {})
    (item,) = summarise_gate(rules, "G")["devices"][0]["controls"][0]["items"]
    assert not item["summarised"]
