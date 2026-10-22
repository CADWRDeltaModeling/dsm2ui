# -*- coding: utf-8 -*-
"""Differences in the operating rules that act on gates between a base and a scenario DSM2 echo file.

A gate's rules differ when a rule that acts on it was added or removed, when a rule's ACTION or TRIGGER changed,
when an OPRULE_EXPRESSION its triggers use changed, or when the values of an OPRULE_TIME_SERIES its rules read
changed (a renamed file or path with identical data is not a difference).
"""
import re
from dataclasses import dataclass, field

_GATE_RE = re.compile(r"\bgate\s*=\s*([\w@.]+)", re.I)
_TS_RE = re.compile(r"\bts\(\s*name\s*=\s*(\w+)\s*\)", re.I)
_WORD_RE = re.compile(r"\b[A-Za-z_]\w*\b")


def _norm(text):
    return re.sub(r"\s+", "", str(text)).lower()


@dataclass
class RuleData:
    """The operating-rule tables of one echo file."""

    rules: dict = field(default_factory=dict)  # {rule name: {"action", "trigger", "gates": {lower-case gate names}}}
    expressions: dict = field(default_factory=dict)  # {lower-case name: (name, definition)}
    series: dict = field(default_factory=dict)  # {lower-case name: {"name", "file", "path", "fillin"}}


def rule_data(tables):
    """Index the operating-rule tables of an echo file (as loaded by ``load_echo_file``)."""

    def records(name):
        table = tables.get(name)
        return [] if table is None else table.to_dict("records")

    data = RuleData()
    for r in records("OPERATING_RULE"):
        action, trigger = str(r["ACTION"]), str(r["TRIGGER"])
        data.rules[str(r["NAME"])] = {
            "action": action, "trigger": trigger, "gates": {g.lower() for g in _GATE_RE.findall(action)},
        }
    for r in records("OPRULE_EXPRESSION"):
        data.expressions[str(r["NAME"]).lower()] = (str(r["NAME"]), str(r["DEFINITION"]))
    for r in records("OPRULE_TIME_SERIES"):
        data.series[str(r["NAME"]).lower()] = {
            "name": str(r["NAME"]), "file": str(r["FILE"]), "path": str(r["PATH"]), "fillin": str(r["FILLIN"]),
        }
    return data


def _references(data, rules):
    """(expression names, series names), lower-case, that the given rules use, following expressions inside expressions."""
    expressions, series = [], []
    seen = set()
    queue = [r["trigger"] for r in rules]
    series += [m.lower() for r in rules for m in _TS_RE.findall(r["action"])]
    while queue:
        text = queue.pop(0)
        series += [m.lower() for m in _TS_RE.findall(text)]
        for word in _WORD_RE.findall(text):
            key = word.lower()
            if key in data.expressions and key not in seen:
                seen.add(key)
                expressions.append(key)
                queue.append(data.expressions[key][1])
    return list(dict.fromkeys(expressions)), list(dict.fromkeys(series))


def _same_definition(b, s):
    return all(_norm(b[k]) == _norm(s[k]) for k in ("file", "path", "fillin"))


def default_series_differs(base_row, scenario_row):
    """Without access to the data, a series differs when its definition does. Returns (differs, note)."""
    return not _same_definition(base_row, scenario_row), None


def gate_rule_diff(base, scen, gate, series_differs=default_series_differs):
    """Compare the rules acting on ``gate`` (lower-case name) in two ``RuleData``.

    Returns ``(reasons, detail)``: ``reasons`` are (kind, text) pairs; ``detail`` holds the changed ``rules``,
    ``expressions`` and ``series`` for display. ``series_differs(base_row, scenario_row)`` returns
    ``(differs, note)`` and decides whether the values of a series differ.
    """
    b_rules = {n: r for n, r in base.rules.items() if gate in r["gates"]}
    s_rules = {n: r for n, r in scen.rules.items() if gate in r["gates"]}
    reasons = []
    rules = []
    for name in sorted(set(b_rules) | set(s_rules)):
        b, s = b_rules.get(name), s_rules.get(name)
        if s is None or b is None:
            status = "removed" if s is None else "added"
            reasons.append((f"Rule {status}", f"Rule {name} {status}"))
        else:
            status = "changed"
            if _norm(b["action"]) == _norm(s["action"]) and _norm(b["trigger"]) == _norm(s["trigger"]):
                continue
            if _norm(b["action"]) != _norm(s["action"]):
                reasons.append(("Rule ACTION", f"Rule {name}: ACTION changed"))
            if _norm(b["trigger"]) != _norm(s["trigger"]):
                reasons.append(("Rule TRIGGER", f"Rule {name}: TRIGGER changed"))
        rules.append({
            "name": name, "status": status,
            "action": (None if b is None else b["action"], None if s is None else s["action"]),
            "trigger": (None if b is None else b["trigger"], None if s is None else s["trigger"]),
            "action_changed": b is None or s is None or _norm(b["action"]) != _norm(s["action"]),
            "trigger_changed": b is None or s is None or _norm(b["trigger"]) != _norm(s["trigger"]),
        })

    b_exprs, b_series = _references(base, b_rules.values())
    s_exprs, s_series = _references(scen, s_rules.values())
    expressions = []
    for key in list(dict.fromkeys(b_exprs + s_exprs)):
        b, s = base.expressions.get(key), scen.expressions.get(key)
        if b is not None and s is not None and _norm(b[1]) == _norm(s[1]):
            continue
        status = "changed" if b is not None and s is not None else "removed" if s is None else "added"
        reasons.append(("OPRULE_EXPRESSION", f"Expression {(b or s)[0]} {status}"))
        expressions.append({
            "name": (b or s)[0], "status": status, "base": None if b is None else b[1],
            "scenario": None if s is None else s[1],
        })

    series = []
    for key in [k for k in b_series if k in s_series]:
        b, s = base.series.get(key), scen.series.get(key)
        if b is None or s is None:
            continue
        differs, note = series_differs(b, s)
        if differs:
            reasons.append(("OPRULE_TIME_SERIES", f"Time series {b['name']}: values differ"))
            series.append({"name": b["name"], "base": b, "scenario": s, "note": note})
    return reasons, {"rules": rules, "expressions": expressions, "series": series}
