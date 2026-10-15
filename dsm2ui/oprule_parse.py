# -*- coding: utf-8 -*-
"""Parse DSM2 operating rules into a per-gate summary (see the DSM2 operating-rule user's guide)."""
import re

import pandas as pd

_NUM = r"[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?"
_OPS = r"<=|>=|<>|==|<|>"
_SET_RE = re.compile(r"^\s*SET\s+(\w+)\s*\(([^)]*)\)\s+TO\s+(.+?)(?:\s+RAMP\s+(\d+)\s*MIN)?\s*$", re.I | re.S)
_TS_RE = re.compile(r"\bts\(\s*name\s*=\s*(\w+)\s*\)", re.I)
_VALUE_SERIES_RE = re.compile(
    rf"^(?:({_NUM})\s*\*\s*)?ts\(\s*name\s*=\s*(\w+)\s*\)(?:\s*([/*])\s*({_NUM}))?$", re.I
)
_DATE_RE = re.compile(
    rf"^(DATETIME|DATE|SEASON)\s*({_OPS})\s*(\d{{2}}[A-Za-z]{{3}})(\d{{4}})?(?:\s+(\d{{2}}:\d{{2}}))?$", re.I
)
_THRESHOLD_RE = re.compile(rf"^ts\(\s*name\s*=\s*(\w+)\s*\)\s*({_OPS})\s*({_NUM})$", re.I)
_OBSERVABLE_RE = re.compile(
    r"\b(chan_flow|chan_vel|chan_stage)\(\s*channel\s*=\s*(\d+)\s*,\s*dist\s*=\s*(\w+)\s*\)", re.I
)
_QUANTITY = {"chan_flow": "flow", "chan_vel": "velocity", "chan_stage": "stage"}
_RES_STAGE_RE = re.compile(r"\bres_stage\(\s*res\s*=\s*(\w+)\s*\)", re.I)
_RES_FLOW_RE = re.compile(r"\bres_flow\(\s*res\s*=\s*(\w+)\s*,\s*node\s*=\s*(\w+)\s*\)", re.I)
_WORDS = {"OPEN": 1.0, "INSTALL": 1.0, "CLOSE": 0.0, "REMOVE": 0.0}
_OP_TEXT = {"<=": "\u2264", ">=": "\u2265", "<>": "\u2260", "==": "=", "<": "<", ">": ">"}

# Variables a rule can set, in display order: (label, unit).
CONTROLS = {
    "gate_op": ("Operating coefficient (gate_op)", ""),
    "gate_coef": ("Flow coefficient (gate_coef)", ""),
    "gate_install": ("Install / remove (gate_install)", ""),
    "gate_elev": ("Elevation (gate_elev)", " ft"),
    "gate_width": ("Width (gate_width)", " ft"),
    "gate_height": ("Height (gate_height)", " ft"),
    "gate_nduplicate": ("Number of duplicates (gate_nduplicate)", ""),
}
DIRECTION_TEXT = {"from_node": "from node", "to_node": "to node", "to_from_node": "from + to node", "bidir": "from + to node"}


def mark(name):
    """Wrap a series name so the renderer can emphasise it."""
    return f"\u2039{name}\u203a"


def describe(text):
    """Rewrite model variables in an expression in plain words; series names are wrapped by :func:`mark`."""
    def observable(m):
        dist = m.group(3)
        where = "the downstream end" if dist.lower() == "length" else f"{dist} ft"
        return f"{_QUANTITY[m.group(1).lower()]} in channel {m.group(2)} at {where}"

    text = _OBSERVABLE_RE.sub(observable, text)
    text = _RES_STAGE_RE.sub(lambda m: f"stage of reservoir {m.group(1)}", text)
    text = _RES_FLOW_RE.sub(lambda m: f"flow from reservoir {m.group(1)} to node {m.group(2)}", text)
    text = _TS_RE.sub(lambda m: mark(m.group(1)), text)
    text = re.sub(_OPS, lambda m: _OP_TEXT[m.group(0)], text)
    return re.sub(r"\s+", " ", text).strip()


def _series_names(text):
    return [m.group(1) for m in _TS_RE.finditer(text)]


def _parse_value(text):
    word = text.strip().upper()
    if word in _WORDS:
        return {"kind": "number", "number": _WORDS[word], "series": [], "text": text, "scale": ""}
    if re.fullmatch(_NUM, text):
        return {"kind": "number", "number": float(text), "series": [], "text": text, "scale": ""}
    m = _VALUE_SERIES_RE.match(text)
    if m:
        scale = ""
        if m.group(1):
            scale = f" \u00d7 {float(m.group(1)):g}"
        if m.group(3):
            sign = "\u00f7" if m.group(3) == "/" else "\u00d7"
            scale += f" {sign} {float(m.group(4)):g}"
        return {"kind": "series", "number": None, "series": [m.group(2)], "text": text, "scale": scale}
    return {"kind": "expr", "number": None, "series": _series_names(text), "text": text, "scale": ""}


def parse_action(action):
    """Return the list of SET targets of an action, or None if the action is not understood."""
    if re.search(r"\bTHEN\b", action, re.I):
        return None
    targets = []
    for part in re.split(r"\s+WHILE\s+", action, flags=re.I):
        m = _SET_RE.match(part)
        if not m:
            return None
        ids = {}
        for kv in m.group(2).split(","):
            if "=" in kv:
                k, v = kv.split("=", 1)
                ids[k.strip().lower()] = v.strip().strip("'\"")
        targets.append({
            "var": m.group(1).lower(), "gate": ids.get("gate"), "device": ids.get("device"),
            "direction": ids.get("direction", "").lower(), "value": _parse_value(m.group(3).strip()),
            "ramp": int(m.group(4)) if m.group(4) else None,
        })
    return targets


def _date_info(m):
    what, op, daymon, year, hhmm = m.group(1).upper(), m.group(2), m.group(3), m.group(4), m.group(5)
    when = None
    if year:
        when = pd.to_datetime(daymon + year, format="%d%b%Y", errors="coerce")
        if hhmm and pd.notna(when):
            when += pd.Timedelta(hours=int(hhmm[:2]), minutes=int(hhmm[3:]))
    day = f"{int(daymon[:2]):02d} {daymon[2:].title()}"
    label = day + (f" {year}" if year else "") + (f" {hhmm}" if hhmm else "")
    if what == "SEASON":
        label = f"{label} (every year)"
    return {"kind": "date", "op": op, "label": label, "sort": when if pd.notna(when) else pd.Timestamp.max}


def parse_trigger(text, expressions):
    """Classify a trigger: startup, date, series threshold, or a general condition."""
    t = text.strip()
    if t.upper() in ("TRUE", "STARTUP"):
        return {"kind": "startup", "series": []}
    m = _DATE_RE.match(t)
    if m:
        info = _date_info(m)
        info["series"] = []
        return info
    m = _THRESHOLD_RE.match(t)
    if m:
        return {
            "kind": "threshold", "series": [m.group(1)], "op": _OP_TEXT[m.group(2)],
            "number": float(m.group(3)), "text": describe(t),
        }
    defs, series, seen, queue = [], _series_names(t), set(), re.findall(r"\b[A-Za-z_]\w*\b", t)
    lowered = {k.lower(): (k, v) for k, v in expressions.items()}
    while queue:
        word = queue.pop(0).lower()
        if word in lowered and word not in seen:
            seen.add(word)
            name, definition = lowered[word]
            defs.append({"name": name, "text": describe(definition)})
            series += _series_names(definition)
            queue += re.findall(r"\b[A-Za-z_]\w*\b", definition)
    return {"kind": "condition", "text": describe(t), "defs": defs, "series": list(dict.fromkeys(series))}


def parse_rules(rows, expressions):
    """Parse OPERATING_RULE rows (dicts with NAME / ACTION / TRIGGER) into rule dicts."""
    rules = []
    for row in rows:
        action, trigger = str(row["ACTION"]), str(row["TRIGGER"])
        targets = parse_action(action)
        hint = re.search(r"gate\s*=\s*([\w@.]+)", action, re.I)
        dev = re.search(r"device\s*=\s*([\w@.]+)", action, re.I)
        rules.append({
            "name": str(row["NAME"]), "action": action, "trigger": trigger, "targets": targets or [],
            "summarised": targets is not None, "when": parse_trigger(trigger, expressions),
            "hint_gate": hint.group(1) if hint else None, "hint_device": dev.group(1) if dev else None,
        })
    return rules


def _norm(text):
    return re.sub(r"\s+", "", text).lower()


def gate_entries(rules, gate):
    """Entries (one per target) of the rules that act on ``gate``; twin from/to-node entries are merged."""
    g = gate.lower()
    merged = {}
    entries = []
    for rule in rules:
        if rule["summarised"]:
            for t in rule["targets"]:
                if (t["gate"] or "").lower() != g:
                    continue
                key = (t["var"], t["device"], _norm(rule["trigger"]), _norm(t["value"]["text"]), t["ramp"])
                entry = merged.get(key)
                if entry is None:
                    entry = merged[key] = {
                        "summarised": True, "var": t["var"], "device": t["device"], "directions": [],
                        "value": t["value"], "ramp": t["ramp"], "when": rule["when"], "rules": [],
                        "trigger": rule["trigger"], "action": rule["action"],
                    }
                    entries.append(entry)
                if t["direction"] and t["direction"] not in entry["directions"]:
                    entry["directions"].append(t["direction"])
                if rule["name"] not in entry["rules"]:
                    entry["rules"].append(rule["name"])
        elif (rule["hint_gate"] or "").lower() == g:
            entries.append({
                "summarised": False, "var": None, "device": rule["hint_device"], "directions": [],
                "value": None, "ramp": None, "when": rule["when"], "rules": [rule["name"]],
                "trigger": rule["trigger"], "action": rule["action"],
            })
    return entries


def _group_key(entry):
    var = entry["var"]
    return (list(CONTROLS).index(var) if var in CONTROLS else len(CONTROLS), var or "")


def summarise_gate(rules, gate):
    """Build the view model for one gate: devices -> controls -> items, plus the series it refers to."""
    entries = gate_entries(rules, gate)
    devices = {}
    for e in entries:
        devices.setdefault(e["device"], []).append(e)
    out_devices, series = [], []
    for device, dev_entries in sorted(devices.items(), key=lambda kv: kv[0] is not None):
        controls = {}
        for e in sorted(dev_entries, key=_group_key):
            controls.setdefault(e["var"], []).append(e)
        out_controls = []
        for var, items in controls.items():
            label = CONTROLS[var][0] if var in CONTROLS else "Other rules"
            out_items, schedule = [], []
            for e in items:
                series += e["when"]["series"] + (e["value"]["series"] if e["value"] else [])
                if e["summarised"] and e["when"]["kind"] == "date":
                    schedule.append(e)
                else:
                    out_items.append(e)
            if schedule:
                schedule.sort(key=lambda e: (e["when"]["op"] in (">", ">=", "=="), e["when"]["sort"]))
                out_items.insert(0, {"kind": "schedule", "rows": schedule})
            out_controls.append({"var": var, "label": label, "items": out_items})
        out_devices.append({"device": device, "controls": out_controls})
    return {
        "devices": out_devices, "series": list(dict.fromkeys(series)),
        "n_rules": len({r for e in entries for r in e["rules"]}),
    }


def value_text(var, value, ramp):
    """Plain-language description of the value a rule sets (series names wrapped by :func:`mark`)."""
    unit = CONTROLS.get(var, ("", ""))[1]
    if value["kind"] == "number":
        n = value["number"]
        if var == "gate_install":
            text = "INSTALL" if n == 1 else "REMOVE" if n == 0 else f"{n:g}"
        elif var == "gate_op":
            text = f"{n:g} (closed)" if n == 0 else f"{n:g} (open)" if n == 1 else f"{n:g} (partly open)"
        else:
            text = f"{n:g}{unit}"
    elif value["kind"] == "series":
        text = f"{mark(value['series'][0])}{value['scale']}"
    else:
        text = describe(value["text"])
    return text + (f", ramped over {ramp} min" if ramp else "")
