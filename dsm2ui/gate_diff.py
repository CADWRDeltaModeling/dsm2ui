# -*- coding: utf-8 -*-
"""Gate differences (location, devices and operating rules) between a base and a scenario DSM2 echo file."""
import math
from dataclasses import dataclass, field

from dsm2ui.oprule_diff import default_series_differs, gate_rule_diff, rule_data

_LOCATION_FIELDS = ("FROM_OBJ", "FROM_IDENTIFIER", "TO_NODE")
_DEVICE_TABLES = (("weir", "GATE_WEIR_DEVICE"), ("pipe", "GATE_PIPE_DEVICE"))
_KIND_ORDER = (
    "New gate", "Removed gate", "FROM_OBJ", "FROM_IDENTIFIER", "TO_NODE", "Weir devices", "Pipe devices",
    "Rule added", "Rule removed", "Rule ACTION", "Rule TRIGGER", "OPRULE_EXPRESSION", "OPRULE_TIME_SERIES",
)
_DEVICE_KIND = {"weir": "Weir devices", "pipe": "Pipe devices"}
_RULE_KINDS = ("Rule added", "Rule removed", "Rule ACTION", "Rule TRIGGER", "OPRULE_EXPRESSION", "OPRULE_TIME_SERIES")


def _norm(value):
    """Comparable form of a table value: a float for numbers, else the lower-case text."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return str(value).strip().lower()


def _same(a, b):
    a, b = _norm(a), _norm(b)
    if isinstance(a, float) and isinstance(b, float):
        return math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-9)
    return a == b


def _text(value):
    """Display form of a table value."""
    n = _norm(value)
    return f"{n:g}" if isinstance(n, float) else str(value)


@dataclass
class GateData:
    """The gate tables of one echo file."""

    gates: dict = field(default_factory=dict)  # {name: {FROM_OBJ, FROM_IDENTIFIER, TO_NODE}}
    devices: dict = field(default_factory=dict)  # {name: {"weir"|"pipe": {device: {column: value}}}}
    columns: dict = field(default_factory=dict)  # {"weir"|"pipe": [column names after GATE_NAME and DEVICE]}


@dataclass
class GateDiff:
    """Reasons per differing gate name (empty when nothing differs)."""

    gates: dict = field(default_factory=dict)
    kinds: dict = field(default_factory=dict)
    base: GateData = field(default_factory=GateData, repr=False)
    scenario: GateData = field(default_factory=GateData, repr=False)
    rule_details: dict = field(default_factory=dict, repr=False)  # {gate name: operating-rule differences}

    def detail(self, name):
        """Side-by-side base / scenario values of gate ``name`` for display."""
        return _detail(self.base, self.scenario, name)

    def rule_detail(self, name):
        """Changed operating rules, expressions and time series that act on gate ``name``."""
        return {"name": name, **self.rule_details.get(name, {"rules": [], "expressions": [], "series": []})}

    def has_gate_diff(self, name):
        """True when gate ``name`` differs in something other than its operating rules."""
        return any(k not in _RULE_KINDS for k in self.kinds.get(name, ()))

    def has_rule_diff(self, name):
        return any(k in _RULE_KINDS for k in self.kinds.get(name, ()))

    def summary(self):
        """High-level view: {"gates": [(what changed, [gate names])], "oprules": [...]} for rule changes."""
        by_kind = {}
        for name, kinds in self.kinds.items():
            for kind in dict.fromkeys(kinds):
                by_kind.setdefault(kind, []).append(name)
        return {
            "gates": [(k, by_kind[k]) for k in _KIND_ORDER if k in by_kind and k not in _RULE_KINDS],
            "oprules": [(k, by_kind[k]) for k in _KIND_ORDER if k in by_kind and k in _RULE_KINDS],
        }


def gate_data(tables):
    """Index the gate tables of an echo file (as loaded by ``load_echo_file``)."""
    data = GateData()
    gate_table = tables.get("GATE")
    for rec in ([] if gate_table is None else gate_table.to_dict("records")):
        data.gates[str(rec["NAME"])] = {
            "FROM_OBJ": str(rec["FROM_OBJ"]).lower(), "FROM_IDENTIFIER": str(rec["FROM_IDENTIFIER"]),
            "TO_NODE": int(rec["TO_NODE"]),
        }
    for kind, table_name in _DEVICE_TABLES:
        table = tables.get(table_name)
        data.columns[kind] = [] if table is None else [c for c in table.columns if c not in ("GATE_NAME", "DEVICE")]
        for rec in ([] if table is None else table.to_dict("records")):
            row = {c: rec[c] for c in data.columns[kind]}
            data.devices.setdefault(str(rec["GATE_NAME"]), {}).setdefault(kind, {})[str(rec["DEVICE"])] = row
    return data


def _device_changes(b, s):
    """{device: "added" | "removed" | "changed"} for the devices of one kind that differ."""
    out = {}
    for device in sorted(set(b) | set(s)):
        if device not in s:
            out[device] = "removed"
        elif device not in b:
            out[device] = "added"
        elif any(not _same(b[device][c], s[device][c]) for c in b[device]):
            out[device] = "changed"
    return out


def _kinds(base, scen, name):
    """(kind, text) pairs describing how gate ``name`` differs between the two files."""
    out = []
    b, s = base.gates[name], scen.gates[name]
    for key in _LOCATION_FIELDS:
        if not _same(b[key], s[key]):
            out.append((key, f"{key}: {_text(b[key])} \u2192 {_text(s[key])}"))
    for kind, _ in _DEVICE_TABLES:
        changes = _device_changes(base.devices.get(name, {}).get(kind, {}), scen.devices.get(name, {}).get(kind, {}))
        for device, status in changes.items():
            out.append((_DEVICE_KIND[kind], f"{kind.capitalize()} device {device} {status}"))
    return out


def diff_gates(base_tables, scenario_tables, series_differs=None):
    """Compare the gates of two echo files (as loaded by ``load_echo_file``).

    A gate differs when it is new or removed, when its FROM_OBJ, FROM_IDENTIFIER or TO_NODE changed, when a
    weir or pipe device was added, removed or changed, or when its operating rules differ (see ``oprule_diff``).
    ``series_differs(base_row, scenario_row)`` returns ``(differs, note)`` for an OPRULE_TIME_SERIES; by default
    a series differs when its definition does.
    """
    base, scen = gate_data(base_tables), gate_data(scenario_tables)
    base_rules, scen_rules = rule_data(base_tables), rule_data(scenario_tables)
    compare = series_differs or default_series_differs
    cache = {}

    def differs(b, s):  # one comparison per series, however many gates read it
        if b["name"].lower() not in cache:
            cache[b["name"].lower()] = compare(b, s)
        return cache[b["name"].lower()]

    gates, kinds, rule_details = {}, {}, {}
    for name in sorted(set(base.gates) | set(scen.gates)):
        if name not in scen.gates:
            reasons = [("Removed gate", "Gate removed")]
        elif name not in base.gates:
            reasons = [("New gate", "New gate")]
        else:
            reasons = _kinds(base, scen, name)
            rule_reasons, rule_detail = gate_rule_diff(base_rules, scen_rules, name.lower(), differs)
            reasons += rule_reasons
            if rule_reasons:
                rule_details[name] = rule_detail
        if reasons:
            kinds[name] = [k for k, _ in reasons]
            gates[name] = [t for _, t in reasons]
    return GateDiff(gates=gates, kinds=kinds, base=base, scenario=scen, rule_details=rule_details)


def _detail(base, scen, name):
    b, s = base.gates.get(name), scen.gates.get(name)
    fields = [
        {
            "label": key,
            "base": None if b is None else _text(b[key]),
            "scenario": None if s is None else _text(s[key]),
            "changed": b is None or s is None or not _same(b[key], s[key]),
        }
        for key in _LOCATION_FIELDS
    ]
    devices = {}
    for kind, _ in _DEVICE_TABLES:
        # A gate missing from one file's GATE table has no devices there, whatever the device tables hold.
        bd = base.devices.get(name, {}).get(kind, {}) if b else {}
        sd = scen.devices.get(name, {}).get(kind, {}) if s else {}
        columns = base.columns.get(kind) or scen.columns.get(kind) or []
        changes = _device_changes(bd, sd)
        devices[kind] = [
            {
                "device": device,
                "status": changes.get(device, "same"),
                "columns": [
                    {
                        "label": c,
                        "base": _text(bd[device][c]) if device in bd else None,
                        "scenario": _text(sd[device][c]) if device in sd else None,
                        "changed": device not in bd or device not in sd or not _same(bd[device][c], sd[device][c]),
                    }
                    for c in columns
                ],
            }
            for device in sorted(set(bd) | set(sd))
        ]
    return {
        "name": name,
        "status": "new" if b is None else "removed" if s is None else "changed",
        "fields": fields,
        "devices": devices,
    }
