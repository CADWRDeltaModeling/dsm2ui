# -*- coding: utf-8 -*-
"""Reservoir differences between a base and a scenario DSM2 echo file."""
import math
from dataclasses import dataclass, field

_SQFT_PER_ACRE = 43560.0
_KIND_ORDER = (
    "New reservoir", "Removed reservoir", "Reservoir connections", "Area / bottom elevation",
    "Elevation vs area", "Initial condition",
)


def _same(a, b):
    """Equal numbers (or both missing)."""
    if a is None or b is None:
        return a is None and b is None
    return math.isclose(float(a), float(b), rel_tol=1e-9, abs_tol=1e-9)


def _same_pair(a, b):
    return all(_same(x, y) for x, y in zip(a, b))


@dataclass
class ReservoirData:
    """The reservoir tables of one echo file, keyed by lower-case reservoir name."""

    # {name: {"area": million sq ft, "bot_elev": ft, "ic_stage": ft or None,
    #         "curve": [(elevation ft, area acres)], "connections": {node: (coef_in, coef_out)}}}
    props: dict = field(default_factory=dict)


@dataclass
class ReservoirDiff:
    """Reasons per differing reservoir name (empty when nothing differs)."""

    reservoirs: dict = field(default_factory=dict)
    kinds: dict = field(default_factory=dict)
    base: ReservoirData = field(default_factory=ReservoirData, repr=False)
    scenario: ReservoirData = field(default_factory=ReservoirData, repr=False)

    def detail(self, name):
        """Side-by-side base / scenario values of reservoir ``name`` for display."""
        return _detail(self.base, self.scenario, name)

    def summary(self):
        """High-level view: {"reservoirs": [(what changed, [reservoir names])]}."""
        by_kind = {}
        for name, kinds in self.kinds.items():
            for kind in dict.fromkeys(kinds):
                by_kind.setdefault(kind, []).append(name)
        return {"reservoirs": [(k, by_kind[k]) for k in _KIND_ORDER if k in by_kind]}


def reservoir_data(tables):
    """Index the reservoir tables of an echo file (as loaded by ``load_echo_file``)."""

    def records(name):
        table = tables.get(name)
        return [] if table is None else table.to_dict("records")

    props = {}
    for r in records("RESERVOIR"):
        props[str(r["NAME"]).lower()] = {
            "area": float(r["AREA"]), "bot_elev": float(r["BOT_ELEV"]), "ic_stage": None, "curve": [],
            "connections": {},
        }
    for r in records("RESERVOIR_VOL"):
        if str(r["RES_NAME"]).lower() in props:
            props[str(r["RES_NAME"]).lower()]["curve"].append((float(r["ELEV"]), float(r["AREA"])))
    for r in records("RESERVOIR_IC"):
        if str(r["RES_NAME"]).lower() in props:
            props[str(r["RES_NAME"]).lower()]["ic_stage"] = float(r["STAGE"])
    for r in records("RESERVOIR_CONNECTION"):
        if str(r["RES_NAME"]).lower() in props:
            props[str(r["RES_NAME"]).lower()]["connections"][int(r["NODE"])] = (
                float(r["COEF_IN"]), float(r["COEF_OUT"])
            )
    for p in props.values():
        p["curve"].sort()
    return ReservoirData(props=props)


def _connection_changes(b, s):
    """Nodes whose connection was added, removed or had its coefficients changed."""
    return [
        node for node in sorted(set(b["connections"]) | set(s["connections"]))
        if node not in b["connections"] or node not in s["connections"]
        or not _same_pair(b["connections"][node], s["connections"][node])
    ]


def _curve_changed(b, s):
    return len(b["curve"]) != len(s["curve"]) or any(
        not _same_pair(x, y) for x, y in zip(b["curve"], s["curve"])
    )


def _kinds(b, s):
    """(kind, text) pairs describing how reservoir properties ``b`` (base) and ``s`` (scenario) differ."""
    out = []
    nodes = _connection_changes(b, s)
    if nodes:
        out.append(("Reservoir connections", f"Connections changed at node{'s' * (len(nodes) > 1)} {', '.join(map(str, nodes))}"))
    for key, label in (("area", "Surface area"), ("bot_elev", "Bottom elevation")):
        if not _same(b[key], s[key]):
            out.append(("Area / bottom elevation", f"{label}: {b[key]:,.3f} \u2192 {s[key]:,.3f}"))
    if _curve_changed(b, s):
        out.append(("Elevation vs area", "Elevation vs area table changed"))
    if not _same(b["ic_stage"], s["ic_stage"]):
        out.append(("Initial condition", "Initial stage changed"))
    return out


def diff_reservoirs(base_tables, scenario_tables):
    """Compare the reservoirs of two echo files (as loaded by ``load_echo_file``).

    A reservoir differs when it is new or removed, or when its connections (node or coefficients), surface
    area, bottom elevation, elevation-area table or initial stage changed.
    """
    base, scen = reservoir_data(base_tables), reservoir_data(scenario_tables)
    reservoirs, kinds = {}, {}
    for name in sorted(set(base.props) | set(scen.props)):
        if name not in scen.props:
            reasons = [("Removed reservoir", "Reservoir removed")]
        elif name not in base.props:
            reasons = [("New reservoir", "New reservoir")]
        else:
            reasons = _kinds(base.props[name], scen.props[name])
        if reasons:
            kinds[name] = [k for k, _ in reasons]
            reservoirs[name] = [t for _, t in reasons]
    return ReservoirDiff(reservoirs=reservoirs, kinds=kinds, base=base, scenario=scen)


def _fmt(value, spec="{:,.3f}", unit=""):
    return None if value is None else spec.format(value) + unit


def _detail(base, scen, name):
    b, s = base.props.get(name), scen.props.get(name)

    def area_text(p):
        if p is None:
            return None
        return f"{p['area']:,.3f} million sq ft ({p['area'] * 1e6 / _SQFT_PER_ACRE:,.0f} acres)"

    def pick(key):
        return (None if b is None else b[key]), (None if s is None else s[key])

    fields = [
        {"label": "Surface area (AREA)", "base": area_text(b), "scenario": area_text(s),
         "changed": b is None or s is None or not _same(b["area"], s["area"])},
    ]
    for key, label in (("bot_elev", "Bottom elevation (BOT_ELEV)"), ("ic_stage", "Initial stage")):
        bv, sv = pick(key)
        fields.append({
            "label": label, "base": _fmt(bv, unit=" ft"), "scenario": _fmt(sv, unit=" ft"),
            "changed": b is None or s is None or not _same(bv, sv),
        })

    b_conn, s_conn = ({} if p is None else p["connections"] for p in (b, s))
    connections = [
        {"node": node, "base": b_conn.get(node), "scenario": s_conn.get(node),
         "changed": node not in b_conn or node not in s_conn or not _same_pair(b_conn[node], s_conn[node])}
        for node in sorted(set(b_conn) | set(s_conn))
    ]

    b_curve, s_curve = ([] if p is None else p["curve"] for p in (b, s))
    rows = []
    for i in range(max(len(b_curve), len(s_curve))):
        bp = b_curve[i] if i < len(b_curve) else None
        sp = s_curve[i] if i < len(s_curve) else None
        if bp is None or sp is None or not _same_pair(bp, sp):
            rows.append({"point": i + 1, "base": bp, "scenario": sp})
    return {
        "name": name,
        "status": "new" if b is None else "removed" if s is None else "changed",
        "fields": fields,
        "connections": connections,
        "curve": {"counts": (len(b_curve), len(s_curve)), "rows": rows},
    }
