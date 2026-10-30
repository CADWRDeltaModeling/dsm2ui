# -*- coding: utf-8 -*-
"""Channel-related differences between a base and a scenario DSM2 echo file."""
import math
from dataclasses import dataclass, field

import numpy as np

# (column, label, format) of the CHANNEL table fields compared one by one.
_CHANNEL_FIELDS = (
    ("LENGTH", "Length", "{:,.0f}"),
    ("MANNING", "Manning's n", "{:.4f}"),
    ("DISPERSION", "Dispersion", "{:,.2f}"),
    ("UPNODE", "Upnode", "{:.0f}"),
    ("DOWNNODE", "Downnode", "{:.0f}"),
    ("DX", "DX", "{:,.2f}"),
)
_XS_FIELDS = (("ELEV", "elevation"), ("AREA", "area"), ("WIDTH", "width"), ("WET_PERIM", "wetted perimeter"))
_IC_FIELDS = (("STAGE", "stage"), ("FLOW", "flow"))
_UNITS = {"LENGTH": " ft", "DISPERSION": " ft\u00b2/s", "DX": " ft"}
_CHANNEL_KIND_ORDER = (
    "New channel", "Removed channel", "Length", "Manning's n", "Dispersion", "Upnode / downnode", "DX",
    "Cross-section", "Initial condition",
)


@dataclass
class ChannelData:
    """The channel-related tables of one echo file, indexed by channel number."""

    rows: dict = field(default_factory=dict)  # {chan: {CHANNEL column: float}}
    xs: dict = field(default_factory=dict)  # {chan: XSECT_LAYER DataFrame}
    ic: dict = field(default_factory=dict)  # {chan: {distance key: (stage, flow)}}


@dataclass
class ChannelDiff:
    """Reasons per differing channel number and node number (empty dicts when nothing differs)."""

    channels: dict = field(default_factory=dict)
    nodes: dict = field(default_factory=dict)
    base: ChannelData = field(default_factory=ChannelData, repr=False)
    scenario: ChannelData = field(default_factory=ChannelData, repr=False)

    def detail(self, chan):
        """Side-by-side base / scenario values of channel ``chan`` for display."""
        return _detail(self.base, self.scenario, chan)

    def summary(self):
        """High-level view: {"channels": [(what changed, [channel numbers])]}."""
        chan_kinds = {}
        for chan in self.channels:
            d = self.detail(chan)
            if d["status"] != "changed":
                kinds = ["New channel" if d["status"] == "new" else "Removed channel"]
            else:
                kinds = [
                    "Upnode / downnode" if f["label"] in ("Upnode", "Downnode") else f["label"]
                    for f in d["fields"] if f["changed"]
                ]
                if any(x["status"] != "same" for x in d["xsections"]):
                    kinds.append("Cross-section")
                if any(ic["changed"] for ic in d["initial"]):
                    kinds.append("Initial condition")
            for kind in dict.fromkeys(kinds):
                chan_kinds.setdefault(kind, []).append(chan)
        return {"channels": [(k, chan_kinds[k]) for k in _CHANNEL_KIND_ORDER if k in chan_kinds]}


def _same(a, b):
    a, b = float(a), float(b)
    if math.isnan(a) and math.isnan(b):  # DX can be unavailable in both files
        return True
    return math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-9)


def _text(fmt, value):
    return "n/a" if math.isnan(value) else fmt.format(value)


def _channel_rows(tables):
    """{chan_no: {column: float}} from the CHANNEL table."""
    table = tables["CHANNEL"]
    cols = [c for c, _, _ in _CHANNEL_FIELDS]
    out = {}
    for rec in table.to_dict("records"):
        out[int(rec["CHAN_NO"])] = {c: float(rec[c]) for c in cols}
    return out


def _xsections(tables):
    """{chan_no: DataFrame of XSECT_LAYER rows sorted by DIST, ELEV}."""
    table = tables.get("XSECT_LAYER")
    if table is None or table.empty:
        return {}
    table = table.copy()
    for col in ("DIST", "ELEV", "AREA", "WIDTH", "WET_PERIM"):
        table[col] = table[col].astype(float)
    table["CHAN_NO"] = table["CHAN_NO"].astype(int)
    return {
        int(n): g.sort_values(["DIST", "ELEV"], kind="stable").reset_index(drop=True)
        for n, g in table.groupby("CHAN_NO")
    }


def _initial_conditions(tables):
    """{chan_no: {distance key: (stage, flow)}}; DISTANCE is a number or the word 'length'."""
    table = tables.get("CHANNEL_IC")
    if table is None or table.empty:
        return {}
    out = {}
    for rec in table.to_dict("records"):
        dist = str(rec["DISTANCE"]).strip().lower()
        try:
            dist = float(dist)
        except ValueError:
            pass
        out.setdefault(int(rec["CHAN_NO"]), {})[dist] = (float(rec["STAGE"]), float(rec["FLOW"]))
    return out


def channel_data(tables):
    """Index the channel-related tables of an echo file (as loaded by ``load_echo_file``)."""
    return ChannelData(rows=_channel_rows(tables), xs=_xsections(tables), ic=_initial_conditions(tables))


def _xs_detail(base, scen):
    """One entry per cross-section location: its status and, when changed, what differs.

    A location present in both files with the same number of layers lists the layers whose values differ,
    each as ``{"layer": 1-based number, "values": {column: (base, scenario)}, "diff": [differing columns]}``.
    """

    def by_location(df):
        return {} if df is None else {d: g for d, g in df.groupby(df["DIST"].round(6))}

    b_loc, s_loc = by_location(base), by_location(scen)
    out = []
    for dist in sorted(set(b_loc) | set(s_loc)):
        layers, n_layers = [], 0
        if dist not in s_loc:
            status, note = "removed", ""
        elif dist not in b_loc:
            status, note = "added", ""
        else:
            b, s = b_loc[dist], s_loc[dist]
            n_layers = len(b)
            if len(b) != len(s):
                status, note = "changed", f"layers {len(b)} \u2192 {len(s)}"
            else:
                for i in range(len(b)):
                    values = {col: (float(b[col].iloc[i]), float(s[col].iloc[i])) for col, _ in _XS_FIELDS}
                    diff_cols = [col for col, _ in _XS_FIELDS if not _same(*values[col])]
                    if diff_cols:
                        layers.append({"layer": i + 1, "values": values, "diff": diff_cols})
                changed = [label for col, label in _XS_FIELDS if any(col in lay["diff"] for lay in layers)]
                status, note = ("changed", ", ".join(changed)) if changed else ("same", "")
        out.append({"dist": float(dist), "status": status, "note": note, "layers": layers, "n_layers": n_layers})
    return out


def _ic_detail(base, scen):
    """One entry per initial-condition location with the (stage, flow) of each side."""
    base, scen = base or {}, scen or {}
    keys = sorted(set(base) | set(scen), key=lambda k: (isinstance(k, str), 0 if isinstance(k, str) else k))
    out = []
    for key in keys:
        b, s = base.get(key), scen.get(key)
        changed = b is None or s is None or any(not _same(x, y) for x, y in zip(b, s))
        out.append({"dist": f"{key:g}" if not isinstance(key, str) else key, "base": b, "scenario": s, "changed": changed})
    return out


def _detail(base, scen, chan):
    b, s = base.rows.get(chan), scen.rows.get(chan)
    # A channel missing from one file's CHANNEL table has no cross sections or initial condition there.
    b_xs, s_xs = (base.xs.get(chan) if b else None), (scen.xs.get(chan) if s else None)
    b_ic, s_ic = (base.ic.get(chan) if b else None), (scen.ic.get(chan) if s else None)
    fields = []
    for col, label, fmt in _CHANNEL_FIELDS:
        unit = _UNITS.get(col, "")
        fields.append({
            "label": label,
            "base": None if b is None else _text(fmt, b[col]) + unit,
            "scenario": None if s is None else _text(fmt, s[col]) + unit,
            "changed": b is None or s is None or not _same(b[col], s[col]),
        })
    return {
        "chan": chan,
        "status": "new" if b is None else "removed" if s is None else "changed",
        "fields": fields,
        "xsections": _xs_detail(b_xs, s_xs),
        "xs_counts": (
            0 if b_xs is None else b_xs["DIST"].round(6).nunique(),
            0 if s_xs is None else s_xs["DIST"].round(6).nunique(),
        ),
        "initial": _ic_detail(b_ic, s_ic),
    }


def _xsection_reasons(base, scen):
    if base is None and scen is None:
        return []
    if base is None or scen is None:
        return ["Cross sections added" if base is None else "Cross sections removed"]
    if len(base) != len(scen) or not np.allclose(base["DIST"], scen["DIST"], rtol=1e-9, atol=1e-6):
        locs_b, locs_s = set(base["DIST"].round(6)), set(scen["DIST"].round(6))
        if locs_b != locs_s:
            return ["Cross-section locations changed"]
        return [f"Cross-section layers changed ({len(base)} \u2192 {len(scen)} rows)"]
    return [
        f"Cross-section {label} changed"
        for col, label in _XS_FIELDS
        if not np.allclose(base[col], scen[col], rtol=1e-9, atol=1e-9)
    ]


def _initial_condition_reasons(base, scen):
    base, scen = base or {}, scen or {}
    if base.keys() != scen.keys():
        return ["Initial-condition locations changed"]
    return [
        f"Initial {label} changed"
        for k, (_, label) in enumerate(_IC_FIELDS)
        if any(not _same(base[d][k], scen[d][k]) for d in base)
    ]


def _connections(rows):
    """{node: (channels starting here, channels ending here)}."""
    out = {}
    for chan, rec in rows.items():
        out.setdefault(int(rec["UPNODE"]), (set(), set()))[0].add(chan)
        out.setdefault(int(rec["DOWNNODE"]), (set(), set()))[1].add(chan)
    return out


def _node_reasons(node, base_conn, scen_conn, scen_chans):
    """Why a node differs: it is new or removed, or a channel that started or ended there in the base no longer does.

    A channel newly attached to an existing node is that channel's own change, so it does not flag the node.
    """
    if node not in scen_conn:
        return ["Node removed"]
    if node not in base_conn:
        return ["New node"]
    reasons = []
    for k, gone in enumerate(("no longer starts here", "no longer ends here")):
        for chan in sorted(base_conn[node][k] - scen_conn[node][k]):
            reasons.append(f"Channel {chan} removed" if chan not in scen_chans else f"Channel {chan} {gone}")
    return reasons


def diff_channels(base_tables, scenario_tables):
    """Compare the channel-related tables of two echo files (as loaded by ``load_echo_file``).

    A channel differs when it is new or removed, when any CHANNEL field (length, Manning's n, dispersion,
    upnode, downnode, DX), its cross sections or its initial condition changed. A node differs when it is
    new or removed, or when a channel that started or ended there in the base no longer does.
    """
    base_data, scen_data = channel_data(base_tables), channel_data(scenario_tables)
    base, scen = base_data.rows, scen_data.rows
    base_xs, scen_xs = base_data.xs, scen_data.xs
    base_ic, scen_ic = base_data.ic, scen_data.ic

    channels = {}
    for chan in sorted(set(base) | set(scen)):
        if chan not in scen:
            channels[chan] = ["Channel removed"]
            continue
        if chan not in base:
            channels[chan] = ["New channel"]
            continue
        reasons = [
            f"{label}: {_text(fmt, base[chan][col])} \u2192 {_text(fmt, scen[chan][col])}"
            for col, label, fmt in _CHANNEL_FIELDS
            if not _same(base[chan][col], scen[chan][col])
        ]
        reasons += _xsection_reasons(base_xs.get(chan), scen_xs.get(chan))
        reasons += _initial_condition_reasons(base_ic.get(chan), scen_ic.get(chan))
        if reasons:
            channels[chan] = reasons

    base_conn, scen_conn = _connections(base), _connections(scen)
    nodes = {}
    for node in sorted(set(base_conn) | set(scen_conn)):
        reasons = _node_reasons(node, base_conn, scen_conn, scen)
        if reasons:
            nodes[node] = reasons
    return ChannelDiff(channels=channels, nodes=nodes, base=base_data, scenario=scen_data)
