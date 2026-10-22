# -*- coding: utf-8 -*-
"""Boundary flow / stage and source flow differences between a base and a scenario DSM2 echo file.

Rows are matched by NAME within their table. A matched row is *changed* when its NODE (RES_NAME for
SOURCE_FLOW_RESERVOIR), SIGN or FILLIN changed or when the values of its time series changed; a renamed file or
path with identical data is not a difference. Rows only in the scenario are *new* and rows only in the base are
*removed*; removed rows are also drawn on the map.
"""
from dataclasses import dataclass, field

# (echo table, summary key, label, location column)
_TABLES = (
    ("BOUNDARY_FLOW", "boundary_flow", "Boundary flow", "NODE"),
    ("BOUNDARY_STAGE", "boundary_stage", "Boundary stage", "NODE"),
    ("SOURCE_FLOW", "source_flow", "Source flow", "NODE"),
    ("SOURCE_FLOW_RESERVOIR", "source_flow_reservoir", "Source flow reservoir", "RES_NAME"),
)
# Which map layer a table is drawn in, and what its base location is: boundary node, source-flow node, reservoir.
_LAYER = {"boundary_flow": "bc", "boundary_stage": "bc", "source_flow": "src", "source_flow_reservoir": "ressrc"}
TITLES = {key: label for _, key, label, _ in _TABLES}
_KIND_ORDER = ("Node changed", "RES_NAME changed", "SIGN changed", "FILLIN changed", "Data changed", "New", "Removed")
# Statuses drawn on the map per layer: changed and removed rows, which have a base location; new rows are not drawn.
_DRAWN = {"bc": ("changed", "removed"), "src": ("changed", "removed"), "ressrc": ("changed", "removed")}


def is_drawn(entry):
    """True when ``entry`` is shown on the map, at its base location."""
    return entry["status"] in _DRAWN[entry["layer"]]


@dataclass
class FlowDiff:
    """Every differing boundary / source-flow row, as dicts with the keys documented in ``_entry``."""

    entries: list = field(default_factory=list)

    def drawn_at(self, layer, where):
        """Drawn entries whose base location is ``where`` (node number or lower-case reservoir name) in ``layer``."""
        return [
            e for e in self.entries
            if e["layer"] == layer and is_drawn(e) and e["base"]["where"].lower() == str(where).lower()
        ]

    def summary(self):
        """{table key: [(what changed, [names])]}; tables without differences are left out."""
        out = {}
        for _, key, _, column in _TABLES:
            by_kind = {}
            for e in (e for e in self.entries if e["table"] == key):
                kinds = ["New"] if e["status"] == "new" else ["Removed"] if e["status"] == "removed" else []
                if e["node_changed"]:
                    kinds.append(f"{column} changed" if column != "NODE" else "Node changed")
                kinds += [k for k, flag in (("SIGN changed", e["sign_changed"]), ("FILLIN changed", e["fillin_changed"])) if flag]
                if e["values_changed"]:
                    kinds.append("Data changed")
                for kind in kinds:
                    by_kind.setdefault(kind, []).append(e["name"])
            if by_kind:
                out[key] = [(k, by_kind[k]) for k in _KIND_ORDER if k in by_kind]
        return out


def _rows(tables, table, column):
    frame = tables.get(table)
    rows = {}
    for rec in ([] if frame is None else frame.to_dict("records")):
        key = str(rec["NAME"]).lower()
        while key in rows:  # a repeated name keeps its own entry
            key += "#"
        where = str(int(rec[column])) if column == "NODE" else str(rec[column])
        rows[key] = {
            "name": str(rec["NAME"]), "where": where, "file": str(rec["FILE"]), "path": str(rec["PATH"]),
            "fillin": str(rec["FILLIN"]), "sign": str(int(rec["SIGN"])) if rec.get("SIGN") is not None else None,
        }
    return rows


def _entry(key, label, column, status, base, scenario, node_changed=False, values_changed=False, note=None):
    changed = status == "changed"
    return {
        "table": key, "label": label, "layer": _LAYER[key], "column": column, "name": (base or scenario)["name"],
        "status": status, "node_changed": node_changed, "values_changed": values_changed, "note": note,
        "sign_changed": changed and base["sign"] != scenario["sign"],
        "fillin_changed": changed and base["fillin"].strip().lower() != scenario["fillin"].strip().lower(),
        "base": base, "scenario": scenario,
    }


def diff_flows(base_tables, scenario_tables, series_differs):
    """Compare the flow inputs of two echo files (as loaded by ``load_echo_file``).

    ``series_differs(base_row, scenario_row)`` returns ``(values differ, note)`` for the two time series.
    """
    entries = []
    for table, key, label, column in _TABLES:
        base, scen = _rows(base_tables, table, column), _rows(scenario_tables, table, column)
        for row_key, b in base.items():
            s = scen.get(row_key)
            if s is None:
                entries.append(_entry(key, label, column, "removed", b, None))
                continue
            moved = b["where"].lower() != s["where"].lower()
            differs, note = series_differs(b, s)
            entry = _entry(key, label, column, "changed", b, s, moved, differs, note)
            if moved or differs or entry["sign_changed"] or entry["fillin_changed"]:
                entries.append(entry)
        entries += [_entry(key, label, column, "new", None, s) for row_key, s in scen.items() if row_key not in base]
    return FlowDiff(entries=entries)
