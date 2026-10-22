# -*- coding: utf-8 -*-
"""SCALAR differences between a base and a scenario DSM2 echo file."""
from dataclasses import dataclass, field

_ORDER = {"changed": 0, "new": 1, "removed": 2}


def _same(a, b):
    """Values are equal as numbers when both are numeric (0.60 == 0.6), otherwise as text ignoring case and padding."""
    try:
        return float(a) == float(b)
    except ValueError:
        return a.strip().lower() == b.strip().lower()


@dataclass
class ScalarDiff:
    """Differing scalars as dicts: name, status ('changed', 'new' or 'removed'), base and scenario values (None if absent)."""

    entries: list = field(default_factory=list)

    def summary(self):
        """{"scalars": [entries]}, empty when nothing differs."""
        return {"scalars": self.entries} if self.entries else {}


def _scalars(tables):
    table = tables.get("SCALAR")
    return {
        str(rec["NAME"]).lower(): (str(rec["NAME"]), str(rec["VALUE"]))
        for rec in ([] if table is None else table.to_dict("records"))
    }


def diff_scalars(base_tables, scenario_tables):
    """Compare the SCALAR tables of two echo files (as loaded by ``load_echo_file``), matching NAME ignoring case."""
    base, scen = _scalars(base_tables), _scalars(scenario_tables)
    entries = []
    for key in set(base) | set(scen):
        b, s = base.get(key), scen.get(key)
        if b and s and _same(b[1], s[1]):
            continue
        status = "new" if b is None else "removed" if s is None else "changed"
        entries.append({
            "name": (b or s)[0], "status": status, "base": b and b[1], "scenario": s and s[1],
        })
    return ScalarDiff(entries=sorted(entries, key=lambda e: (_ORDER[e["status"]], e["name"].lower())))
