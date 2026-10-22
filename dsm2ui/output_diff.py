# -*- coding: utf-8 -*-
"""Output channel location differences between a base and a scenario DSM2 echo file.

Locations are matched by NAME (OUTPUT_CHANNEL). Only CHAN_NO and DISTANCE decide whether a location changed; the
variable, interval, period operation and file do not. A NAME only in the scenario is *new*, one only in the base is
*removed*.
"""
from dataclasses import dataclass, field


@dataclass
class OutputDiff:
    """Differing output names as dicts: name, status, and the base / scenario [(chan_no, distance or None)] lists."""

    entries: list = field(default_factory=list)

    def drawn(self):
        """Entries drawn on the map, at their base location: the changed and the removed ones."""
        return [e for e in self.entries if e["status"] in ("changed", "removed")]

    def summary(self):
        """{"output_locations": [(what changed, [names])]}, empty when nothing differs."""
        labels = (("changed", "Location changed"), ("new", "New"), ("removed", "Removed"))
        rows = [(label, [e["name"] for e in self.entries if e["status"] == status]) for status, label in labels]
        rows = [(label, names) for label, names in rows if names]
        return {"output_locations": rows} if rows else {}


def _locations(tables):
    """{lower-case NAME: (NAME, sorted [(chan_no, distance in ft or None for 'length')])}."""
    table = tables.get("OUTPUT_CHANNEL")
    found = {}
    for rec in ([] if table is None else table.to_dict("records")):
        dist = str(rec["DISTANCE"]).strip()
        name, locs = found.setdefault(str(rec["NAME"]).lower(), (str(rec["NAME"]), set()))
        locs.add((int(rec["CHAN_NO"]), None if dist.lower() == "length" else float(dist)))
    return {k: (name, sorted(locs, key=lambda c: (c[0], -1.0 if c[1] is None else c[1]))) for k, (name, locs) in found.items()}


def diff_outputs(base_tables, scenario_tables):
    """Compare the output locations of two echo files (as loaded by ``load_echo_file``)."""
    base, scen = _locations(base_tables), _locations(scenario_tables)
    entries = []
    for key in sorted(set(base) | set(scen)):
        b, s = base.get(key), scen.get(key)
        if b is None:
            entries.append({"name": s[0], "status": "new", "base": [], "scenario": s[1]})
        elif s is None:
            entries.append({"name": b[0], "status": "removed", "base": b[1], "scenario": []})
        elif b[1] != s[1]:
            entries.append({"name": b[0], "status": "changed", "base": b[1], "scenario": s[1]})
    return OutputDiff(entries=entries)
