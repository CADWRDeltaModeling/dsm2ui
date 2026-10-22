# -*- coding: utf-8 -*-
"""Operating-rules tab of the gate diff window in the Compare tab: the rule changes that act on one gate."""
import html

import panel as pn

from dsm2ui.channel_diff_popup import _section, _val
from dsm2ui.oprule_diff_view import series_chart

_CHANGED = "background:#fdecea;color:#b3261e;font-weight:600"


def _text_table(head, rows):
    """Table of long texts; ``rows`` is a list of (cells, changed flag per cell), cells already escaped."""
    cell = "padding:3px 8px;vertical-align:top;word-break:break-word;font-family:monospace;font-size:11px"
    label = "padding:3px 8px;vertical-align:top;white-space:nowrap;font-size:12px"
    th = "".join(f"<th style='text-align:left;padding:3px 8px;border-bottom:1px solid #ddd'>{h}</th>" for h in head)
    body = ""
    for cells, flags in rows:
        body += f"<tr><td style='{label}'>{cells[0]}</td>" + "".join(
            f"<td style='{cell};{_CHANGED if flag else ''}'>{c}</td>" for c, flag in zip(cells[1:], flags[1:])
        ) + "</tr>"
    return f"<table style='border-collapse:collapse;font-size:12px;width:100%'><tr>{th}</tr>{body}</table>"


def diff_html(detail):
    """HTML body of the pop-up for one gate's rules (see ``GateDiff.rule_detail``), without the time series charts."""
    out = ""
    for r in detail["rules"]:
        rows = [
            (["ACTION", _val(r["action"][0]), _val(r["action"][1])], [False, r["action_changed"], r["action_changed"]]),
            (["TRIGGER", _val(r["trigger"][0]), _val(r["trigger"][1])], [False, r["trigger_changed"], r["trigger_changed"]]),
        ]
        out += (
            f"<div style='font-size:12px;margin:8px 0 4px'><b>{html.escape(r['name'])}</b>: {r['status']}</div>"
            + _text_table(["", "Base", "Scenario"], rows)
        )
    if out:
        out = _section("OPERATING RULES") + out
    if detail["expressions"]:
        rows = [
            ([html.escape(e["name"]), _val(e["base"]), _val(e["scenario"])], [False, True, True])
            for e in detail["expressions"]
        ]
        out += _section("RULE EXPRESSIONS") + _text_table(["Expression", "Base", "Scenario"], rows)
    return out


def rules_tab(detail, load_series):
    """Panel content for one gate's rule changes (see ``GateDiff.rule_detail``).

    ``load_series(side, row)`` reads an OPRULE_TIME_SERIES of the ``"base"`` or ``"scenario"`` echo file as
    ``(DataFrame, units, run period or None)``; it is called only for series whose values differ.
    """
    body = diff_html(detail)
    if not (body or detail["series"]):
        return pn.pane.HTML("<div style='font-size:12px;margin-top:10px'>No operating-rule differences.</div>", margin=0, width=600)
    parts = [pn.pane.HTML(body, margin=0, width=600)]
    if detail["series"]:
        parts.append(pn.pane.HTML(_section("RULE TIME SERIES (VALUES DIFFER)"), margin=0, width=600))
        parts += [series_chart(entry, load_series) for entry in detail["series"]]
    parts.append(pn.pane.HTML(
        "<div style='font-size:11px;color:#777;margin-top:10px'>Highlighted rows differ from the base. A time "
        "series is listed only when its values differ.</div>", margin=0, width=600,
    ))
    return pn.Column(*parts, margin=0, width=600)
