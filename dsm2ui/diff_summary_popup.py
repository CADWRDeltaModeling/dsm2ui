# -*- coding: utf-8 -*-
"""Diff-summary pop-up for the Compare tab of the DSM2 input viewer: a high-level list of what changed."""
import html

import panel as pn

from dsm2ui.xsection_popup import HEADER_STYLES, POPUP_STYLES, POPUP_WIDTH

_TITLE_COLOR = "#b3261e"
_FLOW_TABLES = (
    ("boundary_flow", "BOUNDARY FLOW"), ("boundary_stage", "BOUNDARY STAGE"), ("source_flow", "SOURCE FLOW"),
    ("source_flow_reservoir", "SOURCE FLOW RESERVOIR"), ("output_locations", "OUTPUT LOCATIONS"),
)


def _heading(text):
    return f"<div style='font-size:11px;font-weight:700;letter-spacing:.4px;color:#222;margin:14px 0 4px'>{text}</div>"


def _table(first, second, items):
    th = "padding:4px 8px;text-align:left;border-bottom:1px solid #ddd;font-weight:600"
    body = "".join(
        "<tr style='border-bottom:1px solid #f0f0f0'>"
        f"<td style='padding:5px 8px;white-space:nowrap'>{label} <span style='color:#777'>({len(ids)})</span></td>"
        f"<td style='padding:5px 8px;color:#b3261e;word-break:break-word'>{', '.join(map(str, ids))}</td></tr>"
        for label, ids in items
    )
    return (
        "<table style='border-collapse:collapse;font-size:12px;width:100%'>"
        f"<tr><th style='{th};width:210px'>{first}</th><th style='{th}'>{second}</th></tr>{body}</table>"
    )


def _scalar_table(entries):
    """Status, name, base and scenario value of every differing scalar; a missing value is shown as a dash."""
    th = "padding:4px 8px;text-align:left;border-bottom:1px solid #ddd;font-weight:600"
    td = "padding:5px 8px;word-break:break-word"
    shown = lambda v: "\u2014" if v is None else html.escape(v)
    body = "".join(
        "<tr style='border-bottom:1px solid #f0f0f0'>"
        f"<td style='{td};white-space:nowrap'>{e['status'].capitalize()}</td><td style='{td}'>{html.escape(e['name'])}</td>"
        f"<td style='{td}'>{shown(e['base'])}</td><td style='{td};color:#b3261e'>{shown(e['scenario'])}</td></tr>"
        for e in entries
    )
    heads = "".join(f"<th style='{th}'>{h}</th>" for h in ("Status", "Scalar", "Base", "Scenario"))
    return f"<table style='border-collapse:collapse;font-size:12px;width:100%'><tr>{heads}</tr>{body}</table>"


def summary_html(summary):
    """HTML body for the channel, reservoir, gate, operating-rule, flow-input, output-location and scalar summaries."""
    n_channels = len({c for _, ids in summary.get("channels", []) for c in ids})
    n_reservoirs = len({r for _, names in summary.get("reservoirs", []) for r in names})
    n_gates = len({g for _, names in summary.get("gates", []) for g in names})
    n_rules = len({g for _, names in summary.get("oprules", []) for g in names})
    scalars = summary.get("scalars", [])
    flows = [(key, title) for key, title in _FLOW_TABLES if summary.get(key)]
    if not (n_channels or n_reservoirs or n_gates or n_rules or flows or scalars):
        return "<div style='font-size:12px;margin-top:10px'>No differences found.</div>"
    flow_html = "".join(
        _heading(f"{title} ({len({n for _, names in summary[key] for n in names})} differ)")
        + _table("Properties", "Name", summary[key])
        for key, title in flows
    )
    return (
        (_heading(f"CHANNELS ({n_channels} differ)") + _table("Properties", "Channel Num", summary["channels"])
         if n_channels else "")
        + (_heading(f"RESERVOIRS ({n_reservoirs} differ)")
           + _table("Properties", "Reservoir Name", summary["reservoirs"]) if n_reservoirs else "")
        + (_heading(f"GATES ({n_gates} differ)") + _table("Properties", "Gate Name", summary["gates"])
           if n_gates else "")
        + (_heading(f"OPERATING RULES ({n_rules} gates differ)")
           + _table("Properties", "Gate Name", summary["oprules"]) if n_rules else "")
        + flow_html
        + (_heading(f"SCALARS ({len(scalars)} differ)") + _scalar_table(scalars) if scalars else "")
        + "<div style='font-size:11px;color:#777;margin-top:10px'>An item can appear under more than one heading. "
        "Right-click a red channel, reservoir, gate, boundary or source flow icon to see its differences. New "
        "reservoirs, gates, flow inputs and output locations are listed here only.</div>"
    )


class DiffSummaryPopup:
    """Pop-up window summarising all differences; call :meth:`show` to open it."""

    def __init__(self):
        self._title = pn.pane.HTML(
            "<div class='xs-drag' title='Drag to move' style='cursor:move;user-select:none;padding:6px 0;"
            f"font-size:15px;font-weight:700;color:{_TITLE_COLOR}'>DIFF SUMMARY</div>",
            margin=0, width=556,
        )
        close = pn.widgets.Button(icon="x", button_type="light", width=36, height=36, margin=0)
        close.on_click(lambda _: self.hide())
        header = pn.Column(
            pn.Row(self._title, close, margin=0, width=600),
            margin=0, width=POPUP_WIDTH - 32, styles=HEADER_STYLES,
        )
        self._body = pn.pane.HTML("", margin=0, width=600)
        self.panel = pn.Column(
            header, self._body, width=POPUP_WIDTH, visible=False, css_classes=["xs-popup"], styles=POPUP_STYLES,
        )

    def show(self, summary):
        self._body.object = summary_html(summary)
        self.panel.visible = True

    def hide(self):
        self.panel.visible = False
