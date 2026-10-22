# -*- coding: utf-8 -*-
"""Gate-diff pop-up for the Compare tab of the DSM2 input viewer: base vs scenario values of one gate."""
import html

import panel as pn

from dsm2ui.channel_diff_popup import _section, _table, _val
from dsm2ui.oprule_diff_tab import rules_tab
from dsm2ui.xsection_popup import HEADER_STYLES, POPUP_STYLES, POPUP_WIDTH

_TITLE_COLOR = "#b3261e"
_STATUS_TEXT = {
    "changed": "Gate exists in both files",
    "new": "New gate: not in the base file",
    "removed": "Removed gate: not in the scenario file",
}
_DEVICE_TITLE = {"weir": "WEIR DEVICES", "pipe": "PIPE DEVICES"}


def _devices_html(kind, devices):
    if not devices:
        return ""
    changed = [d for d in devices if d["status"] != "same"]
    unchanged = [d["device"] for d in devices if d["status"] == "same"]
    out = _section(_DEVICE_TITLE[kind])
    for d in changed:
        rows = [([html.escape(c["label"]), _val(c["base"]), _val(c["scenario"])], c["changed"]) for c in d["columns"]]
        out += (
            f"<div style='font-size:12px;margin:8px 0 4px'><b>{html.escape(d['device'])}</b>: {d['status']}</div>"
            + _table(["Property", "Base", "Scenario"], rows)
        )
    if unchanged:
        out += f"<div style='font-size:12px;color:#555;margin-top:6px'>Unchanged: {html.escape(', '.join(unchanged))}</div>"
    return out


def diff_html(detail):
    """HTML body of the pop-up for one gate (see ``GateDiff.detail``)."""
    fields = [([html.escape(f["label"]), _val(f["base"]), _val(f["scenario"])], f["changed"]) for f in detail["fields"]]
    return (
        f"<div style='font-size:12px;color:#333;margin:8px 0 0'>{_STATUS_TEXT[detail['status']]}</div>"
        + _section("LOCATION") + _table(["Property", "Base", "Scenario"], fields)
        + "".join(_devices_html(kind, detail["devices"][kind]) for kind in ("weir", "pipe"))
        + "<div style='font-size:11px;color:#777;margin-top:10px'>Highlighted rows differ from the base.</div>"
    )


class GateDiffPopup:
    """Pop-up window for one gate's differences, with a tab for the gate structure and one for its operating rules.

    ``load_series(side, row)`` reads an OPRULE_TIME_SERIES of the ``"base"`` or ``"scenario"`` echo file as
    ``(DataFrame, units, run period or None)``. Call :meth:`show` to open the window.
    """

    def __init__(self, load_series):
        self._load_series = load_series
        self._title = pn.pane.HTML("", margin=0, width=556)
        close = pn.widgets.Button(icon="x", button_type="light", width=36, height=36, margin=0)
        close.on_click(lambda _: self.hide())
        header = pn.Column(
            pn.Row(self._title, close, margin=0, width=600),
            margin=0, width=POPUP_WIDTH - 32, styles=HEADER_STYLES,
        )
        self._tabs = pn.Tabs(margin=0, width=600)
        self.panel = pn.Column(
            header, self._tabs, width=POPUP_WIDTH, visible=False, css_classes=["xs-popup"], styles=POPUP_STYLES,
        )

    def show(self, gate_diff, name):
        self._title.object = (
            "<div class='xs-drag' title='Drag to move' style='cursor:move;user-select:none;padding:6px 0;"
            f"font-size:15px;font-weight:700;color:{_TITLE_COLOR}'>GATE DIFF \u2013 {html.escape(name)}</div>"
        )
        if gate_diff.has_gate_diff(name):
            structure = pn.pane.HTML(diff_html(gate_diff.detail(name)), margin=0, width=600)
        else:
            structure = pn.pane.HTML(
                "<div style='font-size:12px;margin-top:10px'>No gate structure differences.</div>", margin=0, width=600,
            )
        self._tabs[:] = [
            ("Gate structure", structure),
            ("Operating rules", rules_tab(gate_diff.rule_detail(name), self._load_series)),
        ]
        self._tabs.active = 0 if gate_diff.has_gate_diff(name) else 1
        self.panel.visible = True

    def hide(self):
        self.panel.visible = False
