# -*- coding: utf-8 -*-
"""Flow-input diff pop-up for the Compare tab of the DSM2 input viewer: boundary / source flow time series, base vs scenario."""
import html

import panel as pn

from dsm2ui.channel_diff_popup import _section, _table, _val
from dsm2ui.oprule_diff_view import series_chart
from dsm2ui.xsection_popup import HEADER_STYLES, POPUP_STYLES, POPUP_WIDTH

_TITLE_COLOR = "#b3261e"
_WHAT = {"bc": "BOUNDARY", "src": "SOURCE FLOW", "ressrc": "SOURCE FLOW"}


def entry_html(entry):
    """Heading and property table of one changed or removed row; the time series chart is added separately."""
    column = "Reservoir" if entry["column"] == "RES_NAME" else "Node"
    removed = entry["status"] == "removed"
    b, s = entry["base"], entry["scenario"] or {}
    rows = [([column, _val(b["where"]), _val(s.get("where"))], removed or entry["node_changed"])]
    if b["sign"] is not None:
        rows.append((["SIGN", _val(b["sign"]), _val(s.get("sign"))], removed or entry["sign_changed"]))
    rows.append((["FILLIN", _val(b["fillin"]), _val(s.get("fillin"))], removed or entry["fillin_changed"]))
    if removed:
        rows += [(["FILE", _val(b["file"]), _val(None)], True), (["PATH", _val(b["path"]), _val(None)], True)]
    status = ": removed" if removed else ""
    return (
        _section(f"{entry['label'].upper()}: {html.escape(entry['name'])}{status}")
        + _table(["Property", "Base", "Scenario"], rows)
    )


class FlowDiffPopup:
    """Pop-up window for the changed or removed boundary / source flows at one location; call :meth:`show` to open it.

    ``load_series(side, row)`` reads a time series of the ``"base"`` or ``"scenario"`` echo file as
    ``(DataFrame, units, run period or None)``; it is called only for series whose values differ.
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
        self._body = pn.Column(margin=0, width=600)
        self.panel = pn.Column(
            header, self._body, width=POPUP_WIDTH, visible=False, css_classes=["xs-popup"], styles=POPUP_STYLES,
        )

    def show(self, layer, where, entries):
        place = f"Reservoir {where}" if layer == "ressrc" else f"Node {where}"
        self._title.object = (
            "<div class='xs-drag' title='Drag to move' style='cursor:move;user-select:none;padding:6px 0;"
            f"font-size:15px;font-weight:700;color:{_TITLE_COLOR}'>{_WHAT[layer]} DIFF \u2013 {html.escape(place)}</div>"
        )
        parts = []
        for e in entries:
            parts.append(pn.pane.HTML(entry_html(e), margin=0, width=600))
            if e["status"] == "removed":
                continue
            if e["values_changed"]:
                parts.append(series_chart(
                    {"name": e["name"], "base": e["base"], "scenario": e["scenario"], "note": e["note"]},
                    self._load_series,
                ))
            else:
                parts.append(pn.pane.HTML(
                    "<div style='font-size:11px;color:#777;margin:6px 0'>Time series values are identical.</div>",
                    margin=0, width=600,
                ))
        parts.append(pn.pane.HTML(
            "<div style='font-size:11px;color:#777;margin-top:10px'>Highlighted rows differ from the base. A chart is "
            "shown only when the time series values differ.</div>", margin=0, width=600,
        ))
        self._body[:] = parts
        self.panel.visible = True

    def hide(self):
        self.panel.visible = False
