# -*- coding: utf-8 -*-
"""Reservoir-diff pop-up for the Compare tab of the DSM2 input viewer: base vs scenario values of one reservoir."""
import html

import panel as pn

from dsm2ui.channel_diff_popup import _section, _table, _val
from dsm2ui.xsection_popup import HEADER_STYLES, POPUP_STYLES, POPUP_WIDTH

_TITLE_COLOR = "#b3261e"
_STATUS_TEXT = {
    "changed": "Reservoir exists in both files",
    "new": "New reservoir: not in the base file",
    "removed": "Removed reservoir: not in the scenario file",
}


def _coefs(v):
    return "\u2014" if v is None else f"{v[0]:,.2f} / {v[1]:,.2f}"


def _point(v):
    return "\u2014" if v is None else f"{v[0]:,.3f} / {v[1]:,.3f}"


def _connections_html(detail):
    if not detail["connections"]:
        return ""
    rows = [
        ([str(c["node"]), _coefs(c["base"]), _coefs(c["scenario"])], c["changed"]) for c in detail["connections"]
    ]
    return _section("CONNECTIONS") + _table(["Node", "Base (coef in / out)", "Scenario (coef in / out)"], rows)


def _curve_html(detail):
    nb, ns = detail["curve"]["counts"]
    if not nb and not ns:
        return ""
    rows = detail["curve"]["rows"]
    out = _section("ELEVATION vs AREA") + (
        f"<div style='font-size:12px'>Points: {nb} in base, {ns} in scenario; "
        f"{len(rows)} of {max(nb, ns)} differ</div>"
    )
    if rows:
        out += "<div style='margin-top:4px'>" + _table(
            ["Point", "Base (elevation ft / area acres)", "Scenario (elevation ft / area acres)"],
            [([str(r["point"]), _point(r["base"]), _point(r["scenario"])], True) for r in rows],
        ) + "</div>"
    return out


def diff_html(detail):
    """HTML body of the pop-up for one reservoir (see ``ReservoirDiff.detail``)."""
    fields = [([html.escape(f["label"]), _val(f["base"]), _val(f["scenario"])], f["changed"]) for f in detail["fields"]]
    return (
        f"<div style='font-size:12px;color:#333;margin:8px 0 0'>{_STATUS_TEXT[detail['status']]}</div>"
        + _section("PROPERTIES") + _table(["Property", "Base", "Scenario"], fields)
        + _connections_html(detail) + _curve_html(detail)
        + "<div style='font-size:11px;color:#777;margin-top:10px'>Highlighted rows differ from the base.</div>"
    )


class ReservoirDiffPopup:
    """Pop-up window for one reservoir's differences; call :meth:`show` to open it."""

    def __init__(self):
        self._title = pn.pane.HTML("", margin=0, width=556)
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

    def show(self, detail):
        self._title.object = (
            "<div class='xs-drag' title='Drag to move' style='cursor:move;user-select:none;padding:6px 0;"
            f"font-size:15px;font-weight:700;color:{_TITLE_COLOR}'>RESERVOIR DIFF \u2013 "
            f"{html.escape(detail['name'])}</div>"
        )
        self._body.object = diff_html(detail)
        self.panel.visible = True

    def hide(self):
        self.panel.visible = False
