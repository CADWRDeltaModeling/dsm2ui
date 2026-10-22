# -*- coding: utf-8 -*-
"""Channel-diff pop-up for the Compare tab of the DSM2 input viewer: base vs scenario values of one channel."""
import html

import panel as pn

from dsm2ui.xsection_popup import HEADER_STYLES, POPUP_STYLES, POPUP_WIDTH

_TITLE_COLOR = "#b3261e"
_CHANGED = "background:#fdecea;color:#b3261e;font-weight:600"
_STATUS_TEXT = {
    "changed": "Channel exists in both files",
    "new": "New channel: not in the base file",
    "removed": "Removed channel: not in the scenario file",
}
_XS_TEXT = {"removed": "removed", "added": "added"}
_XS_COLUMNS = (
    ("ELEV", "Elevation (ft)"), ("AREA", "Area (sq ft)"), ("WIDTH", "Width (ft)"), ("WET_PERIM", "Wetted perim. (ft)"),
)


def _section(text):
    return f"<div style='font-size:11px;font-weight:700;letter-spacing:.4px;color:#222;margin:14px 0 4px'>{text}</div>"


def _table(head, rows):
    """``rows`` is a list of (cells, changed) with cells already HTML-escaped."""
    th = "".join(f"<th style='text-align:left;padding:3px 8px;border-bottom:1px solid #ddd'>{h}</th>" for h in head)
    body = "".join(
        f"<tr style='{_CHANGED if changed else ''}'>"
        + "".join(f"<td style='padding:3px 8px'>{c}</td>" for c in cells) + "</tr>"
        for cells, changed in rows
    )
    return f"<table style='border-collapse:collapse;font-size:12px;width:100%'><tr>{th}</tr>{body}</table>"


def _val(v):
    return "\u2014" if v is None else html.escape(v)


def _pair(v):
    return "\u2014" if v is None else f"{v[0]:,.3f} / {v[1]:,.2f}"


def _xsection_html(detail):
    nb, ns = detail["xs_counts"]
    if not nb and not ns:
        return ""
    changed = [x for x in detail["xsections"] if x["status"] != "same"]
    unchanged = len(detail["xsections"]) - len(changed)
    out = _section("CROSS SECTIONS") + f"<div style='font-size:12px'>Locations: {nb} in base, {ns} in scenario"
    out += f"; {unchanged} unchanged</div>" if unchanged else "</div>"
    simple = [x for x in changed if not x["layers"]]
    if simple:
        rows = [
            ([f"{x['dist']:.3f}", _XS_TEXT.get(x["status"]) or f"changed: {html.escape(x['note'])}"], True)
            for x in simple
        ]
        out += "<div style='margin-top:4px'>" + _table(["Location (DIST)", "Difference"], rows) + "</div>"
    for x in changed:
        if x["layers"]:
            out += (
                f"<div style='font-size:12px;margin:10px 0 4px'><b>Location {x['dist']:.3f}</b>: "
                f"{html.escape(x['note'])} differ in {len(x['layers'])} of {x['n_layers']} layers</div>"
                + _layers_table(x["layers"])
            )
    return out


def _num(v):
    return f"{v:,.3f}".rstrip("0").rstrip(".")


def _layers_table(layers):
    """Base and scenario values of the cross-section layers that differ; differing cells are highlighted."""
    cell = "padding:2px 4px;white-space:nowrap"
    head = (
        f"<tr><th rowspan='2' style='{cell};text-align:left;border-bottom:1px solid #ddd'>Layer</th>"
        + "".join(
            f"<th colspan='2' style='{cell};text-align:center'>{label}</th>" for _, label in _XS_COLUMNS
        )
        + "</tr><tr>"
        + "".join(
            f"<th style='{cell};text-align:right;border-bottom:1px solid #ddd;font-weight:500'>{side}</th>"
            for _ in _XS_COLUMNS for side in ("Base", "Scenario")
        )
        + "</tr>"
    )
    body = ""
    for layer in layers:
        body += f"<tr><td style='{cell}'>{layer['layer']}</td>"
        for col, _ in _XS_COLUMNS:
            style = _CHANGED if col in layer["diff"] else ""
            for value in layer["values"][col]:
                body += f"<td style='{cell};text-align:right;{style}'>{_num(value)}</td>"
        body += "</tr>"
    return (
        "<div style='overflow-x:auto'><table style='border-collapse:collapse;font-size:11px;width:100%'>"
        f"{head}{body}</table></div>"
    )


def _initial_html(detail):
    if not detail["initial"]:
        return ""
    rows = [
        ([html.escape(ic["dist"]), _pair(ic["base"]), _pair(ic["scenario"])], ic["changed"])
        for ic in detail["initial"]
    ]
    return (
        _section("INITIAL CONDITION")
        + _table(["Distance", "Base (stage / flow)", "Scenario (stage / flow)"], rows)
    )


def diff_html(detail):
    """HTML body of the pop-up for one channel (see ``ChannelDiff.detail``)."""
    fields = [([html.escape(f["label"]), _val(f["base"]), _val(f["scenario"])], f["changed"]) for f in detail["fields"]]
    status = _STATUS_TEXT[detail["status"]]
    return (
        f"<div style='font-size:12px;color:#333;margin:8px 0 0'>{status}</div>"
        + _section("CHANNEL PROPERTIES") + _table(["Property", "Base", "Scenario"], fields)
        + _xsection_html(detail) + _initial_html(detail)
        + "<div style='font-size:11px;color:#777;margin-top:10px'>Highlighted rows differ from the base.</div>"
    )


class ChannelDiffPopup:
    """Pop-up window for one channel's differences; call :meth:`show` to open it."""

    def __init__(self, on_close=None):
        self._on_close = on_close
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
            f"font-size:15px;font-weight:700;color:{_TITLE_COLOR}'>CHANNEL DIFF \u2013 CHANNEL {detail['chan']}</div>"
        )
        self._body.object = diff_html(detail)
        self.panel.visible = True

    def hide(self):
        self.panel.visible = False
        if self._on_close:
            self._on_close()
