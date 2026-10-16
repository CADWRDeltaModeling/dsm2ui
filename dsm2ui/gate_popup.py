# -*- coding: utf-8 -*-
"""Hydraulic-structure pop-up for the DSM2 input viewer: devices and operating rules of a gate."""
import html

import panel as pn

from dsm2ui.xsection_popup import HEADER_STYLES, POPUP_STYLES, POPUP_WIDTH

_BLUE = "#1a73e8"
_GATE_BLUE = "#2f5fb3"

# (column, heading) per device table; lengths are in feet.
_WEIR_COLUMNS = [
    ("DEVICE", "Device"), ("NDUPLICATE", "No. of devices"), ("WIDTH", "Width (ft)"), ("ELEV", "Elevation (ft)"),
    ("HEIGHT", "Height (ft)"), ("CF_FROM_NODE", "Coeff. from node"), ("CF_TO_NODE", "Coeff. to node"),
    ("DEFAULT_OP", "Default operation"),
]
_PIPE_COLUMNS = [
    ("DEVICE", "Device"), ("NDUPLICATE", "No. of devices"), ("RADIUS", "Radius (ft)"), ("ELEV", "Elevation (ft)"),
    ("CF_FROM_NODE", "Coeff. from node"), ("CF_TO_NODE", "Coeff. to node"), ("DEFAULT_OP", "Default operation"),
]


def _value(v):
    """Numbers lose trailing zeros; everything else is shown as text."""
    try:
        return f"{float(v):.3f}".rstrip("0").rstrip(".")
    except (TypeError, ValueError):
        return str(v)


def _section_title(text):
    return (
        f"<div style='font-size:11px;font-weight:700;letter-spacing:.4px;color:#222;margin:12px 0 4px'>{text}</div>"
    )


def _table(columns, records):
    head = "".join(
        f"<th style='text-align:{'left' if i == 0 else 'right'};padding:6px 6px;color:#555;font-weight:600'>"
        f"{html.escape(label)}</th>"
        for i, (_, label) in enumerate(columns)
    )
    body = "".join(
        "<tr style='border-top:1px solid #e5e7eb'>" + "".join(
            f"<td style='padding:6px 6px;text-align:{'left' if i == 0 else 'right'};"
            f"{'' if i else f'color:{_BLUE};font-weight:500'}'>{html.escape(_value(rec.get(col, '')))}</td>"
            for i, (col, _) in enumerate(columns)
        ) + "</tr>"
        for rec in records
    )
    return (
        "<table style='width:100%;border-collapse:collapse;font-size:11.5px'>"
        f"<tr style='background:#f5f7fb'>{head}</tr>{body}</table>"
    )


class GatePopup:
    """Pop-up with the devices and operating rules of a hydraulic structure; call :meth:`show` to open it."""

    def __init__(self, on_close, load_devices, load_rules, rule_section):
        """``load_devices(name)`` -> ``(weir records, pipe records)``; ``load_rules(name)`` -> the rule summary of
        ``oprule_parse.summarise_gate``; ``rule_section`` is an ``oprule_view.RuleSection``."""
        self._on_close = on_close
        self._load = load_devices
        self._load_rules = load_rules
        self._rule_section = rule_section
        self._view = None
        self._rules_stale = False
        self._rows = []
        self._loading = False

        self._title = pn.pane.HTML("", margin=0, width=556)
        close = pn.widgets.Button(icon="x", button_type="light", width=36, height=36, margin=0)
        close.on_click(lambda _: self.hide())
        self._subtitle = pn.pane.HTML("", margin=0)
        header = pn.Column(
            pn.Row(self._title, close, margin=0, width=600), self._subtitle,
            margin=0, width=POPUP_WIDTH - 32, styles=HEADER_STYLES,
        )
        self._select = pn.widgets.Select(
            name="Hydraulic structure", options={}, visible=False, margin=(8, 0, 0, 0), width=600,
        )
        self._select.param.watch(self._on_pick, "value")
        self._devices = pn.pane.HTML("", margin=0, width=600)
        self._rules = pn.Column(margin=0, width=600)
        self._tabs = pn.Tabs(
            ("Devices", self._devices), ("Operating rules", self._rules),
            dynamic=False, margin=(8, 0, 0, 0), width=600,
        )
        self._tabs.param.watch(self._on_tab, "active")
        self.panel = pn.Column(
            header, self._select, self._tabs, width=POPUP_WIDTH, visible=False,
            css_classes=["xs-popup"], styles=POPUP_STYLES,
        )

    def show(self, rows):
        """Open for ``rows``: dicts with ``name``, ``kind`` ('Channel'/'Reservoir'), ``where`` and ``node``."""
        self._rows = rows
        self._loading = True
        self._select.options = {r["name"]: i for i, r in enumerate(rows)}
        self._select.value = 0
        self._select.visible = len(rows) > 1
        self._tabs.active = 0
        self._loading = False
        self.panel.visible = True
        self._show_row(0)

    def hide(self):
        self.panel.visible = False
        self._on_close()

    def _on_pick(self, event):
        if not self._loading:
            self._show_row(event.new)

    def _on_tab(self, event):
        if event.new == 1:
            self._render_rules()

    def _render_rules(self):
        """Build the rule blocks (reading the series) the first time the tab is looked at."""
        if self._rules_stale and self._view is not None:
            self._rules_stale = False
            self._rules[:] = self._rule_section.layout(self._view)

    def _show_row(self, i):
        row = self._rows[i]
        self._title.object = (
            "<div class='xs-drag' title='Drag to move' style='cursor:move;user-select:none;padding:6px 0;"
            f"font-size:15px;font-weight:700;color:{_GATE_BLUE}'>HYDRAULIC STRUCTURE \u2013 "
            f"{html.escape(row['name'])}</div>"
        )
        sep = "<span style='color:#bbb'>|</span>"
        self._subtitle.object = (
            "<div style='font-size:12px;color:#333;display:flex;gap:10px;align-items:center;flex-wrap:wrap'>"
            f"<span>{row['kind']}: <b style='font-weight:500'>{html.escape(str(row['where']))}</b></span>{sep}"
            f"<span>Node: <b style='color:{_BLUE};font-weight:500'>{row['node']}</b></span></div>"
        )
        weirs, pipes = self._load(row["name"])
        parts = []
        for title, columns, records in (
            ("WEIR DEVICES", _WEIR_COLUMNS, weirs), ("PIPE DEVICES", _PIPE_COLUMNS, pipes),
        ):
            parts.append(_section_title(title))
            parts.append(
                _table(columns, records) if records
                else "<div style='font-size:12px;color:#777'>None defined for this structure.</div>"
            )
        self._devices.object = "".join(parts)

        self._view = self._load_rules(row["name"])
        self._rules_stale = True
        self._rules[:] = []
        if self._tabs.active == 1:
            self._render_rules()
