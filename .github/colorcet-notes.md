# Colorcet Notes — Research & Findings

Captured during development of `DSM2FlowlineMap` colormap widget (May 2026).

---

## Attribute Naming

Colorcet exposes every palette under **two** parallel naming schemes:

| Scheme | Example | Type | Notes |
|--------|---------|------|-------|
| Short alias | `cc.rainbow` | `list[str]` (hex colors) | Human-readable name; best for `hvplot`/`opts` |
| Long `b_` alias | `cc.b_rainbow_bgyrm_35_85_c71` | `list[str]` | Systematic name; fragile — many do NOT exist |
| Short `m_` alias | `cc.m_rainbow` | `matplotlib.colors.Colormap` | Matplotlib-compatible object |
| Long `b_` prefix (most) | `cc.b_diverging_bwr_20_95_c54` | — | **Usually does NOT exist** — do not rely on it |

**Key lesson:** use the plain short alias (`cc.rainbow`, `cc.fire`, `cc.bkr`, etc.) with `hasattr` guards.
The `b_` prefix entries are inconsistent — only a handful exist (e.g. `b_rainbow_bgyrm_35_85_c71` exists but
`b_fire` does not). Always guard with `hasattr(cc, name)`.

```python
# Safe pattern
_SEQ_CMAPS = {name: getattr(cc, name) for name in candidates if hasattr(cc, name)}
```

---

## Sequential Palettes (good for single-file maps)

| Short name | Character |
|---|---|
| `rainbow` | Full spectrum, perceptually uniform |
| `fire` | Black → yellow/white, high contrast |
| `bmy` | Blue → magenta → yellow |
| `blues` | Light → dark blue |
| `bgy` | Blue → green → yellow |
| `colorwheel` | Cyclic full hue wheel |
| `isolum` | Isoluminant (colour-only, no luminance cue) |
| `kbc` | Black → blue → cyan |

---

## Diverging Palettes (good for difference maps)

| Short name | Character |
|---|---|
| `bkr` | Blue → black → red |
| `bky` | Blue → black → yellow |
| `gwv` | Green → white → violet |
| `coolwarm` | Blue → white → red (matplotlib classic) |
| `diverging_bwr_20_95_c54` | Blue → white → red (bright) |
| `diverging_linear_bjy_30_90_c45` | Blue → black → yellow (linear lightness) |
| `diverging_rainbow_bgymr_45_85_c67` | Full diverging rainbow |

---

### Color normalization — `cnorm`

HoloViews Bokeh opts support a `cnorm` parameter controlling how data values are mapped to colors:

| Value | Effect |
|-------|--------|
| `'linear'` | Default — linear mapping from `clim[0]` to `clim[1]` |
| `'log'` | Logarithmic mapping — useful for data with large dynamic range |
| `'eq_hist'` | **Equalized histogram** — redistributes color to show equal counts per color bin; reveals detail in dense clusters regardless of `clim` |

```python
opts.Polygons(
    cmap=cc.bmy,
    color=hv.dim("MANNING"),
    colorbar=True,
    clim=(0.02, 0.05),
    cnorm="eq_hist",   # or "linear" / "log"
)
```

`eq_hist` is particularly useful for:
- Dispersion values that cluster near zero with a few large outliers
- Difference maps where most channels have small changes but a few are extreme

Note: `clim` is still respected as the display range even with `eq_hist`; the histogram equalisation
operates only within that window.

---



Pass the list directly as `cmap=`; colorcet lists are plain Python lists of hex strings so they
work with Bokeh, matplotlib, and HoloViews without conversion.

```python
import colorcet as cc
from holoviews import opts
import holoviews as hv

plot = gdf.hvplot(c="MANNING").opts(
    opts.Polygons(
        cmap=cc.bmy,                  # short-alias list
        color=hv.dim("MANNING"),      # required instead of deprecated color_index=
        colorbar=True,
        clim=(vmin, vmax),            # must be inside opts.Polygons(), not as extra kwarg
        line_alpha=0,
    )
)
```

### Common pitfalls with `.opts()`

- **`clim` must go inside `opts.Polygons()`**, not as a bare keyword alongside it.
  HoloViews raises `ValueError: Options must be defined in one of two formats` if you mix
  `opts.Polygons(...)` with additional keyword arguments in the same `.opts()` call.

  ```python
  # WRONG — raises ValueError
  plot.opts(opts.Polygons(cmap=...), clim=(0, 1))

  # CORRECT
  plot.opts(opts.Polygons(cmap=..., clim=(0, 1)))
  ```

- **`color_index` is deprecated** (as of HoloViews ≥ 1.17).
  Replace with `color=hv.dim('column_name')` in `opts.Polygons()`.

  ```python
  # Old (warns)
  opts.Polygons(color_index="MANNING", cmap=cc.bmy)

  # New
  opts.Polygons(color=hv.dim("MANNING"), cmap=cc.bmy)
  ```

---

## Reactive Widget Pattern (Panel)

When multiple map panels share the same colormap and range, create widgets *once* and pass
the same instances to all `@pn.depends` functions.

```python
import panel as pn
import colorcet as cc
from holoviews import opts
import holoviews as hv

cmap_options = {"rainbow": cc.rainbow, "fire": cc.fire, "bmy": cc.bmy}

cmap_sel = pn.widgets.Select(name="Colormap", options=list(cmap_options), width=160)
lo_input = pn.widgets.FloatInput(name="Min", value=0.0, step=0.001, width=140)
hi_input = pn.widgets.FloatInput(name="Max", value=1.0, step=0.001, width=140)

@pn.depends(cmap_sel.param.value, lo_input.param.value, hi_input.param.value)
def make_plot(cmap_name, lo, hi):
    cmap = cmap_options[cmap_name]
    clim = (lo, hi) if lo < hi else (hi, lo)
    return gdf.hvplot(c="col").opts(
        opts.Polygons(cmap=cmap, color=hv.dim("col"), colorbar=True, clim=clim)
    )

layout = pn.Column(
    pn.Row(cmap_sel, lo_input, hi_input),
    make_plot,           # same reactive pane
    make_plot,           # second map — responds to same widgets
)
```

Use `pn.widgets.FloatInput` (editable number boxes) rather than `RangeSlider` when the
data range spans several orders of magnitude (e.g. dispersion 0–5000, Manning 0.02–0.05).

---

## Colorcet Attribute Discovery Snippet

```python
import colorcet as cc

# All short-name aliases (no b_ or m_ prefix, no CamelCase)
short_names = sorted([
    a for a in dir(cc)
    if not a.startswith("_")
    and not a[0].isupper()
    and not a.startswith("b_")
    and not a.startswith("m_")
    and not a.startswith("CET")
])
print("\n".join(short_names))
```
