# Station Math Expressions — Developer Implementation Summary

## Purpose
Allow arbitrary arithmetic expressions (e.g. `-VCU`, `SDC-GES`, `RSAC128-RSAC123`) for
calibration plots **without adding new CSV columns**. Expressions are encoded directly in
the existing `obs_station_id` (observed) and `dsm2_id` (model) fields — the same fields
that already held the legacy `-` prefix / subtraction conventions.

## Key Design Decisions
- **No new CSV columns.** `obs_station_id` is the observed expression; `dsm2_id` is the
  model expression. When either field is a plain identifier (`^[a-zA-Z_][a-zA-Z0-9_]*$`)
  it is used as a direct DSS B-part lookup. When it contains operators (`-`, `+`, etc.)
  it is evaluated as an arithmetic expression.
- `_is_expression(s)` in `calibplot.py` performs this classification.
- Identifiers inside expressions are B-part station names resolved from the same DSS file.
- Expression engine reuses `_MATH_NAMESPACE` + `_RESERVED_TOKENS` + token regex from
  `dvue.math_reference` — dvue is already in dsm2ui's `environment.yml`.
- Old `subtract` column logic in `PostProcessor` left as-is (backward-compat fallback).

## CSV Convention

| dsm2_id | obs_station_id | Observed interpretation | Model interpretation |
|---|---|---|---|
| `VCU` | `-VCU` | evaluate expression `-VCU` | load station `VCU` directly |
| `CHVCT000` | `-CHVCT000` | evaluate expression `-CHVCT000` | load station `CHVCT000` directly |
| `SDC-GES` | `SDC-GES` | evaluate expression `SDC-GES` | evaluate expression `SDC-GES` |
| `RSAC128-RSAC123` | `SDC-GES` | evaluate expression `SDC-GES` | evaluate expression `RSAC128-RSAC123` |

No `obs_expression` / `model_expression` columns in the CSV — they were considered and
rejected as redundant.

## Files Changed

### `d:\dev\pydsm\pydsm\analysis\postpro.py`
`Location` namedtuple is unchanged — 5 fields only (`name`, `bpart`, `description`,
`time_window_exclusion_list`, `threshold_value`). No new fields were added.

### `d:\dev\dsm2ui\dsm2ui\calib\expression_eval.py` (NEW FILE)
```python
from dvue.math_reference import _MATH_NAMESPACE, _RESERVED_TOKENS
import re, pandas as pd

def parse_expression_tokens(expr: str) -> list[str]:
    tokens = set(re.findall(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\b", expr))
    return sorted(tokens - _RESERVED_TOKENS)

def eval_expression(expr: str, series_map: dict) -> pd.Series:
    ns = {**_MATH_NAMESPACE, **series_map}
    result = eval(expr, ns)  # noqa: S307
    if isinstance(result, pd.DataFrame):
        result = result.iloc[:, 0]
    return result
```

### `d:\dev\dsm2ui\dsm2ui\calib\calibplot.py`
- **`_SIMPLE_ID_RE`** (module level, after `import re`): `re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')`
- **`_is_expression(s)`**: returns `True` when `s` contains operators (not a plain identifier)
- **`_compute_expression(p, expr, study, vartype)`**: parses tokens, loads each token as a
  PostProcessor, evaluates the expression, populates `p.df / p.gdf / p.high / p.low / p.amp`
- **`load_data_for_plotting()`**: expression detection:
  ```python
  if study.name == "Observed":
      expr = location.bpart if _is_expression(location.bpart) else None
  else:
      expr = location.name if _is_expression(location.name) else None
  ```
  Expression path: try cache → compute → store → load. Standard path unchanged.
- **Plot header**: uses `location.name` (which is already the expression for derived stations
  like `SDC-GES`, `RSAC128-RSAC123`)

## Expression Engine — dvue Reuse
`dvue.math_reference._MATH_NAMESPACE` provides:
- `__builtins__: {}` — no Python builtins (safe eval)
- numpy ufuncs: `abs`, `sqrt`, `log`, `exp`, `sin`, `cos`, `nan`, `inf`
- vtools filters: `godin`, `cosine_lanczos`, etc.

Token regex: `r"\b([a-zA-Z_][a-zA-Z0-9_]*)\b"` — same as MathDataReference.  
`_RESERVED_TOKENS` = frozenset of numpy/math/vtools identifiers that are not station names.

## Verification
1. `pytest tests/` in pydsm — `Location` namedtuple unchanged, no regressions expected
2. `pytest tests/` in dsm2ui — expression_eval module + calibplot changes
3. Run `dsm2ui postpro plots postpro_config_v821_v2025.yml` from `D:\delta\dsm2_studies\studies\`
   — check VCU (negated) and SDC-GES (subtracted) plots are correct
4. Confirm rows without expressions produce identical plots to pre-change runs

