"""
Expression evaluation helpers for station math expressions.

Station CSVs may specify an ``obs_expression`` or ``model_expression`` column
containing an arithmetic expression such as ``-VCU`` or ``SDC - GES``.
Identifiers in the expression are B-part station names that are looked up in the
same DSS file as the station row.

Expression parsing and safe-eval namespace are borrowed from ``dvue.math_reference``
to avoid reimplementation.  ``dvue`` is a direct dependency of dsm2ui.
"""

import re

import pandas as pd

from dvue.math_reference import _MATH_NAMESPACE, _RESERVED_TOKENS


def parse_expression_tokens(expr: str) -> list:
    """Return the list of station-ID identifiers found in *expr*.

    Strips out reserved math/numpy/vtools names so only actual station IDs
    remain.  For example::

        parse_expression_tokens("SDC - GES")   # -> ['GES', 'SDC']
        parse_expression_tokens("-VCU")         # -> ['VCU']
        parse_expression_tokens("RSAC128 - RSAC123")  # -> ['RSAC123', 'RSAC128']
    """
    tokens = set(re.findall(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\b", expr))
    return sorted(tokens - _RESERVED_TOKENS)


def eval_expression(expr: str, series_map: dict) -> pd.Series:
    """Evaluate *expr* with station Series from *series_map* as variables.

    Uses the same safe namespace as ``dvue.math_reference`` (numpy ufuncs,
    vtools filters, math constants; **no** Python builtins).

    Parameters
    ----------
    expr:
        Arithmetic expression string, e.g. ``"SDC - GES"`` or ``"-VCU"``.
    series_map:
        Mapping of identifier → ``pd.Series`` (raw station time series).

    Returns
    -------
    pd.Series
        The evaluated result.

    Raises
    ------
    ValueError
        If evaluation fails.
    """
    ns = {**_MATH_NAMESPACE, **series_map}
    try:
        result = eval(expr, ns)  # noqa: S307
    except Exception as exc:
        raise ValueError(f"Failed to evaluate expression {expr!r}: {exc}") from exc
    if isinstance(result, pd.DataFrame):
        result = result.iloc[:, 0]
    return result
