# calib_optimize.py — Gradient-based optimizer for DSM2 DISPERSION/MANNING calibration.
"""
Uses L-BFGS-B (via scipy.optimize.minimize) to minimize the station-weighted
sum of squared EC slope deviations from 1.0:

    f(x) = Σ_i  w_i × (slope_i(x) − 1)²

where ``x`` is the vector of group parameter values (DISPERSION or MANNING).

Gradients are computed by forward finite differences using parallel DSM2 model
runs (up to ``max_workers`` concurrent subprocesses).

Typical usage::

    from dsm2ui.calib.calib_optimize import optimize, OptimizationResult

    result: OptimizationResult = optimize("dsm2ui/calib/calib_config.yml")
    print(result.best_params)
    print(result.history_df)

Or via the CLI::

    dsm2ui calib optimize --config dsm2ui/calib/calib_config.yml
    dsm2ui calib optimize --config dsm2ui/calib/calib_config.yml --dry-run
"""
from __future__ import annotations

import copy
import csv
import gc
import logging
import os
import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import yaml
from scipy.optimize import minimize, differential_evolution, Bounds

from dsm2ui.calib.calib_run import (
    ChannelParamModification,
    ECLocation,
    VALID_METRICS,
    _cfg_to_modifications,
    _resolve_channel_inp_source,
    compute_ec_metric,
    compute_ec_slopes,
    load_yaml_config,
    plot_from_yaml,
    read_ec_locations_csv,
    run_study,
    setup_variation,
)

logger = logging.getLogger(__name__)

_PENALTY_PER_STATION = 1.0   # added to objective when a station slope is missing


def _write_progress_csv(path: Path, rows: list) -> None:
    """Overwrite *path* with current history rows (atomic-ish write)."""
    if not rows:
        return
    tmp = path.with_suffix(".tmp")
    df = pd.DataFrame(rows)
    df.to_csv(tmp, index=False, float_format="%.6f")
    tmp.replace(path)


# ─────────────────────────────────────────────────────────────────────────────
# Data classes
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class EvalResult:
    """Result of a single DSM2 model evaluation."""
    eval_id: str
    params: Dict[str, float]          # group_name → value
    objective: float
    slopes: Dict[str, float]          # station_name → metric value (NaN if missing)
    success: bool
    elapsed_sec: float
    eval_dir: Path = field(default=None)
    metric: str = field(default="slope")  # metric used for this eval


@dataclass
class OptimizationResult:
    """Final result returned by :func:`optimize`."""
    best_params: Dict[str, float]
    best_objective: float
    initial_objective: float
    n_evals: int
    n_iters: int
    converged_reason: str
    history_df: pd.DataFrame
    best_dir: Path
    optimized_yaml: Path


# ─────────────────────────────────────────────────────────────────────────────
# Objective helpers
# ─────────────────────────────────────────────────────────────────────────────

def _build_weights(cfg: dict, locations: List[ECLocation]) -> Dict[str, float]:
    """Build station weight dict from YAML, defaulting unlisted stations to 1.0."""
    raw = cfg.get("station_weights") or {}
    weights: Dict[str, float] = {}
    for loc in locations:
        key_upper = loc.model_bpart.upper()
        # Try bpart match first, then station_name match
        w = raw.get(loc.model_bpart, raw.get(key_upper, raw.get(loc.station_name, 1.0)))
        weights[loc.station_name] = float(w)
    return weights


def _objective_from_metric(
    metric_df: pd.DataFrame,
    weights: Dict[str, float],
) -> float:
    """Compute weighted sum of squared (metric_value - metric_target)².

    Missing / NaN values are skipped; if ALL are missing, returns a
    large penalty so the optimizer is pushed away from that region.
    """
    total = 0.0
    n_valid = 0
    for _, row in metric_df.iterrows():
        v = row.get("metric_value", float("nan"))
        if pd.isna(v):
            continue
        target = float(row.get("metric_target", 1.0))
        w = weights.get(row["station_name"], 1.0)
        total += w * (v - target) ** 2
        n_valid += 1
    if n_valid == 0:
        return float(len(weights)) * _PENALTY_PER_STATION * 4.0
    return total


# Keep backward-compatible alias
def _objective_from_slopes(
    slopes_df: pd.DataFrame,
    weights: Dict[str, float],
) -> float:
    """Backward-compatible wrapper — delegates to :func:`_objective_from_metric`."""
    # Legacy DataFrames may use a 'slope' column; normalise on the fly.
    if "metric_value" not in slopes_df.columns and "slope" in slopes_df.columns:
        slopes_df = slopes_df.rename(columns={"slope": "metric_value"})
        if "metric_target" not in slopes_df.columns:
            slopes_df = slopes_df.copy()
            slopes_df["metric_target"] = 1.0
    return _objective_from_metric(slopes_df, weights)


# ─────────────────────────────────────────────────────────────────────────────
# Objective evaluator
# ─────────────────────────────────────────────────────────────────────────────

class ObjectiveEvaluator:
    """Wraps setup_variation + run_study + compute_ec_slopes into a callable.

    Parameters
    ----------
    cfg :
        Validated config dict from :func:`load_yaml_config`.
    yaml_path :
        Path to the source YAML (used for resolving relative paths).
    weights :
        Station weights dict ({station_name: float}).
    """

    def __init__(self, cfg: dict, yaml_path: Path, weights: Dict[str, float]) -> None:
        self.cfg = cfg
        self.yaml_path = yaml_path
        self.weights = weights

        base = cfg["base_run"]
        base_dir = Path(base["study_dir"])
        self.base_dir = base_dir
        self.base_modifier = base["modifier"]
        self.model_dss_pattern = base.get("model_dss_pattern", "{modifier}_qual.dss")
        self.base_dss = base_dir / "output" / self.model_dss_pattern.format(
            modifier=self.base_modifier
        )
        self.observed_dss = Path(cfg["observed_ec_dss"])
        self.timewindow = cfg.get("metrics", {}).get("timewindow")
        self.channel_inp_name = base.get("channel_inp_name", "channel_std_delta_grid.inp")
        self.channel_inp_source = _resolve_channel_inp_source(
            base_dir, self.channel_inp_name, explicit=base.get("channel_inp_source")
        )
        self.run_steps = cfg["variation"].get("run_steps")
        self.dsm2_bin_dir = cfg.get("dsm2_bin_dir")
        self.envvar_overrides = cfg["variation"].get("envvar_overrides")
        self.copy_timeseries: bool = bool(cfg["variation"].get("copy_timeseries", False))

        # Objective metric — read from metrics.objective_metric, default "slope"
        raw_metric = cfg.get("metrics", {}).get("objective_metric", "slope")
        if raw_metric not in VALID_METRICS:
            raise ValueError(
                f"metrics.objective_metric={raw_metric!r} is not supported. "
                f"Valid choices: {VALID_METRICS}"
            )
        self.metric: str = raw_metric

        active_stations = cfg.get("active_stations")
        self.locations: List[ECLocation] = read_ec_locations_csv(
            cfg["ec_stations_csv"],
            active_stations=active_stations,
        )

        # Base modifications template (names + param types, values replaced per eval)
        self._base_mods: List[ChannelParamModification] = _cfg_to_modifications(cfg)
        self.group_names: List[str] = [m.name for m in self._base_mods]

    def _make_modifications(self, params: Dict[str, float]) -> List[ChannelParamModification]:
        """Build ChannelParamModification list with values from *params*."""
        mods = []
        for m in self._base_mods:
            value = params.get(m.name, m.value)
            mods.append(ChannelParamModification(
                param=m.param,
                channels=m.channels,
                value=float(value),
                name=m.name,
            ))
        return mods

    def evaluate(
        self,
        params: Dict[str, float],
        eval_dir: Path,
        eval_id: str = "eval",
        modifier_override: Optional[str] = None,
    ) -> EvalResult:
        """Run DSM2 for *params* in *eval_dir* and return an :class:`EvalResult`.

        On model subprocess failure, returns a high-penalty result with
        ``success=False`` rather than raising.
        """
        t0 = time.monotonic()
        modifications = self._make_modifications(params)
        modifier = modifier_override or (self.cfg["variation"]["name"] + "_" + eval_id)
        # DSM2 Fortran SCALAR buffers overflow when DSM2MODIFIER expands the
        # title line beyond ~32 chars.  Truncate with a warning so runs never
        # fail silently due to a long study/eval name.
        _MAX_MODIFIER = 32
        if len(modifier) > _MAX_MODIFIER:
            modifier = modifier[:_MAX_MODIFIER]
            logger.warning(
                "modifier truncated to %d chars: %r (eval_dir=%s)",
                _MAX_MODIFIER, modifier, eval_dir.name,
            )

        log_file = eval_dir / "run.log"

        # Write eval_params.yml and pre-pend a parameter header to run.log so
        # that a tailing observer (or post-mortem diagnosis) can immediately see
        # what this eval was running.
        _write_eval_diagnostics(eval_dir, eval_id, params, modifications)

        try:
            variation_info = setup_variation(
                base_study_dir=self.base_dir,
                var_study_dir=eval_dir,
                channel_inp_source=self.channel_inp_source,
                modifications=modifications,
                modifier=modifier,
                channel_inp_name=self.channel_inp_name,
                run_steps=self.run_steps,
                dsm2_bin_dir=self.dsm2_bin_dir,
                envvar_overrides=self.envvar_overrides,
                copy_timeseries=self.copy_timeseries,
            )
            run_result = run_study(
                variation_info["batch_file"],
                eval_dir,
                log_file=log_file,
            )
            success = (run_result.returncode == 0)
        except Exception as exc:
            logger.warning("eval %s setup/run failed: %s", eval_id, exc)
            elapsed = time.monotonic() - t0
            n_stations = len(self.locations)
            return EvalResult(
                eval_id=eval_id,
                params=params,
                objective=n_stations * _PENALTY_PER_STATION * 4.0,
                slopes={loc.station_name: float("nan") for loc in self.locations},
                success=False,
                elapsed_sec=elapsed,
                eval_dir=eval_dir,
            )

        if not success:
            logger.warning("eval %s model returned non-zero exit code.", eval_id)
            elapsed = time.monotonic() - t0
            n_stations = len(self.locations)
            return EvalResult(
                eval_id=eval_id,
                params=params,
                objective=n_stations * _PENALTY_PER_STATION * 4.0,
                slopes={loc.station_name: float("nan") for loc in self.locations},
                success=False,
                elapsed_sec=elapsed,
                eval_dir=eval_dir,
            )

        var_dss = eval_dir / "output" / self.model_dss_pattern.format(modifier=modifier)
        metric_df = compute_ec_metric(
            var_dss, self.observed_dss, self.locations, self.timewindow,
            metric=self.metric,
        )

        obj = _objective_from_metric(metric_df, self.weights)
        slopes_dict = dict(zip(metric_df["station_name"], metric_df["metric_value"].astype(float)))
        elapsed = time.monotonic() - t0

        return EvalResult(
            eval_id=eval_id,
            params=params,
            objective=obj,
            slopes=slopes_dict,
            success=True,
            elapsed_sec=elapsed,
            eval_dir=eval_dir,
            metric=self.metric,
        )


# ─────────────────────────────────────────────────────────────────────────────
# Finite-difference gradient
# ─────────────────────────────────────────────────────────────────────────────

_MIN_H = 10.0   # minimum absolute perturbation (ft²/s)


def _forward_step(x_i: float, h_rel: float, lo: float, hi: float) -> Tuple[float, float]:
    """Return (h, sign) for a forward-difference step on parameter x_i.

    At the upper bound, flips to backward difference (sign = -1).
    """
    h = max(abs(x_i) * h_rel, _MIN_H)
    if x_i + h > hi:
        # Flip to backward difference
        h = min(h, x_i - lo)
        return h, -1.0
    return h, 1.0


def parallel_forward_gradient(
    x: np.ndarray,
    f_x: float,
    x0_params: Dict[str, float],
    evaluator: ObjectiveEvaluator,
    scratch_dirs: List[Path],
    h_rel: float,
    opt_cfg: dict,
    max_workers: int,
    bounds_lo: np.ndarray,
    bounds_hi: np.ndarray,
    iter_id: str = "grad",
) -> np.ndarray:
    """Compute forward-difference gradient using parallel model runs.

    Parameters
    ----------
    x :
        Current parameter vector (length N).
    f_x :
        Model objective at x (already evaluated — avoids re-running base).
    x0_params :
        Dict mapping group_name → value for logging convenience.
    evaluator :
        :class:`ObjectiveEvaluator` instance.
    scratch_dirs :
        List of N pre-allocated directories (one per perturbation).
    h_rel :
        Relative finite-difference step.
    opt_cfg :
        The ``optimizer`` config dict.
    max_workers :
        Max concurrent DSM2 processes.
    bounds_lo, bounds_hi :
        Per-parameter lower/upper bounds arrays (length N).
    iter_id :
        String label for logging (e.g. ``"iter3"``).

    Returns
    -------
    numpy.ndarray  shape (N,)
        Gradient estimate.  Components for failed evaluations are set to 0.0.
    """
    n = len(x)
    grad = np.zeros(n)
    futures = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for i, (xi, name) in enumerate(zip(x, evaluator.group_names)):
            h, sign = _forward_step(xi, h_rel, bounds_lo[i], bounds_hi[i])
            x_pert = x.copy()
            x_pert[i] = xi + sign * h
            params_pert = dict(zip(evaluator.group_names, x_pert))
            eval_id = f"{iter_id}_p{i}"
            fut = executor.submit(
                evaluator.evaluate,
                params_pert,
                scratch_dirs[i],
                eval_id,
            )
            futures[fut] = (i, h, sign)

        for fut in as_completed(futures):
            i, h, sign = futures[fut]
            try:
                res: EvalResult = fut.result()
                if res.success:
                    grad[i] = sign * (res.objective - f_x) / h
                    logger.info(
                        "  grad[%d] (%s) x=%.1f h=%.1f f_base=%.6f f_pert=%.6f → grad=%.6f",
                        i, evaluator.group_names[i], x[i], h, f_x, res.objective, grad[i],
                    )
                else:
                    logger.warning(
                        "  grad[%d] (%s) eval failed — using 0.0",
                        i, evaluator.group_names[i],
                    )
            except Exception as exc:
                logger.warning("  grad[%d] future raised: %s — using 0.0", i, exc)

    return grad


# ─────────────────────────────────────────────────────────────────────────────
# Scratch directory management
# ─────────────────────────────────────────────────────────────────────────────

def _prepare_scratch_dirs(
    scratch_root: Path,
    n_params: int,
    eval_parent: Optional[Path] = None,
    eval_prefix: str = "",
) -> Tuple[Path, List[Path]]:
    """Create scratch_root (for metadata) and eval dirs for model runs.

    When *eval_parent* is supplied the eval dirs are placed there instead of
    inside *scratch_root*.  This keeps them at the same directory depth as the
    base study so that relative paths inside ``config.inp`` resolve correctly.
    """
    scratch_root.mkdir(parents=True, exist_ok=True)
    ep = eval_parent if eval_parent is not None else scratch_root
    pfx = eval_prefix if eval_prefix else ""
    base_dir = ep / f"{pfx}eval_base"
    pert_dirs = [ep / f"{pfx}eval_p{i}" for i in range(n_params)]
    for d in [base_dir] + pert_dirs:
        if d.exists():
            shutil.rmtree(d)
        d.mkdir(parents=True, exist_ok=True)
    return base_dir, pert_dirs


# ─────────────────────────────────────────────────────────────────────────────
# Eval diagnostics
# ─────────────────────────────────────────────────────────────────────────────

def _write_eval_diagnostics(
    eval_dir: Path,
    eval_id: str,
    params: Dict[str, float],
    modifications: List,
) -> None:
    """Write eval_params.yml and a parameter header to run.log.

    Called before setup_variation/run_study so information is available even
    if the model hangs or crashes before producing any output.
    """
    import datetime as _dt
    eval_dir.mkdir(parents=True, exist_ok=True)
    timestamp = _dt.datetime.now().isoformat(timespec="seconds")

    # ── eval_params.yml ──────────────────────────────────────────────────────
    params_data = {
        "eval_id": eval_id,
        "timestamp": timestamp,
        "params": {name: float(val) for name, val in params.items()},
    }
    params_path = eval_dir / "eval_params.yml"
    with params_path.open("w") as fh:
        yaml.dump(params_data, fh, default_flow_style=False, sort_keys=False)

    # ── run.log header ───────────────────────────────────────────────────────
    # Written in append mode so subsequent DSM2 stdout is appended below.
    # run_study() opens the file with "w" (truncate), so we need to write the
    # header after setup_variation but the log is opened fresh each run —
    # instead we write a companion header file that stays alongside run.log.
    header_path = eval_dir / "run_header.txt"
    lines = [
        f"eval_id   : {eval_id}",
        f"timestamp : {timestamp}",
        "params    :",
    ]
    for name, val in params.items():
        lines.append(f"  {name:<25} = {val:.4g}")
    lines.append("-" * 60)
    header_path.write_text("\n".join(lines) + "\n")
    logger.debug("Wrote eval diagnostics to %s", eval_dir)


def _clear_eval_dir(d: Path, retries: int = 6, delay: float = 2.0) -> None:
    """Remove all artefacts from a scratch eval dir, keeping the dir itself.

    Strategy (Windows-safe):
    1. Force Python GC so pyhecdss/h5py finalizers release file handles.
    2. Try a direct rmtree.
    3. If still locked, *rename* the directory to a temp name — rename always
       succeeds on Windows even when files are open (the OS just redirects the
       directory entry).  Recreate the empty dir immediately so the next eval
       can start without waiting.  Delete the renamed dir in a background
       thread so the lock release is asynchronous.
    """
    if d.exists():
        gc.collect()
        try:
            shutil.rmtree(d)
        except FileNotFoundError:
            pass  # another thread already removed the dir — nothing to do
        except PermissionError:
            # Rename out of the way — succeeds even with open handles on Windows
            tombstone = d.parent / (d.name + "_del_" + str(int(time.monotonic() * 1000)))
            try:
                os.rename(d, tombstone)
                logger.debug(
                    "_clear_eval_dir: renamed locked dir to %s; deleting in background.",
                    tombstone.name,
                )
            except OSError as rename_exc:
                logger.warning(
                    "_clear_eval_dir: rename failed (%s) — falling back to retry loop.", rename_exc
                )
                tombstone = None
                if not d.exists():
                    # Already deleted by another thread — nothing left to do.
                    pass
                else:
                    for attempt in range(retries):
                        time.sleep(delay)
                        try:
                            shutil.rmtree(d)
                            break
                        except FileNotFoundError:
                            break  # another thread already removed it — done
                        except PermissionError:
                            if attempt == retries - 1:
                                logger.warning(
                                    "_clear_eval_dir: could not remove %s after %d retries — proceeding.",
                                    d, retries,
                                )

            if tombstone is not None:
                def _bg_delete(p: Path) -> None:
                    for _ in range(8):
                        try:
                            shutil.rmtree(p)
                            return
                        except PermissionError:
                            time.sleep(3.0)
                    logger.debug("_clear_eval_dir: background delete gave up on %s", p)
                threading.Thread(target=_bg_delete, args=(tombstone,), daemon=True).start()

    d.mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# Warm-start YAML writer
# ─────────────────────────────────────────────────────────────────────────────

def _write_optimized_yaml(
    source_cfg: dict,
    best_params: Dict[str, float],
    best_dir: Path,
    original_yaml_path: Path,
    pass_suffix: str = "pass2",
) -> Path:
    """Write a copy of *source_cfg* with updated modification values and a new study_dir.

    The new YAML is saved as ``<best_dir>/calib_config_optimized.yml``.
    """
    cfg_copy = copy.deepcopy(source_cfg)

    # Update modification values
    for entry in cfg_copy["variation"]["channel_modifications"]:
        name = entry.get("name", "")
        if name in best_params:
            entry["value"] = float(best_params[name])

    # Derive new study_dir for next pass
    orig_var_dir = Path(cfg_copy["variation"]["study_dir"])
    new_var_dir = orig_var_dir.parent / (orig_var_dir.name + f"_{pass_suffix}")
    cfg_copy["variation"]["study_dir"] = str(new_var_dir).replace("\\", "/")

    # Give the variation a new name to avoid overwriting old outputs
    orig_name = cfg_copy["variation"]["name"]
    cfg_copy["variation"]["name"] = orig_name + f"_{pass_suffix}"

    out_path = best_dir / "calib_config_optimized.yml"
    with out_path.open("w") as fh:
        yaml.dump(cfg_copy, fh, default_flow_style=False, sort_keys=False, allow_unicode=True)

    return out_path


# ─────────────────────────────────────────────────────────────────────────────
# Main optimize entry point
# ─────────────────────────────────────────────────────────────────────────────

def optimize(
    yaml_path: str | Path,
    dry_run: bool = False,
    skip_init: bool = False,
    cfg_override: Optional[dict] = None,
    active_groups: Optional[List[str]] = None,
    progress_csv: Optional[Path] = None,
) -> OptimizationResult:
    """Run the optimizer against the YAML calibration config.

    Parameters
    ----------
    yaml_path :
        Path to ``calib_config.yml``.  Still used for resolving relative paths
        and as the basis for the written optimized YAML even when
        *cfg_override* is supplied.
    dry_run :
        If ``True``, evaluate only the starting point and return without
        running the optimization loop.
    skip_init :
        Reuse existing ``eval_base`` DSS output instead of re-running DSM2
        for the starting point.  Saves one model run when restarting.
    cfg_override :
        If provided, use this config dict instead of loading from *yaml_path*.
        Intended for cascade use: the caller builds a stage-specific config
        (with frozen group values and target stations already set) and passes
        it in directly.
    active_groups :
        Optional list of channel-modification group names to include in the
        optimizer's parameter vector.  All other groups are frozen at their
        values in *cfg_override* (or the loaded config).  When ``None`` all
        groups are optimized (existing behaviour).

    Returns
    -------
    :class:`OptimizationResult`
    """
    yaml_path = Path(yaml_path).resolve()
    cfg = cfg_override if cfg_override is not None else load_yaml_config(yaml_path)

    base = cfg["base_run"]
    var = cfg["variation"]
    opt_cfg: dict = cfg.get("optimizer", {})

    # ── Optimizer settings ────────────────────────────────────────────────────
    max_model_runs: int = int(opt_cfg.get("max_model_runs", 100))
    max_iter: int = int(opt_cfg.get("max_iter", 20))
    patience: int = int(opt_cfg.get("no_improve_patience", 5))
    improve_tol: float = float(opt_cfg.get("no_improve_tol", 0.005))
    h_rel: float = float(opt_cfg.get("finite_diff_rel_step", 0.05))
    max_workers: int = int(opt_cfg.get("max_workers", 4))
    method: str = opt_cfg.get("method", "lbfgsb").lower().replace("-", "").replace("_", "")

    raw_bounds = opt_cfg.get("bounds", [50, 5000])
    if isinstance(raw_bounds, (list, tuple)) and len(raw_bounds) == 2:
        global_lo, global_hi = float(raw_bounds[0]), float(raw_bounds[1])
    else:
        global_lo, global_hi = 50.0, 5000.0
    bounds_overrides: dict = opt_cfg.get("bounds_overrides") or {}

    var_dir = Path(var["study_dir"])

    scratch_root = Path(opt_cfg["scratch_dir"]) if opt_cfg.get("scratch_dir") else \
        var_dir.parent / (var_dir.name + "_optim_scratch")
    best_dir = Path(opt_cfg["best_dir"]) if opt_cfg.get("best_dir") else \
        var_dir.parent / (var_dir.name + "_optim_best")

    best_dir.mkdir(parents=True, exist_ok=True)

    # ── Build evaluator ───────────────────────────────────────────────────────
    active_stations = cfg.get("active_stations")
    locations = read_ec_locations_csv(cfg["ec_stations_csv"], active_stations=active_stations)
    weights = _build_weights(cfg, locations)
    evaluator = ObjectiveEvaluator(cfg, yaml_path, weights)

    group_names = evaluator.group_names
    n_params = len(group_names)
    x0 = np.array([m.value for m in evaluator._base_mods], dtype=float)

    # Per-parameter bounds
    bounds_lo = np.full(n_params, global_lo)
    bounds_hi = np.full(n_params, global_hi)
    for i, name in enumerate(group_names):
        if name in bounds_overrides:
            lo_hi = bounds_overrides[name]
            bounds_lo[i] = float(lo_hi[0])
            bounds_hi[i] = float(lo_hi[1])

    # ── Active-groups subsetting (cascade support) ────────────────────────────
    # Restricts the optimiser x-vector to a subset of groups.  Frozen groups
    # stay at their m.value (set from cfg by ObjectiveEvaluator) and are still
    # applied to every DSM2 run unchanged via _make_modifications fallback.
    if active_groups is not None:
        active_set = set(active_groups)
        active_idx = [i for i, n in enumerate(group_names) if n in active_set]
        if not active_idx:
            raise ValueError(
                f"active_groups {active_groups!r} matched none of the "
                f"configured groups {group_names!r}"
            )
        group_names = [group_names[i] for i in active_idx]
        x0        = x0[active_idx]
        bounds_lo = bounds_lo[active_idx]
        bounds_hi = bounds_hi[active_idx]
        n_params  = len(group_names)

    scipy_bounds = Bounds(lb=bounds_lo, ub=bounds_hi)

    # Coordinate scaling for L-BFGS-B: normalise x to [0, 1] so that the
    # gradient magnitude is O(1e-2) rather than O(1e-6) per ft²/s.  Without
    # this, L-BFGS-B (identity initial Hessian) takes steps of ~2e-6 ft²/s
    # which round to zero in the inp file and stall the optimiser.
    _scale = bounds_hi - bounds_lo   # shape (n_params,)

    # ── Scratch dirs ──────────────────────────────────────────────────────────
    # Place eval dirs at the same level as the base study (var_dir.parent, e.g.
    # studies/) so that relative paths in config.inp resolve identically to a
    # normal variation run.  scratch_root is only used for metadata (CSV etc.).
    eval_prefix = var_dir.name + "_optim_"
    base_scratch, pert_scratch_dirs = _prepare_scratch_dirs(
        scratch_root, n_params,
        eval_parent=var_dir.parent,
        eval_prefix=eval_prefix,
    )

    # ── History tracking ─────────────────────────────────────────────────────
    history_rows: List[dict] = []
    n_evals = [0]
    n_iters = [0]
    patience_counter = [0]
    best_obj = [np.inf]
    best_params_box = [{}]
    total_t0 = time.monotonic()

    def _log_iter(iter_num: int, x: np.ndarray, obj: float, elapsed: float, reason: str = "") -> None:
        params_str = "  ".join(f"{name}={v:.1f}" for name, v in zip(group_names, x))
        logger.info(
            "iter %3d | obj=%9.6f | %s | %.0fs%s",
            iter_num, obj, params_str, elapsed, f" [{reason}]" if reason else "",
        )

    def _record_history(iter_num: int, x: np.ndarray, obj: float, slopes: Dict[str, float]) -> None:
        row = {"iter": iter_num, "objective": obj, "n_evals": n_evals[0],
               "elapsed_sec": time.monotonic() - total_t0}
        row.update({f"param_{name}": v for name, v in zip(group_names, x)})
        metric_prefix = evaluator.metric
        row.update({f"{metric_prefix}_{sname}": sv for sname, sv in slopes.items()})
        history_rows.append(row)
        if progress_csv is not None:
            try:
                _write_progress_csv(progress_csv, history_rows)
            except Exception:
                pass  # never crash the optimizer due to I/O

    # ── Initial evaluation ────────────────────────────────────────────────────
    # When --skip-init is used, try to reuse an existing eval_base output
    # (e.g. from a previous --dry-run or an interrupted optimizer run).
    _reused_init = False
    if skip_init and base_scratch.exists():
        pattern = evaluator.model_dss_pattern
        dss_suffix = pattern.replace("{modifier}", "")
        output_dir = base_scratch / "output"
        existing_dss: Optional[Path] = None
        if output_dir.exists():
            for f in output_dir.glob(f"*{dss_suffix}"):
                existing_dss = f
                break
        if existing_dss is not None and existing_dss.exists():
            logger.info("--skip-init: reusing existing eval_base output: %s", existing_dss)
            t0_skip = time.monotonic()
            try:
                metric_df = compute_ec_metric(
                    existing_dss, evaluator.observed_dss,
                    evaluator.locations, evaluator.timewindow,
                    metric=evaluator.metric,
                )
                init_obj = _objective_from_metric(metric_df, evaluator.weights)
                init_slopes = dict(zip(metric_df["station_name"], metric_df["metric_value"].astype(float)))
                init_elapsed = time.monotonic() - t0_skip
                init_result = EvalResult(
                    eval_id="opt_init_reused",
                    params=dict(zip(group_names, x0)),
                    objective=init_obj,
                    slopes=init_slopes,
                    success=True,
                    elapsed_sec=init_elapsed,
                    eval_dir=base_scratch,
                    metric=evaluator.metric,
                )
                _reused_init = True
            except Exception as exc:
                logger.warning("--skip-init: reuse failed (%s) — falling back to full eval.", exc)

    if not _reused_init:
        logger.info("Evaluating starting point …")
        _clear_eval_dir(base_scratch)
        init_result = evaluator.evaluate(
            dict(zip(group_names, x0)), base_scratch, eval_id="opt_init"
        )
        n_evals[0] += 1
    initial_objective = init_result.objective
    best_obj[0] = initial_objective
    best_params_box[0] = dict(zip(group_names, x0))
    _log_iter(0, x0, initial_objective, init_result.elapsed_sec, "init")
    _record_history(0, x0, initial_objective, init_result.slopes)

    # Copy initial to best_dir so best_dir is always valid
    if init_result.success:
        _copy_eval_to_best(base_scratch, best_dir, cfg, yaml_path, best_params_box[0])

    if dry_run:
        logger.info("--dry-run: stopping after initial evaluation.")
        history_df = pd.DataFrame(history_rows)
        opt_yaml = _write_optimized_yaml(cfg, best_params_box[0], best_dir, yaml_path)
        return OptimizationResult(
            best_params=best_params_box[0],
            best_objective=best_obj[0],
            initial_objective=initial_objective,
            n_evals=n_evals[0],
            n_iters=0,
            converged_reason="dry_run",
            history_df=history_df,
            best_dir=best_dir,
            optimized_yaml=opt_yaml,
        )

    # ── scipy.optimize closure ────────────────────────────────────────────────
    converged_reason = ["max_iter"]

    def _f_and_grad(x_s: np.ndarray) -> Tuple[float, np.ndarray]:
        """Evaluate objective + gradient.  Called by L-BFGS-B on every iteration.

        *x_s* is the normalised parameter vector (∈ [0, 1] per component).
        All model evaluations use the denormalised physical values (ft²/s).

        NOTE: patience/improvement tracking is intentionally NOT done here.
        L-BFGS-B calls this function at the gradient-evaluation point x_k which
        is the SAME as the previous iterate, so comparing f(x_k) vs best_obj
        always shows "no improvement" before any step is taken.  Patience is
        tracked in _lbfgsb_callback, which is only called after a successful step.
        """
        # Denormalise: [0, 1] → physical ft²/s
        x = bounds_lo + x_s * _scale

        iter_num = n_iters[0] + 1
        t_iter = time.monotonic()

        # --- Base point evaluation ---
        _clear_eval_dir(base_scratch)
        base_res = evaluator.evaluate(
            dict(zip(group_names, x)), base_scratch, eval_id=f"opt_i{iter_num}_base"
        )
        n_evals[0] += 1
        f_x = base_res.objective

        if n_evals[0] >= max_model_runs:
            converged_reason[0] = "max_model_runs"
            raise StopIteration

        # --- Parallel gradient ---
        for d in pert_scratch_dirs:
            _clear_eval_dir(d)

        grad_phys = parallel_forward_gradient(
            x=x,
            f_x=f_x,
            x0_params=dict(zip(group_names, x)),
            evaluator=evaluator,
            scratch_dirs=pert_scratch_dirs,
            h_rel=h_rel,
            opt_cfg=opt_cfg,
            max_workers=max_workers,
            bounds_lo=bounds_lo,
            bounds_hi=bounds_hi,
            iter_id=f"opt_i{iter_num}",
        )
        n_evals[0] += n_params  # conservative count (some may have failed)

        # Chain-rule: transform gradient from physical (per ft²/s) to
        # normalised space (per unit of [0, 1]) so L-BFGS-B step sizes are
        # physically meaningful (hundreds of ft²/s rather than microunits).
        grad_s = grad_phys * _scale

        elapsed = time.monotonic() - t_iter
        _log_iter(iter_num, x, f_x, elapsed)
        _record_history(iter_num, x, f_x, base_res.slopes)
        n_iters[0] = iter_num

        # Track best — but do NOT touch patience counter here
        if f_x < best_obj[0] - improve_tol:
            best_obj[0] = f_x
            best_params_box[0] = dict(zip(group_names, x))
            if base_res.success:
                _copy_eval_to_best(base_scratch, best_dir, cfg, yaml_path, best_params_box[0])
            logger.info("  ★ new best: obj=%.6f", f_x)

        if n_evals[0] >= max_model_runs:
            converged_reason[0] = "max_model_runs"
            raise StopIteration

        return float(f_x), grad_s

    def _lbfgsb_callback(xk: np.ndarray) -> None:
        """Called by L-BFGS-B once per successful step (after the line search).

        This is the right place for patience tracking — xk is the NEW parameter
        vector after L-BFGS-B accepted a step, not the gradient-evaluation point.
        xk is in normalised [0, 1] space; convert to physical for logging.
        """
        x_phys = bounds_lo + xk * _scale  # normalised → ft²/s for display
        elapsed = time.monotonic() - total_t0
        prev_best = _lbfgsb_callback._prev_best  # type: ignore[attr-defined]
        if best_obj[0] < prev_best - improve_tol:
            _lbfgsb_callback._prev_best = best_obj[0]  # type: ignore[attr-defined]
            patience_counter[0] = 0
            logger.info("  ★ improvement: obj=%.6f  params: %s",
                        best_obj[0],
                        "  ".join(f"{n}={v:.1f}" for n, v in zip(group_names, x_phys)))
        else:
            patience_counter[0] += 1
            logger.info("  no improvement (%d/%d)  obj=%.6f  params: %s",
                        patience_counter[0], patience, best_obj[0],
                        "  ".join(f"{n}={v:.1f}" for n, v in zip(group_names, x_phys)))
            if patience_counter[0] >= patience:
                converged_reason[0] = "no_improve"
                raise StopIteration

    _lbfgsb_callback._prev_best = initial_objective  # type: ignore[attr-defined]

    # ── Nelder-Mead objective (no gradient) ──────────────────────────────────
    # NOTE: scipy calls _f_only for EVERY vertex evaluation, including the
    # N extra probes needed to build the initial simplex.  Patience must NOT
    # be checked here or it fires after the very first simplex initialisation
    # (N non-improving evals = patience exhausted before real iteration begins).
    # Patience tracking is done in _nm_callback, called once per NM iteration.
    _nm_eval_slopes: Dict[str, Dict[str, float]] = {}   # keyed by tuple(x) for callback lookup

    def _f_only(x: np.ndarray) -> float:
        """Gradient-free objective for Nelder-Mead.  One model run per call."""
        t_iter = time.monotonic()

        _clear_eval_dir(base_scratch)
        base_res = evaluator.evaluate(
            dict(zip(group_names, x)), base_scratch, eval_id=f"opt_eval{n_evals[0] + 1}"
        )
        n_evals[0] += 1
        f_x = base_res.objective

        elapsed = time.monotonic() - t_iter
        logger.debug("  eval %d: obj=%.6f (%.0fs)", n_evals[0], f_x, elapsed)
        _record_history(n_evals[0], x, f_x, base_res.slopes)

        # Track best seen (used by callback for best_dir copy)
        if f_x < best_obj[0] - improve_tol:
            best_obj[0] = f_x
            best_params_box[0] = dict(zip(group_names, x))
            if base_res.success:
                _copy_eval_to_best(base_scratch, best_dir, cfg, yaml_path, best_params_box[0])
            logger.info("  ★ new best (eval %d): obj=%.6f", n_evals[0], f_x)

        # Store slopes keyed by param vector for callback to retrieve
        _nm_eval_slopes[tuple(x.tolist())] = base_res.slopes

        if n_evals[0] >= max_model_runs:
            converged_reason[0] = "max_model_runs"
            raise StopIteration

        return float(f_x)

    def _nm_callback(xk: np.ndarray) -> None:
        """Called by scipy once per NM iteration (after a full simplex update).

        This is the right place for patience tracking: each call represents a
        completed iteration, not an individual vertex probe.
        """
        iter_num = n_iters[0] + 1
        n_iters[0] = iter_num
        elapsed = time.monotonic() - total_t0
        _log_iter(iter_num, xk, best_obj[0], elapsed)

        # Patience check: has the best objective improved since last iteration?
        # best_obj[0] is updated inside _f_only whenever a new best is found.
        # We compare against the value at the previous callback invocation.
        prev_best = _nm_callback._prev_best  # type: ignore[attr-defined]
        if best_obj[0] < prev_best - improve_tol:
            _nm_callback._prev_best = best_obj[0]  # type: ignore[attr-defined]
            patience_counter[0] = 0
        else:
            patience_counter[0] += 1
            logger.info("  no improvement (%d/%d)", patience_counter[0], patience)
            if patience_counter[0] >= patience:
                converged_reason[0] = "no_improve"
                raise StopIteration

    _nm_callback._prev_best = initial_objective  # type: ignore[attr-defined]

    # ── Differential Evolution parallel objective ─────────────────────────────
    # diffevol evaluates popsize × N_params individuals per generation, all
    # independent of each other — ideal for parallel DSM2 runs.
    # Each worker gets its own numbered eval dir so runs never clobber each other.
    _de_pool_lock = threading.Lock()
    _de_free_dirs: List[Path] = []    # pool of available eval dirs

    _de_worker_count = [0]   # guarded by _de_pool_lock

    def _de_acquire_dir() -> Path:
        """Get a free eval dir from the pool, creating one if needed."""
        with _de_pool_lock:
            if _de_free_dirs:
                return _de_free_dirs.pop()
            # Allocate a new numbered dir while holding the lock to avoid
            # two concurrent threads computing the same index.
            _de_worker_count[0] += 1
            idx = _de_worker_count[0]
        d = var_dir.parent / f"{eval_prefix}de_worker_{idx}"
        _clear_eval_dir(d)
        return d

    def _de_release_dir(d: Path) -> None:
        """Return an eval dir to the free pool (cleared for next use)."""
        _clear_eval_dir(d)
        with _de_pool_lock:
            _de_free_dirs.append(d)

    def _f_parallel(x: np.ndarray) -> float:
        """Objective for differential_evolution. Each call runs in its own dir."""
        eval_dir = _de_acquire_dir()
        try:
            t_iter = time.monotonic()
            res = evaluator.evaluate(
                dict(zip(group_names, x)), eval_dir, eval_id=f"opt_de{n_evals[0] + 1}"
            )
            n_evals[0] += 1
            f_x = res.objective
            elapsed = time.monotonic() - t_iter
            logger.debug("  de eval %d: obj=%.6f (%.0fs)", n_evals[0], f_x, elapsed)
            _record_history(n_evals[0], x, f_x, res.slopes)

            if f_x < best_obj[0] - improve_tol:
                best_obj[0] = f_x
                best_params_box[0] = dict(zip(group_names, x))
                if res.success:
                    _copy_eval_to_best(eval_dir, best_dir, cfg, yaml_path, best_params_box[0])
                logger.info("  ★ new best (de eval %d): obj=%.6f", n_evals[0], f_x)

            if n_evals[0] >= max_model_runs:
                converged_reason[0] = "max_model_runs"
                raise StopIteration

            return float(f_x)
        finally:
            _de_release_dir(eval_dir)

    def _de_callback(xk: np.ndarray, convergence: float) -> bool:
        """Called by differential_evolution after each generation."""
        iter_num = n_iters[0] + 1
        n_iters[0] = iter_num
        elapsed = time.monotonic() - total_t0
        _log_iter(iter_num, xk, best_obj[0], elapsed,
                  f"convergence={convergence:.4f}")

        prev_best = _de_callback._prev_best  # type: ignore[attr-defined]
        if best_obj[0] < prev_best - improve_tol:
            _de_callback._prev_best = best_obj[0]  # type: ignore[attr-defined]
            patience_counter[0] = 0
        else:
            patience_counter[0] += 1
            logger.info("  no improvement (%d/%d)", patience_counter[0], patience)
            if patience_counter[0] >= patience:
                converged_reason[0] = "no_improve"
                return True  # returning True stops differential_evolution
        return False

    _de_callback._prev_best = initial_objective  # type: ignore[attr-defined]

    # ── Run optimizer ─────────────────────────────────────────────────────────
    if method == "neldermead":
        logger.info(
            "Starting Nelder-Mead optimization: %d params, bounds [%.0f, %.0f], "
            "max_iter=%d, max_model_runs=%d (gradient-free, SEQUENTIAL — one run per eval)",
            n_params, global_lo, global_hi, max_iter, max_model_runs,
        )
        nm_bounds = list(zip(bounds_lo.tolist(), bounds_hi.tolist()))
        try:
            scipy_result = minimize(
                _f_only,
                x0,
                method="Nelder-Mead",
                bounds=nm_bounds,
                callback=_nm_callback,
                options={
                    "maxiter": max_iter * n_params * 2,
                    "xatol": 1.0,          # 1 ft²/s convergence tolerance
                    "fatol": improve_tol * 0.1,
                    "adaptive": True,      # better for high-dim simplices
                },
            )
            if converged_reason[0] == "max_iter":
                converged_reason[0] = scipy_result.message
        except StopIteration:
            pass
    elif method == "diffevol":
        # Differential evolution: each generation evaluates popsize×N individuals
        # in parallel using a ThreadPoolExecutor.  Each member runs in its own
        # eval dir so runs never clobber each other.
        popsize = int(opt_cfg.get("de_popsize", 5))   # population multiplier
        mutation = float(opt_cfg.get("de_mutation", 0.7))
        recombination = float(opt_cfg.get("de_recombination", 0.9))
        de_bounds = list(zip(bounds_lo.tolist(), bounds_hi.tolist()))
        logger.info(
            "Starting Differential Evolution: %d params, bounds [%.0f, %.0f], "
            "popsize=%d (pop=%d), max_iter=%d, max_model_runs=%d, max_workers=%d (PARALLEL)",
            n_params, global_lo, global_hi, popsize, popsize * n_params,
            max_iter, max_model_runs, max_workers,
        )
        try:
            # Use a ThreadPoolExecutor as the workers callable so that
            # differential_evolution evaluates the population in parallel via
            # threads rather than multiprocessing.  The multiprocessing backend
            # (the default when workers is an integer > 1) requires the
            # objective function to be picklable, but _f_parallel is a closure
            # and cannot be pickled.  ThreadPoolExecutor.map is pickling-free
            # and works correctly with closures.
            with ThreadPoolExecutor(max_workers=max_workers) as de_executor:
                scipy_result = differential_evolution(
                    _f_parallel,
                    de_bounds,
                    maxiter=max_iter,
                    popsize=popsize,
                    mutation=mutation,
                    recombination=recombination,
                    init="latinhypercube",
                    seed=42,
                    callback=_de_callback,
                    workers=de_executor.map,   # threads — closures can't be pickled for multiprocessing
                    updating="deferred",       # required when workers is a map-callable
                    tol=improve_tol * 0.1,
                    x0=x0,                     # seed first member with starting point
                )
            if converged_reason[0] == "max_iter":
                converged_reason[0] = scipy_result.message
        except StopIteration:
            pass
        except RuntimeError as exc:
            # PEP 479: StopIteration raised inside a generator (executor.map) is
            # converted to RuntimeError.  This is our own early-stop signal from
            # _f_parallel — treat it the same as a bare StopIteration.
            if exc.__cause__ is not None and isinstance(exc.__cause__, StopIteration):
                pass
            else:
                raise
    elif method == "sweep":
        # ── Coordinate sweep ──────────────────────────────────────────────────
        # For each round, sweep each parameter independently with K parallel
        # DSM2 runs (holding all others at their current best value), pick the
        # best sample, then optionally zoom the search range for the next round.
        # Pure parallel, no gradients, diagnostic — ideal for 1–8 parameters
        # and expensive model evaluations.
        sweep_k: int = int(opt_cfg.get("sweep_points_per_param", max_workers))
        zoom: float = float(opt_cfg.get("sweep_zoom_factor", 0.5))

        sweep_lo = bounds_lo.copy().astype(float)
        sweep_hi = bounds_hi.copy().astype(float)
        current_x = x0.copy().astype(float)
        current_obj = initial_objective

        logger.info(
            "Starting coordinate sweep: %d params, %d points/param/round, "
            "zoom=%.2f, max_iter=%d, max_model_runs=%d, max_workers=%d",
            n_params, sweep_k, zoom, max_iter, max_model_runs, max_workers,
        )

        # Allocate K eval dirs per parameter (reused each round)
        sweep_eval_dirs: List[List[Path]] = [
            [var_dir.parent / f"{eval_prefix}sweep_p{i}_s{k}" for k in range(sweep_k)]
            for i in range(n_params)
        ]
        for dirs in sweep_eval_dirs:
            for d in dirs:
                d.mkdir(parents=True, exist_ok=True)

        try:
            for round_num in range(1, max_iter + 1):
                round_improved = False

                for i, param_name in enumerate(group_names):
                    if n_evals[0] >= max_model_runs:
                        converged_reason[0] = "max_model_runs"
                        raise StopIteration

                    # Sample K points uniformly across current search range,
                    # forcing one sample to exactly equal the current best.
                    sample_vals = np.linspace(sweep_lo[i], sweep_hi[i], sweep_k)
                    nearest_idx = int(np.argmin(np.abs(sample_vals - current_x[i])))
                    sample_vals[nearest_idx] = current_x[i]

                    logger.info(
                        "  sweep round %d param [%d] %s: %d pts in [%.1f, %.1f]",
                        round_num, i, param_name, sweep_k, sweep_lo[i], sweep_hi[i],
                    )

                    for d in sweep_eval_dirs[i]:
                        _clear_eval_dir(d)

                    param_dicts = []
                    for k, sv in enumerate(sample_vals):
                        px = current_x.copy()
                        px[i] = sv
                        param_dicts.append(dict(zip(group_names, px)))

                    sweep_results: Dict[int, EvalResult] = {}
                    with ThreadPoolExecutor(max_workers=max_workers) as executor:
                        futures_map = {
                            executor.submit(
                                evaluator.evaluate,
                                param_dicts[k],
                                sweep_eval_dirs[i][k],
                                f"opt_r{round_num}_p{i}_s{k}",
                            ): k
                            for k in range(sweep_k)
                        }
                        for fut in as_completed(futures_map):
                            k = futures_map[fut]
                            try:
                                sweep_results[k] = fut.result()
                            except Exception as exc:
                                logger.warning("  sweep p%d s%d raised: %s", i, k, exc)
                    n_evals[0] += sweep_k

                    # Log all sample results
                    for k, sv in enumerate(sample_vals):
                        res = sweep_results.get(k)
                        if res and res.success:
                            logger.info(
                                "    s%d  %s=%.1f  obj=%.6f",
                                k, param_name, sv, res.objective,
                            )
                        else:
                            logger.info("    s%d  %s=%.1f  FAILED", k, param_name, sv)

                    # Best successful sample
                    best_k = min(
                        (k for k in sweep_results if sweep_results[k].success),
                        key=lambda k: sweep_results[k].objective,
                        default=None,
                    )
                    if best_k is None:
                        logger.warning("  all samples failed for %s — keeping current value", param_name)
                        continue

                    best_res = sweep_results[best_k]
                    best_val = sample_vals[best_k]

                    if best_res.objective < current_obj - improve_tol:
                        logger.info(
                            "  ★ param %s: %.1f → %.1f  obj %.6f → %.6f",
                            param_name, current_x[i], best_val,
                            current_obj, best_res.objective,
                        )
                        current_x[i] = best_val
                        current_obj = best_res.objective
                        round_improved = True

                        if best_res.objective < best_obj[0] - improve_tol:
                            best_obj[0] = best_res.objective
                            best_params_box[0] = dict(zip(group_names, current_x))
                            _copy_eval_to_best(
                                sweep_eval_dirs[i][best_k], best_dir,
                                cfg, yaml_path, best_params_box[0],
                            )
                    else:
                        logger.info(
                            "  param %s: no improvement (best obj=%.6f, current=%.6f)",
                            param_name, best_res.objective, current_obj,
                        )

                    _record_history(round_num, current_x, current_obj, best_res.slopes)

                    # Zoom: contract the search range around best value
                    if zoom < 1.0:
                        half = (sweep_hi[i] - sweep_lo[i]) * zoom / 2.0
                        sweep_lo[i] = max(bounds_lo[i], best_val - half)
                        sweep_hi[i] = min(bounds_hi[i], best_val + half)

                elapsed = time.monotonic() - total_t0
                _log_iter(round_num, current_x, current_obj, elapsed)
                n_iters[0] = round_num

                if not round_improved:
                    patience_counter[0] += 1
                    logger.info(
                        "  no improvement this round (%d/%d)",
                        patience_counter[0], patience,
                    )
                    if patience_counter[0] >= patience:
                        converged_reason[0] = "no_improve"
                        raise StopIteration
                else:
                    patience_counter[0] = 0

                if n_evals[0] >= max_model_runs:
                    converged_reason[0] = "max_model_runs"
                    raise StopIteration

        except StopIteration:
            pass

    else:
        # L-BFGS-B (default)
        logger.info(
            "Starting L-BFGS-B optimization: %d params, bounds [%.0f, %.0f], "
            "max_iter=%d, max_model_runs=%d, max_workers=%d",
            n_params, global_lo, global_hi, max_iter, max_model_runs, max_workers,
        )
        x0_scaled = (x0 - bounds_lo) / _scale          # [0, 1] starting point
        scipy_bounds_scaled = Bounds(
            lb=np.zeros(n_params), ub=np.ones(n_params)
        )
        try:
            scipy_result = minimize(
                _f_and_grad,
                x0_scaled,
                method="L-BFGS-B",
                jac=True,
                bounds=scipy_bounds_scaled,
                callback=_lbfgsb_callback,
                options={"maxiter": max_iter, "ftol": 1e-12, "gtol": 1e-8},
            )
            if converged_reason[0] == "max_iter":
                converged_reason[0] = scipy_result.message
        except StopIteration:
            pass  # our early-stop signal

    # ── Finalize ──────────────────────────────────────────────────────────────
    history_df = pd.DataFrame(history_rows)
    history_csv = best_dir / "optim_history.csv"
    history_df.to_csv(history_csv, index=False)
    logger.info("Optimization history saved to %s", history_csv)

    opt_yaml = _write_optimized_yaml(cfg, best_params_box[0], best_dir, yaml_path)
    logger.info("Optimized YAML saved to %s", opt_yaml)

    return OptimizationResult(
        best_params=best_params_box[0],
        best_objective=best_obj[0],
        initial_objective=initial_objective,
        n_evals=n_evals[0],
        n_iters=n_iters[0],
        converged_reason=converged_reason[0],
        history_df=history_df,
        best_dir=best_dir,
        optimized_yaml=opt_yaml,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Helper: copy best eval dir artefacts
# ─────────────────────────────────────────────────────────────────────────────

def _copy_eval_to_best(
    eval_dir: Path,
    best_dir: Path,
    cfg: dict,
    yaml_path: Path,
    best_params: Dict[str, float],
) -> None:
    """Copy model output + plots from *eval_dir* into *best_dir*."""
    src_output = eval_dir / "output"
    dst_output = best_dir / "output"
    if src_output.exists():
        if dst_output.exists():
            shutil.rmtree(dst_output)
        shutil.copytree(src_output, dst_output)

    # Write a current-best results.txt
    results_src = eval_dir / "results.txt"
    if results_src.exists():
        shutil.copy2(results_src, best_dir / "results.txt")

    # Generate plots using the best eval dir DSS + original observed DSS
    # Build a temporary YAML pointing at best_dir for plot_from_yaml
    try:
        _generate_best_plots(eval_dir, best_dir, cfg, yaml_path, best_params)
    except Exception as exc:
        logger.warning("Could not generate plots for best result: %s", exc)


def _generate_best_plots(
    eval_dir: Path,
    best_dir: Path,
    cfg: dict,
    yaml_path: Path,
    best_params: Dict[str, float],
) -> None:
    """Write a temporary YAML pointing at eval_dir output and call plot_from_yaml."""
    import tempfile as _tempfile

    cfg_tmp = copy.deepcopy(cfg)
    # Detect the actual modifier from the DSS file written to eval_dir/output/.
    # The evaluate() method names the DSS  <variation_name>_<eval_id>_qual.dss,
    # e.g. confluence_dispersion_opt_init_qual.dss.  Using a hardcoded suffix
    # would look for the wrong filename.
    pattern = cfg_tmp.get("base_run", {}).get("model_dss_pattern", "{modifier}_qual.dss")
    dss_suffix = pattern.replace("{modifier}", "")  # e.g. "_qual.dss"
    output_dir = eval_dir / "output"
    detected_modifier: Optional[str] = None
    if output_dir.exists():
        for f in output_dir.glob(f"*{dss_suffix}"):
            detected_modifier = f.name[: -len(dss_suffix)]
            break
    modifier = detected_modifier or (cfg_tmp["variation"]["name"] + "_best")
    cfg_tmp["variation"]["study_dir"] = str(eval_dir).replace("\\", "/")
    cfg_tmp["variation"]["name"] = modifier

    # Update modification values in temp cfg
    for entry in cfg_tmp["variation"]["channel_modifications"]:
        name = entry.get("name", "")
        if name in best_params:
            entry["value"] = float(best_params[name])

    with _tempfile.NamedTemporaryFile(
        mode="w", suffix=".yml", delete=False, dir=best_dir
    ) as tmp:
        yaml.dump(cfg_tmp, tmp, default_flow_style=False, sort_keys=False, allow_unicode=True)
        tmp_path = Path(tmp.name)

    try:
        plots = plot_from_yaml(tmp_path)
        # Move plots into best_dir/plots/
        dst_plots = best_dir / "plots"
        dst_plots.mkdir(exist_ok=True)
        for p in plots:
            shutil.move(str(p), dst_plots / p.name)
    finally:
        tmp_path.unlink(missing_ok=True)
