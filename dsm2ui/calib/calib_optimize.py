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

    python run_optimize.py --config dsm2ui/calib/calib_config.yml
    python run_optimize.py --config dsm2ui/calib/calib_config.yml --dry-run
"""
from __future__ import annotations

import copy
import csv
import logging
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import yaml
from scipy.optimize import minimize, Bounds

from dsm2ui.calib.calib_run import (
    ChannelParamModification,
    ECLocation,
    _cfg_to_modifications,
    compute_ec_slopes,
    load_yaml_config,
    plot_from_yaml,
    read_ec_locations_csv,
    run_study,
    setup_variation,
)

logger = logging.getLogger(__name__)

_PENALTY_PER_STATION = 1.0   # added to objective when a station slope is missing


# ─────────────────────────────────────────────────────────────────────────────
# Data classes
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class EvalResult:
    """Result of a single DSM2 model evaluation."""
    eval_id: str
    params: Dict[str, float]          # group_name → value
    objective: float
    slopes: Dict[str, float]          # station_name → slope (NaN if missing)
    success: bool
    elapsed_sec: float
    eval_dir: Path = field(default=None)


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


def _objective_from_slopes(
    slopes_df: pd.DataFrame,
    weights: Dict[str, float],
) -> float:
    """Compute weighted sum of squared (slope - 1)².

    Missing / NaN slopes are skipped; if ALL slopes are missing, returns a
    large penalty so the optimizer is pushed away from that region.
    """
    total = 0.0
    n_valid = 0
    for _, row in slopes_df.iterrows():
        s = row.get("slope", float("nan"))
        if pd.isna(s):
            continue
        w = weights.get(row["station_name"], 1.0)
        total += w * (s - 1.0) ** 2
        n_valid += 1
    if n_valid == 0:
        return float(len(weights)) * _PENALTY_PER_STATION * 4.0
    return total


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
        self.channel_inp_source = base.get(
            "channel_inp_source",
            str(base_dir.parent.parent / "common_input" / "channel_std_delta_grid.inp"),
        )
        self.channel_inp_name = base.get("channel_inp_name", "channel_std_delta_grid.inp")
        self.run_steps = cfg["variation"].get("run_steps")
        self.dsm2_bin_dir = cfg.get("dsm2_bin_dir")
        self.envvar_overrides = cfg["variation"].get("envvar_overrides")

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

        log_file = eval_dir / "run.log"
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
        slopes_df = compute_ec_slopes(
            var_dss, self.observed_dss, self.locations, self.timewindow
        )

        obj = _objective_from_slopes(slopes_df, self.weights)
        slopes_dict = dict(zip(slopes_df["station_name"], slopes_df["slope"].astype(float)))
        elapsed = time.monotonic() - t0

        return EvalResult(
            eval_id=eval_id,
            params=params,
            objective=obj,
            slopes=slopes_dict,
            success=True,
            elapsed_sec=elapsed,
            eval_dir=eval_dir,
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
                    logger.debug(
                        "  grad[%d] (%s) h=%.1f f_pert=%.6f → grad=%.6f",
                        i, evaluator.group_names[i], h, res.objective, grad[i],
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


def _clear_eval_dir(d: Path) -> None:
    """Remove all artefacts from a scratch eval dir, keeping the dir itself."""
    if d.exists():
        shutil.rmtree(d)
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
) -> OptimizationResult:
    """Run the L-BFGS-B optimizer against the YAML calibration config.

    Parameters
    ----------
    yaml_path :
        Path to ``calib_config.yml``.
    dry_run :
        If ``True``, evaluate only the starting point and return without
        running the optimization loop.

    Returns
    -------
    :class:`OptimizationResult`
    """
    yaml_path = Path(yaml_path).resolve()
    cfg = load_yaml_config(yaml_path)

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
    scipy_bounds = Bounds(lb=bounds_lo, ub=bounds_hi)

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
        row.update({f"slope_{sname}": sv for sname, sv in slopes.items()})
        history_rows.append(row)

    # ── Initial evaluation ────────────────────────────────────────────────────
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

    def _f_and_grad(x: np.ndarray) -> Tuple[float, np.ndarray]:
        """Evaluate objective + gradient.  Called by L-BFGS-B on every iteration."""
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

        grad = parallel_forward_gradient(
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

        elapsed = time.monotonic() - t_iter
        _log_iter(iter_num, x, f_x, elapsed)
        _record_history(iter_num, x, f_x, base_res.slopes)
        n_iters[0] = iter_num

        # --- Improvement check ---
        if f_x < best_obj[0] - improve_tol:
            best_obj[0] = f_x
            best_params_box[0] = dict(zip(group_names, x))
            patience_counter[0] = 0
            if base_res.success:
                _copy_eval_to_best(base_scratch, best_dir, cfg, yaml_path, best_params_box[0])
            logger.info("  ★ new best: obj=%.6f", f_x)
        else:
            patience_counter[0] += 1
            logger.info("  no improvement (%d/%d)", patience_counter[0], patience)
            if patience_counter[0] >= patience:
                converged_reason[0] = "no_improve"
                raise StopIteration

        if n_evals[0] >= max_model_runs:
            converged_reason[0] = "max_model_runs"
            raise StopIteration

        return float(f_x), grad

    # ── Run L-BFGS-B ─────────────────────────────────────────────────────────
    logger.info(
        "Starting L-BFGS-B optimization: %d params, bounds [%.0f, %.0f], "
        "max_iter=%d, max_model_runs=%d, max_workers=%d",
        n_params, global_lo, global_hi, max_iter, max_model_runs, max_workers,
    )
    try:
        scipy_result = minimize(
            _f_and_grad,
            x0,
            method="L-BFGS-B",
            jac=True,
            bounds=scipy_bounds,
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
    # Point variation at eval_dir so plot_from_yaml finds the DSS files there
    modifier = cfg_tmp["variation"]["name"] + "_" + "best"
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
