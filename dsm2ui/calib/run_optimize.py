"""run_optimize.py — CLI driver for DSM2 calibration optimizer.

Usage
-----
    python run_optimize.py --config calib_config.yml [--dry-run] [--log-level INFO]

Options
-------
--config PATH       Path to the YAML config file (default: calib_config.yml
                    next to this script).
--dry-run           Evaluate only the starting point; do not run the optimizer
                    loop.  Useful for verifying setup and initial slopes.
--log-level LEVEL   DEBUG, INFO, WARNING, ERROR (default: INFO).

Output (written to <var_study_dir>_optim_best/)
-------
    optim_history.csv           — objective, params, slopes per iteration
    results.txt                 — best-run slope comparison table
    calib_config_optimized.yml  — ready-to-use warm-start config for next pass
    output/                     — DSS files from best run
    plots/                      — per-station diagnostic PNGs from best run

Monitor while running
---------------------
    powershell -Command "Get-Content '<scratch_dir>\\eval_base\\run.log' -Tail 5"
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from dsm2ui.calib.calib_run import load_yaml_config
from dsm2ui.calib.calib_optimize import optimize, OptimizationResult


def _parse_args() -> argparse.Namespace:
    here = Path(__file__).parent
    parser = argparse.ArgumentParser(
        description="Optimize DSM2 DISPERSION/MANNING values to minimise EC slope deviation."
    )
    parser.add_argument(
        "--config",
        default=str(here / "calib_config.yml"),
        help="Path to the YAML config file.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Evaluate starting point only; skip the optimization loop.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: INFO).",
    )
    return parser.parse_args()


def _print_summary(result: OptimizationResult, group_names: list, station_names: list) -> None:
    """Print a before/after comparison table to stdout."""
    # Parameters table
    print()
    print("=" * 72)
    print("OPTIMIZATION SUMMARY")
    print("=" * 72)
    print(f"  Iterations         : {result.n_iters}")
    print(f"  Model evaluations  : {result.n_evals}")
    print(f"  Converged reason   : {result.converged_reason}")
    print(f"  Initial objective  : {result.initial_objective:.6f}")
    print(f"  Best objective     : {result.best_objective:.6f}")
    improvement_pct = 100 * (result.initial_objective - result.best_objective) / max(result.initial_objective, 1e-12)
    print(f"  Improvement        : {improvement_pct:.1f}%")
    print()

    # Parameter changes
    print("  Parameter changes (DISPERSION ft²/s):")
    print(f"  {'Group':<25}  {'Initial':>10}  {'Optimized':>10}  {'Change':>10}")
    print("  " + "-" * 62)
    hist_df = result.history_df
    for name in group_names:
        col = f"param_{name}"
        initial_val = hist_df[col].iloc[0] if col in hist_df.columns else float("nan")
        best_val = result.best_params.get(name, float("nan"))
        chg = best_val - initial_val
        print(f"  {name:<25}  {initial_val:>10.1f}  {best_val:>10.1f}  {chg:>+10.1f}")
    print()

    # Slope changes
    print("  Slope changes (model/observed — target = 1.000):")
    print(f"  {'Station':<45}  {'Initial':>8}  {'Best':>8}  {'Δ slope':>8}")
    print("  " + "-" * 76)
    for sname in station_names:
        col = f"slope_{sname}"
        if col not in hist_df.columns:
            continue
        initial_s = hist_df[col].iloc[0]
        # Find best slope from the iteration that achieved best objective
        best_iter_df = hist_df.loc[hist_df["objective"] == hist_df["objective"].min()]
        best_s = best_iter_df[col].iloc[-1] if not best_iter_df.empty else float("nan")
        delta = best_s - initial_s
        print(f"  {sname:<45}  {initial_s:>8.4f}  {best_s:>8.4f}  {delta:>+8.4f}")
    print()

    print(f"  Best results dir   : {result.best_dir}")
    print(f"  Optimized YAML     : {result.optimized_yaml}")
    print()
    print("  To continue from best parameters (warm start):")
    print(f"    python run_optimize.py --config {result.optimized_yaml}")
    print("=" * 72)
    print()


def main() -> None:
    args = _parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%H:%M:%S",
    )
    log = logging.getLogger("run_optimize")

    config_path = Path(args.config)
    if not config_path.exists():
        log.error("Config file not found: %s", config_path)
        sys.exit(1)

    cfg = load_yaml_config(config_path)
    var = cfg["variation"]
    opt_cfg = cfg.get("optimizer", {})
    group_names = [m["name"] for m in var["channel_modifications"]]
    station_names = [s.strip() for s in (cfg.get("active_stations") or [])]

    log.info("=" * 60)
    log.info("DSM2 Calibration Optimizer")
    log.info("Config        : %s", config_path)
    log.info("Variation     : %s → %s", var["name"], var["study_dir"])
    log.info("Parameters    : %d groups (%s)", len(group_names), ", ".join(group_names))
    log.info("Stations      : %d", len(station_names))
    log.info("Max runs      : %d", opt_cfg.get("max_model_runs", 100))
    log.info("Max iter      : %d", opt_cfg.get("max_iter", 20))
    log.info("Max workers   : %d", opt_cfg.get("max_workers", 4))
    log.info("Bounds        : %s", opt_cfg.get("bounds", [50, 5000]))
    if args.dry_run:
        log.info("Mode          : --dry-run (single evaluation only)")
    log.info("=" * 60)

    result: OptimizationResult = optimize(config_path, dry_run=args.dry_run)

    _print_summary(result, group_names, station_names)


if __name__ == "__main__":
    main()
