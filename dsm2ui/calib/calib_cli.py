"""calib_cli.py — Click command group for DSM2 calibration.

Registered in ``dsm2ui.cli`` as the ``calib`` sub-group.

Commands
--------
dsm2ui calib run        Run a calibration variation (setup, execute, metrics, plots).
dsm2ui calib optimize   Run the gradient-based DISPERSION/MANNING optimizer.
dsm2ui calib setup      Write a template calib_config.yml to get started.
"""
from __future__ import annotations

import logging
import shutil
import sys
from pathlib import Path

import click

# ---------------------------------------------------------------------------
# Template YAML — written by ``calib init``
# ---------------------------------------------------------------------------

_TEMPLATE_YAML = """\
# DSM2 Calibration Configuration
# --------------------------------
# Edit this file to configure the base run and variation(s) you want to test.
#
# Quick-start commands:
#   dsm2ui calib run      --config calib_config.yml
#   dsm2ui calib optimize --config calib_config.yml [--dry-run]
#
# channel_modifications:
#   Each entry defines a named group of channels with a specific parameter change.
#   - name:     Human-readable label for this group (used in log messages).
#   - param:    MANNING or DISPERSION
#   - channels: A YAML list of integer channel numbers, e.g. [10, 11, 200]
#               OR a Python regex matched against CHAN_NO as text, e.g. "^4[0-9]$"
#   - value:    New float value to assign to every matched channel.
#
# active_stations:
#   List of DSM2 output B-parts to include in metrics.
#   Omit or set to null to use ALL stations in the CSV.
#
# metrics.timewindow:
#   "START - END"  e.g. "01DEC2014 - 30SEP2017"  or  "2014-12-01 - 2017-09-30"
# -----------------------------------------------------------------------

base_run:
  # Directory of the pre-existing (already-run) base study.
  study_dir: /path/to/base/study
  # DSM2MODIFIER used by the base study.
  modifier: base_modifier
  # DSS filename pattern for model output.  {modifier} is substituted at runtime.
  model_dss_pattern: "{modifier}_qual.dss"
  # Batch file name (relative to study_dir).
  batch_file: DSM2_batch.bat

# Path to the DSM2 binary directory (v8.5 or later recommended).
dsm2_bin_dir: /path/to/DSM2-8.5.0-win64/bin

variation:
  # DSM2MODIFIER and output folder name for this variation run.
  name: my_variation
  study_dir: /path/to/variations/my_variation

  # DSM2 modules to run.  Hydro must be re-run when DISPERSION changes.
  run_steps: [hydro, qual]

  # Override ENVVAR values in config.inp for this variation.
  # Useful for restricting the run to the metrics window + 2-month warmup.
  envvar_overrides:
    START_DATE: 01OCT2014       # metrics start minus 2-month Godin warmup
    QUAL_START_DATE: 02OCT2014
    GTM_START_DATE: 02OCT2014
    END_DATE: 30SEP2017
    GTM_END_DATE: 30SEP2017

  channel_modifications:
    # Add one entry per channel group.  Later groups override earlier ones
    # for any overlapping channel IDs.
    - name: group_a
      param: DISPERSION
      channels: [10, 11, 12]    # list of channel numbers
      value: 500.0              # ft2/s
    - name: group_b
      param: DISPERSION
      channels: [20, 21]
      value: 1000.0

# ---- Observed EC data (pyhecdss-readable DSS file) -------------------------
observed_ec_dss: /path/to/observed_data/ec_cal.dss

# ---- EC station mapping CSV -------------------------------------------------
# Required columns: station_name, dsm2_id (model B-part), chan_no, distance
ec_stations_csv: /path/to/location_info/calibration_ec_stations.csv

# ---- Stations to include in metrics ----------------------------------------
# Omit or set to null to use ALL stations in the CSV.
active_stations:
  - RSAC075
  - RSAC081
  - RSAN007

# ---- Metrics settings -------------------------------------------------------
metrics:
  timewindow: "01DEC2014 - 30SEP2017"

# ---- Station weights for optimizer objective --------------------------------
# Unlisted stations default to 1.0.  Stations not in active_stations are ignored.
station_weights:
  RSAN007: 2.0    # increase weight for important western stations
  RSAC081: 1.5
  RSAC075: 1.0

# ---- Optimizer settings (used by: dsm2ui calib optimize) -------------------
optimizer:
  # Hard budget: total model evaluations (base + perturbations).
  max_model_runs: 100
  # L-BFGS-B iteration limit.  Each iteration costs N+1 model runs
  # where N = number of channel_modifications groups.
  max_iter: 20
  # Stop early if no improvement >= no_improve_tol for this many gradient steps.
  no_improve_patience: 5
  no_improve_tol: 0.005
  # Finite-difference step:  h_i = rel_step * |x_i|  (min 10 ft2/s)
  finite_diff_rel_step: 0.05
  # Max concurrent DSM2 processes for parallel gradient perturbations.
  # Safe to set higher than N (extra worker slots simply sit idle).
  max_workers: 8
  # Global [min, max] bounds applied to ALL parameter groups (ft2/s).
  bounds: [50, 5000]
  # Per-group bound overrides (optional).
  # bounds_overrides:
  #   group_a: [200, 3000]
  # Directories for scratch eval runs and best-result output.
  # null = auto-derived from variation.study_dir name.
  scratch_dir: null    # → <var_study_dir>_optim_scratch/
  best_dir: null       # → <var_study_dir>_optim_best/
"""


# ---------------------------------------------------------------------------
# Shared log-level option
# ---------------------------------------------------------------------------

_log_level_option = click.option(
    "--log-level",
    default="INFO",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"], case_sensitive=False),
    show_default=True,
    help="Logging verbosity.",
)

_config_option = click.option(
    "--config",
    default="calib_config.yml",
    show_default=True,
    type=click.Path(dir_okay=False),
    help="Path to the YAML configuration file.",
)


def _setup_logging(log_level: str) -> logging.Logger:
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%H:%M:%S",
    )
    return logging.getLogger("dsm2ui.calib")


# ---------------------------------------------------------------------------
# calib group
# ---------------------------------------------------------------------------

@click.group(name="calib")
def calib():
    """DSM2 calibration: run variations, optimize parameters, or create a config template."""
    pass


# ---------------------------------------------------------------------------
# calib run
# ---------------------------------------------------------------------------

@calib.command(name="run")
@_config_option
@click.option("--setup-only", is_flag=True, help="Create study directory and batch file, then stop.")
@click.option("--run-base", is_flag=True, help="Re-run the base study before computing metrics.")
@click.option("--metrics-only", is_flag=True, help="Recompute metrics from existing output; skip model run.")
@click.option("--plot", is_flag=True, help="Generate per-station diagnostic PNGs from existing output.")
@click.option("--log-file", default=None, type=click.Path(), help="Stream model output to this file (default: <var_dir>/run.log).")
@_log_level_option
def calib_run(config, setup_only, run_base, metrics_only, plot, log_file, log_level):
    """Run a DSM2 calibration variation (setup, execute model, compute EC slope metrics)."""
    log = _setup_logging(log_level)

    from dsm2ui.calib.calib_run import load_yaml_config, run_from_yaml, plot_from_yaml

    config_path = Path(config)
    if not config_path.exists():
        log.error("Config file not found: %s", config_path)
        sys.exit(1)

    cfg = load_yaml_config(config_path)
    var_dir = Path(cfg["variation"]["study_dir"])

    # -- Plot-only mode
    if plot and not metrics_only and not setup_only:
        log.info("--plot: generating diagnostic plots from existing output …")
        saved = plot_from_yaml(config_path)
        log.info("%d plot(s) written to %s", len(saved), saved[0].parent if saved else "(none)")
        return

    if log_file is None and not metrics_only and not setup_only:
        log_file = str(var_dir / "run.log")

    if setup_only:
        log.info("--setup-only: creating variation directory …")
        result = run_from_yaml(config_path, setup_only=True)
        info = result["variation_info"]
        _copy_config(config_path, var_dir, log)
        log.info("Done.  To run the model manually:")
        log.info("  cd %s && %s", info["study_dir"], info["batch_file"])
        return

    result = run_from_yaml(
        config_path,
        run_base=run_base,
        run_variation=not metrics_only,
        log_file=log_file,
    )
    if not metrics_only:
        _copy_config(config_path, var_dir, log)

    if result.get("slopes_df") is not None:
        df = result["slopes_df"]
        log.info("\n%s", df.to_string(index=False))


def _copy_config(config_path: Path, var_dir: Path, log: logging.Logger) -> None:
    dest = var_dir / config_path.name
    if not var_dir.exists():
        log.warning("Variation dir %s does not exist — skipping config copy.", var_dir)
        return
    shutil.copy2(config_path, dest)
    log.info("Config saved to study dir: %s", dest)


# ---------------------------------------------------------------------------
# calib optimize
# ---------------------------------------------------------------------------

@calib.command(name="optimize")
@_config_option
@click.option("--dry-run", is_flag=True, help="Evaluate starting point only; skip the optimization loop.")
@_log_level_option
def calib_optimize(config, dry_run, log_level):
    """Optimize DSM2 DISPERSION/MANNING values to minimise EC slope deviation from 1.0."""
    log = _setup_logging(log_level)

    from dsm2ui.calib.calib_run import load_yaml_config
    from dsm2ui.calib.calib_optimize import optimize, OptimizationResult

    config_path = Path(config)
    if not config_path.exists():
        log.error("Config file not found: %s", config_path)
        sys.exit(1)

    cfg = load_yaml_config(config_path)
    var = cfg["variation"]
    opt_cfg = cfg.get("optimizer", {})
    group_names = [m["name"] for m in var["channel_modifications"]]
    station_names = list(cfg.get("active_stations") or [])

    log.info("=" * 60)
    log.info("DSM2 Calibration Optimizer")
    log.info("Config      : %s", config_path)
    log.info("Variation   : %s → %s", var["name"], var["study_dir"])
    log.info("Parameters  : %d groups (%s)", len(group_names), ", ".join(group_names))
    log.info("Stations    : %d", len(station_names))
    log.info("Max runs    : %d", opt_cfg.get("max_model_runs", 100))
    log.info("Max iter    : %d", opt_cfg.get("max_iter", 20))
    log.info("Max workers : %d", opt_cfg.get("max_workers", 4))
    log.info("Bounds      : %s", opt_cfg.get("bounds", [50, 5000]))
    log.info("Mode        : %s", "--dry-run" if dry_run else "full optimization")
    log.info("=" * 60)

    result: OptimizationResult = optimize(config_path, dry_run=dry_run)

    _print_optimize_summary(result, group_names, station_names)


def _print_optimize_summary(result, group_names: list, station_names: list) -> None:
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

    hist_df = result.history_df
    print("  Parameter changes (ft\u00b2/s):")
    print(f"  {'Group':<25}  {'Initial':>10}  {'Optimized':>10}  {'Change':>10}")
    print("  " + "-" * 62)
    for name in group_names:
        col = f"param_{name}"
        initial_val = hist_df[col].iloc[0] if col in hist_df.columns else float("nan")
        best_val = result.best_params.get(name, float("nan"))
        print(f"  {name:<25}  {initial_val:>10.1f}  {best_val:>10.1f}  {best_val - initial_val:>+10.1f}")
    print()

    print("  Slope changes (target = 1.000):")
    print(f"  {'Station':<45}  {'Initial':>8}  {'Best':>8}  {'Delta':>8}")
    print("  " + "-" * 76)
    for sname in station_names:
        col = f"slope_{sname}"
        if col not in hist_df.columns:
            continue
        initial_s = hist_df[col].iloc[0]
        best_iter = hist_df.loc[hist_df["objective"] == hist_df["objective"].min()]
        best_s = best_iter[col].iloc[-1] if not best_iter.empty else float("nan")
        print(f"  {sname:<45}  {initial_s:>8.4f}  {best_s:>8.4f}  {best_s - initial_s:>+8.4f}")
    print()
    print(f"  Best results dir : {result.best_dir}")
    print(f"  Optimized YAML   : {result.optimized_yaml}")
    print()
    print("  Warm-start next pass:")
    print(f"    dsm2ui calib optimize --config {result.optimized_yaml}")
    print("=" * 72)


# ---------------------------------------------------------------------------
# calib init
# ---------------------------------------------------------------------------

@calib.command(name="setup")
@click.option(
    "--output",
    "-o",
    default="calib_config.yml",
    show_default=True,
    type=click.Path(dir_okay=False),
    help="Destination file path for the template config.",
)
@click.option("--force", is_flag=True, help="Overwrite an existing file.")
def calib_setup(output, force):
    """Write a template calib_config.yml to get started."""
    dest = Path(output)
    if dest.exists() and not force:
        click.echo(f"File already exists: {dest}")
        click.echo("Use --force to overwrite.")
        sys.exit(1)
    dest.write_text(_TEMPLATE_YAML, encoding="utf-8")
    click.echo(f"Template config written to: {dest}")
    click.echo("Edit the paths and channel groups, then run:")
    click.echo(f"  dsm2ui calib run      --config {dest}")
    click.echo(f"  dsm2ui calib optimize --config {dest}")

