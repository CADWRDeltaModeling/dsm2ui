"""calib_cli.py — Click command group for DSM2 calibration.

Registered in ``dsm2ui.cli`` as the ``calib`` sub-group.

Commands
--------
dsm2ui calib run        Run a calibration variation (setup, execute, metrics, plots).
dsm2ui calib optimize   Run the gradient-based DISPERSION/MANNING optimizer.
dsm2ui calib cascade    Run a downstream-to-upstream cascading optimizer sequence.
dsm2ui calib setup      Write a template calib_config.yml to get started.
"""
from __future__ import annotations

import logging
import shutil
import sys
from pathlib import Path

import click
from dsm2ui._logging import setup_logging

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
    return setup_logging(log_level)


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
    var_name = cfg["variation"]["name"]
    var_dir = Path(cfg["variation"]["study_dir"])

    log.info("Loading config  : %s", config_path)
    log.info("Variation name  : %s", var_name)
    log.info("Variation dir   : %s", var_dir)
    log.info("Base modifier   : %s", cfg["base_run"]["modifier"])
    log.info("Metrics window  : %s", cfg.get("metrics", {}).get("timewindow", "(full run)"))
    n_mods = len(cfg["variation"]["channel_modifications"])
    log.info("Channel groups  : %d", n_mods)
    for entry in cfg["variation"]["channel_modifications"]:
        log.info("  %-25s  %s = %.4g", entry.get("name", ""), entry["param"], entry["value"])
    log.info("Run steps       : %s", cfg["variation"].get("run_steps", "all"))

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

    _save_and_print_results(result, cfg, var_dir, log)


def _copy_config(config_path: Path, var_dir: Path, log: logging.Logger) -> None:
    dest = var_dir / config_path.name
    if not var_dir.exists():
        log.warning("Variation dir %s does not exist — skipping config copy.", var_dir)
        return
    shutil.copy2(config_path, dest)
    log.info("Config saved to study dir: %s", dest)


def _save_and_print_results(result: dict, cfg: dict, var_dir: Path, log: logging.Logger) -> None:
    """Print slope comparison table to console and write results.txt + CSV to var_dir."""
    run_res = result.get("run_result")
    if run_res is not None:
        if run_res.returncode == 0:
            log.info("DSM2 run finished successfully.")
        else:
            log.error("DSM2 run exited with code %d — check run.log for details.", run_res.returncode)

    cmp = result.get("comparison")
    if cmp is None:
        return

    var_name = cfg["variation"]["name"]
    table_str = cmp.to_string(index=False, float_format="{:.4f}".format)
    log.info("EC slope comparison  (base vs %s):", var_name)
    print()
    print(table_str)
    print()

    improved = (cmp["delta_slope"].abs() > 0.01).sum()
    summary_line = f"Stations with |delta_slope| > 0.01: {improved} / {len(cmp)}"
    log.info(summary_line)

    out_csv = var_dir / f"slopes_{var_name}.csv"
    cmp.to_csv(out_csv, index=False, float_format="%.6f")
    log.info("CSV saved to: %s", out_csv)

    out_txt = var_dir / "results.txt"
    with out_txt.open("w") as fh:
        fh.write(f"EC slope comparison  (base vs {var_name})\n")
        fh.write("=" * 60 + "\n")
        fh.write(table_str + "\n\n")
        fh.write(summary_line + "\n")
    log.info("Results saved to: %s", out_txt)


# ---------------------------------------------------------------------------
# calib optimize
# ---------------------------------------------------------------------------

@calib.command(name="optimize")
@_config_option
@click.option("--dry-run", is_flag=True, help="Evaluate starting point only; skip the optimization loop.")
@click.option("--skip-init", is_flag=True, help="Reuse existing eval_base output instead of re-running the starting point (e.g. after --dry-run or a crash).")
@_log_level_option
def calib_optimize(config, dry_run, skip_init, log_level):
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
    log.info("Mode        : %s", "--dry-run" if dry_run else ("--skip-init + optimize" if skip_init else "full optimization"))
    log.info("=" * 60)

    result: OptimizationResult = optimize(config_path, dry_run=dry_run, skip_init=skip_init)

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
# calib cascade
# ---------------------------------------------------------------------------

@calib.command(name="cascade")
@click.option(
    "--config", "-c",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Path to the cascade meta-config YAML (calib_meta_*.yml).",
)
@click.option("--resume", is_flag=True,
              help="Skip completed stages found in cascade_checkpoint.yml.")
@click.option("--dry-run", is_flag=True,
              help="Evaluate starting point only in each stage; no optimisation loop.")
@click.option("--skip-init", is_flag=True,
              help="Reuse existing eval_base DSS output for each stage's starting point.")
@_log_level_option
def calib_cascade(config, resume, dry_run, skip_init, log_level):
    """Run a downstream-to-upstream cascading optimisation sequence.

    Each stage frees only the channel groups closest to the target station(s)
    for that stage, freezing all other groups at the best values from the
    previous stage.  Results are checkpointed after every stage; use --resume
    to continue an interrupted run.
    """
    log = _setup_logging(log_level)

    from dsm2ui.calib.calib_cascade import load_cascade_config, run_cascade

    meta_path = Path(config)
    cascade_cfg, base_cfg, base_config_path = load_cascade_config(meta_path)

    stages = cascade_cfg.get("stages", [])
    output_dir = cascade_cfg.get(
        "output_dir",
        str(meta_path.parent / (meta_path.stem + "_output")),
    )

    log.info("=" * 60)
    log.info("DSM2 Cascade Optimizer")
    log.info("Meta config  : %s", meta_path)
    log.info("Base config  : %s", base_config_path)
    log.info("Output dir   : %s", output_dir)
    log.info("Stages       : %d", len(stages))
    for s in stages:
        log.info(
            "  [%d] %-30s  params=%s  targets=%s",
            s["id"], s["label"], s["active_params"], s["target_stations"],
        )
    log.info(
        "Mode         : %s%s%s",
        "--dry-run " if dry_run else "",
        "--resume " if resume else "",
        "--skip-init" if skip_init else "full",
    )
    log.info("=" * 60)

    result = run_cascade(
        meta_path,
        resume=resume,
        dry_run=dry_run,
        skip_init=skip_init,
    )

    print()
    print("=" * 72)
    print("CASCADE SUMMARY")
    print("=" * 72)
    print(f"  {'Stage':<5}  {'Label':<30}  {'Obj-init':>10}  {'Obj-best':>10}  "
          f"{'Evals':>6}  {'Elapsed':>8}")
    print("  " + "-" * 68)
    for row in result.stages:
        print(
            f"  {row['id']:<5}  {row['label']:<30}  "
            f"{row['initial_objective']:>10.6f}  {row['best_objective']:>10.6f}  "
            f"{row['n_evals']:>6}  {row['elapsed_sec']:>7.0f}s"
        )
    print()
    print("  Final parameter values:")
    for name, val in result.final_params.items():
        print(f"    {name:<25} = {val:.1f}")
    print()
    print(f"  Summary CSV : {result.output_dir / 'cascade_summary.csv'}")
    print(f"  Checkpoint  : {result.output_dir / 'cascade_checkpoint.yml'}")
    print("=" * 72)


# ---------------------------------------------------------------------------
# calib stations-csv
# ---------------------------------------------------------------------------

@calib.command(name="stations-csv")
@click.argument(
    "stations_csv",
    type=click.Path(exists=True, dir_okay=False, readable=True),
)
@click.argument(
    "centerlines_geojson",
    type=click.Path(exists=True, dir_okay=False, readable=True),
)
@click.argument(
    "output_csv",
    type=click.Path(dir_okay=False),
)
@click.option(
    "--distance-tolerance",
    type=click.INT,
    default=100,
    show_default=True,
    help="Maximum distance (ft) from a channel centerline for a station to be considered matched.",
)
@click.option(
    "--unmatched",
    "unmatched_csv",
    type=click.Path(dir_okay=False),
    default=None,
    help="Path for unmatched-stations CSV (default: <output>_unmatched.csv).",
)
def calib_stations_csv(stations_csv, centerlines_geojson, output_csv, distance_tolerance, unmatched_csv):
    """Build calibration_ec_stations.csv from a datastore stations CSV.

    STATIONS_CSV is the enriched CSV produced by 'dsm2ui datastore extract --stations'.
    Stations that cannot be snapped to a DSM2 channel are written to a separate
    unmatched CSV for review.

    DSM2 output names (dsm2_id) are written in uppercase.
    """
    from dsm2ui.calib.calib_stations import build_calib_stations_csv
    build_calib_stations_csv(
        stations_csv=stations_csv,
        centerlines_file=centerlines_geojson,
        output_csv=output_csv,
        unmatched_csv=unmatched_csv,
        distance_tolerance=distance_tolerance,
    )


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


# ---------------------------------------------------------------------------
# calib checklist
# ---------------------------------------------------------------------------

@calib.command(name="checklist")
@click.argument(
    "process_name",
    type=click.Choice(["resample", "extract", "plot"], case_sensitive=False),
    default="",
)
@click.argument("json_config_file")
def calib_checklist(process_name, json_config_file):
    """Run a DSM2 calibration checklist step (resample, extract, or plot)."""
    from dsm2ui.calib import checklist_dsm2
    checklist_dsm2.run_checklist(process_name, json_config_file)


# ---------------------------------------------------------------------------
# calib postpro group
# ---------------------------------------------------------------------------

@click.group(name="postpro")
def calib_postpro():
    """Post-process DSM2 calibration output (observed, model, plots, heatmaps, etc.)."""
    pass


@calib_postpro.command(name="run")
@click.argument(
    "process_name",
    type=click.Choice(
        [
            "observed",
            "model",
            "plots",
            "heatmaps",
            "validation_bar_charts",
            "copy_plot_files",
        ],
        case_sensitive=False,
    ),
    default="",
)
@click.argument("json_config_file")
@click.option("--dask/--no-dask", default=False, hidden=True)
@click.option(
    "--skip-cached",
    is_flag=True,
    default=False,
    help="Use existing post-processing cache instead of clearing and recomputing (applies to model and plots).",
)
@click.option(
    "--workers",
    default=1,
    show_default=True,
    type=click.INT,
    help="Number of parallel worker processes for the 'plots' step.",
)
def calib_postpro_run(process_name, json_config_file, dask, skip_cached, workers):
    """Run a DSM2 post-processing step (observed, model, plots, heatmaps, validation_bar_charts, or copy_plot_files)."""
    setup_logging()
    from dsm2ui.calib import postpro_dsm2
    postpro_dsm2.run_process(process_name, json_config_file, dask, skip_if_cached=skip_cached, n_workers=workers)


@calib_postpro.command(name="setup")
@click.option(
    "--study",
    "-s",
    "study_folders",
    multiple=True,
    required=True,
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    help="Study folder path (repeat -s for multiple studies).",
)
@click.option(
    "--postprocessing",
    "-p",
    required=False,
    default=None,
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    help="Postprocessing folder (contains location_info/ and observed_data/). "
         "If omitted, bundled default location CSVs are used and observed DSS paths "
         "must be provided via --observed-file.",
)
@click.option(
    "--output",
    "-o",
    required=True,
    type=click.Path(dir_okay=False),
    help="Output YAML config file path.",
)
@click.option(
    "--module",
    "-m",
    type=click.Choice(["hydro", "qual", "gtm"], case_sensitive=False),
    default="hydro",
    show_default=True,
    help="DSM2 module whose DSS output to reference.",
)
@click.option(
    "--output-folder",
    default="./plots/",
    show_default=True,
    help="Plot output folder written into the YAML options_dict.",
)
@click.option(
    "--timewindow",
    default=None,
    help="Override the simulation time window (e.g. \"01OCT2020 - 30SEP2022\"). "
         "Replaces all named time windows in the generated YAML.",
)
@click.option(
    "--location-file",
    "location_files",
    multiple=True,
    help="Override a vartype location CSV as VARTYPE=/path (e.g. EC=/path/ec.csv). "
         "Repeat for multiple vartypes.",
)
@click.option(
    "--observed-file",
    "observed_files",
    multiple=True,
    help="Override a vartype observed DSS path as VARTYPE=/path (e.g. EC=/path/ec.dss). "
         "Repeat for multiple vartypes.",
)
def calib_postpro_setup(
    study_folders, postprocessing, output, module, output_folder,
    timewindow, location_files, observed_files,
):
    """Generate a calib-ui YAML config from study folders and postprocessing data.

    When --postprocessing is omitted, bundled default station CSVs are used for
    location_files_dict.  Observed DSS paths must then be provided via
    --observed-file if post-processing is needed.
    """
    from dsm2ui.calib import calib_config_builder

    # Parse KEY=VALUE overrides for --location-file and --observed-file.
    def _parse_kv(items):
        result = {}
        for item in items:
            if "=" in item:
                k, v = item.split("=", 1)
                result[k.strip().upper()] = v.strip()
            else:
                raise click.BadParameter(
                    f"Expected VARTYPE=/path, got: {item!r}"
                )
        return result

    loc_overrides = _parse_kv(location_files) if location_files else None
    obs_overrides = _parse_kv(observed_files) if observed_files else None

    tw_override = None
    if timewindow:
        tw_override = {
            "simulation_period": timewindow,
            "hydro_calibration": timewindow,
            "qual_calibration": timewindow,
            "hydro_validation": timewindow,
            "qual_validation": timewindow,
        }

    result = calib_config_builder.build_calib_config(
        study_folders=list(study_folders),
        postprocessing_folder=postprocessing,
        output_file=output,
        module=module,
        output_folder=output_folder,
        observed_files=obs_overrides,
        location_files=loc_overrides,
        timewindow_dict=tw_override,
    )
    click.echo(f"Config written to: {result}")


@calib_postpro.command(name="setup-from-datastore")
@click.option(
    "--study",
    "-s",
    "study_folders",
    multiple=True,
    required=True,
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    help="Study folder path (repeat -s for multiple studies).",
)
@click.option(
    "--datastore",
    "-d",
    required=True,
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    help="Path to the DMS Datastore directory (must contain inventory_datasets_*.csv).",
)
@click.option(
    "--output",
    "-o",
    required=True,
    type=click.Path(dir_okay=False),
    help="Output YAML config file path.",
)
@click.option(
    "--dss-dir",
    default=None,
    type=click.Path(dir_okay=True),
    help="Directory for extracted observed DSS files. Defaults to same directory as --output.",
)
@click.option(
    "--module",
    "-m",
    type=click.Choice(["hydro", "qual", "gtm"], case_sensitive=False),
    default="qual",
    show_default=True,
    help="DSM2 module whose DSS output to reference.",
)
@click.option(
    "--vartype",
    "vartypes",
    multiple=True,
    default=("EC",),
    show_default=True,
    help="Vartype to extract from the datastore (repeat for multiple, e.g. --vartype EC --vartype FLOW).",
)
@click.option(
    "--repo-level",
    default="screened",
    show_default=True,
    type=click.Choice(["screened", "raw"], case_sensitive=False),
    help="Datastore repository level.",
)
@click.option(
    "--output-folder",
    default="./plots/",
    show_default=True,
    help="Plot output folder written into the YAML options_dict.",
)
@click.option(
    "--timewindow",
    default=None,
    help="Override the simulation time window (e.g. \"01OCT2020 - 30SEP2022\").",
)
def calib_postpro_setup_from_datastore(
    study_folders, datastore, output, dss_dir, module, vartypes,
    repo_level, output_folder, timewindow,
):
    """Generate a calib-ui YAML config by extracting observed data from a DMS Datastore.

    Extracts one DSS file per requested vartype from the DMS Datastore, then
    builds a postpro_config.yml that references those files as observed_files_dict.
    Location station CSVs use the bundled defaults (no --postprocessing folder needed).

    Example:

    \b
        dsm2ui calib postpro setup-from-datastore \\
            -s D:/delta/dsm2_studies/studies/historical \\
            -d D:/delta/dms_datastore \\
            -o postpro_config.yml \\
            --vartype EC

    The extracted DSS files are written next to the output YAML unless --dss-dir
    is specified.
    """
    from dsm2ui.calib import calib_config_builder

    output_path = Path(output)
    if dss_dir is None:
        dss_dir = str(output_path.parent)

    click.echo(f"Extracting observed data from datastore: {datastore}")
    click.echo(f"  Vartypes  : {', '.join(vartypes)}")
    click.echo(f"  Repo level: {repo_level}")
    click.echo(f"  DSS output: {dss_dir}")

    observed_paths = calib_config_builder.extract_observed_from_datastore(
        datastore_dir=datastore,
        output_dir=dss_dir,
        vartypes=list(vartypes),
        repo_level=repo_level,
    )

    tw_override = None
    if timewindow:
        tw_override = {
            "simulation_period": timewindow,
            "hydro_calibration": timewindow,
            "qual_calibration": timewindow,
            "hydro_validation": timewindow,
            "qual_validation": timewindow,
        }

    result = calib_config_builder.build_calib_config(
        study_folders=list(study_folders),
        postprocessing_folder=None,   # use bundled default location CSVs
        output_file=output,
        module=module,
        output_folder=output_folder,
        observed_files=observed_paths,
        timewindow_dict=tw_override,
    )

    click.echo("")
    click.echo("Extracted observed DSS files:")
    for vt, path in observed_paths.items():
        click.echo(f"  {vt}: {path}")
    click.echo(f"\nConfig written to: {result}")
    click.echo("\nNext steps:")
    click.echo(f"  dsm2ui calib postpro run model   --config {result}")
    click.echo(f"  dsm2ui calib postpro run observed --config {result}")
    click.echo(f"  dsm2ui calib-ui {result}")


calib.add_command(calib_postpro)


# ---------------------------------------------------------------------------
# calib ui group
# ---------------------------------------------------------------------------

@click.group(name="ui")
def calib_ui():
    """Launch interactive calibration UI viewers."""
    pass


@calib_ui.command(name="plot")
@click.argument("config_file", type=click.Path(exists=True, readable=True))
@click.option("--base_dir", required=False, help="Base directory for config file")
@click.option(
    "--clear-cache",
    is_flag=True,
    default=False,
    help="Clear all post-processing caches before launching the UI.",
)
@click.option(
    "--vartype",
    "vartypes",
    multiple=True,
    help="Restrict active vartypes (repeat for multiple, e.g. --vartype EC --vartype FLOW).",
)
@click.option(
    "--option",
    "options",
    multiple=True,
    help="Override an options_dict entry as KEY=VALUE (e.g. --option write_html=false).",
)
def calib_ui_plot(config_file, base_dir=None, clear_cache=False, vartypes=(), options=()):
    """Launch the interactive calibration plot viewer."""
    from dsm2ui.calib.calibplotui import calib_plot_ui
    calib_plot_ui.callback(
        config_file=config_file,
        base_dir=base_dir,
        clear_cache=clear_cache,
        vartypes=vartypes,
        options=options,
    )


@calib_ui.command(name="heatmap")
@click.argument("summary_file", type=click.Path(exists=True, readable=True))
@click.argument("station_location_file", type=click.Path(exists=True, readable=True))
@click.option("--metric", default="NMSE", help="Name of metric column.", show_default=True)
def calib_ui_heatmap(summary_file, station_location_file, metric):
    """Show a geographic heatmap of calibration metrics."""
    from dsm2ui.calib.geoheatmap import show_metrics_geo_heatmap
    show_metrics_geo_heatmap.callback(summary_file=summary_file, station_location_file=station_location_file, metric=metric)


calib.add_command(calib_ui)

