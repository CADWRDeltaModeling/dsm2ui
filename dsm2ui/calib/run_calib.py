"""run_calib.py — CLI driver for DSM2 calibration variation runs.

Usage
-----
    python run_calib.py [--config calib_config.yml] [--setup-only] [--run-base] [--metrics-only] [--plot] [--log-file PATH]

Options
-------
--config PATH       Path to the YAML config file (default: calib_config.yml
                    in the same directory as this script).
--setup-only        Create the variation study directory and filtered batch file,
                    then stop.  Does not run the model or compute metrics.
--run-base          Also re-execute the base-run batch file before computing
                    base slopes.  Normally the base run is pre-existing.
--metrics-only      Skip model setup/execution; only recompute metrics from
                    existing output DSS files.
--plot              Generate per-station diagnostic PNGs from existing model
                    output (no model run).  Saves to <var_study_dir>/plots/.
--log-file PATH     Write model stdout/stderr to this file during the run
                    (allows monitoring with `type` while the model runs).
                    Default: <variation_study_dir>/run.log
--log-level LEVEL   Logging level: DEBUG, INFO, WARNING (default: INFO).
"""
from __future__ import annotations

import argparse
import logging
import shutil
import sys
from pathlib import Path

# Allow running from the repo root without installing the package.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from dsm2ui.calib.calib_run import load_yaml_config, run_from_yaml, plot_from_yaml


def _parse_args() -> argparse.Namespace:
    here = Path(__file__).parent
    parser = argparse.ArgumentParser(
        description="Run a DSM2 calibration variation and compute EC slope metrics."
    )
    parser.add_argument(
        "--config",
        default=str(here / "calib_config.yml"),
        help="Path to the YAML config file (default: calib_config.yml next to this script).",
    )
    parser.add_argument(
        "--setup-only",
        action="store_true",
        help="Create the variation study directory and filtered batch file, then stop.",
    )
    parser.add_argument(
        "--run-base",
        action="store_true",
        help="Re-execute the base-run batch file before computing base slopes.",
    )
    parser.add_argument(
        "--metrics-only",
        action="store_true",
        help="Skip model setup/execution; recompute metrics from existing output only.",
    )
    parser.add_argument(
        "--log-file",
        default=None,
        help="File to stream model stdout/stderr to (default: <var_study_dir>/run.log).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: INFO).",
    )
    parser.add_argument(
        "--plot",
        action="store_true",
        help=(
            "Generate per-station diagnostic PNGs from existing model output. "
            "Skips model setup and execution. "
            "Saves plots to <variation_study_dir>/plots/."
        ),
    )
    return parser.parse_args()


def _copy_config(config_path: Path, var_dir: Path, log: logging.Logger) -> None:
    """Copy the YAML config into the variation study directory."""
    dest = var_dir / config_path.name
    if not var_dir.exists():
        log.warning("Variation dir %s does not exist yet — skipping config copy.", var_dir)
        return
    shutil.copy2(config_path, dest)
    log.info("Config saved to study dir: %s", dest)


def main() -> None:
    args = _parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%H:%M:%S",
    )
    log = logging.getLogger("run_calib")

    config_path = Path(args.config)
    if not config_path.exists():
        log.error("Config file not found: %s", config_path)
        sys.exit(1)

    log.info("Loading config: %s", config_path)
    cfg = load_yaml_config(config_path)
    var_name = cfg["variation"]["name"]
    var_dir = cfg["variation"]["study_dir"]
    log.info("Variation name  : %s", var_name)
    log.info("Variation dir   : %s", var_dir)
    log.info("Base modifier   : %s", cfg["base_run"]["modifier"])
    log.info("Metrics window  : %s", cfg.get("metrics", {}).get("timewindow", "(full run)"))
    n_mods = len(cfg["variation"]["channel_modifications"])
    log.info("Channel groups  : %d", n_mods)
    for entry in cfg["variation"]["channel_modifications"]:
        log.info("  %-25s  %s = %.4g", entry.get("name", ""), entry["param"], entry["value"])
    log.info("Run steps       : %s", cfg["variation"].get("run_steps", "all"))

    # ── Plot-only mode ────────────────────────────────────────────────────────
    if args.plot and not args.metrics_only and not args.setup_only:
        log.info("=" * 60)
        log.info("--plot: generating diagnostic plots from existing output …")
        saved = plot_from_yaml(config_path)
        log.info("%d plot(s) written to %s", len(saved), saved[0].parent if saved else "(none)")
        for p in saved:
            log.info("  %s", p)
        return

    # Determine log file path (default: <var_study_dir>/run.log)
    log_file = args.log_file
    if log_file is None and not args.metrics_only and not args.setup_only:
        log_file = str(Path(var_dir) / "run.log")

    run_variation = not args.metrics_only and not args.setup_only

    log.info("=" * 60)
    if args.setup_only:
        log.info("--setup-only: creating variation directory …")
        result = run_from_yaml(config_path, setup_only=True)
        info = result["variation_info"]
        _copy_config(config_path, Path(var_dir), log)
        log.info("Done. To run the model manually:")
        log.info("  cd %s", info["study_dir"])
        log.info("  %s", info["batch_file"])
        return
    elif run_variation:
        log.info("Setting up variation study directory and running model …")
        log.info("Model output log: %s", log_file)
    else:
        log.info("--metrics-only: skipping model setup/run.")

    result = run_from_yaml(
        config_path,
        run_base=args.run_base,
        run_variation=run_variation,
        log_file=log_file,
    )

    run_res = result.get("run_result")
    if run_res is not None:
        if run_res.returncode == 0:
            log.info("DSM2 run finished successfully.")
        else:
            log.error("DSM2 run exited with code %d — check stderr above.", run_res.returncode)

    log.info("=" * 60)
    log.info("EC slope comparison  (base vs %s):", var_name)
    cmp = result["comparison"]
    table_str = cmp.to_string(index=False, float_format="{:.4f}".format)
    print()
    print(table_str)
    print()

    # Simple summary
    improved = (cmp["delta_slope"].abs() > 0.01).sum()
    summary_line = f"Stations with |delta_slope| > 0.01: {improved} / {len(cmp)}"
    log.info(summary_line)

    # Save CSV and results text into the variation study directory
    var_dir_path = Path(var_dir)
    out_csv = var_dir_path / f"slopes_{var_name}.csv"
    cmp.to_csv(out_csv, index=False, float_format="%.6f")
    log.info("CSV saved to: %s", out_csv)

    out_txt = var_dir_path / "results.txt"
    with out_txt.open("w") as fh:
        fh.write(f"EC slope comparison  (base vs {var_name})\n")
        fh.write("=" * 60 + "\n")
        fh.write(table_str + "\n\n")
        fh.write(summary_line + "\n")
    log.info("Results text saved to: %s", out_txt)


if __name__ == "__main__":
    main()
