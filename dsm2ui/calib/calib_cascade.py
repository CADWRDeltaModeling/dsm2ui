# calib_cascade.py — Downstream-to-upstream sequential (cascading) calibration.
"""
Orchestrates a sequence of single-stage :func:`optimize` calls where each stage:

* frees only the channel groups that influence the *target station(s)* for
  that stage (``active_params``),
* freezes all other groups at the best values found so far,
* passes only the stage's ``target_stations`` as the active_stations list,
* writes a checkpoint after every stage so the run can be resumed.

Typical usage::

    from dsm2ui.calib.calib_cascade import run_cascade

    result = run_cascade("calib_meta_confluence.yml")
    print(result.summary_df)

Or via the CLI::

    dsm2ui calib cascade --config calib_meta_confluence.yml
    dsm2ui calib cascade --config calib_meta_confluence.yml --resume
"""
from __future__ import annotations

import copy
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import yaml

from dsm2ui.calib.calib_run import load_yaml_config
from dsm2ui.calib.calib_optimize import optimize, OptimizationResult

logger = logging.getLogger(__name__)

_CHECKPOINT_FILE = "cascade_checkpoint.yml"


# ─────────────────────────────────────────────────────────────────────────────
# Data classes
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class StageConfig:
    """A single stage in the cascade sequence."""
    id: int
    label: str
    active_params: List[str]           # group names to optimise this stage
    target_stations: List[str]         # active_stations override for this stage
    optimizer_overrides: dict = field(default_factory=dict)  # optional per-stage cfg


@dataclass
class CascadeResult:
    """Final result returned by :func:`run_cascade`."""
    stages: List[dict]                 # one row per completed stage
    final_params: Dict[str, float]     # best values for ALL groups after all stages
    summary_df: pd.DataFrame
    output_dir: Path


# ─────────────────────────────────────────────────────────────────────────────
# Config loading
# ─────────────────────────────────────────────────────────────────────────────

def load_cascade_config(
    meta_path: str | Path,
) -> tuple[dict, dict, Path]:
    """Load a cascade meta-config YAML.

    Returns
    -------
    (cascade_cfg, base_cfg, base_config_path)
        *cascade_cfg* is the parsed meta YAML dict.
        *base_cfg* is the loaded base calib config dict.
        *base_config_path* is the resolved absolute Path to the base config
        (used by :func:`optimize` for relative-path resolution and as the
        template for optimised-YAML output).
    """
    meta_path = Path(meta_path).resolve()
    with open(meta_path, encoding="utf-8") as fh:
        cascade_cfg = yaml.safe_load(fh)

    raw_base = cascade_cfg.get("base_config")
    if not raw_base:
        raise ValueError("cascade config must contain a 'base_config' key")

    base_config_path = Path(raw_base)
    if not base_config_path.is_absolute():
        base_config_path = meta_path.parent / base_config_path
    base_config_path = base_config_path.resolve()

    base_cfg = load_yaml_config(base_config_path)
    return cascade_cfg, base_cfg, base_config_path


# ─────────────────────────────────────────────────────────────────────────────
# Checkpoint I/O
# ─────────────────────────────────────────────────────────────────────────────

def _read_checkpoint(output_dir: Path) -> dict:
    cp_file = output_dir / _CHECKPOINT_FILE
    if cp_file.exists():
        with open(cp_file, encoding="utf-8") as fh:
            return yaml.safe_load(fh) or {}
    return {}


def _write_checkpoint(output_dir: Path, checkpoint: dict) -> None:
    cp_file = output_dir / _CHECKPOINT_FILE
    with open(cp_file, "w", encoding="utf-8") as fh:
        yaml.dump(checkpoint, fh, default_flow_style=False, sort_keys=False)


# ─────────────────────────────────────────────────────────────────────────────
# Main cascade entry point
# ─────────────────────────────────────────────────────────────────────────────

def run_cascade(
    meta_yaml_path: str | Path,
    resume: bool = False,
    dry_run: bool = False,
    skip_init: bool = False,
) -> CascadeResult:
    """Run the downstream-to-upstream cascade optimisation sequence.

    Parameters
    ----------
    meta_yaml_path :
        Path to the cascade meta-config YAML (e.g. ``calib_meta_confluence.yml``).
    resume :
        If ``True``, read ``cascade_checkpoint.yml`` from *output_dir* and skip
        any stages already marked as completed.
    dry_run :
        Pass ``--dry-run`` to every stage (evaluates starting point only; no
        optimisation loop).
    skip_init :
        Pass ``--skip-init`` to every stage.

    Returns
    -------
    :class:`CascadeResult`
    """
    meta_path = Path(meta_yaml_path).resolve()
    cascade_cfg, base_cfg, base_config_path = load_cascade_config(meta_path)

    output_dir = Path(
        cascade_cfg.get("output_dir",
                        str(meta_path.parent / (meta_path.stem + "_output")))
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    # Parse stage definitions
    stages: List[StageConfig] = []
    for s in cascade_cfg["stages"]:
        stages.append(StageConfig(
            id=int(s["id"]),
            label=str(s["label"]),
            active_params=list(s["active_params"]),
            target_stations=list(s["target_stations"]),
            optimizer_overrides=dict(s.get("optimizer_overrides") or {}),
        ))

    # Checkpoint / resume bookkeeping
    checkpoint = _read_checkpoint(output_dir) if resume else {}
    completed_ids = {row["id"] for row in checkpoint.get("completed_stages", [])}

    # Initialise current_best from base config values, then overlay checkpoint
    current_best: Dict[str, float] = {}
    for mod in base_cfg["variation"]["channel_modifications"]:
        current_best[mod["name"]] = float(mod["value"])
    current_best.update(checkpoint.get("current_best_params", {}))

    summary_rows: List[dict] = []

    # Re-emit already-completed stages into summary
    for row in checkpoint.get("completed_stages", []):
        summary_rows.append(row)

    # ── Stage loop ────────────────────────────────────────────────────────────
    total_t0 = time.monotonic()
    for stage in stages:
        if stage.id in completed_ids:
            logger.info(
                "Stage %d [%s] already completed — skipping (--resume).",
                stage.id, stage.label,
            )
            continue

        logger.info("=" * 70)
        logger.info(
            "CASCADE  stage %d / %d : %s",
            stage.id, len(stages), stage.label,
        )
        logger.info("  Active params    : %s", stage.active_params)
        logger.info("  Target stations  : %s", stage.target_stations)
        logger.info("  Frozen at current best:")
        for name, val in current_best.items():
            if name not in stage.active_params:
                logger.info("    %-25s = %.1f", name, val)
        logger.info("=" * 70)

        # Build a stage-specific config:
        #   - all channel_modifications updated to current_best values
        #   - active_stations = this stage's target_stations
        #   - variation name / study_dir scoped to this stage
        #   - optimizer output dirs scoped to this stage
        stage_cfg = copy.deepcopy(base_cfg)

        for mod in stage_cfg["variation"]["channel_modifications"]:
            if mod["name"] in current_best:
                mod["value"] = current_best[mod["name"]]

        stage_cfg["active_stations"] = stage.target_stations

        stage_var_name = (
            f"{base_cfg['variation']['name']}_casc_s{stage.id:02d}_{stage.label}"
        )
        stage_var_dir = output_dir / f"stage_{stage.id:02d}_{stage.label}"

        stage_cfg["variation"]["name"] = stage_var_name
        stage_cfg["variation"]["study_dir"] = str(stage_var_dir).replace("\\", "/")

        stage_cfg.setdefault("optimizer", {})
        stage_cfg["optimizer"]["scratch_dir"] = str(
            stage_var_dir / "optim_scratch"
        ).replace("\\", "/")
        stage_cfg["optimizer"]["best_dir"] = str(
            stage_var_dir / "optim_best"
        ).replace("\\", "/")

        # Apply per-stage optimizer overrides (e.g. tighter bounds, more iters)
        for k, v in stage.optimizer_overrides.items():
            stage_cfg["optimizer"][k] = v

        # ── Run this stage ────────────────────────────────────────────────────
        stage_t0 = time.monotonic()
        result: OptimizationResult = optimize(
            base_config_path,
            cfg_override=stage_cfg,
            active_groups=stage.active_params,
            dry_run=dry_run,
            skip_init=skip_init,
        )
        stage_elapsed = time.monotonic() - stage_t0

        # Carry best values forward (only active params for this stage)
        current_best.update(result.best_params)

        stage_row: dict = {
            "id": stage.id,
            "label": stage.label,
            "best_objective": result.best_objective,
            "initial_objective": result.initial_objective,
            "n_iters": result.n_iters,
            "n_evals": result.n_evals,
            "elapsed_sec": round(stage_elapsed, 1),
            "converged_reason": result.converged_reason,
        }
        # Embed per-group best values for this stage
        stage_row.update({f"param_{k}": v for k, v in result.best_params.items()})
        summary_rows.append(stage_row)

        # Write checkpoint
        if "completed_stages" not in checkpoint:
            checkpoint["completed_stages"] = []
        # Replace or append (avoid duplicate if retrying a stage)
        checkpoint["completed_stages"] = [
            r for r in checkpoint["completed_stages"] if r["id"] != stage.id
        ]
        checkpoint["completed_stages"].append(stage_row)
        checkpoint["current_best_params"] = dict(current_best)
        _write_checkpoint(output_dir, checkpoint)

        logger.info(
            "Stage %d done — obj %.6f  |  %d evals  |  %.0fs",
            stage.id, result.best_objective, result.n_evals, stage_elapsed,
        )

    total_elapsed = time.monotonic() - total_t0
    logger.info("=" * 70)
    logger.info("CASCADE COMPLETE — total elapsed %.0fs", total_elapsed)
    logger.info("Final parameters:")
    for name, val in current_best.items():
        logger.info("  %-25s = %.1f", name, val)
    logger.info("=" * 70)

    summary_df = pd.DataFrame(summary_rows).sort_values("id").reset_index(drop=True)
    summary_csv = output_dir / "cascade_summary.csv"
    summary_df.to_csv(summary_csv, index=False, float_format="%.4f")
    logger.info("Summary written to %s", summary_csv)

    return CascadeResult(
        stages=summary_rows,
        final_params=current_best,
        summary_df=summary_df,
        output_dir=output_dir,
    )
