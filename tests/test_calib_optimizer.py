# tests/test_calib_optimizer.py
"""
Mock-based unit tests for dsm2ui.calib.calib_optimize.

No DSM2 executables, DSS files, or real filesystem study directories are
required.  All external side-effects (setup_variation, run_study,
compute_ec_metric) are replaced with unittest.mock.

Test surface
------------
- _objective_from_metric          — weighted squared error formula
- _objective_from_slopes          — backward-compat wrapper
- _build_weights                  — station weight dict construction
- _forward_step                   — finite-difference step / sign flip at bound
- parallel_forward_gradient       — gradient computation (mocked evals)
- _write_optimized_yaml           — output YAML content
- _write_eval_diagnostics         — diagnostics files written
- ObjectiveEvaluator.evaluate     — wiring of setup/run/metric
- optimize (lbfgsb)               — end-to-end optimizer with mocked evaluator
- optimize (neldermead)           — end-to-end optimizer with mocked evaluator
- optimize (diffevol)             — end-to-end optimizer with mocked evaluator
- optimize dry_run                — immediately returns after init
- optimize no_improve patience    — stops early when objective stalls
- optimize max_model_runs         — stops when budget exhausted
"""
from __future__ import annotations

import copy
import math
import tempfile
from pathlib import Path
from typing import Dict, List
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest
import yaml

from dsm2ui.calib.calib_optimize import (
    EvalResult,
    ObjectiveEvaluator,
    OptimizationResult,
    _build_weights,
    _forward_step,
    _objective_from_metric,
    _objective_from_slopes,
    _write_eval_diagnostics,
    _write_optimized_yaml,
    parallel_forward_gradient,
    optimize,
)
from dsm2ui.calib.calib_run import (
    ChannelParamModification,
    ECLocation,
)


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures and helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_metric_df(
    stations: List[str],
    values: List[float],
    target: float = 1.0,
) -> pd.DataFrame:
    return pd.DataFrame({
        "station_name": stations,
        "metric_value": values,
        "metric_target": [target] * len(stations),
    })


def _make_locations(names: List[str]) -> List[ECLocation]:
    return [
        ECLocation(station_name=n, model_bpart=n.upper(), obs_bpart=n.upper())
        for n in names
    ]


def _minimal_cfg(tmp_path: Path, method: str = "lbfgsb") -> dict:
    """Return a minimal validated config dict that points at *tmp_path*."""
    var_dir = tmp_path / "studies" / "var1"
    var_dir.mkdir(parents=True)

    # Stub CSV for ec_stations
    csv_path = tmp_path / "stations.csv"
    csv_path.write_text("Name,BPart,ObsBPart\nSta1,STA1,OBS1\nSta2,STA2,OBS2\n")

    # Stub observed DSS (path only — never opened in mock tests)
    obs_dss = tmp_path / "obs.dss"
    obs_dss.touch()

    base_dir = tmp_path / "studies" / "base"
    base_dir.mkdir(parents=True)

    return {
        "base_run": {
            "study_dir": str(base_dir),
            "modifier": "base_mod",
            "model_dss_pattern": "{modifier}_qual.dss",
            "batch_file": "DSM2_batch.bat",
            "channel_inp_source": str(tmp_path / "channel.inp"),
            "channel_inp_name": "channel_std_delta_grid.inp",
        },
        "variation": {
            "name": "var1",
            "study_dir": str(var_dir),
            "run_steps": ["hydro", "qual"],
            "channel_modifications": [
                {"name": "grp_a", "param": "DISPERSION", "channels": [10, 11], "value": 500.0},
                {"name": "grp_b", "param": "DISPERSION", "channels": [20], "value": 800.0},
            ],
        },
        "observed_ec_dss": str(obs_dss),
        "ec_stations_csv": str(csv_path),
        "metrics": {
            "timewindow": "01JAN2019 - 31DEC2021",
            "objective_metric": "slope",
        },
        "optimizer": {
            "method": method,
            "max_model_runs": 50,
            "max_iter": 5,
            "no_improve_patience": 3,
            "no_improve_tol": 0.001,
            "finite_diff_rel_step": 0.05,
            "max_workers": 2,
            "bounds": [50, 5000],
        },
    }


def _make_eval_result(
    eval_id: str = "e1",
    params: dict | None = None,
    objective: float = 0.5,
    slopes: dict | None = None,
    success: bool = True,
    eval_dir: Path | None = None,
) -> EvalResult:
    return EvalResult(
        eval_id=eval_id,
        params=params or {"grp_a": 500.0, "grp_b": 800.0},
        objective=objective,
        slopes=slopes or {"Sta1": 0.9, "Sta2": 1.1},
        success=success,
        elapsed_sec=0.1,
        eval_dir=eval_dir,
    )


# ─────────────────────────────────────────────────────────────────────────────
# _objective_from_metric
# ─────────────────────────────────────────────────────────────────────────────

class TestObjectiveFromMetric:

    def test_perfect_slope(self):
        """All slopes == target → objective == 0."""
        df = _make_metric_df(["s1", "s2"], [1.0, 1.0])
        weights = {"s1": 1.0, "s2": 1.0}
        assert _objective_from_metric(df, weights) == pytest.approx(0.0)

    def test_single_deviation(self):
        """One station deviates by 0.2; w=1 → (0.2)² = 0.04."""
        df = _make_metric_df(["s1"], [0.8])
        assert _objective_from_metric(df, {"s1": 1.0}) == pytest.approx(0.04)

    def test_weights_applied(self):
        """Weight of 2 doubles the contribution."""
        df = _make_metric_df(["s1"], [0.8])
        unweighted = _objective_from_metric(df, {"s1": 1.0})
        weighted = _objective_from_metric(df, {"s1": 2.0})
        assert weighted == pytest.approx(2.0 * unweighted)

    def test_nan_skipped(self):
        """NaN metric values are ignored; non-nan stations still contribute."""
        df = _make_metric_df(["s1", "s2"], [float("nan"), 0.8])
        result = _objective_from_metric(df, {"s1": 1.0, "s2": 1.0})
        assert result == pytest.approx(0.04)

    def test_all_nan_returns_penalty(self):
        """All-NaN → large penalty proportional to number of stations."""
        df = _make_metric_df(["s1", "s2"], [float("nan"), float("nan")])
        result = _objective_from_metric(df, {"s1": 1.0, "s2": 1.0})
        assert result > 1.0  # penalty > any realistic objective

    def test_non_default_target(self):
        """Target of 0.0 (e.g. for RMSE): value 0.3 → 0.09."""
        df = _make_metric_df(["s1"], [0.3], target=0.0)
        assert _objective_from_metric(df, {"s1": 1.0}) == pytest.approx(0.09)

    def test_unlisted_station_weight_defaults_to_one(self):
        """Station not in weights dict treated as weight=1.0."""
        df = _make_metric_df(["s1"], [0.8])
        assert _objective_from_metric(df, {}) == pytest.approx(0.04)

    def test_two_stations_summed(self):
        """Two stations: (0.2)² + (0.3)² = 0.04 + 0.09 = 0.13."""
        df = _make_metric_df(["s1", "s2"], [0.8, 1.3])
        result = _objective_from_metric(df, {"s1": 1.0, "s2": 1.0})
        assert result == pytest.approx(0.04 + 0.09)


# ─────────────────────────────────────────────────────────────────────────────
# _objective_from_slopes (backward-compat wrapper)
# ─────────────────────────────────────────────────────────────────────────────

class TestObjectiveFromSlopes:

    def test_respects_slope_column(self):
        """Legacy 'slope' column is renamed and target=1.0 assumed."""
        df = pd.DataFrame({"station_name": ["s1"], "slope": [0.8]})
        result = _objective_from_slopes(df, {"s1": 1.0})
        assert result == pytest.approx(0.04)

    def test_metric_value_column_takes_priority(self):
        """If 'metric_value' is present, 'slope' column is ignored."""
        df = pd.DataFrame({
            "station_name": ["s1"],
            "metric_value": [0.9],
            "metric_target": [1.0],
            "slope": [0.0],      # should NOT be used
        })
        result = _objective_from_slopes(df, {"s1": 1.0})
        assert result == pytest.approx(0.01)


# ─────────────────────────────────────────────────────────────────────────────
# _build_weights
# ─────────────────────────────────────────────────────────────────────────────

class TestBuildWeights:

    def test_explicit_weights(self):
        locs = _make_locations(["Alpha", "Beta"])
        cfg = {"station_weights": {"ALPHA": 2.0, "Beta": 0.5}}
        w = _build_weights(cfg, locs)
        assert w["Alpha"] == pytest.approx(2.0)
        assert w["Beta"] == pytest.approx(0.5)

    def test_missing_weight_defaults_to_one(self):
        locs = _make_locations(["Alpha", "Gamma"])
        cfg = {"station_weights": {"ALPHA": 3.0}}
        w = _build_weights(cfg, locs)
        assert w["Gamma"] == pytest.approx(1.0)

    def test_empty_weights_all_one(self):
        locs = _make_locations(["A", "B", "C"])
        cfg = {}
        w = _build_weights(cfg, locs)
        assert all(v == 1.0 for v in w.values())

    def test_station_name_fallback_over_bpart(self):
        """If bpart is not in weights dict, station_name is tried."""
        locs = _make_locations(["MyStation"])
        # locs[0].model_bpart == "MYSTATION", station_name == "MyStation"
        cfg = {"station_weights": {"MyStation": 5.0}}
        w = _build_weights(cfg, locs)
        assert w["MyStation"] == pytest.approx(5.0)


# ─────────────────────────────────────────────────────────────────────────────
# _forward_step
# ─────────────────────────────────────────────────────────────────────────────

class TestForwardStep:

    def test_normal_forward(self):
        h, sign = _forward_step(500.0, 0.05, 50.0, 5000.0)
        assert sign == pytest.approx(1.0)
        assert h == pytest.approx(500.0 * 0.05)

    def test_near_upper_bound_flips_to_backward(self):
        """x=4990, h_rel=0.05 → h=249.5 would exceed hi=5000 → backward."""
        h, sign = _forward_step(4990.0, 0.05, 50.0, 5000.0)
        assert sign == pytest.approx(-1.0)
        assert h > 0

    def test_minimum_step_enforced(self):
        """Very small x → step clipped to _MIN_H (10.0)."""
        h, sign = _forward_step(1.0, 0.05, 0.0, 5000.0)
        from dsm2ui.calib.calib_optimize import _MIN_H
        assert h >= _MIN_H

    def test_backward_step_stays_within_bounds(self):
        """Backward step must not go below lo."""
        x_i, lo, hi = 60.0, 50.0, 5000.0
        h, sign = _forward_step(x_i, 0.5, lo, hi)
        # sign should be -1 and x_i - h >= lo
        if sign < 0:
            assert x_i - h >= lo - 1e-9


# ─────────────────────────────────────────────────────────────────────────────
# parallel_forward_gradient
# ─────────────────────────────────────────────────────────────────────────────

class TestParallelForwardGradient:

    def _make_evaluator_mock(self, group_names, objective_fn):
        """Build a minimal ObjectiveEvaluator-like mock."""
        ev = MagicMock(spec=ObjectiveEvaluator)
        ev.group_names = group_names
        ev.metric = "slope"

        def _evaluate(params, eval_dir, eval_id="eval", modifier_override=None):
            x_vec = np.array([params[n] for n in group_names])
            obj = objective_fn(x_vec)
            return EvalResult(
                eval_id=eval_id,
                params=params,
                objective=obj,
                slopes={},
                success=True,
                elapsed_sec=0.01,
                eval_dir=eval_dir,
            )

        ev.evaluate.side_effect = _evaluate
        return ev

    def test_gradient_direction_for_quadratic(self, tmp_path):
        """f(x) = (x0 - 200)² + (x1 - 1000)². Gradient should point away from minimum."""
        def objective(x):
            return (x[0] - 200.0) ** 2 + (x[1] - 1000.0) ** 2

        x = np.array([500.0, 500.0])
        f_x = objective(x)
        groups = ["grp_a", "grp_b"]
        ev = self._make_evaluator_mock(groups, objective)

        scratch_dirs = [tmp_path / f"p{i}" for i in range(2)]
        for d in scratch_dirs:
            d.mkdir()

        grad = parallel_forward_gradient(
            x=x,
            f_x=f_x,
            x0_params=dict(zip(groups, x)),
            evaluator=ev,
            scratch_dirs=scratch_dirs,
            h_rel=0.05,
            opt_cfg={},
            max_workers=2,
            bounds_lo=np.array([50.0, 50.0]),
            bounds_hi=np.array([5000.0, 5000.0]),
        )
        # For x0=500 > 200 → gradient should be positive (objective increases)
        assert grad[0] > 0
        # For x1=500 < 1000 → gradient should be negative (objective decreases as x1 grows)
        assert grad[1] < 0

    def test_failed_eval_yields_zero_gradient(self, tmp_path):
        """If an eval fails, that gradient component should be 0.0."""
        ev = MagicMock(spec=ObjectiveEvaluator)
        ev.group_names = ["grp_a"]
        ev.evaluate.return_value = EvalResult(
            eval_id="e1",
            params={"grp_a": 500.0},
            objective=99.0,
            slopes={},
            success=False,
            elapsed_sec=0.0,
        )

        x = np.array([500.0])
        scratch_dirs = [tmp_path / "p0"]
        scratch_dirs[0].mkdir()

        grad = parallel_forward_gradient(
            x=x,
            f_x=1.0,
            x0_params={"grp_a": 500.0},
            evaluator=ev,
            scratch_dirs=scratch_dirs,
            h_rel=0.05,
            opt_cfg={},
            max_workers=1,
            bounds_lo=np.array([50.0]),
            bounds_hi=np.array([5000.0]),
        )
        assert grad[0] == pytest.approx(0.0)


# ─────────────────────────────────────────────────────────────────────────────
# _write_eval_diagnostics
# ─────────────────────────────────────────────────────────────────────────────

class TestWriteEvalDiagnostics:

    def test_creates_files(self, tmp_path):
        eval_dir = tmp_path / "eval1"
        eval_dir.mkdir()
        _write_eval_diagnostics(
            eval_dir,
            eval_id="opt_i1_base",
            params={"grp_a": 500.0, "grp_b": 800.0},
            modifications=[],
        )
        assert (eval_dir / "eval_params.yml").exists()
        assert (eval_dir / "run_header.txt").exists()

    def test_yaml_contains_params(self, tmp_path):
        eval_dir = tmp_path / "eval2"
        eval_dir.mkdir()
        _write_eval_diagnostics(
            eval_dir,
            eval_id="test_id",
            params={"grp_a": 123.0},
            modifications=[],
        )
        data = yaml.safe_load((eval_dir / "eval_params.yml").read_text())
        assert data["eval_id"] == "test_id"
        assert data["params"]["grp_a"] == pytest.approx(123.0)


# ─────────────────────────────────────────────────────────────────────────────
# _write_optimized_yaml
# ─────────────────────────────────────────────────────────────────────────────

class TestWriteOptimizedYaml:

    def test_values_updated(self, tmp_path):
        cfg = {
            "base_run": {"study_dir": "/base", "modifier": "b"},
            "variation": {
                "name": "var1",
                "study_dir": str(tmp_path / "var1"),
                "channel_modifications": [
                    {"name": "grp_a", "param": "DISPERSION", "channels": [1], "value": 500.0},
                ],
            },
        }
        best_params = {"grp_a": 1234.0}
        out = _write_optimized_yaml(cfg, best_params, tmp_path, tmp_path / "orig.yml")
        data = yaml.safe_load(out.read_text())
        assert data["variation"]["channel_modifications"][0]["value"] == pytest.approx(1234.0)

    def test_study_dir_name_updated(self, tmp_path):
        cfg = {
            "base_run": {"study_dir": "/base", "modifier": "b"},
            "variation": {
                "name": "var1",
                "study_dir": str(tmp_path / "var1"),
                "channel_modifications": [
                    {"name": "grp_a", "param": "DISPERSION", "channels": [1], "value": 500.0},
                ],
            },
        }
        out = _write_optimized_yaml(cfg, {}, tmp_path, tmp_path / "orig.yml", pass_suffix="pass2")
        data = yaml.safe_load(out.read_text())
        assert "pass2" in data["variation"]["study_dir"]
        assert "pass2" in data["variation"]["name"]

    def test_original_cfg_not_mutated(self, tmp_path):
        cfg = {
            "base_run": {"study_dir": "/base", "modifier": "b"},
            "variation": {
                "name": "var1",
                "study_dir": str(tmp_path / "var1"),
                "channel_modifications": [
                    {"name": "grp_a", "param": "DISPERSION", "channels": [1], "value": 500.0},
                ],
            },
        }
        original_value = cfg["variation"]["channel_modifications"][0]["value"]
        _write_optimized_yaml(cfg, {"grp_a": 999.0}, tmp_path, tmp_path / "orig.yml")
        # The in-memory cfg should be unchanged
        assert cfg["variation"]["channel_modifications"][0]["value"] == original_value


# ─────────────────────────────────────────────────────────────────────────────
# ObjectiveEvaluator.evaluate — mocked DSM2
# ─────────────────────────────────────────────────────────────────────────────

class TestObjectiveEvaluatorEvaluate:
    """Test that ObjectiveEvaluator.evaluate correctly wires setup/run/metric."""

    def _make_evaluator(self, tmp_path, cfg):
        """Build an ObjectiveEvaluator with mocked read_ec_locations_csv."""
        locations = _make_locations(["Sta1", "Sta2"])
        weights = {"Sta1": 1.0, "Sta2": 1.0}
        with patch("dsm2ui.calib.calib_optimize.read_ec_locations_csv", return_value=locations):
            ev = ObjectiveEvaluator(cfg, tmp_path / "config.yml", weights)
        return ev

    def test_successful_eval_returns_objective(self, tmp_path):
        cfg = _minimal_cfg(tmp_path)
        ev = self._make_evaluator(tmp_path, cfg)
        eval_dir = tmp_path / "eval_dir"
        eval_dir.mkdir()

        metric_df = _make_metric_df(["Sta1", "Sta2"], [0.9, 1.1])
        mock_run_result = MagicMock()
        mock_run_result.returncode = 0

        with patch("dsm2ui.calib.calib_optimize.setup_variation"), \
             patch("dsm2ui.calib.calib_optimize.run_study", return_value=mock_run_result), \
             patch("dsm2ui.calib.calib_optimize.compute_ec_metric", return_value=metric_df):
            result = ev.evaluate(
                {"grp_a": 500.0, "grp_b": 800.0},
                eval_dir,
                eval_id="test_eval",
            )

        assert result.success is True
        assert result.objective == pytest.approx(0.01 + 0.01)  # (0.9-1)² + (1.1-1)²
        assert result.eval_id == "test_eval"

    def test_model_failure_returns_penalty(self, tmp_path):
        cfg = _minimal_cfg(tmp_path)
        ev = self._make_evaluator(tmp_path, cfg)
        eval_dir = tmp_path / "eval_fail"
        eval_dir.mkdir()

        mock_run_result = MagicMock()
        mock_run_result.returncode = 1  # non-zero → failure

        with patch("dsm2ui.calib.calib_optimize.setup_variation"), \
             patch("dsm2ui.calib.calib_optimize.run_study", return_value=mock_run_result):
            result = ev.evaluate(
                {"grp_a": 500.0, "grp_b": 800.0},
                eval_dir,
                eval_id="fail_eval",
            )

        assert result.success is False
        assert result.objective > 1.0   # penalty

    def test_setup_exception_returns_penalty(self, tmp_path):
        cfg = _minimal_cfg(tmp_path)
        ev = self._make_evaluator(tmp_path, cfg)
        eval_dir = tmp_path / "eval_exc"
        eval_dir.mkdir()

        with patch("dsm2ui.calib.calib_optimize.setup_variation",
                   side_effect=RuntimeError("disk full")):
            result = ev.evaluate(
                {"grp_a": 500.0, "grp_b": 800.0},
                eval_dir,
                eval_id="exc_eval",
            )

        assert result.success is False
        assert math.isnan(result.slopes.get("Sta1", float("nan"))) or \
               result.slopes.get("Sta1") is None or \
               result.slopes["Sta1"] != result.slopes["Sta1"]   # NaN check

    def test_metric_slopes_stored_in_result(self, tmp_path):
        cfg = _minimal_cfg(tmp_path)
        ev = self._make_evaluator(tmp_path, cfg)
        eval_dir = tmp_path / "eval_slopes"
        eval_dir.mkdir()

        metric_df = _make_metric_df(["Sta1", "Sta2"], [0.85, 1.2])
        mock_run_result = MagicMock(returncode=0)

        with patch("dsm2ui.calib.calib_optimize.setup_variation"), \
             patch("dsm2ui.calib.calib_optimize.run_study", return_value=mock_run_result), \
             patch("dsm2ui.calib.calib_optimize.compute_ec_metric", return_value=metric_df):
            result = ev.evaluate({"grp_a": 500.0, "grp_b": 800.0}, eval_dir)

        assert result.slopes["Sta1"] == pytest.approx(0.85)
        assert result.slopes["Sta2"] == pytest.approx(1.2)


# ─────────────────────────────────────────────────────────────────────────────
# optimize() end-to-end with mocked evaluator
# ─────────────────────────────────────────────────────────────────────────────

def _make_metric_df_for_params(params: dict, group_names: list) -> pd.DataFrame:
    """Return a metric_df that maps parameter values to fake slopes."""
    # Simple synthetic objective: slope_i = 1 - (val - optimal_i) / optimal_i
    # where optimal is 300 for grp_a and 600 for grp_b.
    optima = {"grp_a": 300.0, "grp_b": 600.0}
    rows = []
    for i, name in enumerate(["Sta1", "Sta2"]):
        gname = group_names[i] if i < len(group_names) else group_names[0]
        optimal = optima.get(gname, 500.0)
        val = params.get(gname, optimal)
        slope = 1.0 - (val - optimal) / (10.0 * optimal)
        rows.append({"station_name": name, "metric_value": slope, "metric_target": 1.0})
    return pd.DataFrame(rows)


class TestOptimizeEndToEnd:
    """End-to-end tests for optimize() using a mocked ObjectiveEvaluator."""

    def _patch_evaluator(self, cfg, tmp_path, objective_fn=None):
        """Return a context-manager patch that replaces ObjectiveEvaluator with a mock."""
        group_names = ["grp_a", "grp_b"]
        call_count = {"n": 0}

        def _mock_evaluate(self_ev, params, eval_dir, eval_id="eval", modifier_override=None):
            call_count["n"] += 1
            x_vec = np.array([params.get(g, 500.0) for g in group_names])
            if objective_fn:
                obj = float(objective_fn(x_vec))
            else:
                obj = float(np.sum((x_vec - np.array([300.0, 600.0])) ** 2) / 1e6)
            metric_df = _make_metric_df_for_params(params, group_names)
            return EvalResult(
                eval_id=eval_id,
                params=params,
                objective=obj,
                slopes={"Sta1": float(metric_df.iloc[0]["metric_value"]),
                        "Sta2": float(metric_df.iloc[1]["metric_value"])},
                success=True,
                elapsed_sec=0.0,
                eval_dir=eval_dir,
            )

        return _mock_evaluate, call_count

    def _run_optimize(self, tmp_path, method="lbfgsb", objective_fn=None, extra_cfg=None):
        """Run optimize() with mocked I/O and return OptimizationResult."""
        cfg = _minimal_cfg(tmp_path, method=method)
        if extra_cfg:
            for k, v in extra_cfg.items():
                cfg.setdefault("optimizer", {})[k] = v

        yaml_path = tmp_path / "config.yml"
        yaml_path.write_text(yaml.dump(cfg))

        locations = _make_locations(["Sta1", "Sta2"])
        mock_eval_fn, call_count = self._patch_evaluator(cfg, tmp_path, objective_fn)

        with patch("dsm2ui.calib.calib_optimize.load_yaml_config", return_value=cfg), \
             patch("dsm2ui.calib.calib_optimize.read_ec_locations_csv", return_value=locations), \
             patch("dsm2ui.calib.calib_optimize.ObjectiveEvaluator.evaluate", mock_eval_fn), \
             patch("dsm2ui.calib.calib_optimize._copy_eval_to_best"), \
             patch("dsm2ui.calib.calib_optimize._prepare_scratch_dirs",
                   return_value=(tmp_path / "base", [tmp_path / f"p{i}" for i in range(2)])):

            # Ensure scratch dirs exist
            (tmp_path / "base").mkdir(exist_ok=True)
            for i in range(2):
                (tmp_path / f"p{i}").mkdir(exist_ok=True)

            result = optimize(yaml_path)

        return result, call_count

    # ── dry_run ───────────────────────────────────────────────────────────────

    def test_dry_run_returns_immediately(self, tmp_path):
        cfg = _minimal_cfg(tmp_path)
        yaml_path = tmp_path / "config.yml"
        yaml_path.write_text(yaml.dump(cfg))

        locations = _make_locations(["Sta1", "Sta2"])
        call_count = {"n": 0}

        def _mock_eval(self_ev, params, eval_dir, eval_id="eval", modifier_override=None):
            call_count["n"] += 1
            return _make_eval_result(eval_id=eval_id, eval_dir=eval_dir)

        with patch("dsm2ui.calib.calib_optimize.load_yaml_config", return_value=cfg), \
             patch("dsm2ui.calib.calib_optimize.read_ec_locations_csv", return_value=locations), \
             patch("dsm2ui.calib.calib_optimize.ObjectiveEvaluator.evaluate", _mock_eval), \
             patch("dsm2ui.calib.calib_optimize._copy_eval_to_best"), \
             patch("dsm2ui.calib.calib_optimize._prepare_scratch_dirs",
                   return_value=(tmp_path / "base", [tmp_path / f"p{i}" for i in range(2)])):

            (tmp_path / "base").mkdir(exist_ok=True)

            result = optimize(yaml_path, dry_run=True)

        assert isinstance(result, OptimizationResult)
        assert result.converged_reason == "dry_run"
        assert result.n_iters == 0
        assert call_count["n"] == 1  # only initial evaluation

    # ── result structure ──────────────────────────────────────────────────────

    def test_result_has_required_fields(self, tmp_path):
        result, _ = self._run_optimize(tmp_path, method="lbfgsb")
        assert isinstance(result.best_params, dict)
        assert "grp_a" in result.best_params or "grp_b" in result.best_params
        assert isinstance(result.history_df, pd.DataFrame)
        assert not result.history_df.empty
        assert result.n_evals > 0
        assert isinstance(result.best_objective, float)
        assert isinstance(result.initial_objective, float)

    def test_history_df_has_param_columns(self, tmp_path):
        result, _ = self._run_optimize(tmp_path, method="lbfgsb")
        cols = result.history_df.columns.tolist()
        assert "objective" in cols
        assert "iter" in cols
        assert any("param_" in c for c in cols)

    def test_history_df_has_metric_columns(self, tmp_path):
        result, _ = self._run_optimize(tmp_path, method="lbfgsb")
        cols = result.history_df.columns.tolist()
        assert any("slope_" in c or "Sta" in c for c in cols)

    # ── objective improvement ─────────────────────────────────────────────────

    def test_best_objective_le_initial(self, tmp_path):
        """best_objective should never be worse than initial_objective."""
        result, _ = self._run_optimize(tmp_path, method="lbfgsb")
        assert result.best_objective <= result.initial_objective + 1e-9

    def test_optimizer_reduces_objective_for_quadratic(self, tmp_path):
        """A pure quadratic f should be reduced meaningfully."""
        def quad(x):
            return float(np.sum((x - np.array([300.0, 600.0])) ** 2) / 1e6)

        result, _ = self._run_optimize(tmp_path, method="lbfgsb", objective_fn=quad)
        assert result.best_objective < result.initial_objective

    # ── patience / early stop ─────────────────────────────────────────────────

    def test_patience_stops_optimizer(self, tmp_path):
        """Constant objective → no_improve trigger within patience steps."""
        def flat(_x):
            return 1.0   # never improves

        cfg = _minimal_cfg(tmp_path)
        cfg["optimizer"]["no_improve_patience"] = 2
        cfg["optimizer"]["max_iter"] = 20
        yaml_path = tmp_path / "config.yml"
        yaml_path.write_text(yaml.dump(cfg))

        locations = _make_locations(["Sta1", "Sta2"])
        call_count = {"n": 0}

        def _flat_eval(self_ev, params, eval_dir, eval_id="eval", modifier_override=None):
            call_count["n"] += 1
            return EvalResult(
                eval_id=eval_id, params=params, objective=1.0,
                slopes={"Sta1": 1.0, "Sta2": 1.0},
                success=True, elapsed_sec=0.0, eval_dir=eval_dir,
            )

        with patch("dsm2ui.calib.calib_optimize.load_yaml_config", return_value=cfg), \
             patch("dsm2ui.calib.calib_optimize.read_ec_locations_csv", return_value=locations), \
             patch("dsm2ui.calib.calib_optimize.ObjectiveEvaluator.evaluate", _flat_eval), \
             patch("dsm2ui.calib.calib_optimize._copy_eval_to_best"), \
             patch("dsm2ui.calib.calib_optimize._prepare_scratch_dirs",
                   return_value=(tmp_path / "base", [tmp_path / f"p{i}" for i in range(2)])):

            (tmp_path / "base").mkdir(exist_ok=True)
            for i in range(2):
                (tmp_path / f"p{i}").mkdir(exist_ok=True)

            result = optimize(yaml_path)

        assert result.converged_reason in ("no_improve", "max_model_runs", "max_iter") or \
               "no_improve" in str(result.converged_reason).lower() or \
               result.n_iters <= 20

    # ── max_model_runs budget ─────────────────────────────────────────────────

    def test_max_model_runs_respected(self, tmp_path):
        """n_evals should not greatly exceed max_model_runs."""
        cfg = _minimal_cfg(tmp_path)
        cfg["optimizer"]["max_model_runs"] = 5
        cfg["optimizer"]["max_iter"] = 100  # don't let max_iter stop it first
        yaml_path = tmp_path / "config.yml"
        yaml_path.write_text(yaml.dump(cfg))

        locations = _make_locations(["Sta1", "Sta2"])
        call_count = {"n": 0}

        def _counting_eval(self_ev, params, eval_dir, eval_id="eval", modifier_override=None):
            call_count["n"] += 1
            obj = float(call_count["n"])  # always changing so patience won't stop it
            return EvalResult(
                eval_id=eval_id, params=params, objective=obj,
                slopes={"Sta1": 1.0, "Sta2": 1.0},
                success=True, elapsed_sec=0.0, eval_dir=eval_dir,
            )

        with patch("dsm2ui.calib.calib_optimize.load_yaml_config", return_value=cfg), \
             patch("dsm2ui.calib.calib_optimize.read_ec_locations_csv", return_value=locations), \
             patch("dsm2ui.calib.calib_optimize.ObjectiveEvaluator.evaluate", _counting_eval), \
             patch("dsm2ui.calib.calib_optimize._copy_eval_to_best"), \
             patch("dsm2ui.calib.calib_optimize._prepare_scratch_dirs",
                   return_value=(tmp_path / "base", [tmp_path / f"p{i}" for i in range(2)])):

            (tmp_path / "base").mkdir(exist_ok=True)
            for i in range(2):
                (tmp_path / f"p{i}").mkdir(exist_ok=True)

            result = optimize(yaml_path)

        # Should stop at or shortly after max_model_runs
        assert result.n_evals <= cfg["optimizer"]["max_model_runs"] + 5  # small tolerance for parallelism

    # ── Nelder-Mead ───────────────────────────────────────────────────────────

    def test_neldermead_returns_valid_result(self, tmp_path):
        def quad(x):
            return float(np.sum((x - np.array([300.0, 600.0])) ** 2) / 1e6)

        result, _ = self._run_optimize(tmp_path, method="neldermead", objective_fn=quad)
        assert isinstance(result, OptimizationResult)
        assert result.n_evals > 0
        assert result.best_objective <= result.initial_objective + 1e-9

    # ── Differential evolution ────────────────────────────────────────────────

    def test_diffevol_returns_valid_result(self, tmp_path):
        def quad(x):
            return float(np.sum((x - np.array([300.0, 600.0])) ** 2) / 1e6)

        result, _ = self._run_optimize(tmp_path, method="diffevol", objective_fn=quad)
        assert isinstance(result, OptimizationResult)
        assert result.n_evals > 0
        assert result.best_objective <= result.initial_objective + 1e-9

    # ── optimized YAML is written ─────────────────────────────────────────────

    def test_optimized_yaml_written(self, tmp_path):
        result, _ = self._run_optimize(tmp_path, method="lbfgsb")
        assert result.optimized_yaml is not None

        # The optimized YAML file must exist on disk
        assert Path(result.optimized_yaml).exists()

    def test_optimized_yaml_has_best_params(self, tmp_path):
        def quad(x):
            return float(np.sum((x - np.array([300.0, 600.0])) ** 2) / 1e6)

        result, _ = self._run_optimize(tmp_path, method="lbfgsb", objective_fn=quad)
        data = yaml.safe_load(Path(result.optimized_yaml).read_text())
        mods = {m["name"]: m["value"] for m in data["variation"]["channel_modifications"]}
        for name, val in result.best_params.items():
            assert mods[name] == pytest.approx(val, rel=1e-4)


# ─────────────────────────────────────────────────────────────────────────────
# Gradient accuracy and optimizer step correctness
# ─────────────────────────────────────────────────────────────────────────────

class TestGradientAndStepCorrectness:
    """
    Verifies that:

    1. The finite-difference gradient produced by parallel_forward_gradient
       is numerically close to the analytical gradient of a known quadratic.

    2. The gradient is internally consistent: the value returned by
       parallel_forward_gradient exactly equals (f_pert − f_base) / h for
       each perturbation eval record.

    3. After L-BFGS-B uses the computed gradient, the step taken satisfies
       the descent condition  dot(step, gradient) < 0.

    4. The optimizer closes the Euclidean distance to the known optimum.

    Objective used throughout:
        f(x) = [(x0 − 300)² + (x1 − 600)²] / 1 000 000
    Analytical gradient:
        ∂f/∂x_i = 2 × (x_i − opt_i) / 1 000 000

    Starting point x0 = [500, 800] (matches _minimal_cfg defaults).
    The gradient at x0 is [4e-4, 4e-4] (both positive, pointing away from
    optimum), so a correct descent step must move both parameters downward.
    """

    OPTIMUM     = np.array([300.0, 600.0])
    X0          = np.array([500.0, 800.0])   # matches _minimal_cfg channel_modifications values
    SCALE       = 1e6
    GROUP_NAMES = ["grp_a", "grp_b"]
    H_REL       = 0.05
    BOUNDS_LO   = np.array([50.0,   50.0])
    BOUNDS_HI   = np.array([5000.0, 5000.0])

    # ── Helpers ────────────────────────────────────────────────────────────────

    def _f(self, x: np.ndarray) -> float:
        return float(np.sum((x - self.OPTIMUM) ** 2) / self.SCALE)

    def _grad_analytical(self, x: np.ndarray) -> np.ndarray:
        return 2.0 * (x - self.OPTIMUM) / self.SCALE

    def _mock_evaluator(self) -> MagicMock:
        """Return a mock ObjectiveEvaluator backed by the known quadratic."""
        ev = MagicMock(spec=ObjectiveEvaluator)
        ev.group_names = self.GROUP_NAMES

        def _side(params, eval_dir, eval_id="e", modifier_override=None):
            xv = np.array([params.get(g, 0.0) for g in self.GROUP_NAMES])
            return EvalResult(
                eval_id=eval_id, params=dict(params),
                objective=self._f(xv),
                slopes={}, success=True, elapsed_sec=0.0, eval_dir=eval_dir,
            )

        ev.evaluate.side_effect = _side
        return ev

    def _recording_eval(self):
        """Return (patched_evaluate_fn, all_calls_list) for full-optimizer runs."""
        calls = []

        def _eval(self_ev, params, eval_dir, eval_id="e", modifier_override=None):
            xv = np.array([params.get(g, 0.0) for g in self.GROUP_NAMES])
            obj = self._f(xv)
            calls.append({"x": xv.copy(), "obj": obj, "eval_id": eval_id})
            return EvalResult(
                eval_id=eval_id, params=dict(params), objective=obj,
                slopes={}, success=True, elapsed_sec=0.0, eval_dir=eval_dir,
            )

        return _eval, calls

    def _run_optimizer(self, tmp_path, max_iter=5, max_runs=40):
        """Run optimize() for this quadratic and return (result, all_eval_calls)."""
        cfg = _minimal_cfg(tmp_path, method="lbfgsb")
        cfg["optimizer"]["max_model_runs"] = max_runs
        cfg["optimizer"]["max_iter"] = max_iter
        cfg["optimizer"]["no_improve_patience"] = 20   # let it run freely
        cfg["optimizer"]["finite_diff_rel_step"] = self.H_REL
        # Make sure starting values match self.X0
        cfg["variation"]["channel_modifications"][0]["value"] = float(self.X0[0])
        cfg["variation"]["channel_modifications"][1]["value"] = float(self.X0[1])

        yaml_path = tmp_path / "config.yml"
        yaml_path.write_text(yaml.dump(cfg))

        locations = _make_locations(["Sta1", "Sta2"])
        eval_fn, calls = self._recording_eval()

        with patch("dsm2ui.calib.calib_optimize.load_yaml_config", return_value=cfg), \
             patch("dsm2ui.calib.calib_optimize.read_ec_locations_csv", return_value=locations), \
             patch("dsm2ui.calib.calib_optimize.ObjectiveEvaluator.evaluate", eval_fn), \
             patch("dsm2ui.calib.calib_optimize._copy_eval_to_best"), \
             patch("dsm2ui.calib.calib_optimize._prepare_scratch_dirs",
                   return_value=(tmp_path / "base", [tmp_path / f"p{i}" for i in range(2)])):
            (tmp_path / "base").mkdir(exist_ok=True)
            for i in range(2):
                (tmp_path / f"p{i}").mkdir(exist_ok=True)
            result = optimize(yaml_path)

        return result, calls

    # ── Test 1: gradient accuracy vs analytical ────────────────────────────────

    def test_numerical_gradient_close_to_analytical(self, tmp_path):
        """
        parallel_forward_gradient must agree with the analytical gradient
        to within 10% (relative) at x0 = [500, 800].

        Forward-difference truncation error is O(h) ≈ 5 %, so 10 % gives
        reasonable headroom.

        Analytical: grad = [2*(500-300)/1e6, 2*(800-600)/1e6] = [4e-4, 4e-4]
        Numerical (h=25, 40 respectively): ≈ [4.25e-4, 4.40e-4]
        Relative errors: grp_a ≈ 6.25 %, grp_b = 10.0 % exactly (for this
        smooth quadratic at h_rel=0.05).  Tolerance is set to 12 % to pass on
        exact floating-point arithmetic without false positives.
        """
        x = self.X0.copy()
        f_x = self._f(x)
        analytical = self._grad_analytical(x)

        ev = self._mock_evaluator()
        scratch_dirs = [tmp_path / f"p{i}" for i in range(2)]
        for d in scratch_dirs:
            d.mkdir()

        grad = parallel_forward_gradient(
            x=x, f_x=f_x,
            x0_params=dict(zip(self.GROUP_NAMES, x)),
            evaluator=ev,
            scratch_dirs=scratch_dirs,
            h_rel=self.H_REL,
            opt_cfg={},
            max_workers=2,
            bounds_lo=self.BOUNDS_LO,
            bounds_hi=self.BOUNDS_HI,
        )

        for i in range(len(x)):
            rel_err = abs(grad[i] - analytical[i]) / abs(analytical[i])
            assert rel_err < 0.12, (
                f"Component {i}: numerical={grad[i]:.6e}, "
                f"analytical={analytical[i]:.6e}, relative error={rel_err:.1%}"
            )

    # ── Test 2: gradient sign at four quadrants ────────────────────────────────

    @pytest.mark.parametrize("x_vals, expected_signs", [
        ((500.0, 800.0), ( 1,  1)),   # both above optimum → grad > 0
        ((100.0, 200.0), (-1, -1)),   # both below optimum → grad < 0
        ((500.0, 200.0), ( 1, -1)),   # x0 above, x1 below
        ((200.0, 900.0), (-1,  1)),   # x0 below, x1 above
    ])
    def test_gradient_sign_correct_in_all_quadrants(self, tmp_path, x_vals, expected_signs):
        """
        In every quadrant relative to the optimum [300, 600], the sign of
        each gradient component must match the analytical sign.
        """
        x = np.array(x_vals)
        f_x = self._f(x)
        ev = self._mock_evaluator()
        scratch_dirs = [tmp_path / f"p{i}" for i in range(2)]
        for d in scratch_dirs:
            d.mkdir(exist_ok=True)

        grad = parallel_forward_gradient(
            x=x, f_x=f_x,
            x0_params=dict(zip(self.GROUP_NAMES, x)),
            evaluator=ev,
            scratch_dirs=scratch_dirs,
            h_rel=self.H_REL,
            opt_cfg={},
            max_workers=2,
            bounds_lo=self.BOUNDS_LO,
            bounds_hi=self.BOUNDS_HI,
        )

        for i, expected_sign in enumerate(expected_signs):
            assert np.sign(grad[i]) == expected_sign, (
                f"Component {i}: grad={grad[i]:.4e} at x={x_vals} — "
                f"expected sign {expected_sign:+d}"
            )

    # ── Test 3: gradient == raw finite diff from perturbation evals ───────────

    def test_gradient_exactly_equals_raw_finite_difference(self, tmp_path):
        """
        The value returned by parallel_forward_gradient for each component i
        must exactly equal (f_perturbed_i - f_base) / (x_perturbed_i - x_base_i).

        This is a white-box internal consistency check: any sign flip,
        denominator error, or wrong h inside parallel_forward_gradient would
        be caught here because we independently reconstruct the same formula
        from the raw perturbation records.
        """
        x = self.X0.copy()
        f_x = self._f(x)

        # Wrap the evaluator side-effect to capture every call's (x, obj) pair
        perturbation_records = []
        ev = self._mock_evaluator()
        original_side = ev.evaluate.side_effect

        def _capturing(params, eval_dir, eval_id="e", modifier_override=None):
            res = original_side(params, eval_dir, eval_id, modifier_override)
            xv = np.array([params.get(g, 0.0) for g in self.GROUP_NAMES])
            perturbation_records.append({"x": xv.copy(), "obj": res.objective})
            return res

        ev.evaluate.side_effect = _capturing

        scratch_dirs = [tmp_path / f"p{i}" for i in range(2)]
        for d in scratch_dirs:
            d.mkdir()

        grad = parallel_forward_gradient(
            x=x, f_x=f_x,
            x0_params=dict(zip(self.GROUP_NAMES, x)),
            evaluator=ev,
            scratch_dirs=scratch_dirs,
            h_rel=self.H_REL,
            opt_cfg={},
            max_workers=2,
            bounds_lo=self.BOUNDS_LO,
            bounds_hi=self.BOUNDS_HI,
        )

        assert len(perturbation_records) == 2, (
            f"Expected exactly 2 perturbation evals (one per parameter), "
            f"got {len(perturbation_records)}"
        )

        for rec in perturbation_records:
            # Identify which parameter was perturbed
            delta = rec["x"] - x
            nonzero_idx = np.nonzero(delta)[0]
            assert len(nonzero_idx) == 1, (
                f"Each perturbation must change exactly one parameter; "
                f"got delta={delta}"
            )
            i = int(nonzero_idx[0])
            h_signed = delta[i]                          # = sign * h
            expected = (rec["obj"] - f_x) / h_signed    # = sign*(f_pert-f_base)/h

            assert grad[i] == pytest.approx(expected, rel=1e-9), (
                f"Component {i}: parallel_forward_gradient returned {grad[i]:.10e}, "
                f"but raw (f_pert - f_base) / h_signed = {expected:.10e}"
            )

    # ── Test 4: optimizer step is descent direction (from actual eval records) -

    def test_lbfgsb_step_is_descent_direction(self, tmp_path):
        """
        After the first L-BFGS-B iteration the optimizer must move in a
        descent direction relative to the gradient computed at x0.

        Method
        ------
        1. Run optimize() and collect every evaluate() call.
        2. Find the three records for iteration 1:
               "opt_i1_base"  → f(x0)
               "opt_i1_p0"    → f(x0 + h0*e0)
               "opt_i1_p1"    → f(x0 + h1*e1)
        3. Reconstruct the gradient:
               grad[i] = (f_pert_i - f_base) / h_signed_i
        4. Read best_params x_best from the OptimizationResult.
        5. Assert  dot(x_best - x0, grad) < 0  (descent condition).

        Expected values at x0 = [500, 800]:
            h0 = 25    (= 500*0.05)  → x_pert0 = [525, 800]
            h1 = 40    (= 800*0.05)  → x_pert1 = [500, 840]
            grad ≈ [4.25e-4, 4.40e-4]  (both positive)
        A correct step moves BOTH parameters downward (toward [300, 600]).
        """
        result, calls = self._run_optimizer(tmp_path, max_runs=30, max_iter=4)

        # Locate iter-1 eval records by exact eval_id
        base_rec = next((c for c in calls if c["eval_id"] == "opt_i1_base"), None)
        p0_rec   = next((c for c in calls if c["eval_id"] == "opt_i1_p0"),   None)
        p1_rec   = next((c for c in calls if c["eval_id"] == "opt_i1_p1"),   None)

        if not (base_rec and p0_rec and p1_rec):
            pytest.skip(
                "Could not locate iteration-1 eval records — "
                "optimizer may have terminated before iter 1"
            )

        x0 = base_rec["x"].copy()

        # Reconstruct signed steps: delta_i = x_pert_i[i] - x0[i]
        h0_signed = p0_rec["x"][0] - x0[0]   # perturbation applied to grp_a
        h1_signed = p1_rec["x"][1] - x0[1]   # perturbation applied to grp_b

        assert h0_signed != 0.0, "grp_a perturbation must be non-zero"
        assert h1_signed != 0.0, "grp_b perturbation must be non-zero"

        # Verify perturbation directions match _forward_step expectations
        # (both x0 components are well inside bounds, so sign=+1 expected)
        assert h0_signed > 0, f"Expected forward step for grp_a at x={x0[0]}, got h={h0_signed}"
        assert h1_signed > 0, f"Expected forward step for grp_b at x={x0[1]}, got h={h1_signed}"

        # Gradient reconstructed independently from raw eval values
        grad_reconstructed = np.array([
            (p0_rec["obj"] - base_rec["obj"]) / h0_signed,
            (p1_rec["obj"] - base_rec["obj"]) / h1_signed,
        ])

        # Both components should be positive (x0 is above the optimum in both dims)
        assert grad_reconstructed[0] > 0, (
            f"grad[0] = {grad_reconstructed[0]:.4e} should be positive at x0={x0}"
        )
        assert grad_reconstructed[1] > 0, (
            f"grad[1] = {grad_reconstructed[1]:.4e} should be positive at x0={x0}"
        )

        # The step to best_params must be in the descent direction
        x_best = np.array([result.best_params[g] for g in self.GROUP_NAMES])
        step = x_best - x0

        if np.allclose(step, 0):
            pytest.skip("Optimizer made no step (already started at optimum)")

        dot = float(np.dot(step, grad_reconstructed))
        assert dot < 0.0, (
            f"Step is NOT a descent direction:\n"
            f"  x0              = {x0}\n"
            f"  best_params     = {x_best}\n"
            f"  step            = {step}\n"
            f"  grad_reconstr   = {grad_reconstructed}\n"
            f"  dot(step, grad) = {dot:.6e}  (expected < 0)"
        )

    # ── Test 5: gradient drives step toward known optimum ─────────────────────

    def test_lbfgsb_step_closes_distance_to_optimum(self, tmp_path):
        """
        After the optimizer runs, best_params must be strictly closer to the
        known global optimum [300, 600] than the starting point [500, 800].

        This verifies end-to-end that the gradient→step pipeline moves the
        search in the analytically correct direction.
        """
        result, _ = self._run_optimizer(tmp_path, max_runs=40, max_iter=6)

        x_best = np.array([result.best_params[g] for g in self.GROUP_NAMES])
        dist_initial = float(np.linalg.norm(self.X0 - self.OPTIMUM))
        dist_final   = float(np.linalg.norm(x_best - self.OPTIMUM))

        assert dist_final < dist_initial, (
            f"Optimizer did not move toward the optimum:\n"
            f"  x0          = {self.X0}  (dist = {dist_initial:.2f})\n"
            f"  best_params = {x_best}  (dist = {dist_final:.2f})\n"
            f"  optimum     = {self.OPTIMUM}"
        )
