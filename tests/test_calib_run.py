from pathlib import Path
from unittest.mock import patch

from dsm2ui.calib.calib_run import (
    _resolve_model_dss_pattern,
    _write_filtered_batch,
    run_from_yaml,
)


def test_write_filtered_batch_filters_qual_exe_for_gtm_steps(tmp_path: Path):
    source = tmp_path / "DSM2_batch.bat"
    dest = tmp_path / "DSM2_batch_var.bat"
    source.write_text(
        "\n".join(
            [
                r"D:\\delta\\DSM2\\bin\\hydro.exe hydro.inp",
                r"D:\\delta\\DSM2\\bin\\qual.exe qual_ec.inp",
                r"D:\\delta\\DSM2\\bin\\gtm.exe gtm.inp",
            ]
        )
        + "\n"
    )

    _write_filtered_batch(source, dest, run_steps=["hydro", "gtm"])

    filtered = dest.read_text()
    assert "hydro.exe" in filtered
    assert "gtm.exe" in filtered
    assert "qual.exe" not in filtered


def test_write_filtered_batch_keeps_extensionless_invocations(tmp_path: Path):
    source = tmp_path / "DSM2_batch.bat"
    dest = tmp_path / "DSM2_batch_var.bat"
    source.write_text(
        "\n".join(
            [
                r"..\\..\\bin\\hydro hydro.inp",
                r"..\\..\\bin\\qual qual_ec.inp",
                r"..\\..\\bin\\gtm gtm.inp",
            ]
        )
        + "\n"
    )

    _write_filtered_batch(source, dest, run_steps=["hydro", "gtm"])

    filtered = dest.read_text()
    assert r"..\\..\\bin\\hydro hydro.inp" in filtered
    assert r"..\\..\\bin\\gtm gtm.inp" in filtered
    assert "qual" not in filtered


def test_write_filtered_batch_uncomments_selected_module_lines(tmp_path: Path):
    source = tmp_path / "DSM2_batch.bat"
    dest = tmp_path / "DSM2_batch_var.bat"
    source.write_text(
        "\n".join(
            [
                r"D:\\delta\\DSM2\\bin\\hydro.exe hydro.inp",
                r"REM D:\\delta\\DSM2\\bin\\gtm.exe gtm.inp",
                r"D:\\delta\\DSM2\\bin\\qual.exe qual_ec.inp",
            ]
        )
        + "\n"
    )

    _write_filtered_batch(source, dest, run_steps=["hydro", "gtm"])

    filtered = dest.read_text().splitlines()
    assert any("gtm.exe" in line.lower() for line in filtered)
    assert not any(line.lower().startswith("rem ") and "gtm.exe" in line.lower() for line in filtered)
    assert all("qual.exe" not in line.lower() for line in filtered)


def test_resolve_model_dss_pattern_gtm_only_default():
    assert _resolve_model_dss_pattern({}, ["hydro", "gtm"]) == "{modifier}_gtm.dss"


def test_resolve_model_dss_pattern_respects_explicit_setting():
    base_cfg = {"model_dss_pattern": "{modifier}_qual.dss"}
    assert (
        _resolve_model_dss_pattern(base_cfg, ["hydro", "gtm"])
        == "{modifier}_qual.dss"
    )


def test_run_from_yaml_uses_gtm_pattern_default_when_not_configured(tmp_path: Path):
    cfg = {
        "base_run": {
            "study_dir": str(tmp_path / "base"),
            "modifier": "base_mod",
        },
        "variation": {
            "name": "var_mod",
            "study_dir": str(tmp_path / "var"),
            "run_steps": ["hydro", "gtm"],
            "channel_modifications": [],
        },
        "observed_ec_dss": str(tmp_path / "obs.dss"),
        "ec_stations_csv": str(tmp_path / "stations.csv"),
    }

    with patch("dsm2ui.calib.calib_run.load_yaml_config", return_value=cfg), patch(
        "dsm2ui.calib.calib_run.read_ec_locations_csv", return_value=[]
    ), patch("dsm2ui.calib.calib_run._cfg_to_modifications", return_value=[]), patch(
        "dsm2ui.calib.calib_run.run_calibration_variation", return_value={}
    ) as mock_run:
        run_from_yaml("dummy.yml", run_variation=False)

    assert mock_run.call_args.kwargs["model_dss_pattern"] == "{modifier}_gtm.dss"
