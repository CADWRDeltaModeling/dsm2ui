# calib_run.py — DSM2 calibration variation runner and EC slope comparator.
"""
Workflow
--------
1. Define MANNING / DISPERSION modifications as regex → value mappings.
2. Set up a variation study directory from a base run (copies .inp/.bat,
   applies channel modifications, patches the GRID reference in hydro.inp).
3. Execute the model via the study's batch file.
4. Load model and observed EC time series from DSS files.
5. Apply the Godin tidal filter and compute linear-regression slopes.
6. Compare slopes between the base run and the variation run.

Typical directory layout (not hard-wired)::

    dsm2_studies/
        common_input/
            channel_std_delta_grid.inp   ← channel_inp_source
        studies/
            historical/                  ← base_study_dir
                config.inp
                hydro.inp
                qual_ec.inp
                DSM2_batch.bat
                output/
                    hist_fc_mss_qual.dss

    postprocessing/
        observed_data/
            ec_cal.dss                   ← observed_ec_dss
        location_info/
            calibration_ec_stations.csv  ← read_ec_locations_csv()

Example usage::

    from dsm2ui.calib.calib_run import (
        ChannelParamModification, read_ec_locations_csv,
        run_calibration_variation,
    )

    mods = [
        ChannelParamModification("MANNING",    r"^4[0-9]$", 0.030),
        ChannelParamModification("DISPERSION", r"^4[0-9]$", 400.0),
    ]

    locations = read_ec_locations_csv(
        r"D:/delta/postprocessing/location_info/calibration_ec_stations.csv"
    )

    result = run_calibration_variation(
        base_study_dir=r"D:/delta/dsm2_studies/studies/historical",
        var_study_dir=r"D:/delta/dsm2_studies/studies/historical_var1",
        channel_inp_source=r"D:/delta/dsm2_studies/common_input/channel_std_delta_grid.inp",
        modifications=mods,
        observed_ec_dss=r"D:/delta/postprocessing/observed_data/ec_cal.dss",
        ec_locations=locations,
        modifier="hist_fc_mss_var1",
        timewindow="01OCT2014 - 31DEC2024",
    )

    print(result["comparison"])
"""

from __future__ import annotations

import contextlib
import logging
import os
import re
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Union

import yaml

import numpy as np
import pandas as pd
from scipy.stats import linregress

import pyhecdss
from vtools.functions.filter import godin
from pydsm.functions.tsmath import (
    mse, nmse, rmse, nrmse,
    mean_error, nmean_error,
    nash_sutcliffe, kling_gupta_efficiency,
    percent_bias, rsr,
)

logger = logging.getLogger(__name__)

# ── Metric registry ───────────────────────────────────────────────────────────
# Maps YAML metric name → (callable(model_series, obs_series) → float, ideal_target)
# "slope" is handled as a special case via linregress.
_METRIC_REGISTRY: dict = {
    "slope":  (None,                    1.0),   # special: uses linregress
    "rmse":   (rmse,                    0.0),
    "nrmse":  (nrmse,                   0.0),
    "mse":    (mse,                     0.0),
    "nmse":   (nmse,                    0.0),
    "nse":    (nash_sutcliffe,          1.0),
    "kge":    (kling_gupta_efficiency,  1.0),
    "bias":   (mean_error,              0.0),
    "nbias":  (nmean_error,             0.0),
    "pbias":  (percent_bias,            0.0),
    "rsr":    (rsr,                     0.0),
}

VALID_METRICS = list(_METRIC_REGISTRY.keys())


# ── Data classes ──────────────────────────────────────────────────────────────


@dataclass
class ChannelParamModification:
    """A channel parameter modification for one named group.

    Attributes
    ----------
    param :
        Column to modify: ``"MANNING"`` or ``"DISPERSION"``.
    channels :
        Either a Python regex string applied via ``re.search`` to ``CHAN_NO``
        (e.g. ``r"^4[0-9]$"``), or a list of channel numbers / IDs
        (e.g. ``[10, 11, 12]`` or ``["10", "11", "12"]``).
        When a list is given it is converted to an exact-match regex.
    value :
        New float value to assign to the matched channels.
    name :
        Human-readable label for this group (used in logging).

    Examples
    --------
    >>> ChannelParamModification("DISPERSION", r"^4[0-9]$", 500.0, name="group_A")
    >>> ChannelParamModification("DISPERSION", [10, 11, 12], 400.0, name="group_B")
    """

    param: str
    channels: Union[str, List[Union[int, str]]]
    value: float
    name: str = ""

    def __post_init__(self):
        if self.param not in ("MANNING", "DISPERSION"):
            raise ValueError(
                f"param must be 'MANNING' or 'DISPERSION', got {self.param!r}"
            )
        # Normalise a list of channel IDs to an exact-match regex.
        if isinstance(self.channels, list):
            ids = [str(c) for c in self.channels]
            self.channels = "^(" + "|".join(re.escape(i) for i in ids) + ")$"

    @property
    def channel_regex(self) -> str:
        return self.channels if isinstance(self.channels, str) else ""

    # Keep backward-compat: old code passed channel_regex as positional arg
    @classmethod
    def from_regex(cls, param: str, channel_regex: str, value: float, name: str = ""):
        return cls(param=param, channels=channel_regex, value=value, name=name)


@dataclass
class ECLocation:
    """Pairing of DSM2 model B-part and observed B-part for one EC station.

    Attributes
    ----------
    station_name :
        Human-readable label (used in output tables).
    model_bpart :
        DSS B-part in the *model* output DSS file (e.g. ``"RSAC075"``).
    obs_bpart :
        DSS B-part in the *observed* DSS file (e.g. ``"MALUPPER"``).
    """

    station_name: str
    model_bpart: str
    obs_bpart: str


# ── Channel parameter modification ───────────────────────────────────────────


def apply_channel_modifications(
    channel_inp_file: str | Path,
    modifications: List[ChannelParamModification],
) -> int:
    """Apply MANNING/DISPERSION modifications to a DSM2 channel .inp file *in-place*.

    Uses direct text-based replacement: only the target column values on matched
    CHANNEL data rows are changed; every other byte of the file is preserved
    exactly (avoids floating-point reformatting that causes Fortran EOF errors).

    Returns
    -------
    int
        Total number of (channel, modification) matches applied.

    Raises
    ------
    ValueError
        If no ``CHANNEL`` table is found in the file.
    """
    channel_inp_file = Path(channel_inp_file)
    text = channel_inp_file.read_text()
    lines = text.splitlines(keepends=True)

    # ── Locate the CHANNEL section and its header ─────────────────────────
    chan_start = None
    header_idx = None
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.upper() == "CHANNEL" and chan_start is None:
            chan_start = i
        elif chan_start is not None and header_idx is None and stripped and not stripped.startswith("#"):
            header_idx = i
            break

    if chan_start is None or header_idx is None:
        raise ValueError(f"No CHANNEL table found in {channel_inp_file}")

    header_line = lines[header_idx].rstrip("\r\n")
    col_names = header_line.split()

    if "CHAN_NO" not in col_names:
        raise ValueError(f"CHAN_NO column not found in CHANNEL header: {header_line!r}")

    chan_no_idx = col_names.index("CHAN_NO")

    # Derive character ranges for each column from the header token positions
    def _col_ranges(hdr: str) -> dict:
        tokens = list(re.finditer(r'\S+', hdr))
        ranges = {}
        for k, tok in enumerate(tokens):
            col_start = tok.start()
            col_end = tokens[k + 1].start() if k + 1 < len(tokens) else len(hdr) + 20
            ranges[tok.group()] = (col_start, col_end)
        return ranges

    col_ranges = _col_ranges(header_line)

    # Pre-compile modification patterns
    compiled = [(re.compile(m.channels if isinstance(m.channels, str) else ""),
                 m) for m in modifications]

    total_modified = 0

    # ── Scan CHANNEL data rows (between header and END) ───────────────────
    i = header_idx + 1
    while i < len(lines):
        raw = lines[i]
        stripped = raw.strip()
        if stripped.upper() == "END":
            break
        if not stripped or stripped.startswith("#"):
            i += 1
            continue

        tokens = raw.rstrip("\r\n").split()
        if len(tokens) <= chan_no_idx:
            i += 1
            continue
        chan_no_str = tokens[chan_no_idx]

        for pattern, mod in compiled:
            if not pattern.search(chan_no_str):
                continue
            param = mod.param
            if param not in col_ranges:
                logger.warning("Column %s not found in CHANNEL header — skipping.", param)
                continue

            c_start, c_end = col_ranges[param]
            row_str = raw.rstrip("\r\n")
            eol = raw[len(row_str):]

            field_match = re.search(r'\S+', row_str[c_start:c_end])
            if field_match is None:
                continue

            abs_start = c_start + field_match.start()
            abs_end = c_start + field_match.end()
            new_val = f"{mod.value:g}"
            width = abs_end - abs_start
            new_val_padded = new_val.rjust(width) if len(new_val) <= width else new_val

            lines[i] = row_str[:abs_start] + new_val_padded + row_str[abs_end:] + eol
            total_modified += 1

        i += 1

    for _, mod in compiled:
        label = mod.name or mod.channels
        logger.info("Set %s=%.6g for channels in group %r", mod.param, mod.value, label)

    channel_inp_file.write_text("".join(lines))
    return total_modified


# ── Variation study set-up ────────────────────────────────────────────────────


def _copy_study_files(base_dir: Path, var_dir: Path) -> None:
    """Copy .inp, .bat, and .json files from *base_dir* to *var_dir*."""
    var_dir.mkdir(parents=True, exist_ok=True)
    (var_dir / "output").mkdir(exist_ok=True)
    for pattern in ("*.inp", "*.bat", "*.json"):
        for src in base_dir.glob(pattern):
            shutil.copy2(src, var_dir / src.name)


def _patch_channel_file_reference(
    inp_path: Path,
    channel_inp_name: str,
    new_channel_path: str,
) -> bool:
    """Replace the GRID reference to *channel_inp_name* with *new_channel_path*.

    Performs a single string replacement on the full text of *inp_path*,
    replacing the first line that ends with *channel_inp_name* (stripping any
    leading ``${ENVVAR}/`` prefix and trailing inline comment).

    Returns ``True`` if a replacement was made.
    """
    text = inp_path.read_text()
    # Match a non-whitespace token ending in channel_inp_name, optionally followed
    # by whitespace and an inline comment.
    pattern = re.compile(
        r"(?m)^([^\S\r\n]*)\S*"
        + re.escape(channel_inp_name)
        + r"([^\S\r\n]*(?:#[^\r\n]*)?)$",
    )
    new_text, count = pattern.subn(
        lambda m: m.group(1) + new_channel_path + m.group(2),
        text,
        count=1,
    )
    if count:
        inp_path.write_text(new_text)
        logger.info(
            "Patched channel file reference in %s → %s", inp_path.name, new_channel_path
        )
    else:
        logger.debug(
            "No reference to %s found in %s", channel_inp_name, inp_path.name
        )
    return bool(count)


def _write_filtered_batch(
    source_bat: Path,
    dest_bat: Path,
    run_steps: List[str],
    dsm2_bin_dir: Optional[str] = None,
) -> None:
    """Write a filtered copy of *source_bat* keeping only lines for *run_steps*.

    Each line in the batch file is kept if it contains one of the step keywords
    (e.g. ``"hydro"``, ``"qual"``).  Lines that appear to invoke a DSM2 module
    not in *run_steps* are dropped; all other lines (blank, SET, REM, @echo)
    are preserved.

    If *dsm2_bin_dir* is given, any ``..\\..\\bin\\<module>`` or similar relative
    binary reference is replaced with the absolute path from *dsm2_bin_dir*.
    """
    known_modules = {"hydro", "qual", "gtm", "ptm"}
    steps_lower = {s.lower() for s in run_steps}
    # Build a regex that matches the binary invocation token so we can replace it
    bin_pattern = re.compile(
        r"(\S*[/\\])(" + "|".join(re.escape(m) for m in known_modules) + r")(\.exe)?(?=\s|$)",
        re.IGNORECASE,
    )
    kept_lines = []
    for raw_line in source_bat.read_text().splitlines():
        line_lower = raw_line.strip().lower()
        module_match = None
        for mod in known_modules:
            if re.search(r"(?:^|[/\\])" + re.escape(mod) + r"(?:\.exe)?(?:\s|$)", line_lower):
                module_match = mod
                break
        if module_match is not None:
            if module_match not in steps_lower:
                continue  # drop this module step
            # If a selected module line is disabled via REM/@REM, re-enable it.
            raw_line = re.sub(r"^\s*@?rem\s+", "", raw_line, flags=re.IGNORECASE)
            if dsm2_bin_dir:
                # Replace the binary path prefix with the specified bin dir
                bin_dir = dsm2_bin_dir.rstrip("/\\")
                raw_line = bin_pattern.sub(
                    lambda m, bd=bin_dir: bd + "\\" + m.group(2) + (m.group(3) or ""),
                    raw_line,
                )
        kept_lines.append(raw_line)
    dest_bat.write_text("\n".join(kept_lines) + "\n")
    logger.info(
        "Wrote filtered batch %s (steps: %s%s)",
        dest_bat.name,
        ", ".join(run_steps),
        f", bin: {dsm2_bin_dir}" if dsm2_bin_dir else "",
    )


def _ensure_tempdir(config_inp: Path) -> None:
    """Check the TEMPDIR in *config_inp*; if it doesn't exist, replace it with
    a valid alternative so DSM2 can write its two-pass scratch files.

    DSM2 uses TEMPDIR for temporary binary buffers during the two-pass text
    read.  If the configured path is missing (e.g. a now-disconnected network
    drive), hydro exits immediately with IOSTAT=-3 (EOF) on the first grid file.
    """
    text = config_inp.read_text()
    m = re.search(r"(?m)^(TEMPDIR\s+)(\S+)", text)
    if not m:
        return
    configured = m.group(2)
    # Expand env vars (e.g. ${...}) — skip if still contains substitution
    if "${" in configured:
        return
    tempdir = Path(configured.replace("/", os.sep).replace("\\", os.sep))
    if tempdir.exists():
        return
    # Fall back to the system temp directory
    fallback = Path(os.environ.get("TEMP", os.environ.get("TMP", "C:/Temp")))
    fallback.mkdir(parents=True, exist_ok=True)
    fallback_str = str(fallback).replace("\\", "/")
    new_text = text[:m.start(2)] + fallback_str + text[m.end(2):]
    config_inp.write_text(new_text)
    logger.warning(
        "TEMPDIR %s does not exist — redirected to %s in %s",
        configured, fallback_str, config_inp.name,
    )


def _patch_envvars(config_inp: Path, overrides: dict) -> None:
    """Patch arbitrary ENVVAR values in *config_inp*.

    For each ``{NAME: new_value}`` pair, replaces the first occurrence of
    ``^NAME   <current_value>`` in the ENVVAR section.  Lines that don't match
    are left unchanged.  Logs every change made.
    """
    text = config_inp.read_text()
    for name, value in overrides.items():
        new_text, n = re.subn(
            r"(?m)^(" + re.escape(name) + r"\s+)\S+",
            lambda m, v=str(value): m.group(1) + v,
            text,
            count=1,
        )
        if n:
            logger.info("Set %s=%s in %s", name, value, config_inp.name)
            text = new_text
        else:
            logger.warning(
                "ENVVAR %s not found in %s — skipped.", name, config_inp.name
            )
    config_inp.write_text(text)


def _patch_dsm2inputdir(config_inp: Path, new_inputdir: str) -> None:
    """Redirect ``DSM2INPUTDIR`` in *config_inp* to *new_inputdir*.

    This allows the variation study to serve grid files from a local
    ``local_input/`` directory while keeping the GRID section of ``hydro.inp``
    unchanged (DSM2 requires ``${DSM2INPUTDIR}/filename`` resolution).
    """
    text = config_inp.read_text()
    new_text = re.sub(
        r"(?m)^(DSM2INPUTDIR\s+)\S+",
        lambda m: m.group(1) + new_inputdir,
        text,
        count=1,
    )
    config_inp.write_text(new_text)
    logger.info("Set DSM2INPUTDIR=%s in %s", new_inputdir, config_inp.name)


def _copy_timeseries_dss(
    config_inp: Path,
    var_dir: Path,
    base_dir: Optional[Path] = None,
) -> None:
    """Copy all DSS timeseries files into a local directory and redirect TSINPUTDIR.

    Reads the current ``TSINPUTDIR`` ENVVAR from *config_inp*, resolves the
    source directory, copies every ``*.dss`` file found there into
    ``<var_dir>/local_timeseries/``, and patches ``TSINPUTDIR`` in *config_inp*
    to point at the new absolute local path.

    This eliminates the DSS exclusive-lock contention that occurs when multiple
    parallel DSM2 runs simultaneously open the same shared timeseries DSS files.

    Resolution order for a relative ``TSINPUTDIR`` value:
    1. Relative to *base_dir* (the original base study dir — where the path
       was authored).  This is robust when eval dirs are nested at a
       different depth than the base study.
    2. Relative to *var_dir* (legacy fallback when *base_dir* is not given).

    If ``TSINPUTDIR`` is not found, contains an unresolved ``${...}``
    substitution, or the resolved directory does not exist in either location,
    a warning is logged and the function returns without modifying *config_inp*.
    """
    text = config_inp.read_text()
    m = re.search(r"(?m)^TSINPUTDIR\s+(\S+)", text)
    if not m:
        logger.warning("TSINPUTDIR not found in %s — timeseries copy skipped.", config_inp.name)
        return
    raw_value = m.group(1)
    if "${" in raw_value:
        logger.warning(
            "TSINPUTDIR=%r contains unresolved substitution — timeseries copy skipped.",
            raw_value,
        )
        return

    # Resolve the source directory.  For relative paths, try base_dir first
    # (the canonical origin of the path), then fall back to var_dir.
    src_dir: Optional[Path] = None
    raw_path = Path(raw_value)
    if raw_path.is_absolute():
        candidate = raw_path.resolve()
        if candidate.is_dir():
            src_dir = candidate
    else:
        for anchor in ([base_dir] if base_dir else []) + [var_dir]:
            candidate = (anchor / raw_value).resolve()
            if candidate.is_dir():
                src_dir = candidate
                break

    if src_dir is None:
        logger.warning(
            "TSINPUTDIR=%r could not be resolved to an existing directory "
            "(tried base_dir=%s, var_dir=%s) — timeseries copy skipped.",
            raw_value, base_dir, var_dir,
        )
        return
    local_ts_dir = var_dir / "local_timeseries"
    local_ts_dir.mkdir(parents=True, exist_ok=True)
    n_copied = 0
    for dss_src in src_dir.glob("*.dss"):
        shutil.copy2(dss_src, local_ts_dir / dss_src.name)
        n_copied += 1
    logger.info("Copied %d DSS file(s) from %s to %s", n_copied, src_dir, local_ts_dir.name)
    # Patch TSINPUTDIR to a relative path ("local_timeseries") rather than
    # an absolute path.  DSM2 validates absolute DSS paths at parse time and
    # raises a fatal error for any missing file, even ENVVARs that are never
    # referenced by hydro.inp / qual_ec.inp (e.g. DICUFILE-ECS).  Relative
    # paths are NOT validated at parse time, so optional / non-existent files
    # listed as ENVVARs are silently skipped — matching the historical study
    # behaviour where TSINPUTDIR is also a relative path.
    local_ts_str = local_ts_dir.name   # = "local_timeseries" (relative to var_dir CWD)
    new_text = re.sub(
        r"(?m)^(TSINPUTDIR\s+)\S+",
        lambda mo: mo.group(1) + local_ts_str,
        text,
        count=1,
    )
    config_inp.write_text(new_text)
    logger.info("Set TSINPUTDIR=%s in %s", local_ts_str, config_inp.name)


def _patch_dsm2_modifier(config_inp: Path, modifier: str) -> None:
    """Overwrite the ``DSM2MODIFIER`` ENVVAR value in *config_inp*."""
    text = config_inp.read_text()
    new_text = re.sub(
        r"(?m)^(DSM2MODIFIER\s+)\S+",
        lambda m: m.group(1) + modifier,
        text,
        count=1,
    )
    config_inp.write_text(new_text)
    logger.info("Set DSM2MODIFIER=%s in %s", modifier, config_inp.name)


def setup_variation(
    base_study_dir: str | Path,
    var_study_dir: str | Path,
    channel_inp_source: str | Path,
    modifications: List[ChannelParamModification],
    modifier: Optional[str] = None,
    channel_inp_name: str = "channel_std_delta_grid.inp",
    run_steps: Optional[List[str]] = None,
    dsm2_bin_dir: Optional[str] = None,
    envvar_overrides: Optional[dict] = None,
    copy_timeseries: bool = False,
) -> dict:
    """Set up a DSM2 variation study directory from a base run.

    Steps
    -----
    1. Copy all ``.inp``, ``.bat``, and ``.json`` files from *base_study_dir*
       to *var_study_dir* (the output directory is **not** copied).
    2. Copy *channel_inp_source* to ``var_study_dir / channel_inp_name``.
    3. Apply *modifications* to MANNING/DISPERSION values in the copied file.
    4. Patch the ``GRID`` reference in ``hydro.inp`` so it points to the local
       copy (absolute path), leaving all other ``${DSM2INPUTDIR}/...`` entries
       unchanged.
    5. If *modifier* is given, update ``DSM2MODIFIER`` in ``config.inp`` so
       that all output files carry the new study name.

    Parameters
    ----------
    base_study_dir :
        Folder containing ``hydro.inp``, ``config.inp``, ``DSM2_batch.bat``.
    var_study_dir :
        Destination for the variation study (created if absent).
    channel_inp_source :
        The base channel ``.inp`` file to copy and modify.
    modifications :
        MANNING/DISPERSION changes to apply.
    modifier :
        New ``DSM2MODIFIER`` value.  Output DSS will use this as its prefix.
    channel_inp_name :
        Filename for the local channel file copy.
    copy_timeseries :
        When ``True``, copy all DSS files referenced by ``TSINPUTDIR`` into a
        ``local_timeseries/`` subdirectory and redirect ``TSINPUTDIR`` to it.
        Required when running multiple parallel DSM2 studies that share the
        same timeseries directory, because DSS-6 opens files with an exclusive
        lock that prevents concurrent access.

    Returns
    -------
    dict
        Keys: ``study_dir``, ``channel_inp`` (absolute path to modified copy),
        ``batch_file``, ``modifier``.
    """
    base_dir = Path(base_study_dir).resolve()
    var_dir = Path(var_study_dir).resolve()

    _copy_study_files(base_dir, var_dir)

    # ── Create a local input directory that DSM2INPUTDIR can point to ──────
    # DSM2 requires ${ENVVAR}/filename.inp path resolution in GRID sections;
    # bare filenames and absolute paths do not work.  We create a
    # ``local_input/`` directory inside the variation study, put the modified
    # channel file there, copy (or hard-link) the other grid files from the
    # base DSM2INPUTDIR, and redirect DSM2INPUTDIR to this new directory.
    local_input_dir = var_dir / "local_input"
    local_input_dir.mkdir(parents=True, exist_ok=True)

    local_channel = local_input_dir / channel_inp_name
    shutil.copy2(channel_inp_source, local_channel)
    n_modified = apply_channel_modifications(local_channel, modifications)
    logger.info(
        "Applied %d channel modification(s) to %s", n_modified, local_channel.name
    )

    # Copy all OTHER .inp files from the base DSM2INPUTDIR so the GRID section
    # can still find reservoir, gate, etc.
    base_input_dir = Path(channel_inp_source).parent
    for src in base_input_dir.glob("*.inp"):
        dest = local_input_dir / src.name
        if not dest.exists() or src.name != channel_inp_name:
            shutil.copy2(src, dest)
    logger.info("Copied grid .inp files to %s", local_input_dir.name)

    # Patch hydro.inp to keep the ${DSM2INPUTDIR}/channel_std_delta_grid.inp
    # form UNCHANGED — we redirect DSM2INPUTDIR instead.
    # (No change to hydro.inp GRID section needed.)

    if modifier:
        config_inp = var_dir / "config.inp"
        if config_inp.exists():
            _patch_dsm2_modifier(config_inp, modifier)
            _patch_dsm2inputdir(config_inp, str(local_input_dir).replace("\\", "/"))
            _ensure_tempdir(config_inp)
            if envvar_overrides:
                _patch_envvars(config_inp, envvar_overrides)
            if copy_timeseries:
                _copy_timeseries_dss(config_inp, var_dir, base_dir=base_dir)
        else:
            logger.warning("config.inp not found in %s; DSM2MODIFIER not updated.", var_dir)

    base_bat = var_dir / "DSM2_batch.bat"
    if run_steps:
        filtered_bat = var_dir / "DSM2_batch_var.bat"
        _write_filtered_batch(base_bat, filtered_bat, run_steps, dsm2_bin_dir=dsm2_bin_dir)
        batch_file = filtered_bat
    else:
        batch_file = base_bat
    return {
        "study_dir": str(var_dir),
        "channel_inp": str(local_channel),
        "batch_file": str(batch_file),
        "modifier": modifier,
    }


# ── Model execution ───────────────────────────────────────────────────────────


def run_study(
    batch_file: str | Path,
    study_dir: Optional[str | Path] = None,
    timeout: Optional[int] = None,
    log_file: Optional[str | Path] = None,
) -> subprocess.CompletedProcess:
    """Execute a DSM2 batch file and return the completed-process result.

    Parameters
    ----------
    batch_file :
        Path to the ``.bat`` file (e.g. ``DSM2_batch.bat``).
    study_dir :
        Working directory for the subprocess.  Defaults to the folder that
        contains *batch_file*.
    timeout :
        Optional wall-clock timeout in seconds.
    log_file :
        If given, stdout and stderr are written to this file in real time
        (no buffering), allowing the run to be monitored with ``tail -f``.
        The file is created/truncated at the start of the run.

    Returns
    -------
    subprocess.CompletedProcess
        Inspect ``.returncode`` for diagnostics.
        A non-zero return code is logged as an error but does *not* raise.
    """
    batch_file = Path(batch_file)
    cwd = Path(study_dir).resolve() if study_dir else batch_file.parent
    logger.info("Executing %s in %s", batch_file.name, cwd)

    if log_file is not None:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info("Streaming model output to %s", log_path)
        with log_path.open("w", buffering=1) as fh:
            proc = subprocess.run(
                [str(batch_file)],
                cwd=str(cwd),
                shell=True,
                stdout=fh,
                stderr=subprocess.STDOUT,
                timeout=timeout,
            )
        result = proc
    else:
        result = subprocess.run(
            [str(batch_file)],
            cwd=str(cwd),
            shell=True,
            capture_output=True,
            text=True,
            timeout=timeout,
        )

    if result.returncode != 0:
        logger.error("DSM2 run failed (returncode=%d).", result.returncode)
        if log_file is None and hasattr(result, "stderr") and result.stderr:
            logger.error("STDERR:\n%s", result.stderr[-4000:])
    else:
        logger.info("DSM2 run completed successfully (returncode=0).")
    return result


# ── DSS utilities ─────────────────────────────────────────────────────────────


def load_dss_ts(
    dss_file: str | Path,
    b_part: str,
    c_part: str = "EC",
) -> Optional[pd.DataFrame]:
    """Load a time series from a DSS file matching ``//<b_part>/<c_part>////*``.

    Uses ``pyhecdss.get_ts`` (generator) and merges multiple record segments
    via :meth:`pandas.DataFrame.combine_first`.

    Parameters
    ----------
    dss_file :
        Path to the ``.dss`` file.
    b_part :
        Station identifier (DSS B-part).
    c_part :
        Variable type (DSS C-part), default ``"EC"``.

    Returns
    -------
    pandas.DataFrame or None
        Single-column DataFrame with a DatetimeIndex and column named
        *b_part*, or ``None`` if no matching records are found.
    """
    pathname = f"//{b_part.upper()}/{c_part.upper()}////"
    frames = []
    if not Path(str(dss_file)).exists():
        logger.warning("DSS file not found: %s", dss_file)
        return None
    generator = pyhecdss.get_ts(str(dss_file), pathname)
    with contextlib.closing(generator) as dfgen:
        for rec in dfgen:
            df = rec.data
            if df is not None and not df.empty:
                frames.append(df)

    if not frames:
        logger.warning("No data for %s in %s", pathname, os.path.basename(str(dss_file)))
        return None

    result = frames[0]
    for extra in frames[1:]:
        result = result.combine_first(extra)

    result.index = pd.to_datetime(result.index)
    result.columns = [b_part]
    return result


def _apply_timewindow(
    df: Optional[pd.DataFrame], timewindow: Optional[str]
) -> Optional[pd.DataFrame]:
    """Slice *df* to the given time window string.

    Accepts common DSM2 formats such as ``"01OCT2014 - 31DEC2024"`` (separated
    by ``" - "``) or ISO formats separated by ``":"`` or ``" : "``.
    """
    if timewindow is None or df is None:
        return df
    for sep in (" - ", " : ", " to ", ":"):
        if sep in timewindow:
            parts = [p.strip() for p in timewindow.split(sep, 1)]
            if len(parts) == 2:
                try:
                    start = pd.to_datetime(parts[0])
                    end = pd.to_datetime(parts[1])
                    return df.loc[start:end]
                except Exception:
                    continue
    logger.warning("Could not parse timewindow %r — returning full series.", timewindow)
    return df


# ── EC metric computation ─────────────────────────────────────────────────────


def compute_ec_metric(
    model_dss: str | Path,
    observed_dss: str | Path,
    locations: List[ECLocation],
    timewindow: Optional[str] = None,
    c_part: str = "EC",
    min_points: int = 10,
    metric: str = "slope",
) -> pd.DataFrame:
    """Compute a calibration metric between Godin-filtered model and observed EC.

    Supported *metric* values and their ideal target:

    ======  =======  =============================================
    Name    Target   Description
    ======  =======  =============================================
    slope   1.0      Linear-regression slope (obs→model)
    rmse    0.0      Root mean squared error
    nrmse   0.0      RMSE normalised by obs mean
    mse     0.0      Mean squared error
    nmse    0.0      MSE normalised by obs mean
    nse     1.0      Nash-Sutcliffe efficiency
    kge     1.0      Kling-Gupta efficiency
    bias    0.0      Mean error (model − obs)
    nbias   0.0      Mean error normalised by obs mean
    pbias   0.0      Percent bias
    rsr     0.0      RMSE / std(obs)
    ======  =======  =============================================

    Parameters
    ----------
    model_dss :
        Model output DSS file.
    observed_dss :
        Observed EC DSS file.
    locations :
        List of :class:`ECLocation` objects.
    timewindow :
        Optional time filter, e.g. ``"01OCT2014 - 31DEC2024"``.
    c_part :
        DSS C-part for the EC variable.  Default ``"EC"``.
    min_points :
        Minimum aligned, non-NaN data points required.
        Stations with fewer are skipped (``metric_value = NaN``).
    metric :
        Name of the metric to compute.  Must be one of :data:`VALID_METRICS`.

    Returns
    -------
    pandas.DataFrame
        One row per location with columns:
        ``station_name, model_bpart, obs_bpart, metric_value, metric_target, n_points``.
        For ``metric="slope"`` the additional linregress columns
        ``(intercept, r_value, r_squared, p_value)`` are also included.
    """
    if metric not in _METRIC_REGISTRY:
        raise ValueError(
            f"Unknown metric {metric!r}.  Valid choices: {VALID_METRICS}"
        )
    metric_fn, metric_target = _METRIC_REGISTRY[metric]

    rows = []
    for loc in locations:
        model_ts = load_dss_ts(model_dss, loc.model_bpart, c_part)
        obs_ts = load_dss_ts(observed_dss, loc.obs_bpart, c_part)

        if model_ts is None or obs_ts is None:
            logger.warning("Skipping %s — missing data in one or both DSS files.", loc.station_name)
            rows.append(_empty_metric_row(loc, metric_target))
            continue

        model_ts = _apply_timewindow(model_ts, timewindow)
        obs_ts = _apply_timewindow(obs_ts, timewindow)

        model_g = godin(model_ts)
        obs_g = godin(obs_ts)

        combined = pd.concat([model_g, obs_g], axis=1, join="inner").dropna()
        if len(combined) < min_points:
            logger.warning(
                "Too few aligned points for %s (%d < %d) — skipping.",
                loc.station_name, len(combined), min_points,
            )
            rows.append(_empty_metric_row(loc, metric_target))
            continue

        model_arr = combined.iloc[:, 0].to_numpy(dtype=float)
        obs_arr   = combined.iloc[:, 1].to_numpy(dtype=float)

        row: dict = {
            "station_name": loc.station_name,
            "model_bpart": loc.model_bpart,
            "obs_bpart": loc.obs_bpart,
            "metric_target": metric_target,
            "n_points": len(combined),
        }

        if metric == "slope":
            reg = linregress(obs_arr, model_arr)
            row["metric_value"] = float(reg.slope)
            row["intercept"]  = float(reg.intercept)
            row["r_value"]    = float(reg.rvalue)
            row["r_squared"]  = float(reg.rvalue ** 2)
            row["p_value"]    = float(reg.pvalue)
            logger.info(
                "%-20s  slope=%.4f  R²=%.4f  n=%d",
                loc.station_name, reg.slope, reg.rvalue ** 2, len(combined),
            )
        else:
            model_s = pd.Series(model_arr)
            obs_s   = pd.Series(obs_arr)
            row["metric_value"] = float(metric_fn(model_s, obs_s))
            logger.info(
                "%-20s  %s=%.4f  n=%d",
                loc.station_name, metric, row["metric_value"], len(combined),
            )

        rows.append(row)

    return pd.DataFrame(rows)


def _empty_metric_row(loc: ECLocation, metric_target: float = 1.0) -> dict:
    return {
        "station_name": loc.station_name,
        "model_bpart": loc.model_bpart,
        "obs_bpart": loc.obs_bpart,
        "metric_value": np.nan,
        "metric_target": metric_target,
        "n_points": 0,
    }


def compute_ec_slopes(
    model_dss: str | Path,
    observed_dss: str | Path,
    locations: List[ECLocation],
    timewindow: Optional[str] = None,
    c_part: str = "EC",
    min_points: int = 10,
) -> pd.DataFrame:
    """Compute linear-regression slopes of Godin-filtered model vs observed EC.

    For each location:

    1. Load model time series from *model_dss* (B-part = ``location.model_bpart``).
    2. Load observed time series from *observed_dss* (B-part = ``location.obs_bpart``).
    3. Slice both series to *timewindow*.
    4. Apply the Godin tidal filter (``vtools.functions.filter.godin``).
    5. Inner-join on the common DatetimeIndex, drop NaN pairs.
    6. Run ``scipy.stats.linregress(observed, model)`` to get the slope.

    Parameters
    ----------
    model_dss :
        Model output DSS file (e.g. ``output/hist_fc_mss_qual.dss``).
    observed_dss :
        Observed EC DSS file (e.g. ``observed_data/ec_cal.dss``).
    locations :
        List of :class:`ECLocation` objects.
    timewindow :
        Optional time filter, e.g. ``"01OCT2014 - 31DEC2024"``.
    c_part :
        DSS C-part for the EC variable.  Default ``"EC"``.
    min_points :
        Minimum aligned, non-NaN data points required to compute a slope.
        Stations with fewer points are skipped (slope = NaN).

    Returns
    -------
    pandas.DataFrame
        One row per location with columns:
        ``station_name, model_bpart, obs_bpart,
        slope, intercept, r_value, r_squared, p_value, n_points``.
    """
    rows = []
    for loc in locations:
        model_ts = load_dss_ts(model_dss, loc.model_bpart, c_part)
        obs_ts = load_dss_ts(observed_dss, loc.obs_bpart, c_part)

        if model_ts is None or obs_ts is None:
            logger.warning("Skipping %s — missing data in one or both DSS files.", loc.station_name)
            rows.append(_empty_slope_row(loc))
            continue

        model_ts = _apply_timewindow(model_ts, timewindow)
        obs_ts = _apply_timewindow(obs_ts, timewindow)

        model_g = godin(model_ts)
        obs_g = godin(obs_ts)

        combined = pd.concat([model_g, obs_g], axis=1, join="inner").dropna()
        if len(combined) < min_points:
            logger.warning(
                "Too few aligned points for %s (%d < %d) — skipping.",
                loc.station_name, len(combined), min_points,
            )
            rows.append(_empty_slope_row(loc))
            continue

        x = combined.iloc[:, 1].to_numpy(dtype=float)  # observed
        y = combined.iloc[:, 0].to_numpy(dtype=float)  # model
        reg = linregress(x, y)

        rows.append(
            {
                "station_name": loc.station_name,
                "model_bpart": loc.model_bpart,
                "obs_bpart": loc.obs_bpart,
                "slope": float(reg.slope),
                "intercept": float(reg.intercept),
                "r_value": float(reg.rvalue),
                "r_squared": float(reg.rvalue**2),
                "p_value": float(reg.pvalue),
                "n_points": len(combined),
            }
        )
        logger.info(
            "%-20s  slope=%.4f  R²=%.4f  n=%d",
            loc.station_name, reg.slope, reg.rvalue**2, len(combined),
        )

    return pd.DataFrame(rows)


def _empty_slope_row(loc: ECLocation) -> dict:
    return {
        "station_name": loc.station_name,
        "model_bpart": loc.model_bpart,
        "obs_bpart": loc.obs_bpart,
        "slope": np.nan,
        "intercept": np.nan,
        "r_value": np.nan,
        "r_squared": np.nan,
        "p_value": np.nan,
        "n_points": 0,
    }


# ── EC locations from CSV ─────────────────────────────────────────────────────


def read_ec_locations_csv(
    csv_path: str | Path,
    dsm2_col: str = "dsm2_id",
    obs_col: str = "obs_station_id",
    name_col: str = "station_name",
    active_stations: Optional[List[str]] = None,
) -> List[ECLocation]:
    """Read EC station locations from a calibration CSV file.

    The CSV must have at least the columns ``dsm2_id`` and ``obs_station_id``,
    matching the format of
    ``location_info/calibration_ec_stations.csv``.

    Parameters
    ----------
    csv_path :
        Path to the station CSV file.
    dsm2_col :
        Column containing the DSM2 / model DSS B-part.
    obs_col :
        Column containing the observed DSS B-part.
    name_col :
        Column containing a human-readable station name.
    active_stations :
        Optional list of ``dsm2_id`` values to include.  When given, only
        those stations are returned (order preserved).  When ``None``, all
        rows are returned.

    Returns
    -------
    list of :class:`ECLocation`
    """
    df = pd.read_csv(csv_path)
    if active_stations is not None:
        keep = [s.upper() for s in active_stations]
        df = df[df[dsm2_col].str.upper().isin(keep)]
        # preserve the order the caller requested
        order = {s.upper(): i for i, s in enumerate(active_stations)}
        df = df.assign(_order=df[dsm2_col].str.upper().map(order)).sort_values("_order").drop(columns="_order")
    locations = []
    for _, row in df.iterrows():
        locations.append(
            ECLocation(
                station_name=str(row.get(name_col, row[dsm2_col])),
                model_bpart=str(row[dsm2_col]),
                obs_bpart=str(row[obs_col]),
            )
        )
    return locations


# ── Slope comparison ──────────────────────────────────────────────────────────


def compare_slopes(
    base_slopes: pd.DataFrame,
    var_slopes: pd.DataFrame,
    base_label: str = "base",
    var_label: str = "variation",
) -> pd.DataFrame:
    """Compare EC regression slopes between a base run and a variation run.

    Outer-joins the two slope DataFrames on ``station_name``, computes the
    absolute and percentage change in slope.

    Parameters
    ----------
    base_slopes, var_slopes :
        DataFrames returned by :func:`compute_ec_slopes`.
    base_label, var_label :
        Column suffixes to identify each run.

    Returns
    -------
    pandas.DataFrame
        Columns:
        ``station_name``,
        ``slope_{base_label}``,
        ``r_sq_{base_label}``,
        ``slope_{var_label}``,
        ``r_sq_{var_label}``,
        ``delta_slope`` (var − base),
        ``pct_change_slope`` (100 × delta / |base|).
        Sorted by |delta_slope| descending.
    """
    keep = ["station_name", "slope", "r_squared", "n_points"]
    base_s = base_slopes[keep].rename(
        columns={
            "slope": f"slope_{base_label}",
            "r_squared": f"r_sq_{base_label}",
            "n_points": f"n_{base_label}",
        }
    )
    var_s = var_slopes[keep].rename(
        columns={
            "slope": f"slope_{var_label}",
            "r_squared": f"r_sq_{var_label}",
            "n_points": f"n_{var_label}",
        }
    )
    merged = base_s.merge(var_s, on="station_name", how="outer")
    sb = merged[f"slope_{base_label}"]
    sv = merged[f"slope_{var_label}"]
    merged["delta_slope"] = sv - sb
    merged["pct_change_slope"] = np.where(
        sb.abs() > 1e-9,
        (sv - sb) / sb.abs() * 100.0,
        np.nan,
    )
    return (
        merged.sort_values("delta_slope", key=abs, ascending=False)
        .reset_index(drop=True)
    )


# ── Station result plotting ───────────────────────────────────────────────────


def _extract_channel_list(channels_regex: str) -> str:
    """Return a compact human-readable channel list from the synthetic regex."""
    nums = re.findall(r"\d+", channels_regex)
    if len(nums) > 7:
        return ", ".join(nums[:7]) + f" … +{len(nums) - 7}"
    return ", ".join(nums)


def plot_station_results(
    base_dss: str | Path,
    var_dss: str | Path,
    observed_dss: str | Path,
    locations: List[ECLocation],
    base_slopes: pd.DataFrame,
    var_slopes: pd.DataFrame,
    modifications: List[ChannelParamModification],
    output_dir: str | Path,
    timewindow: Optional[str] = None,
    base_label: str = "base",
    var_label: str = "variation",
    c_part: str = "EC",
) -> List[Path]:
    """Generate one diagnostic PNG per EC station.

    Each figure has three panels:

    * **Top (full width)** — Godin-filtered time series: observed, base, variation.
    * **Bottom-left** — scatter plot with regression lines (model vs observed),
      one series per run.  A dashed 1:1 reference line is included.
    * **Bottom-right** — table of channel parameter modifications from the YAML.

    Parameters
    ----------
    base_dss, var_dss, observed_dss :
        DSS files for the base run, variation run, and observed EC.
    locations :
        EC station definitions (:class:`ECLocation`).
    base_slopes, var_slopes :
        Slope DataFrames from :func:`compute_ec_slopes`.
    modifications :
        Channel modifications applied to the variation.
    output_dir :
        Directory where PNG files are written.
    timewindow :
        Optional time-window string for slicing the time-series panel.
    base_label, var_label :
        Short labels used in plot legends / titles.
    c_part :
        DSS C-part for EC.  Default ``"EC"``.

    Returns
    -------
    list of Path
        Paths of the PNG files written (one per station).
    """
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.gridspec as gridspec
    except ImportError:
        raise ImportError(
            "matplotlib is required for plotting.  Install with: pip install matplotlib"
        )

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Pre-index slope DataFrames for O(1) lookup
    b_idx = base_slopes.set_index("station_name") if not base_slopes.empty else pd.DataFrame()
    v_idx = var_slopes.set_index("station_name") if not var_slopes.empty else pd.DataFrame()

    # Build modification table rows once (shared across all station figures)
    mod_rows = [
        [
            mod.name or f"group_{i}",
            mod.param,
            _extract_channel_list(mod.channels),
            f"{mod.value:.4g}",
        ]
        for i, mod in enumerate(modifications)
    ]

    _COLORS = {"obs": "black", "base": "#2166ac", "var": "#d6604d"}
    saved: List[Path] = []

    for loc in locations:
        logger.info("Plotting %s …", loc.station_name)

        # ── Load and filter time series ───────────────────────────────────────
        obs_raw = load_dss_ts(observed_dss, loc.obs_bpart, c_part)
        base_raw = load_dss_ts(base_dss, loc.model_bpart, c_part)
        var_raw = load_dss_ts(var_dss, loc.model_bpart, c_part)

        if obs_raw is None:
            logger.warning("No observed data for %s — skipping plot.", loc.station_name)
            continue

        obs_g = godin(_apply_timewindow(obs_raw, timewindow))
        base_g = godin(_apply_timewindow(base_raw, timewindow)) if base_raw is not None else None
        var_g = godin(_apply_timewindow(var_raw, timewindow)) if var_raw is not None else None

        # ── Slope rows ────────────────────────────────────────────────────────
        # Use .iloc[0] when the station name appears multiple times in the index
        # (e.g. two dsm2_ids with the same station_name like ROLD024 / BAC).
        def _first_row(idx, name):
            if name not in idx.index:
                return None
            row = idx.loc[name]
            return row.iloc[0] if isinstance(row, pd.DataFrame) else row

        b_row = _first_row(b_idx, loc.station_name)
        v_row = _first_row(v_idx, loc.station_name)

        # ── Figure layout ─────────────────────────────────────────────────────
        fig = plt.figure(figsize=(14, 10))
        gs = gridspec.GridSpec(
            2, 2,
            figure=fig,
            height_ratios=[1.1, 1.0],
            hspace=0.38,
            wspace=0.28,
        )
        ax_ts = fig.add_subplot(gs[0, :])   # top — full width
        ax_sc = fig.add_subplot(gs[1, 0])   # bottom-left — scatter
        ax_tb = fig.add_subplot(gs[1, 1])   # bottom-right — table

        # ── Time series panel ─────────────────────────────────────────────────
        if obs_g is not None:
            ax_ts.plot(
                obs_g.index, obs_g.iloc[:, 0],
                color=_COLORS["obs"], lw=1.0, label="Observed", zorder=3,
            )
        if base_g is not None:
            ax_ts.plot(
                base_g.index, base_g.iloc[:, 0],
                color=_COLORS["base"], lw=0.9, alpha=0.85,
                label=f"Base  ({base_label})", zorder=2,
            )
        if var_g is not None:
            ax_ts.plot(
                var_g.index, var_g.iloc[:, 0],
                color=_COLORS["var"], lw=0.9, alpha=0.85,
                label=f"Variation  ({var_label})", zorder=2,
            )
        ax_ts.set_ylabel("EC  (µS/cm)")
        ax_ts.set_title(
            f"{loc.station_name}  ({loc.model_bpart})", fontweight="bold", fontsize=11,
        )
        ax_ts.legend(loc="upper right", fontsize=8, framealpha=0.85)
        ax_ts.grid(True, alpha=0.3)

        # ── Scatter + regression panel ────────────────────────────────────────
        if obs_g is not None:
            obs_vals = obs_g.iloc[:, 0]
            for ts_g, slope_row, color, label in [
                (base_g, b_row, _COLORS["base"], f"Base ({base_label})"),
                (var_g, v_row, _COLORS["var"], f"Variation ({var_label})"),
            ]:
                if ts_g is None or slope_row is None:
                    continue
                combined = pd.concat(
                    [ts_g.iloc[:, 0], obs_vals], axis=1, join="inner"
                ).dropna()
                if combined.empty:
                    continue
                y_all = combined.iloc[:, 0].to_numpy(dtype=float)  # model
                x_all = combined.iloc[:, 1].to_numpy(dtype=float)  # observed

                # Subsample for scatter (≤3 000 points keeps the PNG fast to render)
                step = max(1, len(x_all) // 3000)
                ax_sc.scatter(
                    x_all[::step], y_all[::step],
                    s=3, color=color, alpha=0.25, linewidths=0,
                )

                sl = float(slope_row["slope"])
                ic = float(slope_row["intercept"])
                r2 = float(slope_row["r_squared"])
                x_line = np.array([x_all.min(), x_all.max()])
                ax_sc.plot(
                    x_line, sl * x_line + ic,
                    color=color, lw=2,
                    label=f"{label}\nslope={sl:.3f}  R²={r2:.3f}",
                )

        # 1:1 reference line
        all_x = ax_sc.get_xlim()
        all_y = ax_sc.get_ylim()
        lo = min(all_x[0], all_y[0])
        hi = max(all_x[1], all_y[1])
        ax_sc.plot([lo, hi], [lo, hi], "k--", lw=1.0, alpha=0.5, label="1 : 1")
        ax_sc.set_xlim(lo, hi)
        ax_sc.set_ylim(lo, hi)
        ax_sc.set_xlabel("Observed EC  (µS/cm)")
        ax_sc.set_ylabel("Model EC  (µS/cm)")
        ax_sc.set_title("Model vs Observed  (Godin-filtered)")
        ax_sc.legend(fontsize=7.5, framealpha=0.85)
        ax_sc.grid(True, alpha=0.3)
        ax_sc.set_aspect("equal", adjustable="box")

        # ── Modifications table panel ─────────────────────────────────────────
        ax_tb.axis("off")
        col_labels = ["Group", "Param", "Channels", "Value  (ft²/s)"]
        tbl = ax_tb.table(
            cellText=mod_rows,
            colLabels=col_labels,
            cellLoc="left",
            loc="center",
            bbox=[0.0, 0.0, 1.0, 1.0],
        )
        tbl.auto_set_font_size(False)
        tbl.set_fontsize(8)
        tbl.auto_set_column_width(list(range(len(col_labels))))
        # Header styling
        for j in range(len(col_labels)):
            cell = tbl[0, j]
            cell.set_facecolor("#404040")
            cell.set_text_props(color="white", fontweight="bold")
        # Alternating row shading
        for i in range(1, len(mod_rows) + 1):
            fc = "#f0f0f0" if i % 2 == 0 else "white"
            for j in range(len(col_labels)):
                tbl[i, j].set_facecolor(fc)
        ax_tb.set_title("Channel Modifications", fontweight="bold", pad=6)

        # ── Save ──────────────────────────────────────────────────────────────
        out_path = out_dir / f"{loc.model_bpart}.png"
        fig.savefig(out_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        logger.info("Saved %s", out_path)
        saved.append(out_path)

    return saved


def plot_from_yaml(yaml_path: str | Path) -> List[Path]:
    """Generate station diagnostic plots from a YAML config without re-running the model.

    Reads DSS paths and station definitions from the YAML, recomputes slopes
    from existing output files, then calls :func:`plot_station_results`.

    Parameters
    ----------
    yaml_path :
        Path to the calibration YAML config file.

    Returns
    -------
    list of Path
        Paths of the PNG files written.
    """
    cfg = load_yaml_config(yaml_path)
    base = cfg["base_run"]
    var = cfg["variation"]
    metrics_cfg = cfg.get("metrics", {})

    base_dir = Path(base["study_dir"])
    var_dir = Path(var["study_dir"])
    base_modifier = base["modifier"]
    var_modifier = var["name"]
    model_dss_pattern = _resolve_model_dss_pattern(base, var.get("run_steps"))
    timewindow = metrics_cfg.get("timewindow")

    base_dss = base_dir / "output" / model_dss_pattern.format(modifier=base_modifier)
    var_dss = var_dir / "output" / model_dss_pattern.format(modifier=var_modifier)
    observed_dss = cfg["observed_ec_dss"]

    active_stations = cfg.get("active_stations")
    ec_locations = read_ec_locations_csv(
        cfg["ec_stations_csv"], active_stations=active_stations
    )
    modifications = _cfg_to_modifications(cfg)

    logger.info("Computing base slopes for plots …")
    base_slopes = compute_ec_slopes(base_dss, observed_dss, ec_locations, timewindow)
    logger.info("Computing variation slopes for plots …")
    var_slopes = compute_ec_slopes(var_dss, observed_dss, ec_locations, timewindow)

    out_dir = var_dir / "plots"
    return plot_station_results(
        base_dss=base_dss,
        var_dss=var_dss,
        observed_dss=observed_dss,
        locations=ec_locations,
        base_slopes=base_slopes,
        var_slopes=var_slopes,
        modifications=modifications,
        output_dir=out_dir,
        timewindow=timewindow,
        base_label=base_modifier,
        var_label=var_modifier,
    )


# ── High-level orchestration ──────────────────────────────────────────────────


def _read_modifier_from_config(config_inp: Path) -> Optional[str]:
    """Extract the ``DSM2MODIFIER`` value from a ``config.inp`` ENVVAR section."""
    if not config_inp.exists():
        return None
    m = re.search(r"(?m)^DSM2MODIFIER\s+(\S+)", config_inp.read_text())
    return m.group(1) if m else None


def run_calibration_variation(
    base_study_dir: str | Path,
    var_study_dir: str | Path,
    channel_inp_source: str | Path,
    modifications: List[ChannelParamModification],
    observed_ec_dss: str | Path,
    ec_locations: List[ECLocation],
    modifier: str,
    model_dss_pattern: str = "{modifier}_qual.dss",
    timewindow: Optional[str] = None,
    base_modifier: Optional[str] = None,
    channel_inp_name: str = "channel_std_delta_grid.inp",
    run_model: bool = True,
    run_steps: Optional[List[str]] = None,
    log_file: Optional[str | Path] = None,
    dsm2_bin_dir: Optional[str] = None,
    envvar_overrides: Optional[dict] = None,
    config_to_copy: Optional[str | Path] = None,
    copy_timeseries: bool = False,
) -> dict:
    """End-to-end calibration variation: set up, optionally run, compute slopes.

    Parameters
    ----------
    base_study_dir :
        Base historical study folder (contains ``hydro.inp``, ``DSM2_batch.bat``).
        **Example:** ``D:/delta/dsm2_studies/studies/historical``
    var_study_dir :
        New variation study folder (created if absent).
    channel_inp_source :
        Base channel ``.inp`` to copy and modify.
        **Example:** ``D:/delta/dsm2_studies/common_input/channel_std_delta_grid.inp``
    modifications :
        MANNING / DISPERSION changes; see :class:`ChannelParamModification`.
    observed_ec_dss :
        Observed EC DSS file.
        **Example:** ``D:/delta/postprocessing/observed_data/ec_cal.dss``
    ec_locations :
        EC station pairings; use :func:`read_ec_locations_csv` to build this list.
    modifier :
        ``DSM2MODIFIER`` for the variation run (drives output DSS filename).
    model_dss_pattern :
        Template for the model output DSS filename.  ``{modifier}`` is
        substituted with the actual modifier.  Default: ``"{modifier}_qual.dss"``.
    timewindow :
        Metric time window, e.g. ``"01OCT2014 - 31DEC2024"``.
    base_modifier :
        ``DSM2MODIFIER`` of the base run.  Auto-inferred from ``config.inp``
        when ``None``.
    channel_inp_name :
        Local filename for the modified channel ``.inp`` copy.
    run_model :
        Set to ``False`` to skip model execution and only compute metrics
        from existing output (useful when re-analyzing a completed run).

    Returns
    -------
    dict with keys:

    ``variation_info``
        Output of :func:`setup_variation`.
    ``run_result``
        :class:`subprocess.CompletedProcess` (or ``None`` if *run_model* is False).
    ``base_slopes``
        :class:`pandas.DataFrame` from :func:`compute_ec_slopes` for the base run.
    ``var_slopes``
        :class:`pandas.DataFrame` from :func:`compute_ec_slopes` for the variation.
    ``comparison``
        :class:`pandas.DataFrame` from :func:`compare_slopes`.
    """
    base_dir = Path(base_study_dir).resolve()
    var_dir = Path(var_study_dir).resolve()

    if base_modifier is None:
        base_modifier = _read_modifier_from_config(base_dir / "config.inp")
        if base_modifier is None:
            raise ValueError(
                "Could not infer base DSM2MODIFIER from config.inp; pass base_modifier explicitly."
            )
        logger.info("Inferred base_modifier=%s from config.inp", base_modifier)

    variation_info = setup_variation(
        base_dir,
        var_dir,
        channel_inp_source,
        modifications,
        modifier=modifier,
        channel_inp_name=channel_inp_name,
        run_steps=run_steps,
        dsm2_bin_dir=dsm2_bin_dir,
        envvar_overrides=envvar_overrides,
        copy_timeseries=copy_timeseries,
    )

    # Copy config snapshot immediately after the directory is created,
    # before the model starts — prevents parallel runs from overwriting
    # each other's config copy.
    if config_to_copy is not None:
        dest = var_dir / Path(config_to_copy).name
        shutil.copy2(config_to_copy, dest)
        logger.info("Config snapshot saved to %s", dest)

    run_result = None
    if run_model:
        run_result = run_study(variation_info["batch_file"], var_dir, log_file=log_file)

    base_model_dss = base_dir / "output" / model_dss_pattern.format(modifier=base_modifier)
    var_model_dss = var_dir / "output" / model_dss_pattern.format(modifier=modifier)

    logger.info("Computing base slopes from %s", base_model_dss.name)
    base_slopes = compute_ec_slopes(
        base_model_dss, observed_ec_dss, ec_locations, timewindow
    )

    logger.info("Computing variation slopes from %s", var_model_dss.name)
    var_slopes = compute_ec_slopes(
        var_model_dss, observed_ec_dss, ec_locations, timewindow
    )

    comparison = compare_slopes(
        base_slopes, var_slopes, base_label="base", var_label=modifier
    )

    return {
        "variation_info": variation_info,
        "run_result": run_result,
        "base_slopes": base_slopes,
        "var_slopes": var_slopes,
        "comparison": comparison,
    }


# ── YAML configuration loading ────────────────────────────────────────────────


def load_yaml_config(yaml_path: str | Path) -> dict:
    """Load and validate a calibration YAML configuration file.

    Returns the raw config dict; use :func:`run_from_yaml` for end-to-end
    execution.
    """
    yaml_path = Path(yaml_path)
    with yaml_path.open("r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)

    required_top = {"base_run", "variation", "observed_ec_dss", "ec_stations_csv"}
    missing = required_top - set(cfg.keys())
    if missing:
        raise ValueError(f"YAML config missing required keys: {missing}")

    base = cfg["base_run"]
    for key in ("study_dir", "modifier"):
        if key not in base:
            raise ValueError(f"base_run is missing required key: {key!r}")

    var = cfg["variation"]
    for key in ("name", "study_dir", "channel_modifications"):
        if key not in var:
            raise ValueError(f"variation is missing required key: {key!r}")

    for i, mod in enumerate(var["channel_modifications"]):
        for key in ("param", "channels", "value"):
            if key not in mod:
                raise ValueError(
                    f"variation.channel_modifications[{i}] missing key {key!r}"
                )

    # Optional top-level keys accepted without error
    # (station_weights, optimizer — used by calib_optimize.py)

    return cfg


def _cfg_to_modifications(cfg: dict) -> List[ChannelParamModification]:
    mods = []
    for entry in cfg["variation"]["channel_modifications"]:
        mods.append(
            ChannelParamModification(
                param=entry["param"].upper(),
                channels=entry["channels"],
                value=float(entry["value"]),
                name=entry.get("name", ""),
            )
        )
    return mods


def _resolve_channel_inp_source(
    base_dir: Path,
    channel_inp_name: str,
    explicit: Optional[str] = None,
) -> str:
    """Return the channel .inp source path to use for a variation setup.

    Priority:
    1. *explicit* — when set in the YAML ``base_run.channel_inp_source``.
    2. ``<base_dir>/local_input/<channel_inp_name>`` — when the base run itself
       was a variation and already has a patched local copy.
    3. ``<base_dir>/../../common_input/<channel_inp_name>`` — the default
       historical fallback.
    """
    if explicit is not None:
        return explicit
    local_candidate = base_dir / "local_input" / channel_inp_name
    if local_candidate.exists():
        logger.debug(
            "Using local_input channel file from base run: %s", local_candidate
        )
        return str(local_candidate)
    return str(base_dir.parent.parent / "common_input" / channel_inp_name)


def _resolve_model_dss_pattern(base_cfg: dict, run_steps: Optional[List[str]]) -> str:
    """Resolve model DSS filename pattern from config and selected run steps.

    Explicit ``base_run.model_dss_pattern`` takes precedence. If no explicit
    value is provided, GTM-only runs default to ``{modifier}_gtm.dss``;
    all other cases default to ``{modifier}_qual.dss``.
    """
    configured = base_cfg.get("model_dss_pattern")
    if configured:
        return configured

    steps_lower = {s.lower() for s in (run_steps or [])}
    if "gtm" in steps_lower and "qual" not in steps_lower:
        return "{modifier}_gtm.dss"
    return "{modifier}_qual.dss"


def run_from_yaml(
    yaml_path: str | Path,
    run_base: bool = False,
    run_variation: bool = True,
    setup_only: bool = False,
    log_file: Optional[str | Path] = None,
) -> dict:
    """Run calibration variation and compute EC slope metrics from a YAML config.

    Parameters
    ----------
    yaml_path :
        Path to the calibration YAML config file.
    run_base :
        Set to ``True`` to also execute the base-run batch file before computing
        base slopes.  Normally the base run is pre-existing and this is ``False``.
    run_variation :
        Set to ``False`` to skip model execution and only recompute metrics from
        an already-completed variation output.

    Returns
    -------
    dict
        Same structure as :func:`run_calibration_variation`.
    """
    cfg = load_yaml_config(yaml_path)

    base = cfg["base_run"]
    var = cfg["variation"]
    metrics_cfg = cfg.get("metrics", {})

    base_dir = Path(base["study_dir"])
    channel_inp_name = base.get("channel_inp_name", "channel_std_delta_grid.inp")
    run_steps = var.get("run_steps")
    channel_inp_source = _resolve_channel_inp_source(
        base_dir, channel_inp_name, explicit=base.get("channel_inp_source")
    )
    model_dss_pattern = _resolve_model_dss_pattern(base, run_steps)
    timewindow = metrics_cfg.get("timewindow")

    active_stations = cfg.get("active_stations")
    ec_locations = read_ec_locations_csv(
        cfg["ec_stations_csv"],
        active_stations=active_stations,
    )

    modifications = _cfg_to_modifications(cfg)

    if setup_only:
        # Just create the variation directory; stop before running the model.
        info = setup_variation(
            base_dir,
            var["study_dir"],
            channel_inp_source,
            modifications,
            modifier=var["name"],
            channel_inp_name=channel_inp_name,
            run_steps=run_steps,
            dsm2_bin_dir=cfg.get("dsm2_bin_dir"),
            envvar_overrides=var.get("envvar_overrides"),
            copy_timeseries=var.get("copy_timeseries", False),
        )
        logger.info("Setup complete. Batch file: %s", info["batch_file"])
        return {"variation_info": info}

    if run_base:
        base_batch = base_dir / base.get("batch_file", "DSM2_batch.bat")
        logger.info("Running base model: %s", base_batch)
        run_study(base_batch, base_dir)

    result = run_calibration_variation(
        base_study_dir=base_dir,
        var_study_dir=var["study_dir"],
        channel_inp_source=channel_inp_source,
        modifications=modifications,
        observed_ec_dss=cfg["observed_ec_dss"],
        ec_locations=ec_locations,
        modifier=var["name"],
        model_dss_pattern=model_dss_pattern,
        timewindow=timewindow,
        base_modifier=base["modifier"],
        channel_inp_name=channel_inp_name,
        run_model=run_variation,
        run_steps=run_steps,
        log_file=log_file,
        dsm2_bin_dir=cfg.get("dsm2_bin_dir"),
        envvar_overrides=var.get("envvar_overrides"),
        config_to_copy=yaml_path,
        copy_timeseries=var.get("copy_timeseries", False),
    )

    return result

