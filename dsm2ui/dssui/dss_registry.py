"""Registry-based DSS browser plugin for dvue.

Provides a :class:`DSSRegistryReader` that scans any HEC-DSS file and
produces :class:`~dvue.catalog.DataReference` objects (one per DSS path),
and a :class:`DSSRegistryUIManager` that wraps it behind the
:class:`~dvue.registry_ui.RegistryUIManager` interface.

The reader is registered under ``ref_type="dss"`` with **no** extension entry
to avoid conflicting with :class:`~dsm2ui.dsm2ui.DSM2DSSReader` which is
registered for ``ref_type="dsm2_dss"`` with extension ``.dss``.
:class:`DSSRegistryUIManager` bypasses extension dispatch and calls
:meth:`DSSRegistryReader.scan` directly.

Example::

    from dsm2ui.dssui.dss_registry import DSSRegistryUIManager
    mgr = DSSRegistryUIManager()
    mgr.add_source_files(["path/to/data.dss"])
"""
from __future__ import annotations

import logging
import os
from typing import List

import pandas as pd

from dvue.catalog import DataReference
from dvue.registry import ReaderRegistry
from dvue.registry_ui import RegistryUIManager

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DataReference subclass
# ---------------------------------------------------------------------------

class _DSSRef(DataReference):
    """DataReference for a single DSS time series entry."""
    ref_type: str = "dss"


# ---------------------------------------------------------------------------
# Reader: scan + load for any HEC-DSS file
# ---------------------------------------------------------------------------

class DSSRegistryReader:
    """Open a HEC-DSS file, enumerate all paths, and load time series.

    The reader is instantiated once per unique source path (flyweight via
    :class:`~dvue.registry.ReaderRegistry`) and caches the open
    ``pyhecdss.DSSFile`` handle.

    Parameters
    ----------
    source : str
        Absolute path to the DSS file.
    """

    def __init__(self, source: str) -> None:
        self._source = source
        self._fh = None  # lazy open

    def _get_fh(self):
        if self._fh is None:
            import pyhecdss as dss
            self._fh = dss.DSSFile(self._source)
        return self._fh

    @classmethod
    def scan(cls, path: str) -> List[DataReference]:
        """Enumerate all DSS paths in *path* and return :class:`DataReference` objects."""
        try:
            import pyhecdss as dss
        except ImportError:
            logger.warning("DSSRegistryReader.scan: pyhecdss not available")
            return []

        refs: List[DataReference] = []
        try:
            with dss.DSSFile(path) as fh:
                dfcat = fh.read_catalog()
        except Exception as exc:
            logger.warning("DSSRegistryReader.scan: could not open %s: %s", path, exc)
            return refs

        if dfcat is None or dfcat.empty:
            return refs

        required = {"A", "B", "C", "E", "F"}
        missing = required - set(dfcat.columns)
        if missing:
            logger.warning(
                "DSSRegistryReader.scan: catalog for %s missing columns %s",
                path,
                missing,
            )
            return refs

        for _, row in dfcat.iterrows():
            a = str(row.get("A", ""))
            b = str(row.get("B", ""))
            c = str(row.get("C", ""))
            e = str(row.get("E", ""))
            f = str(row.get("F", ""))
            d = str(row.get("D", ""))
            pathname = f"/{a}/{b}/{c}//{e}/{f}/"
            ref = _DSSRef(
                source=path,
                cache=True,
                name=f"{path}::{pathname}",
                station=b,
                variable=c.lower(),
                A=a, B=b, C=c, D=d, E=e, F=f,
                pathname=pathname,
            )
            refs.append(ref)

        logger.info(
            "DSSRegistryReader.scan: %s → %d paths", os.path.basename(path), len(refs)
        )
        return refs

    def load(self, **attributes) -> pd.DataFrame:
        """Load one DSS time series given its path-part attributes."""
        a = attributes.get("A", "")
        b = attributes.get("B", "")
        c = attributes.get("C", "")
        e = attributes.get("E", "")
        f = attributes.get("F", "")
        pathname = attributes.get("pathname", f"/{a}/{b}/{c}//{e}/{f}/")
        time_range = attributes.get("time_range")

        is_irregular = str(e).startswith("IR-")

        if time_range is not None:
            start_str = pd.Timestamp(time_range[0]).strftime("%Y-%m-%d")
            end_str = pd.Timestamp(time_range[1]).strftime("%Y-%m-%d")
        else:
            start_str = "1753-01-01"
            end_str = "2200-12-31"

        try:
            fh = self._get_fh()
            dfcatp = fh.read_catalog()
            if "pathname" not in dfcatp.columns:
                dfcatp["pathname"] = dfcatp.apply(
                    lambda r: f"/{r.get('A','')}/{r.get('B','')}/{r.get('C','')}//{r.get('E','')}/{r.get('F','')}/",
                    axis=1,
                )
            match = dfcatp[dfcatp["pathname"] == pathname]
            pathnames = fh.get_pathnames(match)
            if not pathnames:
                logger.warning(
                    "DSSRegistryReader: no match for %s in %s", pathname, self._source
                )
                return pd.DataFrame()
            actual = pathnames[0]
            if is_irregular:
                df, unit, ptype = fh.read_its(actual, start_str, end_str)
            else:
                df, unit, ptype = fh.read_rts(actual, start_str, end_str)
            fvi = df.first_valid_index()
            lvi = df.last_valid_index()
            if fvi is not None and lvi is not None:
                df = df[fvi:lvi]
            df.attrs["unit"] = unit.lower() if unit else ""
            df.attrs["ptype"] = ptype or "inst-val"
            return df
        except Exception as exc:
            logger.error(
                "DSSRegistryReader: error loading %s from %s: %s",
                pathname,
                self._source,
                exc,
            )
            return pd.DataFrame()

    def __del__(self):
        if self._fh is not None:
            try:
                self._fh.close()
            except Exception:
                pass

    def __repr__(self) -> str:
        return f"DSSRegistryReader(source={self._source!r})"


# Register with NO extension entry — DSSRegistryUIManager calls scan() directly.
ReaderRegistry.register("dss", DSSRegistryReader)


# ---------------------------------------------------------------------------
# Manager
# ---------------------------------------------------------------------------

class DSSRegistryUIManager(RegistryUIManager):
    """Generic HEC-DSS file browser using the dvue registry pattern.

    All DSS paths that ``pyhecdss`` can enumerate are presented in the
    catalog.  Use ``station_id_column`` + geo data via
    :meth:`~dvue.registry_ui.RegistryUIManager.add_geo_source` for map display.

    Unlike :class:`~dsm2ui.dssui.dssui.DSSDataUIManager`, this manager uses
    the registry dispatch path and supports drag-and-drop file loading.

    Because ``DSSRegistryReader`` is registered without an extension entry,
    :meth:`add_source_files` detects ``.dss`` files manually and calls
    :meth:`DSSRegistryReader.scan` directly instead of going through the
    extension map.
    """

    def __init__(self, **kwargs):
        from dvue.catalog import DataCatalog
        super().__init__(**kwargs)
        # Override primary key — DSS paths are unique by (source, pathname).
        # We expose A/B/C/E/F columns; "name" is the full registry key.
        self._dvue_catalog = DataCatalog(
            primary_key=["source_num", "station", "variable", "A", "F"]
        )
        self._display_dfcat = pd.DataFrame(
            columns=["name", "A", "B", "C", "D", "E", "F", "station", "variable", "source"]
        )
        self.station_id_column = "B"
        self.color_cycle_column = "B"
        self.dashed_line_cycle_column = "source"
        self.marker_cycle_column = "F"

    def add_source_files(self, paths):
        """Accept ``.dss`` files; bypass extension dispatch for DSS paths."""
        added = []
        for p in paths:
            ext = os.path.splitext(p)[1].lower()
            if ext == ".dss":
                refs = DSSRegistryReader.scan(p)
            else:
                # Fall through to normal extension dispatch for other types.
                try:
                    refs = ReaderRegistry.scan(p)
                except KeyError:
                    logger.warning(
                        "DSSRegistryUIManager: no reader for extension %r", ext
                    )
                    continue

            n_before = len(self._dvue_catalog)
            for ref in refs:
                self.normalize_ref(ref)
                try:
                    self._dvue_catalog.add(ref)
                except Exception:
                    pass

            if len(self._dvue_catalog) > n_before:
                added.append(p)
                self.on_file_added(p, refs)

        return added

    def normalize_ref(self, ref: DataReference) -> None:
        """Ensure station=B and variable=C for DSS refs."""
        if not ref._attributes.get("station"):
            ref.set_attribute("station", str(ref._attributes.get("B", "")))
        if not ref._attributes.get("variable"):
            ref.set_attribute("variable", str(ref._attributes.get("C", "")).lower())

    def _get_table_column_width_map(self) -> dict:
        return {
            "A": "14%",
            "B": "14%",
            "C": "14%",
            "E": "9%",
            "F": "14%",
            "D": "20%",
        }
