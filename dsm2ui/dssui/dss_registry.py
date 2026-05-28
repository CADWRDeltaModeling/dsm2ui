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
import warnings
from typing import List

import pandas as pd

from dvue.catalog import DataReference, DataReferenceReader
from dvue.registry import ReaderRegistry
from dvue.registry_ui import RegistryUIManager

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DataReference subclass
# ---------------------------------------------------------------------------

class _DSSRef(DataReference):
    """DataReference for a single DSS time series entry."""
    ref_type: str = "dss"


class _DSM2DSSRef(DataReference):
    """DataReference for DSM2 output-channel DSS time series."""
    ref_type: str = "dsm2_dss"


# ---------------------------------------------------------------------------
# Reader: scan + load for any HEC-DSS file
# ---------------------------------------------------------------------------

class UnifiedDSSReader(DataReferenceReader):
    """Unified HEC-DSS reader implementation.

    Subclasses select scan behavior by configuring ``ref_type`` and
    optional ``output_cparts`` filtering.

    The reader is instantiated once per unique source path (flyweight via
    :class:`~dvue.registry.ReaderRegistry`) and caches the open
    ``pyhecdss.DSSFile`` handle.

    Parameters
    ----------
    source : str
        Absolute path to the DSS file.
    """

    ref_type: str = "dss"
    output_cparts: set[str] | None = None

    def __init__(self, source: str) -> None:
        self._source = source
        self._fh = None
        self._catalog_df = None

    def _get_fh(self):
        if self._fh is None:
            import pyhecdss as dss
            self._fh = dss.DSSFile(self._source)
        return self._fh

    @staticmethod
    def _ensure_pathname_column(dfcat: pd.DataFrame) -> pd.DataFrame:
        if "pathname" not in dfcat.columns:
            dfcat = dfcat.copy()
            dfcat["pathname"] = dfcat.apply(
                lambda r: f"/{r.get('A','')}/{r.get('B','')}/{r.get('C','')}//{r.get('E','')}/{r.get('F','')}/",
                axis=1,
            )
        return dfcat

    def _get_catalog_df(self) -> pd.DataFrame:
        if self._catalog_df is None:
            self._catalog_df = self._ensure_pathname_column(self._get_fh().read_catalog())
        return self._catalog_df

    @classmethod
    def _build_ref(cls, path: str, row: pd.Series) -> DataReference:
        a = str(row.get("A", ""))
        b = str(row.get("B", ""))
        c = str(row.get("C", ""))
        d = str(row.get("D", ""))
        e = str(row.get("E", ""))
        f = str(row.get("F", ""))
        pathname = f"/{a}/{b}/{c}//{e}/{f}/"
        if cls.ref_type == "dsm2_dss":
            return _DSM2DSSRef(
                source=path,
                cache=True,
                name=f"{path}::{b}/{c}",
                station=b,
                variable=c.lower(),
                station_name=b,
                NAME=b,
                VARIABLE=c,
                FILE=path,
                INTERVAL=e,
                A=a,
                B=b,
                C=c,
                D=d,
                E=e,
                F=f,
                pathname=pathname,
            )
        return _DSSRef(
            source=path,
            cache=True,
            name=f"{path}::{pathname}",
            station=b,
            variable=c.lower(),
            A=a,
            B=b,
            C=c,
            D=d,
            E=e,
            F=f,
            pathname=pathname,
        )

    @classmethod
    def scan(cls, path: str) -> List[DataReference]:
        """Enumerate all DSS paths in *path* and return :class:`DataReference` objects."""
        try:
            import pyhecdss as dss
        except ImportError:
            logger.warning("%s.scan: pyhecdss not available", cls.__name__)
            return []

        refs: List[DataReference] = []
        try:
            with dss.DSSFile(path) as fh:
                dfcat = fh.read_catalog()
        except Exception as exc:
            logger.warning("%s.scan: could not open %s: %s", cls.__name__, path, exc)
            return refs

        if dfcat is None or dfcat.empty:
            return refs

        required = {"A", "B", "C", "E", "F"}
        missing = required - set(dfcat.columns)
        if missing:
            logger.warning(
                "%s.scan: catalog for %s missing columns %s",
                cls.__name__,
                path,
                missing,
            )
            return refs

        if cls.output_cparts is not None and "C" in dfcat.columns:
            cparts = {c.upper() for c in cls.output_cparts}
            dfcat = dfcat[dfcat["C"].astype(str).str.upper().isin(cparts)]

        for _, row in dfcat.iterrows():
            refs.append(cls._build_ref(path, row))

        logger.info(
            "%s.scan: %s → %d paths", cls.__name__, os.path.basename(path), len(refs)
        )
        return refs

    def _resolve_path_row(self, attributes: dict) -> pd.Series | None:
        dfcat = self._get_catalog_df()

        pathname = attributes.get("pathname")
        if pathname:
            match = dfcat[dfcat["pathname"] == pathname]
            if not match.empty:
                return match.iloc[0]

        a = attributes.get("A")
        b = attributes.get("B") or attributes.get("NAME")
        c = attributes.get("C") or attributes.get("VARIABLE")
        e = attributes.get("E")
        f = attributes.get("F")
        if b and c:
            match = dfcat[
                (dfcat["B"].astype(str) == str(b))
                & (dfcat["C"].astype(str).str.upper() == str(c).upper())
            ]
            if a is not None:
                match = match[match["A"].astype(str) == str(a)]
            if e is not None:
                match = match[match["E"].astype(str) == str(e)]
            if f is not None:
                match = match[match["F"].astype(str) == str(f)]
            if not match.empty:
                return match.iloc[0]
        return None

    def load(self, **attributes) -> pd.DataFrame:
        """Load one DSS time series given its path-part attributes."""
        row = self._resolve_path_row(attributes)
        if row is None:
            pathname = attributes.get("pathname", "") or f"//{attributes.get('NAME', '')}/{attributes.get('VARIABLE', '')}////"
            logger.warning("%s: no match for %s in %s", type(self).__name__, pathname, self._source)
            return pd.DataFrame()

        pathname = str(row.get("pathname", ""))
        e = str(row.get("E", ""))
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
            pathnames = fh.get_pathnames(pd.DataFrame([row]))
            if not pathnames:
                logger.warning(
                    "%s: no match for %s in %s", type(self).__name__, pathname, self._source
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
                "%s: error loading %s from %s: %s",
                type(self).__name__,
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
        return f"{type(self).__name__}(source={self._source!r})"


class DSM2DSSReader(UnifiedDSSReader):
    """Deprecated compatibility name for DSM2 mode of the unified DSS reader."""

    ref_type = "dsm2_dss"
    output_cparts = {"FLOW", "STAGE", "EC", "VELOCITY", "SALINITY", "TEMP", "DO"}

    def __init__(self, source: str) -> None:
        warnings.warn(
            "DSM2DSSReader is a compatibility wrapper; prefer UnifiedDSSReader-based APIs.",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(source)


class DSSRegistryReader(UnifiedDSSReader):
    """Deprecated compatibility name for generic mode of the unified DSS reader."""

    ref_type = "dss"
    output_cparts = None

    def __init__(self, source: str) -> None:
        warnings.warn(
            "DSSRegistryReader is a compatibility wrapper; prefer UnifiedDSSReader-based APIs.",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(source)


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

    def get_table_schema(self, df: pd.DataFrame | None = None) -> dict:
        if df is None:
            df = self.get_data_catalog()
        optional = [c for c in ["name", "pathname", "source"] if c in df.columns]
        return {
            "required_columns": ["A", "B", "C", "D", "E", "F"],
            "optional_columns": optional,
            "hidden_by_default": ["name", "pathname", "source", "ref_type"],
            "drop_if_all_null": True,
            "column_widths": {
                "A": "14%",
                "B": "14%",
                "C": "14%",
                "E": "9%",
                "F": "14%",
                "D": "20%",
            },
            "filters": {
                "A": {"type": "input", "func": "like", "placeholder": "Enter match"},
                "B": {"type": "input", "func": "like", "placeholder": "Enter match"},
                "C": {"type": "input", "func": "like", "placeholder": "Enter match"},
                "E": {"type": "input", "func": "like", "placeholder": "Enter match"},
                "F": {"type": "input", "func": "like", "placeholder": "Enter match"},
            },
        }

    def _get_table_column_width_map(self) -> dict:
        return super()._get_table_column_width_map()
