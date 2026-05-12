"""dsm2ui.session — Session persistence for dsm2ui CLI apps.

Thin re-export shim over :mod:`dvue.session_persistence`.  All logic lives
in dvue so that other dvue-based apps can reuse it.

The diskcache default for dsm2ui is ``~/.dsm2ui_sessions`` (rather than
dvue's generic ``~/.dvue_sessions``) so sessions from different dvue apps
on the same machine are kept separate.
"""

from __future__ import annotations

from pathlib import Path
from dvue.session_persistence import (
    install_session_handler,
    snapshot,
    restore,
)
from dvue.session_persistence import serve_session_app as _serve_session_app

__all__ = ["install_session_handler", "snapshot", "restore", "serve_session_app"]

_DEFAULT_CACHE_DIR = Path.home() / ".dsm2ui_sessions"


def serve_session_app(
    build_manager_fn,
    title: str,
    port: int = 0,
    crs=None,
    station_id_column: str | None = None,
    cache_dir: str | Path | None = None,
    **pn_serve_kwargs,
) -> None:
    """Launch a session-aware Panel app for a dsm2ui manager.

    Delegates to :func:`dvue.session_persistence.serve_session_app` with
    ``cache_dir`` defaulting to ``~/.dsm2ui_sessions``.

    Parameters
    ----------
    build_manager_fn:
        Zero-argument callable returning a fresh ``DataUIManager`` instance.
    title:
        Browser title; also used as the URL path key.
    port:
        TCP port (``0`` = random available port).
    crs:
        Cartopy CRS for the map panel.  ``None`` → no map.
    station_id_column:
        Column identifying stations in the catalog.
    cache_dir:
        Diskcache directory.  Defaults to ``~/.dsm2ui_sessions``.
    **pn_serve_kwargs:
        Forwarded to ``pn.serve()``.
    """
    _serve_session_app(
        build_manager_fn,
        title=title,
        port=port,
        crs=crs,
        station_id_column=station_id_column,
        cache_dir=cache_dir if cache_dir is not None else _DEFAULT_CACHE_DIR,
        **pn_serve_kwargs,
    )
