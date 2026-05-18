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
    make_reset_session_button,
    snapshot,
    restore,
)
from dvue.session_persistence import serve_session_app as _serve_session_app
from dvue.session_persistence import serve_desktop_app as _serve_desktop_app

__all__ = [
    "install_session_handler",
    "make_reset_session_button",
    "snapshot",
    "restore",
    "serve_session_app",
    "serve_desktop_app",
]

_DEFAULT_CACHE_DIR = Path.home() / ".dsm2ui_sessions"
_DEFAULT_COOKIE_NAME = "dsm2ui_user_id"


def serve_session_app(
    build_manager_fn,
    title: str,
    port: int = 0,
    crs=None,
    station_id_column: str | None = None,
    cookie_name: str = _DEFAULT_COOKIE_NAME,
    cache_dir: str | Path | None = None,
    persist: bool = True,
    show_reset_session_button: bool = True,
    **pn_serve_kwargs,
) -> None:
    """Launch a session-aware Panel app for a dsm2ui manager.

    Delegates to :func:`dvue.session_persistence.serve_session_app` with
    dsm2ui-specific defaults:

    - ``cookie_name`` defaults to ``"dsm2ui_user_id"`` to avoid collisions
      with other dvue apps served on the same origin.
    - ``cache_dir`` defaults to ``~/.dsm2ui_sessions``.
    - ``persist`` defaults to ``True`` (disk persistence across server
      restarts is enabled by default for dsm2ui apps).
    - ``show_reset_session_button`` defaults to ``True``.

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
    cookie_name:
        Name of the persistent user-identity cookie.  Default
        ``"dsm2ui_user_id"``.
    cache_dir:
        Diskcache directory.  Defaults to ``~/.dsm2ui_sessions``.
    persist:
        Enable Layer 2 disk persistence: save/restore ``time_range`` and
        table ``selection`` across server restarts using diskcache.
        Default ``True``.
    show_reset_session_button:
        When ``True`` (default), automatically set
        ``show_reset_session_button=True`` and
        ``session_cookie_name=cookie_name`` on the manager returned by
        *build_manager_fn* so that ``DataUI`` renders a "Reset Session"
        button in the action bar.
    **pn_serve_kwargs:
        Forwarded to ``pn.serve()``.
    """
    _cookie = cookie_name
    _build_fn = build_manager_fn

    if show_reset_session_button:
        def _build_fn_with_session():
            mgr = _build_fn()
            if hasattr(mgr, "show_reset_session_button"):
                mgr.show_reset_session_button = True
            if hasattr(mgr, "session_cookie_name"):
                mgr.session_cookie_name = _cookie
            return mgr

        _effective_build = _build_fn_with_session
    else:
        _effective_build = _build_fn

    _serve_session_app(
        _effective_build,
        title=title,
        port=port,
        crs=crs,
        station_id_column=station_id_column,
        cookie_name=_cookie,
        cache_dir=cache_dir if cache_dir is not None else _DEFAULT_CACHE_DIR,
        persist=persist,
        **pn_serve_kwargs,
    )


def serve_desktop_app(
    build_manager_fn,
    title: str,
    port: int = 0,
    server_timeout: float = 15.0,
    crs=None,
    station_id_column: str | None = None,
    cookie_name: str = _DEFAULT_COOKIE_NAME,
    cache_dir=None,
    persist: bool = True,
    show_reset_session_button: bool = True,
    **pn_serve_kwargs,
) -> None:
    """Open a dsm2ui app in a native desktop window via pywebview.

    Drop-in replacement for :func:`serve_session_app` that uses a native OS
    window instead of a browser tab.  Applies the same dsm2ui defaults as
    :func:`serve_session_app` (cookie name, cache dir, persist).

    Parameters
    ----------
    build_manager_fn:
        Zero-argument callable returning a fresh ``DataUIManager`` instance.
    title:
        Window title; also used as the URL path key.
    port:
        TCP port (``0`` = random available port).
    server_timeout:
        Seconds to wait for the Panel server before raising ``TimeoutError``.
    crs:
        Cartopy CRS for the map panel.  ``None`` → no map.
    station_id_column:
        Column identifying stations in the catalog.
    cookie_name:
        Persistent user-identity cookie name.  Default ``"dsm2ui_user_id"``.
    cache_dir:
        Diskcache directory.  Defaults to ``~/.dsm2ui_sessions``.
    persist:
        Enable disk persistence across server restarts.  Default ``True``.
    show_reset_session_button:
        Automatically wire a "Reset Session" button on the manager.
    **pn_serve_kwargs:
        Forwarded to ``pn.serve()``.
    """
    _cookie = cookie_name
    _build_fn = build_manager_fn

    if show_reset_session_button:
        def _build_fn_with_session():
            mgr = _build_fn()
            if hasattr(mgr, "show_reset_session_button"):
                mgr.show_reset_session_button = True
            if hasattr(mgr, "session_cookie_name"):
                mgr.session_cookie_name = _cookie
            return mgr

        _effective_build = _build_fn_with_session
    else:
        _effective_build = _build_fn

    _serve_desktop_app(
        _effective_build,
        title=title,
        port=port,
        server_timeout=server_timeout,
        crs=crs,
        station_id_column=station_id_column,
        cookie_name=_cookie,
        cache_dir=cache_dir if cache_dir is not None else _DEFAULT_CACHE_DIR,
        persist=persist,
        **pn_serve_kwargs,
    )
