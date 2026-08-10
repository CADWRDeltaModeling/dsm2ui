"""Server plugin registrations for embedding dsm2ui views in repoui host."""

from pathlib import Path
import logging


def register(config):
    """Register dsm2ui server routes.

    Parameters
    ----------
    config : dict
        Plugin configuration from server YAML under key ``plugins.dsm2ui``.

    Returns
    -------
    dict
        Mapping with keys ``apps`` and optionally ``static_dirs``.
    """
    config = config or {}
    log = logging.getLogger(__name__)

    h5file = config.get("qual_h5")
    if not h5file:
        log.warning(
            "dsm2ui plugin not registering QUAL route: missing required key 'qual_h5'"
        )
        return {"name": "dsm2ui", "apps": {}, "static_dirs": {}}

    h5path = Path(h5file)
    if not h5path.exists():
        log.warning(
            "dsm2ui plugin not registering QUAL route: qual_h5 file does not exist: %s",
            h5file,
        )
        return {"name": "dsm2ui", "apps": {}, "static_dirs": {}}

    route = str(config.get("route", "animate/qual"))
    constituent = str(config.get("constituent", "ec"))
    shapefile = config.get("shapefile")
    simplify_tolerance = float(config.get("simplify_tolerance", 50.0))
    x2_threshold = config.get("x2_threshold")
    channel_id_column = config.get("channel_id_column")

    mgr_kwargs = {}
    for key in ("title", "colormap", "vmin", "vmax", "size"):
        if key in config and config[key] is not None:
            mgr_kwargs[key] = config[key]

    def _build_qual_view():
        from dsm2ui.animate import animate_qual

        return animate_qual(
            h5file=h5file,
            constituent=constituent,
            shapefile=shapefile,
            simplify_tolerance=simplify_tolerance,
            x2_threshold=x2_threshold,
            channel_id_column=channel_id_column,
            **mgr_kwargs,
        )

    return {
        "name": "dsm2ui",
        "description": "DSM2 QUAL animation",
        "apps": {route: _build_qual_view},
        "static_dirs": {},
    }
