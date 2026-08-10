# -*- coding: utf-8 -*-

"""DSM2 UI tools - Python Delta Modeling User Interface Package."""

import os
import warnings

# NumPy 2.4 deprecated align=0 (int) in dtype(); old .npy files from
# cartopy and other scientific packages trigger this at import time.
warnings.filterwarnings(
    "ignore",
    message="dtype\\(\\): align should be passed as Python or NumPy boolean",
    category=DeprecationWarning,
)

# Disable PROJ's on-demand network grid downloads (cdn.proj.org). By default
# recent PROJ/pyproj versions may fetch high-accuracy datum-shift grids (e.g.
# us_noaa_cnhpgn.tif) over the network for CRS reprojection. Behind corporate
# proxies/firewalls this can fail with a certificate revocation check error
# (pyproj.exceptions.ProjError) and crash the UI. Our map visualizations do
# not need that level of geodetic accuracy, so force offline/local grids only.
os.environ.setdefault("PROJ_NETWORK", "OFF")
try:
    import pyproj
    pyproj.network.set_network_enabled(False)
except Exception:
    pass

__author__ = """Kijin Nam"""
__email__ = 'knam@water.ca.gov'

try:
    from ._version import __version__
except (ImportError, AttributeError):
    __version__ = '0.0.0+unknown'
