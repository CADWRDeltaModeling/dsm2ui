# -*- coding: utf-8 -*-

"""DSM2 UI tools - Python Delta Modeling User Interface Package."""

import warnings
# NumPy 2.4 deprecated align=0 (int) in dtype(); old .npy files from
# cartopy and other scientific packages trigger this at import time.
warnings.filterwarnings(
    "ignore",
    message="dtype\\(\\): align should be passed as Python or NumPy boolean",
    category=DeprecationWarning,
)

__author__ = """Kijin Nam"""
__email__ = 'knam@water.ca.gov'

try:
    from ._version import __version__
except (ImportError, AttributeError):
    __version__ = '0.0.0+unknown'
