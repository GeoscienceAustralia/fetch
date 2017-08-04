# -*- coding: utf-8 -*-
# By definition this file will have combined Py 2/3 syntax that insults pylint.
# pylint: skip-file
"""
Compatibility helpers for Python 2 and 3.

Expected to be brought across from AGDC (https://github.com/data-cube/agdc-v2/blob/develop/datacube/compat.py)
as needed.
"""

import sys

PY2 = sys.version_info[0] == 2

if not PY2:
    from urllib.parse import urljoin
else:
    from urlparse import urljoin


# setproctitle is only supported on some platforms (Linux).
try:
    from setproctitle import setproctitle
except ImportError:
    # On non-support platforms we won't bother setting the process name.
    def setproctitle(title):
        return None
