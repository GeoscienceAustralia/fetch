"""
Automatically download ancillary, given
a YAML config file. See the auto module.
"""
from __future__ import absolute_import

if __name__ == '__main__':
    from .scripts import service
    service.main()
