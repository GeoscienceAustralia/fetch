"""
Automatically download ancillary, given
a YAML config file. See the auto module.
"""
from __future__ import absolute_import

import logging
import sys


def main():
    """
    Run
    """
    # Default logging levels. These can be overridden when the config file is loaded.
    logging.getLogger().setLevel(logging.WARNING)
    logging.getLogger('neocommon').setLevel(logging.INFO)
    logging.getLogger('fetch').setLevel(logging.INFO)

    if len(sys.argv) != 2:
        sys.stderr.writelines([
            'Usage: fetch-service <config.yaml>\n'
        ])
        sys.exit(1)

    from . import auto
    auto.logging_init()
    run_config = auto.init_run_config(sys.argv[1])
    auto.run_loop(run_config)

if __name__ == '__main__':
    main()
