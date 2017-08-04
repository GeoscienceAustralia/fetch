# -*- coding: utf-8 -*-
"""
Automatically and continuously download ancillary according to schedules set in a
YAML file.
"""
from __future__ import absolute_import

import sys


def main():
    """
    Run
    """
    if len(sys.argv) != 2:
        sys.stderr.writelines([
            'Usage: fetch-service <config.yaml>\n'
        ])
        sys.exit(1)
    config_location = sys.argv[1]

    from fetch import auto
    auto.logging_init()
    run_config = auto.init_run_config(config_location)
    auto.run_loop(run_config)


if __name__ == '__main__':
    main()
