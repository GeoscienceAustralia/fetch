# -*- coding: utf-8 -*-
"""
Download specific rules immediately (if not currently running/locked)
"""
from __future__ import absolute_import

import sys


def main():
    """
    Run once for the given rules.
    """
    return_usage = any((sys.argv < 2, '-h' in sys.argv, '--help' in sys.argv))

    if return_usage:
        sys.stderr.writelines([
            'Usage: fetch-now <config.yaml> [rules...]\n',
            '',
            'eg. fetch-now rules.yaml LS7_BPF\n'
        ])
        sys.exit(1)

    config_location = sys.argv[1]
    rule_names = sys.argv[2:]

    from fetch import auto
    auto.logging_init()
    run_config = auto.init_run_config(config_location)
    auto.run_items(run_config, *rule_names)


if __name__ == '__main__':
    main()
