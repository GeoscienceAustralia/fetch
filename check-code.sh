#!/usr/bin/env bash
# Convenience script for running Travis-like checks.

set -eu

export py_files="fetch"

pylint --reports no ${py_files}

# Check for basic Python 3 incompatiblities.
pylint --py3k --reports no ${py_files}

# Run tests
# -> But not those that require the neocommon library.
py.test fetch test -m 'not with_neocommon'

