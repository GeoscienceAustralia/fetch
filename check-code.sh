#!/usr/bin/env bash
# Convenience script for running Travis-like checks.

set -eu

export py_files="fetch"

pylint --reports no ${py_files}

# Check for basic Python 3 incompatiblities.
pylint --py3k --reports no ${py_files}

pep8 ${py_files} --max-line-length 120

# Run tests
py.test fetch test
