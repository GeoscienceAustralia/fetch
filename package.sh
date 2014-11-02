#!/usr/bin/env bash

pkg_version="$(python2.7 ./setup.py --version)"

[ -n $TC_BUILD_NUMBER ] && echo "##teamcity[buildNumber '${pkg_version}']"

# We use --fix-python to ensure it uses "python2.7" and not plain "python"
# On NEO nodes "python" is the default RHEL python, and python2.7
# is a custom built one.

exec python2.7 ./setup.py bdist_rpm \
    --requires "python27-requests python27-feedparser python27-lxml neocommon" \
    --fix-python

