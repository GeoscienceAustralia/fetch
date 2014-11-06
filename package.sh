#!/usr/bin/env bash

# Load RPM python dependencies from setup.py
rpm_deps=''
for package in $(python2.7 setup.py --requires)
do
    if [[ "${package}" == "neocommon" ]]
    then
        rpm_deps="${rpm_deps} ${package}"
    else
        rpm_deps="${rpm_deps} python27-${package}"
    fi
done

pkg_version="$(python2.7 ./setup.py --version)"

[ -n $TC_BUILD_NUMBER ] && echo "##teamcity[buildNumber '${pkg_version}']"

# We use --fix-python to ensure it uses "python2.7" and not plain "python"
# On NEO nodes "python" is the default RHEL python, and python2.7
# is a custom built one.

exec python2.7 ./setup.py bdist_rpm \
    --requires "${rpm_deps}" \
    --fix-python

