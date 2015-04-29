#!/usr/bin/env bash
# This script is used in tle_rules.yaml to post process fetched TLE files
# make sure this file is installed in /usr/local/bin and executable chmod u+x


# Commandline Usage:
#   This_script path2/TLEfile &>> /tmp/tle_loader.log

echo "Invoking a python script to ingest TLE-files data into target DB table "

##  project home, intallation dir,
#TLESERV_HOME=/home/rms_usr/gitblit/tleserv
#
## which python verison to use (it must have MySQLDb driver,...)
#PYTHON_INTERPRETER=python2.6  # OR python2.7
#
#$PYTHON_INTERPRETER $TLESERV_HOME/tle2db/tle_loader.py "$@"

