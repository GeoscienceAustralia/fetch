#!/usr/bin/env bash

# Commandline Usage:   nohup bin/fetch-service tle_rules.yaml &>> ~/logs/fetch/service.log &


# Load such environment vars if present (eg. proxy settings):
# [ -e /etc/neo-env.sh ] && . /etc/neo-env.sh

# production server proxy setting must be enabled
export ftp_proxy=http://10.7.64.209:8080
export http_proxy=http://10.7.64.209:8080
export https_proxy=http://10.7.64.209:8080

export FTP_PROXY=http://10.7.64.209:8080
export HTTP_PROXY=http://10.7.64.209:8080
export HTTPS_PROXY=http://10.7.64.209:8080

export _JAVA_OPTIONS='-Dhttp.proxyHost=10.7.64.52 -Dhttp.proxyPort=8080'

#  Python2.7.9/setup.sh
export PY27_INST_DIR=/home/tleserv/python2.7.9
export PATH="$PY27_INST_DIR/bin:$PATH"
export LD_LIBRARY_PATH="$PY27_INST_DIR/lib:$LD_LIBRARY_PATH"
#export CPATH="$PY27_INST_DIR/include:$CPATH"
#export MANPATH="$PY27_INST_DIR/share/man:$MANPATH"


PYTHON27EXE=$PY27_INST_DIR/bin/python2.7
exec $PYTHON27EXE -m fetch "$@"

######################## PRODUCTION NOTE   ##########################################
#For more persistent running of this daemon in a production server, install a file into /etc/init/
#For Example:  [ads@pe-test fetch]$ cat /etc/init/fetch-service.conf
# with the following content:

#start on runlevel [345]
#stop on starting shutdown

#respawn

#exec su -s /bin/sh -c 'exec "$0" "$@"' tleserv -- /path2/tle_fetch_service.sh /path2/tle_rules.yaml &>> /path2logdir/service.log
