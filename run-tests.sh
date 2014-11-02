#!/bin/bash

set -e

# Find the directory where this script exists
# This is slightly convoluted as it has to work when called through a soft link.
SCRIPT_PATH="${BASH_SOURCE[0]}";
if([ -h "${SCRIPT_PATH}" ]) then
  while([ -h "${SCRIPT_PATH}" ]) do SCRIPT_PATH=`readlink "${SCRIPT_PATH}"`; done
fi
pushd . > /dev/null
cd `dirname ${SCRIPT_PATH}` > /dev/null
SCRIPT_PATH=`pwd`;
popd  > /dev/null

# The httpretty library can get confused when there's a proxy (?)
unset http_proxy
unset HTTP_PROXY

# If no arguments, run all tests. Otherwise run the given filename.
if [[ $# < 1 ]]; then
    exec python2.7 -m neocommon.test.runner
else
    exec python2.7 $1
fi
