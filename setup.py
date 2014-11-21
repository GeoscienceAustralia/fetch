#!/usr/bin/env python2.7
from __future__ import print_function
from distutils.core import setup
import os

version = '1.1.1b'

# Append TeamCity build number if it gives us one.
if 'TC_BUILD_NUMBER' in os.environ and version.endswith('b'):
    version += '' + os.environ['TC_BUILD_NUMBER']

setup(name='fetch',
      maintainer='Jeremy Hooke',
      maintainer_email='jeremy.hooke@ga.gov.au',
      version=version,
      description='Automatic retrieval of ancillary and data',
      packages=[
          'fetch',
      ],
      scripts=[
          'bin/fetch-service'
      ],
      requires=[
          'arrow',
          'croniter',
          'feedparser',
          'lxml',
          'neocommon',
          'pathlib',
          'pyyaml',
          'requests',
          'setproctitle',
      ]
)
