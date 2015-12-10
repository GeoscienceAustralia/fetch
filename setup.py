#!/usr/bin/env python2.7
from __future__ import print_function
from setuptools import setup
import os

version = '1.1.4b'

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
          'bin/fetch-service', 'bin/fetch-service-tle',
          'bin/post-fetch-proc.sh',
      ],
      install_requires=[
          'arrow',
          'croniter',
          'feedparser',
          'lxml',
          'pathlib',
          'pyyaml',
          'requests',
          'setproctitle',
      ]
)
