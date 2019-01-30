#!/usr/bin/env python2.7
from __future__ import print_function

from __future__ import absolute_import
import os
import sys

from setuptools import setup
import versioneer

setup(name='fetch',
      maintainer='Jeremy Hooke',
      maintainer_email='jeremy.hooke@ga.gov.au',
      version=versioneer.get_version(),
      cmdclass=versioneer.get_cmdclass(),
      description='Automatic retrieval of ancillary and data',
      packages=[
          'fetch',
          'fetch.scripts'
      ],
      install_requires=[
          'arrow',
          'croniter',
          'feedparser',
          'lxml',
          'pathlib',
          'pyyaml',
          'requests',
          'future;python_version<"3"',
      ] + (
          # Setting subprocess names is only support on Linux
          ['setproctitle'] if 'linux' in sys.platform else []
      ),
      extras_require={
          'ecmwf': ['ecmwf-api-client']
      },
      entry_points={
          'console_scripts': [
              'fetch-service = fetch.scripts.service:main',
              'fetch-now = fetch.scripts.now:main'
          ]
      },
      )
