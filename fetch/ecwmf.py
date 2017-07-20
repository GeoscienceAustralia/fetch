"""
File download from European Centre for Medium-Range Weather Forecasts 
using ECMWF Web API
"""
from __future__ import absolute_import

import logging
import re
from contextlib import closing

import feedparser
import requests
from lxml import etree

from ._core import SimpleObject, DataSource, fetch_file, RemoteFetchException
from .compat import urljoin

_log = logging.getLogger(__name__)


class EcmwfApiSource(DataSource):
    """
    Class for data retrievals using the ECMWF API.

    """

    def __init__(self, target_dir, settings=None, filename_transform=None):
        """
        :type urls: list of str
        :type url: str
        :type target_dir: str
        :type setting: dict
        :type filename_transform: FilenameTransform
        """
        super(DataSource, self).__init__()
        self.target_dir = target_dir
        self.filename_transform = filename_transform

        # Can either specify one URL or a list of URLs
        self.settings = settings

    def trigger(self, reporter):
        """
        Trigger a download based on settings

        :type reporter: ResultHandler
        """

        _log.debug("Triggering %s", self.settings)
        from ecmwfapi import ECMWFDataServer
        server = ECMWFDataServer()
        server.retrieve(self.settings)
