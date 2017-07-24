"""
File download from European Centre for Medium-Range Weather Forecasts 
using ECMWF Web API
"""
from __future__ import absolute_import
import os
import json
import logging
import urllib

from ._core import SimpleObject, DataSource, fetch_file, RemoteFetchException
# from .compat import urljoin

_log = logging.getLogger(__name__)


class EcmwfApiSource(DataSource):
    """
    Class for data retrievals using the ECMWF API.

    """

    def __init__(self, target_dir, settings=None, filename_transform=None):
        """
        :type target_dir: str
        :type setting: dict used to specify ALL ECMWF API request parameters
        :type filename_transform: FilenameTransform
        """
        super(DataSource, self).__init__()
        self.target_dir = target_dir
        self.filename_transform = filename_transform

        # Can either specify one URL or a list of URLs
        self.settings = settings

    def get_uri(self):
        """
        Synthesise a URI from the configured ECMWF host and requestsettings
        """
        try:
            with open(os.environ.get("HOME")+"/.ecmwfapirc") as f:
                d = json.loads(f.read())
                uri = d["url"]
        except Exception as e:
            uri = "ecmwfapi://UnknownHost"
        query = urllib.parse.urlencode(self.settings)
        return uri + "?" + query

    def trigger(self, reporter):
        """
        Trigger a download based on settings

        :type reporter: ResultHandler
        """

        _log.debug("Triggering %s", self.settings)
        from ecmwfapi import ECMWFDataServer
        server = ECMWFDataServer()
        self._fetch_file(server, reporter)

    def _fetch_file(self, server, reporter, override_existing=False):
        
        def do_fetch(t):
            settings = self.settings.copy()
            settings["target"] = t
            try:
            	server.retrieve(settings)
            except Exception as e:
                _log.debug("ECMWFDataServer rasied " + e)
                return False
            return True

        fetch_file(
            self.get_uri(),
            do_fetch,
            reporter,
            os.path.basename(self.settings["target"]),
            os.path.dirname(self.settings["target"]),
            filename_transform=self.filename_transform,
            override_existing=override_existing
	)
	
	
