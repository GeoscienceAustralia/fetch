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

    def __init__(self, 
        cls=None,
        dataset=None,
        date=None,
        expver=None,
        grid=None,
        area=None,
        levtype=None,
        param=None,
        step=None,
        stream=None,
        time=None,
        typ=None,
        target=None,
        filename_transform=None):
        """
        :type kwargs: dict used to specify ALL ECMWF API request parameters
        """
        super(DataSource, self).__init__()
        self.cls = cls
        self.dataset = dataset
        self.date = date
        self.expver = expver
        self.grid = grid
        self.area = area
        self.levtype = levtype
        self.param = param
        self.step = step
        self.stream = stream
        self.time = time
        self.typ = typ
        self.target = target
        self.filename_transform = filename_transform

    def _get_api_settings(self):
        """
        return a dict containing the sanitised settings required by the ECMWF API
        """
        settings = self.__dict__.copy()
        for key in ["filename_transform", ]:
            if key in settings:
                del settings[key]
        settings['class'] = settings['cls']
        del settings['cls']
        settings['type'] = settings['typ']
        del settings['typ']
        for key in settings.keys():
            if settings[key] is None:
                del settings[key]
        return settings

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
        query = urllib.parse.urlencode(self._get_api_settings())
        return uri + "?" + query

    def trigger(self, reporter):
        """
        Trigger a download based on settings

        :type reporter: ResultHandler
        """

        _log.debug("Triggering %s", self._get_api_settings())
        from ecmwfapi import ECMWFDataServer
        server = ECMWFDataServer()
        self._fetch_file(server, reporter)

    def _fetch_file(self, server, reporter, override_existing=False):
        
        def do_fetch(t):
            settings = self._get_api_settings()
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
            os.path.basename(self.target),
            os.path.dirname(self.target),
            filename_transform=self.filename_transform,
            override_existing=override_existing
	)
	
	
