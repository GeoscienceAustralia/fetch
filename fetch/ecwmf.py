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


def _rename(the_dict, old, new):
    """
    rename a key in a dict

    :type the_dict: dict
    :type old: str
    :type new: str
    """

    if new in the_dict:
        the_dict[new] = the_dict[old]
        del the_dict[old]


def _remove_nones(dict_):
    """
    Remove fields from the dict whose values are None.

    Returns a new dict.
    :type dict_: dict
    :rtype dict

    >>> _remove_nones({'a': 4, 'b': None, 'c': None})
    {'a': 4}
    >>> sorted(_remove_nones({'a': 'a', 'b': 0}).items())
    [('a', 'a'), ('b', 0)]
    >>> _remove_nones({})
    {}
    """
    return {k: v for k, v in dict_.items() if v is not None}

class EcmwfApiSource(DataSource):
    """
    Class for data retrievals using the ECMWF API.

    """
    # pylint: disable=too-many-instance-attributes
    # pylint: disable=too-many-arguments
    # Providing an instance variable for each paramater
    # available in the ECMWF API.
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
                 override_existing=False,
                 filename_transform=None):
        """
        :type kwargs: dict used to specify ALL ECMWF API request parameters
        """
        super(EcmwfApiSource, self).__init__()
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
        self.override_existing = override_existing
        self.filename_transform = filename_transform

    def _get_api_settings(self):
        """
        return a dict containing the sanitised settings required by the ECMWF API
        """
        settings = self.__dict__.copy()
        for key in ["filename_transform", "override_existing"]:
            if key in settings:
                del settings[key]
        _rename(settings, "cls", "class")
        _rename(settings, "typ", "type")
        return _remove_nones(settings)

    def get_uri(self):
        """
        Synthesise a URI from the configured ECMWF host and requestsettings
        """
        try:
            with open(os.environ.get("HOME") + "/.ecmwfapirc") as f:
                d = json.loads(f.read())
                uri = d["url"]
        except IOError as e:
            uri = "ecmwfapi://UnknownHost"
        query = urllib.parse.urlencode(self._get_api_settings())
        return uri + "?" + query

    def trigger(self, reporter):
        """
        Trigger a download based on settings

        :type reporter: ResultHandler
        """

        _log.debug("Triggering %s", self._get_api_settings())
        # Optional library.
        #: pylint: disable=import-error
        from ecmwfapi import ECMWFDataServer
        from urllib2 import URLError

        server = ECMWFDataServer()
        self._fetch_file(server, reporter, self.override_existing)

    def _fetch_file(self, server, reporter, override_existing):

        def do_fetch(t):
            settings = self._get_api_settings()
            settings["target"] = t
            try:
                server.retrieve(settings)
            except URLError as e:
                _log.debug("ECMWFDataServer rasied %s. Do you have the correct URL in ~/.ecmwfapirc?" % e)
                return False
            except Exception as e:    # pylint: disable-broad-except
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
