"""
File download from European Centre for Medium-Range Weather Forecasts
using ECMWF Web API
"""
from __future__ import absolute_import
import os
import json
import logging

try:
    from urllib.error import URLError
    from http.client import HTTPException
    from urllib.parse import urlencode
except ImportError:
    from urllib2 import URLError
    from httplib import HTTPException
    from urllib import urlencode

from ._core import DataSource, fetch_file, RemoteFetchException
from .util import remove_nones

_log = logging.getLogger(__name__)

try:
    # Optional library
    from ecmwfapi import (
        APIRequest, APIException,
        ECMWFDataServer as _ECMWFDataServer
    )

    class ECMWFDataServer(_ECMWFDataServer):

        def retrieve(self, req):
            """
            Override the nativee method to return the result object
            Required to verify the downloaded object's size
            """
            target = req.get("target")
            dataset = req.get("dataset")
            c = APIRequest(
                self.url,
                "datasets/%s" % (dataset, ),
                self.email,
                self.key,
                self.trace,
                verbose=self.verbose
            )
            result = c.execute(req, target)

            return result


except ImportError:
    class APIException(Exception):
        pass

    class ECMWFDataServer(object):
        def __init__(self, *args, **kwargs):
            raise NotImplementedError("ECMWF client libraries not installed")


def _rename(the_dict, old, new):
    """
    rename a key in a dict

    :type the_dict: dict
    :type old: str
    :type new: str

    >>> _rename({'a': 10}, 'a', 'b')
    {'b': 10}
    >>> _rename({'a': 10}, 'c', 'b')
    {'a': 10}
    >>> _rename({'a': 5}, 'a', 'a')
    {'a': 5}
    """

    if old in the_dict:
        the_dict[new] = the_dict.pop(old)

    return the_dict


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
        return remove_nones(settings)

    def get_uri(self):
        """
        Synthesise a URI from the configured ECMWF host and request settings
        """
        try:
            config_location = os.path.normpath(
                os.path.expanduser("~/.ecmwfapirc")
            )
            with open(config_location, 'r') as f:
                uri = json.loads(f.read())['url']
        except (IOError, KeyError) as e:
            _log.debug('Unable to read url configuration: %s', str(e))
            raise RemoteFetchException(
                'Unable to read url configuration at %s',
                config_location
            )

        query = urlencode(self._get_api_settings())
        return uri + "?" + query

    def trigger(self, reporter):
        """
        Trigger a download based on settings

        :type reporter: ResultHandler
        """

        _log.debug("Triggering %s", self._get_api_settings())
        server = ECMWFDataServer(log=_log.debug)
        self._fetch_file(server, reporter, self.override_existing)

    def _fetch_file(self, server, reporter, override_existing):

        def do_fetch(t):
            settings = self._get_api_settings()
            settings["target"] = t
            try:
                result = server.retrieve(settings)
                target_size = os.path.getsize(t)
                if result['size'] != target_size:
                    _log.debug('ECMWFDataServer target: %s expected size: %s, actual size: %s',
                               t, str(result['size']), str(target_size))
                    return False

            except (URLError) as e:
                message = "ECMWFDataServer raised %s. Do you have the correct URL in ~/.ecmwfapirc?" % e
                _log.debug(message)
                raise RemoteFetchException(
                    summary='ECMWFDataServer failed to retrieve file. Check ~/.ecmwfapirc file',
                    detailed='target: {}, message:{}'.format(t, message))
            except (HTTPException, APIException) as e:
                message = "ECMWFDataServer raised %s." % e
                _log.debug(message)
                raise RemoteFetchException(
                    summary='ECMWFDataServer failed to retrieve file',
                    detailed='target: {}, message:{}'.format(t, message))
            except Exception as e:    # pylint: disable=broad-except
                _log.debug("ECMWFDataServer raised " + str(e))
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
