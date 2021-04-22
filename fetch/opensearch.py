from __future__ import absolute_import

import logging
import os

from sentinelsat import SentinelAPI

from ._core import DataSource, fetch_file

DEFAULT_CONNECT_TIMEOUT_SECS = 100

_log = logging.getLogger(__name__)


class OpenSearchApiSource(DataSource):
    """
    Class for data retrievals using the OpenSearch API.
    """

    def __init__(self, target_dir, api_url, username, password, query, show_progressbars=False, timeout=DEFAULT_CONNECT_TIMEOUT_SECS,
                 filename_transform=None, override_existing=False):
        self.target_dir = target_dir
        self.filename_transform = filename_transform
        self.override_existing = override_existing
        self.api_url = api_url
        self.username = username
        self.password = password
        self.query = query
        self.show_progressbars = show_progressbars
        self.timeout = timeout

        self.api = SentinelAPI(self.username, self.password, self.api_url, self.show_progressbars, self.timeout)

    def trigger(self, reporter):
        """
        :type reporter: ResultHandler
        """

        query_results = self.api.query(**self.query)

        for (uuid, result) in query_results.items():
            _log.info('Found %s with uuid %s', result['filename'], uuid)

            def create_fetch_function(key):
                def opensearch_fetch(target):
                    download = self.api.download(key)

                    # Workaround for fixed filename
                    _log.debug('Renaming %s to %s', download['path'], target)
                    os.rename(download['path'], target)

                    return True

                return opensearch_fetch

            fetch_file(
                result['link'].replace("'", '%27'),
                create_fetch_function(uuid),
                reporter,
                os.path.basename(result['filename']),
                self.target_dir,
                filename_transform=self.filename_transform,
                override_existing=self.override_existing
            )
