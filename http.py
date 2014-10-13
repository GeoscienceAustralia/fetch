from . import DataSource
import os
import tempfile

import requests
import logging
from contextlib import closing

_log = logging.getLogger(__name__)


# class FetchResult(object):
#     def __init__(self, uri, filename, success):
#         """
#         Result of one file/(dataset?) fetched
#         :type uri: neocommon.Uri
#         :param filename:
#         :param success:
#         :return:
#         """
#         super(FetchResult, self).__init__()
#
#         self.


def filename_from_url(url):
    """
    Get the filename component of the URL

    >>> filename_from_url('http://example.com/somefile.zip')
    'somefile.zip'
    >>> filename_from_url('http://oceandata.sci.gsfc.nasa.gov/Ancillary/LUTs/modis/utcpole.dat')
    'urcpole.dat'
    """
    return url.split('/')[-1]


class HttpSource(DataSource):
    def __init__(self, source_urls, target_dir):
        """
        Get static HTTP urls.

        This is useful for unchanging URLs that need to be
        repeatedly updated.

        :type source_urls: list of str
        :type target_dir: str
        :return:
        """
        super(HttpSource, self).__init__()

        self.source_urls = source_urls
        self.target_dir = target_dir

    def trigger(self, reporter):
        """

        :type reporter: FetchReporter
        :return:
        """

        for url in self.source_urls:
            # Fetch file.

            name = filename_from_url(url)

            with closing(requests.get(url, stream=True)) as res:
                if res.status_code != 200:
                    _log.debug('Received text %r', res.text)
                    reporter.file_error(url, "Status code %r" % res.status_code)
                    continue

                t = tempfile.mktemp(
                    dir=self.target_dir
                )

                with open(t, 'wb') as f:
                    for chunk in res.iter_content(4096):
                        if chunk:
                            f.write(chunk)
                            f.flush()

            size_bytes = os.path.getsize(t)
            if size_bytes == 0:
                _log.debug('Empty file returned for url %r', url)
                reporter.file_error(url, "Empty return")
                continue

            # Move to destination
            target_path = os.path.join(self.target_dir, name)
            os.rename(t, target_path)

            # Report as complete.
            reporter.file_complete(url, name, target_path)


class RssSource(DataSource):
    def __init__(self, rss_url, target_dir):
        """
        Fetch any files from the given RSS URL.

        Title of entries is assumed to be the filename.

        :type source_urls: list of str
        :type target_dir: str
        :return:
        """
        super(RssSource, self).__init__()

        self.rss_feed_url = rss_url
        self.target_dir = target_dir

    def trigger(self, reporter):
        return super(RssSource, self).trigger(reporter)


