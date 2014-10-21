"""
HTTP-based download of files.
"""

import os
import re
import tempfile
import requests
import logging
from contextlib import closing
import feedparser
from lxml import etree
from urlparse import urljoin

from . import DataSource


_log = logging.getLogger(__name__)


def filename_from_url(url):
    """
    Get the filename component of the URL

    >>> filename_from_url('http://example.com/somefile.zip')
    'somefile.zip'
    >>> filename_from_url('http://oceandata.sci.gsfc.nasa.gov/Ancillary/LUTs/modis/utcpole.dat')
    'urcpole.dat'
    """
    return url.split('/')[-1]


def fetch_file(target_dir, name, reporter, url, override_existing=False):
    """
    Fetch the given URL to the target folder.

    :type target_dir: str
    :type name: str
    :type reporter: FetchReporter
    :type url: str
    """

    if not os.path.exists(target_dir):
        _log.info('Creating dir %r', target_dir)
        os.makedirs(target_dir)

    target_path = os.path.join(target_dir, name)

    if os.path.exists(target_path) and not override_existing:
        _log.info('Path exists (%r). Skipping', target_path)
        return

    with closing(requests.get(url, stream=True)) as res:
        if res.status_code != 200:
            _log.debug('Received text %r', res.text)
            reporter.file_error(url, "Status code %r" % res.status_code)
            return

        t = tempfile.mktemp(
            dir=target_dir
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
        return

    # Move to destination
    os.rename(t, target_path)
    # Report as complete.
    reporter.file_complete(url, name, target_path)


class HttpSource(DataSource):
    """
    Source static HTTP URLs.

    This is useful for unchanging URLs that need to be
    repeatedly updated.
    """

    def __init__(self, source_urls, target_dir):
        """
        :type source_urls: list of str
        :type target_dir: str
        :return:
        """
        super(HttpSource, self).__init__()

        self.source_urls = source_urls
        self.target_dir = target_dir

    def trigger(self, reporter):
        """
        Download all URLs, overriding existing.
        :type reporter: FetchReporter
        :return:
        """
        for url in self.source_urls:
            name = filename_from_url(url)
            fetch_file(self.target_dir, name, reporter, url, override_existing=True)


class HttpListingSource(DataSource):
    """
    Fetch files from a HTTP listing page.

    A pattern can be supplied to limit files by filename.
    """
    def __init__(self, listing_url, target_dir, filename_pattern='.*', filename_proxy=None):
        super(HttpListingSource, self).__init__()

        self.listing_url = listing_url
        #: :type: re.Regexp
        self.filename_pattern_re = re.compile(filename_pattern)
        self.target_dir = target_dir
        self.filename_proxy = filename_proxy

    def trigger(self, reporter):
        """
        Download the given listing page, and any links that match the name pattern.
        """
        res = requests.get(self.listing_url)
        if res.status_code != 200:
            _log.debug('Received text %r', res.text)
            reporter.file_error(self.listing_url, "Status code %r" % res.status_code)
            return

        page = etree.fromstring(res.text, parser=etree.HTMLParser())
        url = res.url

        anchors = page.xpath('//a')
        name_paths = [(anchor.text, urljoin(url, anchor.attrib['href'])) for anchor in anchors]

        for name, target_url in name_paths:
            if not self.filename_pattern_re.match(name):
                _log.info('Filename (%r) doesn\'t match pattern, skipping.', name)
                continue

            target_location = self.target_dir

            if self.filename_proxy:
                target_location = self.filename_proxy.transform_destination_path(
                    target_location,
                    source_filename=name
                )

            fetch_file(target_location, name, reporter, target_url)


class RssSource(DataSource):
    """
    Fetch any files from the given RSS URL.

    The title of feed entries is assumed to be the filename.
    """

    def __init__(self, rss_url, target_dir, filename_transform=None):
        """
        :type rss_url: str
        :type target_dir: str
        :type filename_transform: FilenameTransform
        :return:
        """
        super(RssSource, self).__init__()

        self.rss_url = rss_url
        self.target_dir = target_dir

        self.filename_transform = filename_transform

    def trigger(self, reporter):
        """
        Download RSS feed and fetch missing files.
        """
        # Fetch feed.
        res = requests.get(self.rss_url)

        if res.status_code != 200:
            _log.debug('Received text %r', res.text)
            reporter.file_error(self.rss_url, "Status code %r" % res.status_code)
            return

        feed = feedparser.parse(res.text)

        for entry in feed.entries:
            name = entry.title
            url = entry.link

            target_location = self.target_dir

            if self.filename_transform:
                target_location = self.filename_transform.transform_destination_path(
                    target_location,
                    source_filename=name
                )

            # TODO: Destination folder calculated with date pattern?
            fetch_file(target_location, name, reporter, url)




