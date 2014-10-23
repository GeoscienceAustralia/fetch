"""
HTTP-based download of files.
"""

import re
import requests
import logging
from contextlib import closing
import feedparser
from lxml import etree
from urlparse import urljoin
import datetime

from . import DataSource, fetch_file


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


def _fetch_file(target_dir, target_name, reporter, url, override_existing=False, filename_transform=None):
    """
    Fetch the given URL to the target folder.

    :type target_dir: str
    :type target_name: str
    :type reporter: FetchReporter
    :type url: str
    """

    def do_fetch(t):
        """Fetch data to filename t"""
        with closing(requests.get(url, stream=True)) as res:
            if res.status_code != 200:
                _log.debug('Received text %r', res.text)
                reporter.file_error(url, "Status code %r" % res.status_code)
                return

            with open(t, 'wb') as f:
                for chunk in res.iter_content(4096):
                    if chunk:
                        f.write(chunk)
                        f.flush()

    fetch_file(
        url,
        do_fetch,
        reporter,
        target_name,
        target_dir,
        filename_transform=filename_transform,
        override_existing=override_existing
    )


class HttpSource(DataSource):
    """
    Fetch static HTTP URLs.

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
            _fetch_file(self.target_dir, name, reporter, url, override_existing=True)


def _date_range(from_days_from_now, to_days_from_now):
    """
    Get a range of dates relative to the current date.

    :type from_days_from_now: int
    :type to_days_from_now: int
    :rtype: list od datetime.datetime
    """
    start_day = datetime.datetime.utcnow() + datetime.timedelta(days=from_days_from_now)
    days = to_days_from_now - from_days_from_now

    for day in (start_day + datetime.timedelta(days=n) for n in range(days + 1)):
        yield day


class DateRangeSource(DataSource):
    """
    Repeat a source multiple times with different dates.

    The URL and directory names can have date patterns replaced using string.format syntax.
    """
    def __init__(self, source_prototype, source_url=None, target_dir=None, from_days=-1, to_days=1):
        """
        :type source_prototype: DataSource
        :type source_url: str
        :type target_dir: str
        :type from_days: int
        :type to_days: int
        """
        super(DateRangeSource, self).__init__()
        self.source_url = source_url
        self.target_dir = target_dir

        #: :type: DataSource
        self.source_prototype = source_prototype

        self.from_days = from_days
        self.to_days = to_days

    def trigger(self, reporter):
        """
        Run the DataSource prototype once for each date in the range.
        """
        for day in _date_range(self.from_days, self.to_days):
            date_params = {
                'year': day.strftime('%Y'),
                'month': day.strftime('%m'),
                'day': day.strftime('%d'),
                'julday': day.strftime('%j')
            }

            if self.source_url:
                self.source_prototype.source_url = self.source_url.format(**date_params)
                _log.debug('Source URL %r', self.source_prototype.source_url)

            if self.target_dir:
                self.source_prototype.target_dir = self.target_dir.format(**date_params)
                _log.debug('Target dir %r', self.source_prototype.target_dir)

            _log.info('Triggering %r', self.source_prototype)
            self.source_prototype.trigger(reporter)


class HttpListingSource(DataSource):
    """
    Fetch files from a HTTP listing page.

    A pattern can be supplied to limit files by filename.
    """

    def __init__(self, source_url, target_dir, listing_name_filter='.*', filename_transform=None):
        super(HttpListingSource, self).__init__()

        self.source_url = source_url
        self.listing_name_filter = listing_name_filter
        self.target_dir = target_dir
        self.filename_transform = filename_transform

    def trigger(self, reporter):
        """
        Download the given listing page, and any links that match the name pattern.
        """
        res = requests.get(self.source_url)
        if res.status_code == 404:
            _log.debug("Listing page doesn't exist yet. Skipping.")
            return

        if res.status_code != 200:
            _log.debug('Received text %r', res.text)
            reporter.file_error(self.source_url, "Status code %r" % res.status_code)
            return

        page = etree.fromstring(res.text, parser=etree.HTMLParser())
        url = res.url

        anchors = page.xpath('//a')

        for anchor in anchors:
            #: :type: str
            name = anchor.text
            source_url = urljoin(url, anchor.attrib['href'])

            if not anchor.attrib['href'].endswith(name):
                _log.info('Not a filename %r, skipping.', name)
                continue

            if not re.match(self.listing_name_filter, name):
                _log.info("Filename (%r) doesn't match pattern, skipping.", name)
                continue

            _fetch_file(
                self.target_dir,
                name,
                reporter,
                source_url,
                filename_transform=self.filename_transform
            )


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

            _fetch_file(
                self.target_dir,
                name,
                reporter,
                url,
                filename_transform=self.filename_transform
            )




