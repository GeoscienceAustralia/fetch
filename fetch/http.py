"""
HTTP-based download of files.
"""
from __future__ import absolute_import

import logging
import re
import time
from contextlib import closing
from typing import Tuple, Sequence

import feedparser
import requests
from lxml import etree
from requests import Session

from ._core import SimpleObject, DataSource, fetch_file, RemoteFetchException, ResultHandler, FilenameTransform
from urllib.parse import urljoin

DEFAULT_CONNECT_TIMEOUT_SECS = 100

_log = logging.getLogger(__name__)


class SessionWithRedirection(requests.Session):
    """
    Enables authentication headers to be retained for configured hosts
    """

    TRUSTED_HOSTS = ['urs.earthdata.nasa.gov']

    def should_strip_auth(self, old_url, new_url):
        original_parsed = requests.utils.urlparse(old_url)
        redirect_parsed = requests.utils.urlparse(new_url)

        if (original_parsed.hostname in self.TRUSTED_HOSTS or
                redirect_parsed.hostname in self.TRUSTED_HOSTS):
            return False
        return super(SessionWithRedirection, self).should_strip_auth(old_url, new_url)


def filename_from_url(url):
    """
    Get the filename component of the URL

    >>> filename_from_url('http://example.com/somefile.zip')
    'somefile.zip'
    >>> filename_from_url('http://oceandata.sci.gsfc.nasa.gov/Ancillary/LUTs/modis/utcpole.dat')
    'utcpole.dat'
    """
    return url.split('/')[-1]


class HttpPostAction(SimpleObject):
    """
    Perform a simple HTTP-Post. Intended for use as a 'beforehand' action.

    (such as posting login credentials before retrievals)
    """

    def __init__(self, url, params):
        """
        :type url: str
        :type params: dict of (str, str)
        """
        self.url = url
        self.params = params

    def get_result(self, session):
        """
        Return the closing result of the action.

        :type session: requests.Session
        """
        return closing(session.post(self.url, params=self.params))


URL = str


class HttpAuthAction(SimpleObject):
    """
    Performs authentication for the session provided.
    """

    def __init__(self, url, username, password,
                 connection_timeout=DEFAULT_CONNECT_TIMEOUT_SECS):
        self.url = url
        self.params = (username, password)
        self.connection_timeout = connection_timeout

    def get_result(self, session):
        # This was uncommented,
        # but I think it is asking for a redirect to the login page?
        login_url = session.request('get', self.url, timeout=self.connection_timeout).url

        session.auth = self.params

        res = session.get(login_url, auth=self.params, timeout=self.connection_timeout)
        if res.status_code != 200:
            # We don't bother with reporter.file_error() as this initial fetch is critical.
            # Throw an exception instead.
            raise RemoteFetchException(
                "Status code %r" % res.status_code,
                '{url}\n\n{body}'.format(url=login_url, body=res.text)
            )

        return closing(res)


class _HttpBaseSource(DataSource):
    """
    Base class for HTTP retrievals.
    """

    def __init__(self,
                 target_dir: str,
                 url: URL = None,
                 urls: Sequence[URL] = None,
                 filename_transform: FilenameTransform = None,
                 beforehand: HttpPostAction = None,
                 connection_timeout: float = DEFAULT_CONNECT_TIMEOUT_SECS,
                 retry_count: int = 3,
                 retry_delay_seconds: float = 5.0, ):
        super(_HttpBaseSource, self).__init__()
        self.target_dir = target_dir
        self.beforehand = beforehand

        self.filename_transform = filename_transform

        # Can either specify one URL or a list of URLs
        self.url = url
        self.urls = urls

        # Connection timeout in seconds
        self.connection_timeout = connection_timeout

        self.retry_count = retry_count
        self.retry_delay_seconds = retry_delay_seconds

    def _get_all_urls(self) -> Sequence[URL]:
        """
        """
        all_urls = []
        if self.urls:
            all_urls.extend(self.urls)
        if self.url:
            all_urls.append(self.url)
        return all_urls

    def trigger(self, reporter: ResultHandler):
        """
        Trigger a download from the configured URLs.

        This will call the overridden trigger_url() function
        for each URL.

        """
        all_urls = self._get_all_urls()
        if not all_urls:
            raise RuntimeError("HTTP type requires either 'url' or 'urls'.")

        session = SessionWithRedirection()

        if self.beforehand:
            _log.debug('Triggering %r', self.beforehand)
            with self.beforehand.get_result(session) as res:
                if res.status_code != 200:
                    _log.error('Status code %r received for %r.', res.status_code, self.beforehand)
                    _log.debug('Error received text: %r', res.text)
        for url in all_urls:
            _log.debug("Triggering %r", url)
            self.trigger_url(reporter, session, url)

    def trigger_url(self, reporter: ResultHandler, session: Session, url: URL):
        """
        Trigger for the given URL. Overridden by subclasses.
        """
        raise NotImplementedError("Individual URL trigger not implemented")

    def _fetch_files(self,
                     urls_filenames: Sequence[Tuple[URL, str]],
                     reporter: ResultHandler,
                     session: Session = requests,
                     override_existing=False):
        """
        Utility method for fetching HTTP URL to the target folder.
        """

        def do_fetch(t):
            """Fetch data to filename t"""
            with closing(session.get(url, stream=True, timeout=self.connection_timeout)) as res:
                if res.status_code != 200:
                    body = res.text
                    _log.debug('Received text %r', res.text)
                    reporter.file_error(url, "Status code %r" % res.status_code, body)
                    return False

                with open(t, 'wb') as f:
                    for chunk in res.iter_content(4096):
                        if chunk:
                            f.write(chunk)
                            f.flush()
            return True

        for url, target_name in urls_filenames:
            attempt_count = 0
            while True:
                did_succeed = fetch_file(
                    url,
                    do_fetch,
                    reporter,
                    target_name,
                    self.target_dir,
                    filename_transform=self.filename_transform,
                    override_existing=override_existing
                )
                if did_succeed or attempt_count > self.retry_count:
                    break

                attempt_count += 1
                time.sleep(self.retry_delay_seconds * attempt_count)


class HttpSource(_HttpBaseSource):
    """
    Fetch static HTTP URLs.

    This is useful for unchanging URLs that need to be
    repeatedly updated.
    """

    def trigger_url(self, reporter, session, url):
        """
        Download URL, overriding existing.
        :type reporter: ResultHandler
        :type session: requests.Session
        :type url: str
        """
        name = filename_from_url(url)
        self._fetch_files([(url, name)], reporter, session=session, override_existing=True)


class HttpListingSource(_HttpBaseSource):
    """
    Fetch files from a HTTP listing page.

    A pattern can be supplied to limit files by filename.
    """

    def __init__(self,
                 target_dir,
                 url=None,
                 urls=None,
                 name_pattern='.*',
                 filename_transform=None,
                 beforehand=None,
                 connection_timeout=DEFAULT_CONNECT_TIMEOUT_SECS,
                 retry_count: int = 3,
                 retry_delay_seconds: float = 5.0):
        super(HttpListingSource, self).__init__(target_dir,
                                                url=url,
                                                urls=urls,
                                                filename_transform=filename_transform,
                                                beforehand=beforehand,
                                                connection_timeout=connection_timeout,
                                                retry_count=retry_count,
                                                retry_delay_seconds=retry_delay_seconds)
        self.name_pattern = name_pattern

    def trigger_url(self, reporter, session, url):
        """
        Download the given listing page, and any links that match the name pattern.
        :type reporter: ResultHandler
        :type session: requests.Session
        :type url: str
        """
        res = session.get(url, timeout=self.connection_timeout)
        if res.status_code == 404:
            _log.debug("Listing page doesn't exist yet. Skipping.")
            return

        if res.status_code != 200:
            # We don't bother with reporter.file_error() as this initial fetch is critical.
            # Throw an exception instead.
            raise RemoteFetchException(
                "Status code %r" % res.status_code,
                '{url}\n\n{body}'.format(url=url, body=res.text)
            )

        # pylint fails to identify native functions under our virtualenv...
        #: pylint: disable=no-member
        page = etree.fromstring(res.text, parser=etree.HTMLParser())

        url = res.url

        anchors = page.xpath('//a')

        # Build a list of URLs to fetch
        urls_names = []
        for anchor in anchors:
            # : :type: str
            name = anchor.text
            if 'href' not in anchor.attrib:
                continue

            href_ = anchor.attrib['href']

            if not name:
                _log.info("Skipping empty anchor for %r", href_)
                continue

            source_url = urljoin(url, href_)

            if not href_.endswith(name):
                _log.info('Not a filename %r, skipping.', name)
                continue

            if not re.match(self.name_pattern, name):
                _log.info("Filename (%r) doesn't match pattern, skipping.", name)
                continue
            urls_names.append((source_url, name))

        self._fetch_files(
            urls_names,
            reporter,
            session=session
            # ,override_existing=True
        )


class RssSource(_HttpBaseSource):
    """
    Fetch any files from the given RSS URL.

    The title of feed entries is assumed to be the filename.
    """

    def trigger_url(self, reporter, session, url):
        """
        Download RSS feed and fetch missing files.
        :type reporter: ResultHandler
        :type session: requests.Session
        :type url: str
        """
        # Fetch feed.
        res = session.get(url, timeout=self.connection_timeout)

        if res.status_code != 200:
            # We don't bother with reporter.file_error() as this initial fetch is critical.
            # Throw an exception instead.
            raise RemoteFetchException(
                "Status code %r" % res.status_code,
                '{url}\n\n{body}'.format(url=url, body=res.text)
            )

        feed = feedparser.parse(res.text)
        self._fetch_files(
            [(entry.link, entry.title) for entry in feed.entries],
            reporter,
            session=session
            # ,override_existing=True
        )
