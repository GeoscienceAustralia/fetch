"""
FTP-based retrieval of files.
"""
from __future__ import absolute_import

import ftplib
import logging
import os
import re
import time
from typing import Iterable, Callable

from ._core import DataSource, fetch_file, RemoteFetchException, ResultHandler

_log = logging.getLogger(__name__)
DEFAULT_SOCKET_TIMEOUT_SECS = 60 * 5.0


def _fetch_files(hostname: str,
                 target_dir: str,
                 reporter: ResultHandler,
                 get_filepaths_fn: Callable[[ftplib.FTP], Iterable[str]],
                 override_existing=False,
                 filename_transform=None,
                 retries: int = 3,
                 retry_delay: float = 5):
    """
    Fetch fetch files on the given FTP server.

    The get_filepaths_fn callback is used to get a list of files to download.

    It it passed an instance of the connection so that it can query the server if needed.
    """

    try:
        ftp = ftplib.FTP(hostname, timeout=DEFAULT_SOCKET_TIMEOUT_SECS)
    except BaseException:
        _log.exception('Error connecting to FTP')
        raise RemoteFetchException(
            'Error connecting to FTP',
            'host: {}, timeout: {}'.format(hostname, DEFAULT_SOCKET_TIMEOUT_SECS)
        )

    try:
        ftp.login()

        files_itr = iter(get_filepaths_fn(ftp))
        filename = next(files_itr)
        retry_count = 0

        while True:
            try:
                retry_count += 1
                _log.debug('Next filename: %r', filename)

                def ftp_fetch(t):
                    """Fetch data to filename t"""
                    # https://bitbucket.org/logilab/pylint/issue/271/spurious-warning-w0640-issued-for-loop
                    # pylint: disable=cell-var-from-loop
                    _log.debug('Retrieving %r to %r', filename, t)
                    with open(t, 'wb') as f:
                        ftp.retrbinary('RETR ' + filename, f.write)
                    return True

                fetch_file(
                    'ftp://%s%s' % (hostname, filename),
                    ftp_fetch,
                    reporter,
                    os.path.basename(filename),
                    target_dir,
                    filename_transform=filename_transform,
                    override_existing=override_existing
                )
                filename = next(files_itr)
                retry_count = 0

            except (EOFError, ftplib.error_temp):
                # ftplib.error_temp represents a 4XX error by the server

                if retry_count >= retries:
                    _log.debug('Error fetching file. Reconnecting to ftp server...')
                    raise
                _log.debug('Error fetching file. Reconnecting to ftp server...')

                # Connection was closed; try to re-connect
                time.sleep(retry_delay)
                try:
                    ftp = ftplib.FTP(hostname, timeout=DEFAULT_SOCKET_TIMEOUT_SECS)
                except BaseException:
                    _log.exception('Error connecting to FTP')
                    raise RemoteFetchException(
                        'Error re-connecting to FTP',
                        'host: {}, timeout: {}'.format(hostname, DEFAULT_SOCKET_TIMEOUT_SECS)
                    )

                ftp.login()
    except StopIteration:
        # Completed download of matching files
        pass
    except Exception as e:
        # Log exception message
        _log.exception('Exception raised during FTP process: %s', getattr(e, 'message', str(e)))
        raise
    finally:
        ftp.quit()


class FtpSource(DataSource):
    """
    Download specific files from FTP.

    This is useful for unchanging URLs that need to be
    repeatedly updated.
    """

    def __init__(self, hostname, paths, target_dir, filename_transform=None):
        """
        :type paths: list of str
        :type target_dir: str
        :return:
        """
        super(FtpSource, self).__init__()

        self.hostname = hostname
        self.paths = paths
        self.target_dir = target_dir
        self.filename_transform = filename_transform

    def trigger(self, reporter):
        """
        Download all files, overriding existing.
        :type reporter: ResultHandler
        :return:
        """

        def get_files(_):
            """Return a static set of file paths to download"""
            return self.paths

        _fetch_files(
            self.hostname,
            self.target_dir,
            reporter,
            get_files,
            override_existing=True,
            filename_transform=self.filename_transform
        )


class FtpListingSource(DataSource):
    """
    Download files matching a pattern in an FTP directory.
    """

    def __init__(self, hostname, source_dir, name_pattern, target_dir, filename_transform=None):
        """
        :type source_urls: list of str
        :type target_dir: str
        :return:
        """
        super(FtpListingSource, self).__init__()

        self.hostname = hostname
        self.source_dir = source_dir
        self.name_pattern = name_pattern
        self.target_dir = target_dir
        self.filename_transform = filename_transform

    def trigger(self, reporter):
        """
        Download all matching files.

        :type reporter: ResultHandler
        :return:
        """

        def get_files(ftp: ftplib.FTP) -> Iterable[str]:
            """Get files that match the name_pattern in the target directory."""
            try:
                files = ftp.nlst(self.source_dir)
            except ftplib.error_perm as resp:
                if str(resp) == "550 No files found":
                    _log.info("No files in remote directory")
                    files = []
                else:
                    raise
            except ftplib.error_temp as resp:
                if str(resp).strip().startswith('450'):
                    _log.info("No remote directory")
                    files = []
                else:
                    raise

            _log.debug('File list of length %r', len(files))
            files = [
                os.path.join(self.source_dir, f)
                for f in files if re.match(self.name_pattern, os.path.basename(f))
            ]
            _log.debug('Filtered list of length %r', len(files))
            return files

        _fetch_files(
            self.hostname,
            self.target_dir,
            reporter,
            get_files,
            override_existing=True,
            filename_transform=self.filename_transform
        )
