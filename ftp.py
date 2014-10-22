"""
FTP-based retrieval of files.
"""
import ftplib
import logging
import os
import re

from . import DataSource, fetch_file


_log = logging.getLogger(__name__)


def _fetch_files(hostname, remote_dir, name_pattern, target_dir, reporter,
                 override_existing=False,
                 filename_transform=None):
    """
    Fetch fetch files matching a pattern at the given FTP server.

    """

    if not os.path.exists(target_dir):
        _log.info('Creating dir %r', target_dir)
        os.makedirs(target_dir)

    ftp = ftplib.FTP(hostname)
    try:
        ftp.login()

        _log.debug('Changing dir %r', remote_dir)
        ftp.cwd(remote_dir)

        try:
            files = ftp.nlst()
        except ftplib.error_perm, resp:
            if str(resp) == "550 No files found":
                _log.info("No files in remote directory")
                files = []
            else:
                raise

        _log.debug('File list of length %r', len(files))

        for filename in files:
            _log.debug('Next filename: %r', filename)
            if not re.match(name_pattern, filename):
                _log.debug('Filename %r doesn\'t match pattern, skipping.', filename)
                continue

            def ftp_fetch(t):
                """Fetch data to filename t"""
                with open(t, 'wb') as f:
                    ftp.retrbinary('RETR ' + filename, f.write)

            fetch_file(
                'ftp://%s%s%s' % (hostname, remote_dir, filename),
                ftp_fetch,
                reporter,
                filename,
                target_dir,
                filename_transform=filename_transform,
                override_existing=override_existing
            )
    finally:
        ftp.quit()


class FtpSource(DataSource):
    """
    Download from an FTP listing.

    This is useful for unchanging URLs that need to be
    repeatedly updated.
    """

    def __init__(self, hostname, source_dir, name_pattern, target_dir):
        """
        :type source_urls: list of str
        :type target_dir: str
        :return:
        """
        super(FtpSource, self).__init__()

        self.hostname = hostname
        self.source_dir = source_dir
        self.name_pattern = name_pattern
        self.target_dir = target_dir

    def trigger(self, reporter):
        """
        Download all matching files.

        :type reporter: FetchReporter
        :return:
        """

        _fetch_files(
            self.hostname,
            self.source_dir,
            self.name_pattern,
            self.target_dir,
            reporter,
            override_existing=True
        )

