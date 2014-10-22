"""
FTP-based retrieval of files.
"""
import ftplib
import logging
import os
import re
import tempfile

from . import DataSource


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

            target_file_dir = target_dir
            target_filename = filename

            if filename_transform:
                target_file_dir = filename_transform.transform_output_path(
                    target_dir,
                    source_filename=filename
                )
                target_filename = filename_transform.transform_filename(filename)

            target_path = os.path.join(target_file_dir, target_filename)

            if os.path.exists(target_path) and not override_existing:
                _log.info('Path exists %r. Skipping', target_path)
                return

            t = tempfile.mktemp(
                dir=target_dir,
                prefix='.fetch-'
            )
            # TODO: Cleanup tmp files on failure

            with open(t, 'wb') as f:
                ftp.retrbinary('RETR ' + filename, f.write)

            size_bytes = os.path.getsize(t)
            if size_bytes == 0:
                _log.debug('Empty file returned for file %r/%r', remote_dir, filename)
                reporter.file_error(filename, "Empty return")
                return

            # Move to destination
            os.rename(t, target_path)
            # Report as complete.
            reporter.file_complete('ftp://%s%s%s' % (hostname, remote_dir, filename), target_filename, target_path)
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

