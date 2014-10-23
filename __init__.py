"""
A package for automatically fetching files (eg. Ancillary).
"""
import datetime
import os
import re
import logging
import shutil
import tempfile

_log = logging.getLogger(__name__)


class DataSource(object):
    """
    A base class for data downloaders.

    Overridden by specific subclasses: HTTP, FTP, RSS and others.
    """

    def __init__(self):
        """
        Base class constructor.
        """
        super(DataSource, self).__init__()

    def trigger(self, reporter):
        """
        Trigger download from the source.

        :type reporter: FetchReporter
        :return:
        """
        raise NotImplementedError("Trigger was not implemented")

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self.__dict__)


class FetchReporter(object):
    """
    A series of callbacks to report on the status of downloads.
    """

    def __init__(self):
        """
        Base class constructor.
        """
        super(FetchReporter, self).__init__()

    def file_error(self, uri, message):
        """
        Call on failure of a file.
        :type uri: str
        :type message: str
        """
        pass

    def file_complete(self, uri, name, path):
        """
        Call on completion of a file
        :type uri: str
        :type name: str
        :type path: str
        """
        pass


class FilenameTransform(object):
    """
    A base class for objects that modify output filenames and directories.

    Primarily useful for situations such as putting files in folders by date.
    """

    def __init__(self):
        super(FilenameTransform, self).__init__()

    def transform_filename(self, source_filename):
        """
        Modify output filename
        """
        return source_filename

    def transform_output_path(self, output_path, source_filename):
        """
        Modify output folder path
        """
        return output_path

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self.__dict__)


class RegexpOutputPathTransform(FilenameTransform):
    """
    Extract fields from a filename using regexp groups.

    Replace patterns matching the group name in the destination path.
    """

    def __init__(self, pattern):
        """
        :type pattern: str
        """
        super(RegexpOutputPathTransform, self).__init__()

        # Validate the pattern immediately on startup.

        # We don't bother keeping the compiled version -- there's no perf. benefit, and
        # serialisation is more complicated.
        try:
            re.compile(pattern)
        except re.error:
            _log.error('Invalid pattern %r', pattern)
            raise

        self.pattern = pattern

    def transform_output_path(self, path, source_filename=None):
        """

        :param path:
        :param source_filename:

        >>> t = RegexpOutputPathTransform(r'LS8_(?P<year>\\d{4})')
        >>> t.transform_output_path('/tmp/out/{year}', 'LS8_2003')
        '/tmp/out/2003'
        >>> t = RegexpOutputPathTransform(r'LS8_(?P<year>\\d{4})')
        >>> t.transform_output_path('/tmp/out', 'LS8_2003')
        '/tmp/out'
        """
        m = re.match(self.pattern, source_filename)

        if not m:
            _log.info('No regexp match for %r', path)
            return path

        groups = m.groupdict()

        return path.format(**groups)


class DateFilenameTransform(FilenameTransform):
    """
    Add date information to filenames according to a format string.

    Defaults to current date.
    """
    def __init__(self, format_, fixed_date=None):
        """
        :type format_: str

        >>> d = DateFilenameTransform('{year}{month}{day}.{filename}')
        >>> d.fixed_date = datetime.datetime(year=2013, month=8, day=6)
        >>> d.transform_filename('output.log')
        '20130806.output.log'
        >>> d.format_ = '{filename}'
        >>> d.transform_filename('output.log')
        'output.log'
        """
        super(DateFilenameTransform, self).__init__()
        self.format_ = format_
        self.fixed_date = fixed_date

    def transform_filename(self, source_filename):

        day = self.fixed_date if self.fixed_date else datetime.datetime.utcnow()
        date_params = {
            'year': day.strftime('%Y'),
            'month': day.strftime('%m'),
            'day': day.strftime('%d'),
            'julday': day.strftime('%j')
        }
        return self.format_.format(
            filename=source_filename,
            **date_params
        )


def fetch_file(uri,
               fetch_fn,
               reporter,
               target_filename,
               target_dir,
               filename_transform=None,
               override_existing=False):
    """
    Common code for fetching a file.

    The actual transfer is handled by a callback (fetch_fn), and
    so is not specific to a protocol.

    :param uri: A URI identifier for this file.
    :param fetch_fn: Function taking a filename argument to download to.
    :param reporter: The fetch reporter
    :param target_filename: The destination filename
    :param target_dir: The destination directory
    :param filename_transform: A transform for output filenames/folders.
    :param override_existing: Should files be re-downloaded if they already exist?
    """
    if filename_transform:
        target_dir = filename_transform.transform_output_path(
            target_dir,
            source_filename=target_filename
        )
        target_filename = filename_transform.transform_filename(target_filename)

    if not os.path.exists(target_dir):
        _log.info('Creating dir %r', target_dir)
        os.makedirs(target_dir)

    target_path = os.path.join(target_dir, target_filename)

    if os.path.exists(target_path) and not override_existing:
        _log.info('Path exists %r. Skipping', target_path)
        return

    t = None
    try:
        t = tempfile.mktemp(
            dir=target_dir,
            prefix='.fetch-'
        )

        _log.debug('Running fetch for file %r', t)
        fetch_fn(t)
        _log.debug('Fetch complete')

        size_bytes = os.path.getsize(t)
        if size_bytes == 0:
            _log.debug('Empty file returned for file %r', uri)
            reporter.file_error(uri, "Empty return")
            return

        # Move to destination
        _log.debug('Rename %r -> %r', t, target_path)
        os.rename(t, target_path)
        # Report as complete.
        reporter.file_complete(uri, target_filename, target_path)
    finally:
        if t and os.path.exists(t):
            os.remove(t)

