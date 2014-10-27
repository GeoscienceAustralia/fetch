"""
A package for automatically fetching files (eg. Ancillary).
"""
import datetime
from neocommon import files
import os
import re
import logging
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
        Trigger a download from the source.

        Abstract method

        :type reporter: FetchReporter
        :return:
        """
        raise NotImplementedError("Trigger was not overridden")

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
        _log.debug('Path exists %r. Skipping', target_path)
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


class RsyncMirrorSource(DataSource):
    """
    Perform a transfer between machines.

    Currently uses rsync and assumes no authentication is required between the machines
    (ie. public key pairs are configured).
    """
    def __init__(self, source_path, target_path, source_host=None, target_host=None):
        """
        Hostnames are optional, defaulting to the current machine.

        :type source_path: str
        :type target_path: str
        :type source_host: str or None
        :type target_host: str on None
        """
        super(RsyncMirrorSource, self).__init__()
        self.source_host = source_host
        self.target_host = target_host
        self.source_path = source_path
        self.target_path = target_path

    def trigger(self, reporter):
        transferred_files = files.rsync(
            self.source_path,
            self.target_path,
            source_host=self.source_host,
            destination_host=self.target_host
        )
        # TODO: We'll eventually track/announce newly arriving files.
        _log.debug('Transferred: %r', transferred_files)


class DateRangeSource(DataSource):
    """
    Repeat a source multiple times with different dates.
    """

    def __init__(self, source_prototype, overridden_properties, from_days=-1, to_days=1):
        """
        :type source_prototype: DataSource
        :type overridden_properties: dict of (str, str)
        :type from_days: int
        :type to_days: int
        """
        super(DateRangeSource, self).__init__()
        self.overidden_properties = overridden_properties

        # : :type: DataSource
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

            for name, pattern in self.overidden_properties.iteritems():
                value = pattern.format(**date_params)
                _log.debug('Setting %r=%r', name, value)
                setattr(self.source_prototype, name, value)

            _log.info('Triggering %r', self.source_prototype)
            self.source_prototype.trigger(reporter)
