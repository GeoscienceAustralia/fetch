# coding=utf-8
"""
A package for automatically fetching files (eg. Ancillary).
"""
from __future__ import absolute_import

import datetime
import errno
import logging
import multiprocessing
import os
import re
import smtplib
import socket
import subprocess
import tempfile
from email.mime.text import MIMEText
from email.header import Header

from pathlib import Path

from .util import rsync, Uri

_log = logging.getLogger(__name__)


# pylint: disable=eq-without-hash
class SimpleObject(object):
    """
    An object with matching constructor arguments and properties.

    Implements repr and eq methods that print/compare all properties.

    Beware of cyclic dependencies in properties
    """

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self.__dict__)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        else:
            return False


class FileProcessError(Exception):
    """
    An error in file processing.
    """
    pass


class DataSource(SimpleObject):
    """
    A base class for data downloaders.

    Overridden by specific subclasses: HTTP, FTP, RSS and others.
    """

    def trigger(self, reporter):
        """
        Trigger a download from the source.

        Abstract method.

        :type reporter: ResultHandler
        """
        raise NotImplementedError("Trigger was not overridden")


class RemoteFetchException(Exception):
    """
    A failure while retrieving a remote file.
    """

    def __init__(self, summary, detailed):
        super(RemoteFetchException, self).__init__()
        self.summary = summary
        self.detailed = detailed


class ResultHandler(object):
    """
    A series of callbacks to report on the status of downloads.
    """

    def file_error(self, uri, summary, body):
        """
        Call on failure of a file.
        :type uri: str
        :type summary: str
        :type body: str
        """
        pass

    def files_complete(self, source_uri, paths, msg_metadata=None):
        """
        Call on completion of multiple files.

        Some implementations may override this for more efficient bulk handling files.
        :type source_uri: str
        :type paths: list of str
        :type msg_metadata: dict of (str, str)
        """
        for path in paths:
            self.file_complete(source_uri, path, msg_metadata=msg_metadata)

    def file_complete(self, source_uri, path, msg_metadata=None):
        """
        Call on completion of a file
        :type source_uri: str
        :type path: str
        :type msg_metadata: dict of (str, str)
        """
        pass


class FilenameTransform(SimpleObject):
    """
    A base class for objects that modify output filenames and directories.

    Primarily useful for situations such as putting files in folders by date.
    """

    def transform_filename(self, source_filename):
        """
        Modify output filename
        :type source_filename: str
        """
        return source_filename

    def transform_output_path(self, output_path, source_filename):
        """
        Modify output folder path
        :type source_filename: str
        :type output_path: str
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
        self.groups = {}

    def transform_output_path(self, output_path, source_filename):
        """

        :param output_path:
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
            _log.info('No regexp match for %r', output_path)
            return output_path

        self.groups = m.groupdict()
        return output_path.format(**self.groups)


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
        >>> d.format_ = '{path.stem}-{date:%Y-%m}{path.suffix}'
        >>> d.transform_filename('output.log')
        'output-2013-08.log'
        """
        super(DateFilenameTransform, self).__init__()
        self.format_ = format_
        self.fixed_date = fixed_date

    def transform_filename(self, source_filename):
        """
        :type source_filename: str
        """
        day = self.fixed_date if self.fixed_date else datetime.datetime.utcnow()
        date_params = {
            'path': Path(source_filename),
            'date': day,
            # Specifics are sometimes clearer. The above are more flexible.
            'year': day.strftime('%Y'),
            'month': day.strftime('%m'),
            'day': day.strftime('%d'),
            'julday': day.strftime('%j'),
        }
        return self.format_.format(
            filename=source_filename,
            **date_params
        )


def mkdirs(target_dir):
    """
    Create directory and all parents. Don't complain if exists.
    """
    try:
        os.makedirs(target_dir)
    except OSError as e:
        # be happy if someone already created the path
        if e.errno != errno.EEXIST:
            raise


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

    target_path = os.path.join(target_dir, target_filename)

    if os.path.exists(target_path) and not override_existing:
        _log.debug('Path exists %r. Skipping', target_path)
        return

    # Create directories if needed.
    # (We can't use 'target_dir', because the 'filename' can contain folder offsets too.)
    actual_target_dir = os.path.dirname(target_path)
    if not os.path.exists(actual_target_dir):
        _log.info('Creating dir %r', actual_target_dir)
        mkdirs(actual_target_dir)

    t = None
    try:
        t = tempfile.mktemp(
            dir=target_dir,
            prefix='.fetch-'
        )

        _log.debug('Running fetch for file %r', uri)
        was_success = fetch_fn(t)
        if not was_success:
            _log.debug("Download function reported error.")
            return

        if not os.path.exists(t):
            _log.debug('No file returned for %r', uri)
            reporter.file_error(uri, "No file", "")
            return

        size_bytes = os.path.getsize(t)
        if size_bytes == 0:
            _log.debug('Empty file returned for %r', uri)
            reporter.file_error(uri, "Empty file", "")
            return

        _log.debug('Fetch complete')

        # Move to destination
        _log.debug('Rename %r -> %r', t, target_path)
        os.rename(t, target_path)
        # Report as complete.
        reporter.file_complete(uri, target_path)
    finally:
        if t and os.path.exists(t):
            os.remove(t)


def _date_range(from_days_from_now, to_days_from_now):
    """
    Get a range of dates relative to the current date.

    :type from_days_from_now: int
    :type to_days_from_now: int
    :rtype: list od datetime.datetime

    >>> len(list(_date_range(-1, 1)))
    3
    >>> len(list(_date_range(0, 1)))
    2
    >>> len(list(_date_range(-2, 0)))
    3
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
        """
        :type reporter: ResultHandler
        """
        transferred_files = rsync(
            self.source_path,
            self.target_path,
            source_host=self.source_host,
            destination_host=self.target_host
        )
        _log.debug('Transferred: %r', transferred_files)
        reporter.files_complete(
            Uri.from_host_path(self.source_host, self.source_path).get_qualified_uri(),
            transferred_files
        )


class EmptySource(DataSource):
    """
    Do nothing: useful for tests.
    """

    def trigger(self, reporter):
        """
        Do nothing: useful for tests.
        """
        _log.info('Triggered empty source.')


class DateRangeSource(DataSource):
    """
    Repeat a source multiple times with different dates.
    """

    def __init__(self, using, overridden_properties, start_day=-1, end_day=1):
        """
        :type using: DataSource
        :type overridden_properties: dict of (str, str)
        :type start_day: int
        :type end_day: int
        """
        super(DateRangeSource, self).__init__()
        self.overridden_properties = overridden_properties

        # : :type: DataSource
        self.using = using

        self.start_day = start_day
        self.end_day = end_day

    def trigger(self, reporter):
        """
        Run the DataSource prototype once for each date in the range.
        :type reporter: ResultHandler
        """
        for day in _date_range(self.start_day, self.end_day):
            date_params = {
                'year': day.strftime('%Y'),
                'month': day.strftime('%m'),
                'day': day.strftime('%d'),
                'julday': day.strftime('%j'),
                # More flexible: can use any date formats.
                'date': day
            }

            for name, pattern in self.overridden_properties.items():
                value = pattern.format(**date_params)
                _log.debug('Setting %r=%r', name, value)
                setattr(self.using, name, value)

            _log.info('Triggering %r', self.using)
            self.using.trigger(reporter)


class TaskFailureListener(object):
    """
    Interface for listening to failures.
    """

    def on_file_failure(self, process_name, file_uri, summary, body_text):
        """
        On failure of a file download
        """
        pass

    def on_process_failure(self, process):
        """
        On process failure (Eg. error return code)
        :type process: ScheduledProcess
        """
        pass


class TaskFailureEmailer(TaskFailureListener):
    """
    Send failure information via email
    """

    def __init__(self, addresses):
        """
        :type addresses: list of str
        """
        self.addresses = addresses

    def on_file_failure(self, process_name, file_uri, summary, body_text):
        """
        Send mail on a
        :param process_name:
        :param file_uri:
        :param summary:
        :param body_text:
        :return:
        """
        self._send_mail(
            u'uri: {uri}\n{summary}\n\n{body}'.format(
                uri=file_uri,
                summary=summary,
                body=body_text
            ),
            process_name
        )

    def on_process_failure(self, process):
        """
        :type process: ScheduledProcess
        """

        # A negative exit code means it was killed via a signal. Probably by the user.
        # Not worth emailing.
        if process.exitcode < 0:
            return

        with open(process.log_file, 'rt') as f:
            msg = f.read()

        self._send_mail(msg, process.name)

    def _send_mail(self, body_text, process_name):
        """
        :type body_text: str
        :type process_name: str
        """
        hostname = socket.getfqdn()
        msg = MIMEText(body_text.encode('utf-8'), 'plain', 'utf-8')
        msg['Subject'] = Header(u'{name} failure on {hostname}'.format(
            name=process_name,
            hostname=hostname
        ).encode('utf-8'), 'utf-8')
        from_address = 'fetch-{pid}@{hostname}'.format(
            pid=multiprocessing.current_process().pid,
            hostname=hostname
        )
        msg['from'] = from_address
        msg['to'] = ", ".join(self.addresses)
        s = smtplib.SMTP('localhost')
        s.sendmail(
            from_address,
            self.addresses,
            msg.as_string()
        )
        s.quit()


class FileProcessor(SimpleObject):
    """
    Any action that will process a file after retrieval. (base class)
    """

    def process(self, file_path):
        """
        Process the given file (possibly returning a new filename to replace it.)
        :type file_path: str
        :return: file path
        :rtype str
        """
        raise NotImplementedError('process() was not implemented')


class ShellFileProcessor(FileProcessor):
    """
    A file processor that executes a (patterned) shell command.

    :type command: str
    """

    def __init__(self, command=None, expect_file=None, required_files=None):
        super(ShellFileProcessor, self).__init__()
        self.command = command
        self.expect_file = expect_file
        self.required_files = required_files

    def _apply_file_pattern(self, pattern, file_path, **keywords):
        """
        Format the given pattern.
        :type file_path: str

        :rtype: str

        >>> p = ShellFileProcessor()
        >>> p._apply_file_pattern('{file_stem} extension {file_suffix}', '/tmp/something.txt')
        'something extension .txt'
        >>> p._apply_file_pattern('{filename} in {parent_dir}', '/tmp/something.txt')
        'something.txt in /tmp'
        >>> p._apply_file_pattern('{parent_dirs[0]}', '/tmp/something.txt')
        '/tmp'
        >>> p._apply_file_pattern('{parent_dirs[1]}', '/tmp/something.txt')
        '/'
        """
        path = Path(file_path)
        return pattern.format(
            # Full filename
            filename=path.name,
            # Suffix of filename (with dot: '.txt')
            file_suffix=path.suffix,
            # Name without suffix
            file_stem=path.stem,
            # Parent (directory)
            parent_dir=str(path.parent),
            parent_dirs=[str(p) for p in path.parents],

            # A more flexible alternative to the above.
            path=path,
            **keywords
        )

    def process(self, file_path):
        """
        :type file_path: str
        :rtype: str
        :raises: FileProcessError
        """
        command = self.command
        if self.required_files:
            path_transform = RegexpOutputPathTransform(self.required_files[0])
            if not all([os.path.isfile(path_transform.transform_output_path(f, file_path))
                        for f in self.required_files[1]]):
                _log.info('Not all of the required_files are present.')
                # what is expected path used for?
                # It seems like reporting, so it is returning the file_path
                return file_path
            else:
                # format the path based on the group from
                # transform output path
                # command = path_transform.transform_output_path(command)
                required_files_formating = path_transform.groups
        else:
            required_files_formating = {}
        command = self._apply_file_pattern(command, file_path, **required_files_formating)
        _log.info('Running %r', command)

        # Trigger command
        returned = subprocess.call(command, shell=True)
        if returned != 0:
            raise FileProcessError('Return code %r from command %r' % (returned, command))

        # Check that output exists
        expected_path = self._apply_file_pattern(self.expect_file, file_path)

        if not os.path.exists(expected_path):
            raise FileProcessError('Expected output not found {!r} for command {!r}'.format(expected_path, command))

        _log.debug('File available %r', expected_path)
        return expected_path
