"""
A package for automatically fetching files (eg. Ancillary).
"""
import re
import logging

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


class FilenameProxy(object):
    """
    A base class for objects that read filenames and alter destination paths.

    Eg. Reading the date from a source filename so that output can be stored in year/month folders.
    """

    def __init__(self):
        super(FilenameProxy, self).__init__()

    def transform_destination_path(self, path, source_filename=None):
        """
        Override this method to modify output path of a file.
        """
        return path


class RegexpFilenameProxy(FilenameProxy):
    """
    To extract fields from a filename with a regexp, and
    replace similar fields in destionation paths.
    """

    def __init__(self, regexp):
        """
        :type regexp: str
        """
        super(RegexpFilenameProxy, self).__init__()

        #: :type: re.Regexp
        self.regexp = re.compile(regexp)

    def transform_destination_path(self, path, source_filename=None):
        """

        :param path:
        :param source_filename:

        >>> RegexpFilenameProxy(r'LS8_(?P<year>\\d{4})').transform_destination_path('LS8_2003', '/tmp/out/{year}')
        '/tmp/out/2003'
        >>> RegexpFilenameProxy(r'LS8_(?P<year>\\d{4})').transform_destination_path('LS8_2003', '/tmp/out/{year}')
        '/tmp/out/2003'
        """
        m = self.regexp.match(source_filename)

        if not m:
            _log.info('No regexp match for %r', path)
            return path

        groups = m.groupdict()

        return path.format(**groups)

