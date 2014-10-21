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

        >>> RegexpOutputPathTransform(r'LS8_(?P<year>\\d{4})').transform_output_path('/tmp/out/{year}', 'LS8_2003')
        '/tmp/out/2003'
        >>> RegexpOutputPathTransform(r'LS8_(?P<year>\\d{4})').transform_output_path('/tmp/out', 'LS8_2003')
        '/tmp/out'
        """
        m = re.match(self.pattern, source_filename)

        if not m:
            _log.info('No regexp match for %r', path)
            return path

        groups = m.groupdict()

        return path.format(**groups)




