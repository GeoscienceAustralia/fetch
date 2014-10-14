"""
A package for automatically fetching files (eg. Ancillary).
"""


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
