class DataSource(object):
    def __init__(self):
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
    def __init__(self):
        super(FetchReporter, self).__init__()

    def file_error(self, uri, message):
        pass

    def file_complete(self, uri, name, path):
        """
        Call on completion of a file
        :type uri: str
        :type name: str
        :type path: str
        :return:
        """
        pass
