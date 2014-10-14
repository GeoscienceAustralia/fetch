"""
Auto-download of ancillary files.

It allows Operations to specify a serious of source locations (http/ftp/rss URLs)
and destination locations to download to.

This is intended to replace Operations maintenance of many diverse and
complicated scripts with a single, central configuration file.
"""
from . import http, DataSource, FetchReporter
import logging
import sys

_log = logging.getLogger(__name__)


class _PrintReporter(FetchReporter):
    """
    Send events to the log.
    """
    def file_complete(self, uri, name, path):
        """
        :type uri: str
        :type name: str
        :type path: str
        """
        _log.info('Completed %r: %r -> %r', name, uri, path)

    def file_error(self, uri, message):
        """
        :type uri: str
        :type message: str
        """
        _log.info('Error (%r): %r)', uri, message)


def execute_modules(modules):
    """
    Execute the given modules once.

    :type modules: list of DataSource
    :return:
    """
    reporter = _PrintReporter()
    # TODO: Filter based on module period (daily, hourly etc).
    for module in modules:
        _log.info('Running %s: %r', DataSource.__name__, module)

        module.trigger(reporter)


def _run():
    """
    Fetch each configured ancillary file.
    """
    logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s %(message)s",
                        stream=sys.stderr, level=logging.WARNING)
    _log.setLevel(logging.DEBUG)
    logging.getLogger('onreceipt').setLevel(logging.DEBUG)

    # Hard-code the modules for now.
    # TODO: Load dynamically.
    modules = [
        http.HttpSource(
            [
                'http://oceandata.sci.gsfc.nasa.gov/Ancillary/LUTs/modis/utcpole.dat',
                'http://oceandata.sci.gsfc.nasa.gov/Ancillary/LUTs/modis/leapsec.dat'
            ],
            '/tmp'
        )
    ]
    execute_modules(modules)


if __name__ == '__main__':
    _run()
