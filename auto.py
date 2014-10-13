
from . import http, DataSource, FetchReporter
import logging
import sys

_log = logging.getLogger(__name__)


class _PrintReporter(FetchReporter):
    def file_complete(self, uri, name, path):
        _log.info('Completed %r: %r -> %r', name, uri, path)

    def file_error(self, uri, message):
        _log.info('Error (%r): %r)', uri, message)


def execute_modules(modules):
    """

    :type modules: list of DataSource
    :return:
    """
    reporter = _PrintReporter()
    # TODO: Filter based on module period (daily, hourly etc).
    for module in modules:
        _log.info('Running module %r', module)
        module.trigger(reporter)


def _main():
    logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s %(message)s", stream=sys.stderr, level=logging.WARNING)
    _log.setLevel(logging.DEBUG)
    logging.getLogger('onreceipt').setLevel(logging.DEBUG)

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
    _main()
