"""
Auto-download of ancillary files.

It allows Operations to specify a serious of source locations (http/ftp/rss URLs)
and destination locations to download to.

This is intended to replace Operations maintenance of many diverse and
complicated scripts with a single, central configuration file.
"""
import logging
import sys
import heapq
import time
import multiprocessing

from . import http, ftp, DataSource, FetchReporter, RegexpOutputPathTransform, RsyncMirrorSource
import signal
from onreceipt.fetch import DateFilenameTransform, DateRangeSource

from croniter import croniter
from setproctitle import setproctitle

PROCESS_EXIT_WAIT = 900

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


def schedule_module(scheduled, now, item):
    """

    :type scheduled: list of (float, ScheduledItem)
    :param now: float
    :param item: ScheduledItem
    :return:
    """
    next_trigger = croniter(item.cron_pattern, start_time=now).get_next()
    heapq.heappush(scheduled, (next_trigger, item))
    return next_trigger


def schedule_modules(modules):
    """
    :type modules: dict of (str, (str, DataSource))
    """
    scheduled = []
    now = time.time()
    for name, (cron_pattern, module) in modules.iteritems():
        schedule_module(scheduled, now, ScheduledItem(name, cron_pattern, module))

    return scheduled


def load_modules():
    """
    Load the configuration of things to fetch.

    In the future this will come from an external text/YAML/JSON file.
    """
    # Hard-code the modules for now.
    # TODO: Load dynamically.
    anc_data = '/tmp/anc'
    return {
        'LS5 CPF': (
            '0 * * * *',
            http.RssSource(
                'https://landsat.usgs.gov/L5CPFRSS.rss',
                anc_data + '/sensor-specific/LANDSAT5/CalibrationParameterFile'
            )
        ),
        'LS7 CPF': (
            '10 * * * *',
            http.RssSource(
                'http://landsat.usgs.gov/L7CPFRSS.rss',
                anc_data + '/sensor-specific/LANDSAT7/CalibrationParameterFile'
            )
        ),
        'LS8 CPF': (
            '*/30 * 1 1,4,7,10 *',
            http.RssSource(
                'http://landsat.usgs.gov/cpf.rss',
                anc_data + '/sensor-specific/LANDSAT8/CalibrationParameterFile'
            )
        ),
        'LS8 BPF': (
            '*/15 * * * *',
            # -> Avail. 2-4 hours after acquisition
            http.RssSource(
                'http://landsat.usgs.gov/bpf.rss',
                anc_data + '/sensor-specific/LANDSAT8/BiasParameterFile/{year}/{month}',
                filename_transform=RegexpOutputPathTransform(
                    # Extract year and month from filenames to use in destination directory
                    'L[TO]8BPF(?P<year>[0-9]{4})(?P<month>[0-9]{2})(?P<day>[0-9]{2}).*'
                ),

            )
        ),
        'LS8 TLE': ('20 * * * *',
                    http.RssSource(
                        'http://landsat.usgs.gov/exchange_cache/outgoing/TLE/TLE.rss',
                        anc_data + '/sensor-specific/LANDSAT8/TLE/LS8_YEAR/{year}',
                        filename_transform=RegexpOutputPathTransform(
                            # Extract year from the filename to use in the output directory.
                            # Example filename: 506_MOE_ACQ_2014288120000_2014288120000_2014288123117_OPS_TLE.txt
                            '([A-Z0-9]+_){3}(?P<year>[0-9]{4})(?P<jul>[0-9]{3})[0-9]{6}.*_OPS_TLE.txt'
                        )
                    )
        ),
        'Modis utcpole/leapsec': (
            '0 7 * * mon',
            http.HttpSource(
                [
                    'http://oceandata.sci.gsfc.nasa.gov/Ancillary/LUTs/modis/utcpole.dat',
                    'http://oceandata.sci.gsfc.nasa.gov/Ancillary/LUTs/modis/leapsec.dat'
                ],
                anc_data + '/sensor-specific/MODIS/',
            )
        ),
        'Water vapour': (
            '0 1 * * *',
            ftp.FtpListingSource(
                'ftp.cdc.noaa.gov',
                source_dir='/Datasets/ncep.reanalysis/surface',
                name_pattern='pr_wtr.eatm.[0-9]{4}.nc',
                target_dir=anc_data + '/water_vapour/source'
            )
        ),
        'NPP GDAS/forecast': (
            '58 */2 * * *',
            DateRangeSource(
                # Download a date range of 3 days
                http.HttpListingSource(
                    # Added via the date range pattern
                    source_url='',
                    # Added via the date range pattern
                    target_dir='',
                    # Match three file types:
                    # gdas1.pgrb00.1p0deg.20110617_12_000.grib2
                    # NISE_SSMISF17_20110617.HDFEOS
                    # gfs.press_gr.0p5deg_pt.20110617_00_003.npoess.grib2
                    listing_name_filter='(gdas.*\\.npoess\\.grib2|NISE.*HDFEOS|gfs\\.press_gr.*grib2)'
                ),
                overridden_properties={
                    'source_url': 'http://jpssdb.ssec.wisc.edu/ancillary/{year}_{month}_{day}_{julday}',
                    'target_dir': anc_data + '/sensor-specific/NPP/VIIRS/CSPP/anc/cache/{year}_{month}_{day}_{julday}',
                },
                # Repeat between 1 day ago to 1 day in the future:
                from_days=-1,
                to_days=1,
            )
        ),
        'NPP LUTS': (
            '0 16 25 * *',
            http.HttpListingSource(
                source_url='http://jpssdb.ssec.wisc.edu/ancillary/LUTS_V_1_3',
                target_dir=anc_data + '/sensor-specific/NPP/VIIRS/CSPP/anc/cache/luts',
            )
        ),
        'Modis TLE': (
            '30 0-23/2 * * *',
            ftp.FtpSource(
                hostname='is.sci.gsfc.nasa.gov',
                source_paths=[
                    '/ancillary/ephemeris/tle/drl.tle',
                    '/ancillary/ephemeris/tle/norad.tle',
                ],
                # Prepend the current date to the output filename
                filename_transform=DateFilenameTransform('{year}{month}{day}.{filename}'),
                target_dir=anc_data + '/sensor-specific/MODIS/tle',
            )
        ),
        'NOAA TLE': (
            '40 0-23/2 * * *',
            ftp.FtpSource(
                hostname='is.sci.gsfc.nasa.gov',
                source_paths=[
                    '/ancillary/ephemeris/tle/noaa/noaa.tle',
                ],
                # Prepend the current date to the output filename
                filename_transform=DateFilenameTransform('{year}{month}{day}.{filename}'),
                target_dir=anc_data + '/sensor-specific/NOAA/tle',
            )
        ),
        'Modis GDAS': (
            '3 0-23/2 * * *',
            DateRangeSource(
                ftp.FtpListingSource(
                    hostname='ftp.ssec.wisc.edu',
                    # Added via the date range pattern
                    source_dir='',
                    # Added via the date range pattern
                    target_dir='',
                    name_pattern='gdas.*'
                ),
                overridden_properties={
                    'source_dir': '/pub/eosdb/ancillary/{year}_{month}_{day}_{julday}',
                    'target_dir': anc_data + '/sensor-specific/MODIS/ancillary/{year}/{month}',
                },
                from_days=-1,
                to_days=1
            )
        ),
        'Modis GFS': (
            '53 0-23/2 * * *',
            DateRangeSource(
                ftp.FtpListingSource(
                    hostname='ftp.ssec.wisc.edu',
                    # Added via the date range pattern
                    source_dir='',
                    # Added via the date range pattern
                    target_dir='',
                    name_pattern='gfs.*'
                ),
                overridden_properties={
                    'source_dir': '/pub/eosdb/ancillary/{year}_{month}_{day}_{julday}/forecast',
                    'target_dir': anc_data + '/sensor-specific/MODIS/ancillary/{year}/{month}/forecast',
                },
                from_days=-1,
                to_days=1
            )
        ),
        'Modis Att & Ephem': (
            '20 */2 * * *',
            DateRangeSource(
                http.HttpListingSource(
                    # Added via the date range pattern
                    source_url='',
                    # Added via the date range pattern
                    target_dir='',
                    listing_name_filter='[AP]M1(ATT|EPH).*'
                ),
                overridden_properties={
                    'source_url': 'http://oceandata.sci.gsfc.nasa.gov/Ancillary/Attitude-Ephemeris/{year}/{julday}',
                    'target_dir': anc_data + '/sensor-specific/MODIS/ancillary/{year}/{julday}',
                },
                from_days=-3,
                to_days=0,
            )
        ),
        'BRDF from NCI': (
            '0 9 * * 6',
            RsyncMirrorSource(
                source_path='/g/data1/u39/public/data/modis/lpdaac-mosaics-cmar/v1-hdf4/aust/MCD43A1.005/*',
                source_host='lpgs@r-dm.nci.org.au',
                target_path=anc_data + '/BRDF/CSIRO_mosaic',
            )
        )
    }


def run_module(reporter, name, module):
    setproctitle('fetch %s' % name)
    set_signals(enabled=False)
    _log.info('Running %s: %r', DataSource.__name__, module)

    try:
        module.trigger(reporter)
    except KeyboardInterrupt:
        raise
    except:
        _log.exception('Module %r failure', module)


def spawn_module(reporter, name, module):
    _log.info('Spawning %s', name)
    _log.debug('Spawning %r', module)
    p = multiprocessing.Process(
        target=run_module,
        name='fetch %s' % name,
        args=(reporter, name, module)
    )
    p.start()
    return p

should_exit = False
_scheduled_items = []


class ScheduledItem(object):
    def __init__(self, name, cron_pattern, module):
        super(ScheduledItem, self).__init__()
        self.name = name
        self.cron_pattern = cron_pattern
        self.module = module


def trigger_reload():
    _log.info('Reloading modules...')
    global _scheduled_items
    _scheduled_items = schedule_modules(load_modules())


def trigger_exit(signal, frame):
    _log.info('Should exit')
    global should_exit
    should_exit = True


def set_signals(enabled=True):
    # For a SIGINT signal (Ctrl-C) or SIGTERM signal (`kill <pid>` command), we start a graceful shutdown.
    signal.signal(signal.SIGINT, trigger_exit if enabled else signal.SIG_DFL)
    signal.signal(signal.SIGTERM, trigger_exit if enabled else signal.SIG_DFL)
    signal.signal(signal.SIGHUP, trigger_reload if enabled else signal.SIG_DFL)

def run_loop():
    global should_exit
    should_exit = False

    set_signals()
    reporter = _PrintReporter()

    trigger_reload()

    global _scheduled_items

    while not should_exit:
        _log.info('%r children', len(multiprocessing.active_children()))

        if not _scheduled_items:
            _log.info('No scheduled items. Sleeping.')
            time.sleep(500)
            continue

        now = time.time()

        #: :type: (int, ScheduledItem)
        next_time, scheduled_item = _scheduled_items[0]

        if next_time < now:
            # Pop
            #: :type: (int, ScheduledItem)
            next_time, scheduled_item = heapq.heappop(_scheduled_items)

            # Execute
            spawn_module(reporter, scheduled_item.name, scheduled_item.module)

            # Schedule next time
            next_trigger = schedule_module(_scheduled_items, now, scheduled_item)

            _log.debug('Next trigger in %.1s seconds', next_trigger - now)
        else:
            # Sleep until time
            sleep_seconds = (next_time - now) + 0.1
            _log.debug('Sleeping for %.1sm, until action %r', sleep_seconds / 60.0, scheduled_item.name)
            time.sleep(sleep_seconds)

    # TODO: Do something about error return codes from children?
    _log.info('Shutting down. Joining %r children', len(multiprocessing.active_children()))
    for p in multiprocessing.active_children():
        p.join()


def _run():
    """
    Fetch each configured ancillary file.
    """
    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
        level=logging.WARNING
    )
    _log.setLevel(logging.DEBUG)
    logging.getLogger('onreceipt').setLevel(logging.DEBUG)

    run_loop()


if __name__ == '__main__':
    _run()
