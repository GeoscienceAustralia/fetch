from __future__ import print_function, absolute_import

import tempfile
import unittest

from pathlib import Path

from fetch import load
from neocommon import files


class TestLoad(unittest.TestCase):
    def _fail_with_diff(self, reparsed_config, source_config):
        print('-' * 20)
        print_simple_obj_diff(source_config, reparsed_config)
        print('-' * 20)
        self.assertTrue(False, msg='Reparsed config not equal: see print output above.')

    def test_dump_load_dict(self):
        source_config = _make_config()

        raw_yaml = load._dump_config_dict(source_config)

        reparsed_config = load._load_config_dict(raw_yaml)

        if source_config != reparsed_config:
            self._fail_with_diff(reparsed_config, source_config)

    def _check_load_dump_config(self, make_config):
        original_config = load.Config.from_dict(make_config())
        config_file_path = Path(files.temp_dir(tempfile.tempdir, prefix='testrun'), 'config.yaml')
        with config_file_path.open(mode='w') as f:
            yaml = load.dump_yaml(original_config)
            f.write(unicode(yaml))
        reparsed_config = load.load_yaml(str(config_file_path))
        if make_config() != reparsed_config.to_dict():
            self._fail_with_diff(make_config(), reparsed_config.to_dict())

    def test_dump_load_obj_full(self):
        self._check_load_dump_config(_make_config)

    def test_dump_load_obj_no_messaing(self):
        def make_config_no_messaging():
            c = _make_config()
            del c['messaging']
            return c

        self._check_load_dump_config(make_config_no_messaging)


def print_simple_obj_diff(dict1, dict2):
    if type(dict2) in (int, float, str, unicode):
        print('-   {!r}'.format(dict1))
        print('+   {!r}'.format(dict2))
        return

    if type(dict1) == list:
        print_simple_list_diff(dict1, dict2)
        return

    if type(dict1) != dict:
        dict1 = dict1.__dict__
    if type(dict2) != dict:
        dict2 = dict2.__dict__

    for n in dict1:
        if n not in dict2 and (dict1[n] is not None):
            print('-   {!r}'.format(n))
    for n in dict2:
        if n not in dict1:
            print('+   {!r}'.format(n))
            continue
        if dict2[n] != dict1[n]:
            print('Not equal %r, \n%r\nand\n%r\n' % (n, dict1[n], dict2[n]))
            print_simple_obj_diff(dict1[n], dict2[n])

    return


def print_simple_list_diff(list1, list2):
    print('list\n')
    for i in range(len(list1)):
        if len(list2) <= i:
            print('-   {}: {!r}'.format(i, list1[i]))

        if list1[i] != list2[i]:
            print_simple_obj_diff(list1[i], list2[i])

    if len(list2) > len(list1):
        for i in range(len(list1), len(list2)):
            print('+   {}: {!r}'.format(i, list2[i]))
    print()


def _make_config():
    """
    Load a config dict (this is our old schedule)
    """
    from fetch import http, ftp, RegexpOutputPathTransform, \
        DateRangeSource, DateFilenameTransform, \
        RsyncMirrorSource, ShellFileProcessor
    # Dump / Load / Dump to test our routines.
    anc_data = '/tmp/anc'
    schedule = {
        'directory': '/tmp/anc-fetch',
        'notify': {
            'email': ['jeremy.hooke@ga.gov.au']
        },
        'messaging': {
            'host': 'rhe-pma-test08.test.lan',
            'username': 'fetch',
            'password': 'fetch'
        },
        'log': {
            'fetch': 'DEBUG'
        },
        'rules': {
            'LS5 CPF': {
                'schedule': '0 * * * *',
                'source': http.RssSource(
                    url='https://landsat.usgs.gov/L5CPFRSS.rss',
                    target_dir=anc_data + '/sensor-specific/LANDSAT5/CalibrationParameterFile'
                )
            },
            'LS7 CPF': {
                'schedule': '10 * * * *',
                'source': http.RssSource(
                    url='http://landsat.usgs.gov/L7CPFRSS.rss',
                    target_dir=anc_data + '/sensor-specific/LANDSAT7/CalibrationParameterFile'
                )
            },
            'LS8 CPF': {
                'schedule': '*/30 * 1 1,4,7,10 *',
                'source': http.RssSource(
                    url='http://landsat.usgs.gov/cpf.rss',
                    target_dir=anc_data + '/sensor-specific/LANDSAT8/CalibrationParameterFile'
                )
            },
            'LS8 BPF': {
                'schedule': '*/15 * * * *',
                # -> Avail. 2-4 hours after acquisition
                'source': http.RssSource(
                    url='http://landsat.usgs.gov/bpf.rss',
                    target_dir=anc_data + '/sensor-specific/LANDSAT8/BiasParameterFile/{year}/{month}',
                    filename_transform=RegexpOutputPathTransform(
                        # Extract year and month from filenames to use in destination directory
                        'L[TO]8BPF(?P<year>[0-9]{4})(?P<month>[0-9]{2})(?P<day>[0-9]{2}).*'
                    ),
                )
            },
            'LS8 TLE': {
                'schedule': '20 * * * *',
                'source': http.RssSource(
                    url='http://landsat.usgs.gov/exchange_cache/outgoing/TLE/TLE.rss',
                    target_dir=anc_data + '/sensor-specific/LANDSAT8/TLE/LS8_YEAR/{year}',
                    filename_transform=RegexpOutputPathTransform(
                        # Extract year from the filename to use in the output directory.
                        # Example filename: 506_MOE_ACQ_2014288120000_2014288120000_2014288123117_OPS_TLE.txt
                        '([A-Z0-9]+_){3}(?P<year>[0-9]{4})(?P<jul>[0-9]{3})[0-9]{6}.*_OPS_TLE.txt'
                    ),
                    beforehand=http.HttpPostAction(
                        url='https://landsat.usgs.gov/up_login.php',
                        params={"username": "australia",
                                "password": "fake-password"})
                )
            },
            'Modis utcpole-leapsec': {
                'schedule': '0 7 * * mon',
                'source': http.HttpSource(
                    urls=[
                        'http://oceandata.sci.gsfc.nasa.gov/Ancillary/LUTs/modis/utcpole.dat',
                        'http://oceandata.sci.gsfc.nasa.gov/Ancillary/LUTs/modis/leapsec.dat'
                    ],
                    target_dir=anc_data + '/sensor-specific/MODIS/',
                )
            },
            'Water vapour': {
                'schedule': '0 1 * * *',
                'source': ftp.FtpListingSource(
                    'ftp.cdc.noaa.gov',
                    source_dir='/Datasets/ncep.reanalysis/surface',
                    name_pattern='pr_wtr.eatm.[0-9]{4}.nc',
                    target_dir=anc_data + '/water_vapour/source'
                ),
                'process': ShellFileProcessor(
                    command='/usr/local/bin/gdal_translate -a_srs "+proj=latlong +datum=WGS84" '
                            '{parent_dir}/{file_stem}.nc {parent_dir}/{file_stem}.tif',
                    expect_file='{parent_dir}/{file_stem}.tif'
                )
            },
            'NPP GDAS-forecast': {
                'schedule': '58 */2 * * *',
                'source': DateRangeSource(
                    # Download a date range of 3 days
                    http.HttpListingSource(
                        # Added via the date range pattern
                        url='',
                        # Added via the date range pattern
                        target_dir='',
                        # Match three file types:
                        # gdas1.pgrb00.1p0deg.20110617_12_000.grib2
                        # NISE_SSMISF17_20110617.HDFEOS
                        # gfs.press_gr.0p5deg_pt.20110617_00_003.npoess.grib2
                        name_pattern='(gdas.*\\.npoess\\.grib2|NISE.*HDFEOS|gfs\\.press_gr.*grib2)'
                    ),
                    overridden_properties={
                        'url': 'http://jpssdb.ssec.wisc.edu/ancillary/{year}_{month}_{day}_{julday}',
                        'target_dir': (anc_data +
                                       '/sensor-specific/NPP/VIIRS/CSPP/anc/cache/'
                                       '{year}_{month}_{day}_{julday}'),
                    },
                    # Repeat between 1 day ago to 1 day in the future:
                    start_day=-1,
                    end_day=1,
                )
            },
            'NPP LUTS': {
                'schedule': '0 16 25 * *',
                'source': http.HttpListingSource(
                    url='http://jpssdb.ssec.wisc.edu/ancillary/LUTS_V_1_3',
                    target_dir=anc_data + '/sensor-specific/NPP/VIIRS/CSPP/anc/cache/luts',
                )
            },
            'Modis TLE': {
                'schedule': '30 0-23/2 * * *',
                'source': ftp.FtpSource(
                    hostname='is.sci.gsfc.nasa.gov',
                    paths=[
                        '/ancillary/ephemeris/tle/drl.tle',
                        '/ancillary/ephemeris/tle/norad.tle',
                    ],
                    # Prepend the current date to the output filename
                    filename_transform=DateFilenameTransform('{year}{month}{day}.{filename}'),
                    target_dir=anc_data + '/sensor-specific/MODIS/tle',
                )
            },
            'NOAA TLE': {
                'schedule': '40 0-23/2 * * *',
                'source': ftp.FtpSource(
                    hostname='is.sci.gsfc.nasa.gov',
                    paths=[
                        '/ancillary/ephemeris/tle/noaa/noaa.tle',
                    ],
                    # Prepend the current date to the output filename
                    filename_transform=DateFilenameTransform('{year}{month}{day}.{filename}'),
                    target_dir=anc_data + '/sensor-specific/NOAA/tle',
                )
            },
            'Modis GDAS': {
                'schedule': '3 0-23/2 * * *',
                'source': DateRangeSource(
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
                    start_day=-1,
                    end_day=1
                )
            },
            'Modis GFS': {
                'schedule': '53 0-23/2 * * *',
                'source': DateRangeSource(
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
                    start_day=-1,
                    end_day=1
                )
            },
            'Modis Att-Ephem': {
                'schedule': '20 */2 * * *',
                'source': DateRangeSource(
                    http.HttpListingSource(
                        # Added via the date range pattern
                        url='',
                        # Added via the date range pattern
                        target_dir='',
                        name_pattern='[AP]M1(ATT|EPH).*'
                    ),
                    overridden_properties={
                        'url': 'http://oceandata.sci.gsfc.nasa.gov/Ancillary/Attitude-Ephemeris/{year}/{julday}',
                        'target_dir': anc_data + '/sensor-specific/MODIS/ancillary/{year}/{julday}',
                    },
                    start_day=-3,
                    end_day=0,
                )
            },
            'BRDF from NCI': {
                'schedule': '0 9 * * 6',
                'source': RsyncMirrorSource(
                    source_path='/g/data/u39/public/data/modis/lpdaac-mosaics-cmar/v1-hdf4/aust/MCD43A1.005/*',
                    source_host='lpgs@r-dm.nci.org.au',
                    target_path=anc_data + '/BRDF/CSIRO_mosaic',
                )
            }
        }
    }
    return schedule
