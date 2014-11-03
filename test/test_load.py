import unittest
from fetch import http, RegexpOutputPathTransform, ftp, DateRangeSource, DateFilenameTransform, \
    RsyncMirrorSource, load

__author__ = 'u63606'


class TestLoad(unittest.TestCase):
    def test_dump_load(self):
        source_config = _make_config()

        raw_yaml = load._dump_config_dict(source_config)
        reparsed_config = load._load_config_dict(raw_yaml)

        if source_config != reparsed_config:
            print '-' * 20
            print_simple_obj_diff(source_config, reparsed_config)
            print '-' * 20
            self.assertTrue(False, msg='Reparsed config not equal: see print output above.')


def print_simple_obj_diff(dict1, dict2):
    if type(dict1) != dict:
        dict1 = dict1.__dict__
    if type(dict2) != dict:
        dict2 = dict2.__dict__

    for n in dict1:
        if n not in dict2:
            print('-   "' + str(n) + '":')
    for n in dict2:
        if n not in dict1:
            print('+   "' + str(n) + '":')
            continue
        if dict2[n] != dict1[n]:
            print 'Not equal %r' % n
            if type(dict2[n]) in (int, float, str, unicode, list):
                print('-   "' + str(n) + '" : "' + str(dict1[n]))
                print('+   "' + str(n) + '" : "' + str(dict2[n]))
            else:
                first = dict1[n]
                second = dict2[n]
                print_simple_obj_diff(first, second)
    return


def _make_config():
    """
    Load a config dict (this is our old schedule)
    """
    # Dump / Load / Dump to test our routines.
    anc_data = '/tmp/anc'
    schedule = {
        'directory': '/tmp/anc-fetch',
        'notify': {
            'email': ['jeremy.hooke@ga.gov.au']
        },
        'rules': {
            'LS5 CPF': {
                'schedule': '0 * * * *',
                'source': http.RssSource(
                    'https://landsat.usgs.gov/L5CPFRSS.rss',
                    anc_data + '/sensor-specific/LANDSAT5/CalibrationParameterFile'
                )
            },
            'LS7 CPF': {
                'schedule': '10 * * * *',
                'source': http.RssSource(
                    'http://landsat.usgs.gov/L7CPFRSS.rss',
                    anc_data + '/sensor-specific/LANDSAT7/CalibrationParameterFile'
                )
            },
            'LS8 CPF': {
                'schedule': '*/30 * 1 1,4,7,10 *',
                'source': http.RssSource(
                    'http://landsat.usgs.gov/cpf.rss',
                    anc_data + '/sensor-specific/LANDSAT8/CalibrationParameterFile'
                )
            },
            'LS8 BPF': {
                'schedule': '*/15 * * * *',
                # -> Avail. 2-4 hours after acquisition
                'source': http.RssSource(
                    'http://landsat.usgs.gov/bpf.rss',
                    anc_data + '/sensor-specific/LANDSAT8/BiasParameterFile/{year}/{month}',
                    filename_transform=RegexpOutputPathTransform(
                        # Extract year and month from filenames to use in destination directory
                        'L[TO]8BPF(?P<year>[0-9]{4})(?P<month>[0-9]{2})(?P<day>[0-9]{2}).*'
                    ),
                )
            },
            'LS8 TLE': {
                'schedule': '20 * * * *',
                'source': http.RssSource(
                    'http://landsat.usgs.gov/exchange_cache/outgoing/TLE/TLE.rss',
                    anc_data + '/sensor-specific/LANDSAT8/TLE/LS8_YEAR/{year}',
                    filename_transform=RegexpOutputPathTransform(
                        # Extract year from the filename to use in the output directory.
                        # Example filename: 506_MOE_ACQ_2014288120000_2014288120000_2014288123117_OPS_TLE.txt
                        '([A-Z0-9]+_){3}(?P<year>[0-9]{4})(?P<jul>[0-9]{3})[0-9]{6}.*_OPS_TLE.txt'
                    )
                )
            },
            'Modis utcpole-leapsec': {
                'schedule': '0 7 * * mon',
                'source': http.HttpSource(
                    [
                        'http://oceandata.sci.gsfc.nasa.gov/Ancillary/LUTs/modis/utcpole.dat',
                        'http://oceandata.sci.gsfc.nasa.gov/Ancillary/LUTs/modis/leapsec.dat'
                    ],
                    anc_data + '/sensor-specific/MODIS/',
                )
            },
            'Water vapour': {
                'schedule': '0 1 * * *',
                'source': ftp.FtpListingSource(
                    'ftp.cdc.noaa.gov',
                    source_dir='/Datasets/ncep.reanalysis/surface',
                    name_pattern='pr_wtr.eatm.[0-9]{4}.nc',
                    target_dir=anc_data + '/water_vapour/source'
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
                        'target_dir': anc_data + '/sensor-specific/NPP/VIIRS/CSPP/anc/cache/{year}_{month}_{day}_{julday}',
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
                    source_path='/g/data1/u39/public/data/modis/lpdaac-mosaics-cmar/v1-hdf4/aust/MCD43A1.005/*',
                    source_host='lpgs@r-dm.nci.org.au',
                    target_path=anc_data + '/BRDF/CSIRO_mosaic',
                )
            }
        }
    }
    return schedule
