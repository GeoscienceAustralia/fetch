from onreceipt.fetch import http, RegexpOutputPathTransform, ftp, DateRangeSource, DateFilenameTransform, \
    RsyncMirrorSource

__author__ = 'u63606'


def load_modules():
    """
    Load the rules for fetching.

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
