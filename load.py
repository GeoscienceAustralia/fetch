"""
Logic to load configuration.

"""
import functools

from . import http, RegexpOutputPathTransform, ftp, DateRangeSource, DateFilenameTransform, \
    RsyncMirrorSource
import os
import yaml
import yaml.resolver


class ScheduledItem(object):
    """
    Scheduling information for a module.
    :type name: str
    :type cron_pattern: str
    :type module: onreceipt.fetch.DataSource
    """

    def __init__(self, name, cron_pattern, module):
        super(ScheduledItem, self).__init__()
        self.name = name
        self.cron_pattern = cron_pattern
        self.module = module


def load_yaml(file_path):
    """
    Load configuration.

    :rtype: Config
    """
    if not os.path.exists(file_path):
        raise ValueError('Config path does not exist: %r', file_path)

    conf_dict = yaml.load(open(file_path, 'r'))
    return _parse_config_dict(conf_dict)


class Config(object):
    """
    Configuration.
    """

    def __init__(self, directory, rules, notify_addresses):
        """
        :type directory: str
        :type rules: list of ScheduledItem
        """
        super(Config, self).__init__()
        self.directory = directory
        self.rules = rules
        self.notify_addresses = notify_addresses


def _parse_config_dict(config):
    """

    :rtype: list of ScheduledItem
    """
    directory = config['directory']
    notify_email_addresses = config['notify']['email']

    rules = []
    for name, fields in config['rules'].iteritems():
        rules.append(ScheduledItem(name, fields['schedule'], fields['source']))

    return Config(directory, rules, notify_email_addresses)


def _init_yaml_handling():
    """
    Allow load/dump of our custom classes in YAML.
    """

    def _yaml_default_constructor(cls, loader, node):
        """
        A YAML parser that maps fields ot parameter names of the class constructor.

        :type loader: yaml.Loader
        :param node:
        :return:
        """
        fields = loader.construct_mapping(node)
        return cls(**fields)

    def _yaml_item_constructor(cls, loader, node):
        """
        A YAML parser that that maps a single string to a one-argument class constructor.

        :type loader: yaml.Loader
        :param node:
        :return:
        """
        field = loader.construct_scalar(node)
        return cls(field)

    def _yaml_default_representer(tag, flow_style, dumper, data):
        """
        Represent the (__dict__) fields of an object as a YAML map.

        Null fields are ignored.

        :param dumper: yaml.Dumper
        :param data:
        :return:
        """
        clean_dict = dict((k, v) for k, v in data.__dict__.iteritems() if v is not None)
        return dumper.represent_mapping(
            tag,
            clean_dict,
            flow_style=flow_style
        )

    def _yaml_item_representer(tag, attr_name, dumper, data):
        """
        Represent an attribute of the given object as a simple yaml string.
        """
        return dumper.represent_scalar(tag, getattr(data, attr_name))

    def add_default_constructor(source, type_annotation, flow_style=None):
        """
        A default object-to-map association for YAML.

        The class being mapped must have exactly matching fields and constructor arguments.
        """
        yaml.add_constructor(type_annotation, functools.partial(_yaml_default_constructor, source))
        yaml.add_representer(source, functools.partial(_yaml_default_representer, type_annotation, flow_style))

    def add_item_constructor(source, type_annotation, attribute):
        """
        A string-to-object association for YAML

        The object class must have exactly one constructor argument.

        :param attribute: The name of the attribute to fetch the string from.
        """
        yaml.add_constructor(type_annotation, functools.partial(_yaml_item_constructor, source))
        yaml.add_representer(source, functools.partial(_yaml_item_representer, type_annotation, attribute))

    add_default_constructor(DateRangeSource, '!date-range')
    add_default_constructor(RsyncMirrorSource, '!rsync')
    add_default_constructor(http.HttpListingSource, '!http-directory')
    add_default_constructor(http.HttpSource, '!http-files')
    add_default_constructor(http.RssSource, '!rss')
    add_default_constructor(ftp.FtpSource, '!ftp-files')
    add_default_constructor(ftp.FtpListingSource, '!ftp-directory')
    add_item_constructor(RegexpOutputPathTransform, '!regexp-extract', 'pattern')
    add_item_constructor(DateFilenameTransform, '!date-pattern', 'format_')


_init_yaml_handling()


def dump_old_schedule():
    """
    Dump the hard-coded schedule into YAML.

    Does a load/dump/load to test our parsing/etc.

    :return:
    """
    # Dump / Load / Dump to test our routines.
    schedule = {
        'directory': '/tmp/anc-fetch',
        'rules': _old_schedule(),
        'notify': {
            'email': ['jeremy.hooke@ga.gov.au']
        }
    }
    doc = yaml.dump(schedule, default_flow_style=False)
    new_rules = yaml.load(doc)
    print yaml.dump(new_rules, default_flow_style=False)


def _old_schedule():
    """
    Load the download rules.

    In the future this will come from an external text/YAML/JSON file.

    """
    # Hard-code the modules for now.
    # TODO: Load dynamically.
    anc_data = '/tmp/anc'
    return {
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


if __name__ == '__main__':
    dump_old_schedule()
