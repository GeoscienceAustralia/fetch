"""
Logic to load configuration.

"""
import functools

from . import http, ftp, RegexpOutputPathTransform, DateRangeSource, DateFilenameTransform, \
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

    file_io = open(file_path, 'r')
    return _parse_config_dict(_load_config_dict(file_io))


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


def _load_config_dict(file_io):
    return yaml.load(file_io)


def _dump_config_dict(dic):
    return yaml.dump(dic, default_flow_style=False)


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

