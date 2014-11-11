"""
Logic to load configuration.

"""
import functools
import logging
import os

from croniter import croniter
import yaml
import yaml.resolver

from . import http, ftp, RegexpOutputPathTransform, DateRangeSource, DateFilenameTransform, \
    RsyncMirrorSource

from pathlib import Path

_log = logging.getLogger(__name__)


class ConfigError(ValueError):
    """
    An invalid config file.
    """
    pass


def _sanitize_for_filename(text):
    """
    Sanitize the given text for use in a filename.

    (particularly log and lock files under Unix. So we lowercase them.)

    :type text: str
    :rtype: str
    >>> _sanitize_for_filename('some one')
    'some-one'
    >>> _sanitize_for_filename('s@me One')
    's-me-one'
    >>> _sanitize_for_filename('LS8 BPF')
    'ls8-bpf'
    """
    return "".join([x if x.isalnum() else "-" for x in text.lower()])


class FileProcessor(object):
    def process(self, file_path):
        """
        Process the given file (possibly returning a new filename to replace it.)
        :type file_path: str
        :return: file path
        :rtype str
        """
        return file_path


class ShellFileProcessor(FileProcessor):

    def __init__(self, command, expect_file):
        """
        A file processor that executes a (patterned) shell command.

        :type command: str
        """
        super(ShellFileProcessor, self).__init__()
        self.patterned_command = command
        self.expected_patterned_file = expect_file

    def _build_command(self, pattern, file_path):
        path = Path(file_path)
        return pattern.format(
            # Full filename
            filename=path.name,
            # Suffix of filename
            file_suffix=path.suffix,
            # Name without suffix
            file_stem=path.stem,
            # Parent (directory)
            parent_dir=str(path.parent),
            parent_dirs=[str(p) for p in path.parents]
        )

    def process(self, file_path):
        """
        """
        command = self._build_command(self.patterned_command, file_path)
        # Trigger command
        # Check that output exists
        expected_path = self._build_command(self.expected_patterned_file, file_path)

        return expected_path


class ScheduledItem(object):
    """
    Scheduling information for a module.
    :type name: str
    :type cron_pattern: str
    :type module: fetch.DataSource
    :type process:
    """

    def __init__(self, name, cron_pattern, module, process=None):
        super(ScheduledItem, self).__init__()
        self.name = name
        if not name:
            raise ValueError('No name provided for item (%r, %r)' % (cron_pattern, module))

        self.module = module
        if not module:
            raise ValueError('No source module for item %r' % (name,))

        # Optional file processor.
        self.process = process

        self.cron_pattern = cron_pattern
        if not cron_pattern:
            raise ValueError('No cron schedule provided for item %r' % (name,))

        # Validate cron expression immediately.
        try:
            croniter(cron_pattern)
        except ValueError as v:
            raise ValueError('Cron parse error on {!r}: {!r}'.format(name, cron_pattern), v)

    @property
    def sanitized_name(self):
        return _sanitize_for_filename(self.name)


def load_yaml(file_path):
    """
    Load configuration.

    :rtype: Config
    :raises: ConfigError
    """
    if not os.path.exists(file_path):
        raise ConfigError('Config path does not exist: %r', file_path)

    file_io = open(file_path, 'r')
    try:
        config_dict = _load_config_dict(file_io)
    # TODO: What parse exceptions does yaml throw?
    except Exception as e:
        raise ConfigError(e)

    try:
        config = _parse_config_dict(config_dict)
    except ValueError as v:
        raise ConfigError(v)

    return config


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
        if not directory:
            raise ValueError("No 'directory' specified in config")

        # Empty list of rules is ok: they may be added after startup (a config reload/SIGHUP).
        self.rules = rules

        self.notify_addresses = notify_addresses


def _load_config_dict(file_io):
    """Load YAML file into config dict"""
    return yaml.load(file_io)


def _dump_config_dict(dic):
    """Dump a config dict into a YAML string"""
    return yaml.dump(dic, default_flow_style=False)


def _parse_config_dict(config):
    """
    :rtype: list of ScheduledItem
    :raises: ValueError
    """

    directory = config.get('directory')

    notify_email_addresses = []
    if 'notify' in config:
        notify_config = config['notify']
        if 'email' in notify_config:
            notify_email_addresses = notify_config['email']

    rules = []
    if 'rules' in config:
        for name, fields in config['rules'].iteritems():
            item = ScheduledItem(name, fields.get('schedule'), fields.get('source'), process=fields.get('process'))
            rules.append(item)

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

