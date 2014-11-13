"""
Logic to load configuration.

"""
import functools
import logging
import os
import inspect
from croniter import croniter
import yaml
import yaml.resolver

from . import http, ftp, RegexpOutputPathTransform, DateRangeSource, DateFilenameTransform, \
    RsyncMirrorSource, SimpleObject, ShellFileProcessor


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


class ScheduledItem(SimpleObject):
    """
    Scheduling information for a module.
    :type name: str
    :type cron_pattern: str
    :type module: fetch.DataSource
    :type process: fetch.FileProcessor
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
        """
        The name with whitespace and special chars stripped out.
        :rtype: str
        """
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
        config = Config.from_dict(config_dict)
    except ValueError as v:
        raise ConfigError(v)

    return config


def dump_yaml(config):
    """
    :type config: Config
    :rtype: str
    """
    return _dump_config_dict(config.to_dict())


class Config(object):
    """
    Configuration.
    """

    def __init__(self, directory, rules, notify_addresses):
        """
        :type directory: str
        :type rules: set of ScheduledItem
        """
        super(Config, self).__init__()
        self.directory = directory
        if not directory:
            raise ValueError("No 'directory' specified in config")

        # Empty list of rules is ok: they may be added after startup (a config reload/SIGHUP).
        self.rules = rules

        self.notify_addresses = notify_addresses

    @classmethod
    def from_dict(cls, config):
        """
        Create Config object from dict (typically the dict output by YAML parsing)
        :type config: dict
        :rtype: Config
        :raises: ValueError
        """

        directory = config.get('directory')

        notify_email_addresses = []
        if 'notify' in config:
            notify_config = config['notify']
            if 'email' in notify_config:
                notify_email_addresses = notify_config['email']

        rules = set()
        if 'rules' in config:
            for name, fields in config['rules'].iteritems():
                item = ScheduledItem(name, fields.get('schedule'), fields.get('source'),
                                     process=fields.get('process'))
                rules.add(item)

        return Config(directory, rules, notify_email_addresses)

    def to_dict(self):
        """
        Convert to simple dict format (expected by our YAML output)
        :return:
        """
        return {
            'directory': self.directory,
            'notify': {
                'email': self.notify_addresses
            },
            'rules': dict([
                (
                    r.name, _remove_nones({
                        'schedule': r.cron_pattern,
                        'source': r.module,
                        'process': r.process
                    })
                )
                for r in self.rules
            ])
        }


def _remove_nones(dict_):
    """
    Remove fields from the dict whose values are None.

    Returns a new dict.
    :type dict_: dict
    :rtype dict

    >>> _remove_nones({'a': 4, 'b': None})
    {'a': 4}
    >>> _remove_nones({'a': 'a', 'b': 0})
    {'a': 'a', 'b': 0}
    >>> _remove_nones({})
    {}
    """
    return dict([(k, v) for k, v in dict_.iteritems() if v is not None])


def _load_config_dict(file_io):
    """Load YAML file into config dict"""
    return yaml.load(file_io)


def _dump_config_dict(dic):
    """Dump a config dict into a YAML string"""
    return yaml.dump(dic, default_flow_style=False)


def verify_can_construct(target_class, fields, identifier=None):
    """
    Verify that a class can be constructed with given arguments.
    :type target_class: class
    :type fields: dict
    :type identifier: str

    >>> # All Args
    >>> verify_can_construct(DateRangeSource, {'using': 1, 'overridden_properties': 2, 'start_day': 3, 'end_day': 4})
    >>> # Just required args
    >>> verify_can_construct(DateRangeSource, {'using': 1, 'overridden_properties': 2})
    >>> # With one optional arg
    >>> verify_can_construct(DateRangeSource, {'using': 1, 'overridden_properties': 2, 'start_day': 3})
    >>> # A required arg missing
    >>> verify_can_construct(DateRangeSource, {'overridden_properties': 2})
    Traceback (most recent call last):
    ...
    ValueError: Required field 'using' not found for 'DateRangeSource'
    >>> # Invalid argument
    >>> verify_can_construct(DateRangeSource, {'not_an_arg': 2}, identifier='!date-range')
    Traceback (most recent call last):
    ...
    ValueError: Unknown field 'not_an_arg' for '!date-range'. (Supports 'using', 'overridden_properties', 'start_day', 'end_day')
    """
    if not identifier:
        identifier = target_class.__name__
    arg_spec = inspect.getargspec(target_class.__init__)

    # Does the class take no arguments (just 'self')?
    if len(arg_spec.args) == 1 and len(fields) == 0:
        # Have no arguments and it takes no arguments, so we're done.
        return

    # Get argument names other than 'self' (the first)
    arg_names = arg_spec.args[1:]
    optional_arg_count = len(arg_spec.defaults) if arg_spec.defaults else 0
    # Required args are those that precede the optional args.
    required_arg_names = arg_names[:-optional_arg_count]

    # Are there any invalid fields?
    for name in fields:
        if name not in arg_names:
            raise ValueError("Unknown field %r for '%s'. (Supports '%s')" % (name, identifier, "', '".join(arg_names)))

    # Are there any missing required fields?
    for name in required_arg_names:
        if name not in fields.keys():
            raise ValueError("Required field %r not found for '%s'" % (name, identifier))


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
        #: :type: dict
        fields = loader.construct_mapping(node)

        if not hasattr(cls, '__init__'):
            raise RuntimeError('Class has no init method: %r' % cls)

        verify_can_construct(cls, fields, node.tag)

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

    def add_default_constructor(object_class, type_annotation, flow_style=None):
        """
        A default object-to-map association for YAML.

        The class being mapped must have exactly matching fields and constructor arguments.
        """
        yaml.add_constructor(type_annotation, functools.partial(_yaml_default_constructor, object_class))
        yaml.add_representer(object_class, functools.partial(_yaml_default_representer, type_annotation, flow_style))

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
    add_default_constructor(ShellFileProcessor, '!shell')
    add_default_constructor(http.HttpListingSource, '!http-directory')
    add_default_constructor(http.HttpSource, '!http-files')
    add_default_constructor(http.RssSource, '!rss')
    add_default_constructor(ftp.FtpSource, '!ftp-files')
    add_default_constructor(ftp.FtpListingSource, '!ftp-directory')
    add_item_constructor(RegexpOutputPathTransform, '!regexp-extract', 'pattern')
    add_item_constructor(DateFilenameTransform, '!date-pattern', 'format_')


_init_yaml_handling()

