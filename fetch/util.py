# coding=utf-8
"""
Common functions. (mostly imported from legacy 'neocommon' library.)
"""
from __future__ import absolute_import

import logging
import os
import socket
import subprocess

_log = logging.getLogger()


class UnsupportedUriError(Exception):
    """
    The given URI cannot be handled by this code (yet?).
    """
    pass


class Uri(object):
    """
    Represents a parsed URI. Use Uri.parse() to create them from a string.
    """

    def __init__(self, scheme, body):
        """
        :param scheme: lowercase, trimmed scheme. eg. 'file', 'ftp' etc.
        :type scheme: str
        :param body: The non-scheme specific part. eg '//google.com/index.html'
        :type body: str
        """
        if not scheme:
            raise Exception('No scheme provided for Uri')

        self.scheme = scheme
        self.body = body

    @staticmethod
    def parse(uri):
        """
        Split a uri string into scheme and body.

        :param uri:
        Input URI could be a path: (expected to reside on all nodes for the processor):
            "/scratch/something.txt"
        A URL:
            "file:///tmp/something.txt", "ftp://cen-eods-test/tmp/something.txt", "http:// ..." etc.
        or an eods URI containing a dataset id:
            "eods:LS5_TM_OTH_P51_GALPGS01-002_090_075_20050106"

        :type uri: str
        :rtype: Uri
        """
        uri = uri.strip()

        if ':' not in uri:
            if not uri.startswith('/'):
                raise UnsupportedUriError("File paths must be absolute paths (start with /). Received: '%s'" % uri)
            uri = 'file://' + uri

        scheme, body = uri.split(':', 1)
        scheme = scheme.lower()

        if '/' in scheme or ' ' in scheme:
            # A file/folder path that happened to contain a colon.
            scheme = 'file'
            body = '//' + uri

        return Uri(scheme, body)

    @classmethod
    def from_eods_dataset_id(cls, eods_dataset_id):
        """
        Create URI from the given eods dataset id.

        >>> str(Uri.from_eods_dataset_id('LS8_OLITIRS_OTH_P41_GALPGS01-002_089_079_20131004'))
        'eods:LS8_OLITIRS_OTH_P41_GALPGS01-002_089_079_20131004'
        """
        if not eods_dataset_id:
            raise ValueError('Blank EODS Dataset Id')

        return Uri.parse('eods:' + eods_dataset_id)

    @classmethod
    def from_host_path(cls, hostname, path):
        """
        Create a file uri from a hostname and file path.

        A relative path will be converted to absolute.

        :type path: str
        :type hostname: str
        :rtype: Uri

        >>> Uri.from_host_path('somehost', '/tmp/test.txt')
        Uri('file', '//somehost/tmp/test.txt')
        >>> Uri.from_host_path('somehost', '/tmp/test.txt')
        Uri('file', '//somehost/tmp/test.txt')
        >>> Uri.from_host_path('', '/tmp/test.txt')
        Uri('file', '///tmp/test.txt')
        """
        path = path.strip()
        if not path.startswith('/'):
            path = os.path.abspath(path)

        return Uri.parse('file://%s%s' % (hostname, path))

    def to_local_path(self):
        """
        Get a file Uri as a local path name.

        file uris have format 'file://hostname/local/path'.

        The hostname can be blank if localhost: eg. "file:///local.path".
        Uris with no schema are assumed to be local paths.

        This method returns the local path component of the uri.

        :raise: ValueError if not a file uri.

        eg.
        >>> Uri.parse('file:///etc/something.txt').to_local_path()
        '/etc/something.txt'
        >>> Uri.parse('file://cen-jm-dev02/home/lpgs/something.txt').to_local_path()
        '/home/lpgs/something.txt'
        >>> Uri.parse('/etc/something.txt').to_local_path()
        '/etc/something.txt'
        >>> Uri.parse('ftp://google.com/something.txt').to_local_path()
        Traceback (most recent call last):
        ...
        ValueError: Cannot get local path of non-file Uri
        """
        if self.scheme != 'file':
            raise ValueError('Cannot get local path of non-file Uri')

        if self.body.count('/') < 3:
            raise ValueError("Invalid or empty Uri body: '%s'" % self.body)

        return '/' + '/'.join(self.body.split('/')[3:])

    def get_hostname(self):
        """
        Get hostname component of Uri

        >>> Uri.parse('file://dev-server/etc/something.txt').get_hostname()
        'dev-server'
        >>> Uri.parse('/etc/something.txt').get_hostname()
        ''
        >>> Uri.parse('something.txt').get_hostname()
        Traceback (most recent call last):
        ...
        UnsupportedUriError: File paths must be absolute paths (start with /). Received: 'something.txt'
        """
        if self.scheme == 'file':
            if self.body.count('/') < 3:
                raise SyntaxError("Invalid or empty Uri body: '%s'" % self.body)

            return self.body.split('/')[2]
        else:
            # TODO: Add support for schemes when they're needed.
            raise UnsupportedUriError("get_hostname() not supported for scheme '%s'" % self.scheme)

    def get_query_str(self):
        """
        Get query section of uri

        >>> Uri.parse('eods:?rid=123').get_query_str()
        'rid=123'
        >>> Uri.parse('eods:LS7_TEST').get_query_str()
        ''

        :return:
        """
        if '?' not in self.body:
            return ''

        return self.body.rpartition('?')[2]

    def get_query(self):
        """
        Get query args as a dict

        >>> Uri.parse('eods:?rid=123').get_query()
        {'rid': '123'}
        >>> sorted(Uri.parse('http://pma-dev/job?status=PENDING&parent=123').get_query().items())
        [('parent', '123'), ('status', 'PENDING')]
        >>> Uri.parse('file:///something.txt').get_query()
        {}

        :rtype: dict of (str, str)
        """
        query_string = self.get_query_str()
        if not query_string:
            return {}
        params = query_string.split('&')
        if not params:
            return {}

        return dict([param.split('=') for param in params if param])

    def get_qualified_uri(self):
        """
        Get a fully qualified Uri for distribution to other hosts.

        Local Uris will be given a fully qualified hostname.
        :rtype: Uri
        """

        # Only file uris need to be qualified (?)
        if self.scheme == 'file':
            hostname = self.get_hostname()
            if not hostname or hostname in ('localhost',):
                hostname = socket.getfqdn()
                path = self.to_local_path()
                return Uri('file', '//%s%s' % (hostname, path))

        return Uri(self.scheme, self.body)

    def __repr__(self):
        return "Uri('%s', '%s')" % (self.scheme, self.body)

    def __str__(self):
        return '%s:%s' % (self.scheme, self.body)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __hash__(self):
        return hash((self.scheme, self.body))

    def __ne__(self, other):
        return not self.__eq__(other)


def rsync(source_path, destination_path, source_host=None, destination_host=None):
    """
    Thing wrapper for rsync command, using default options used in NEO.

    Returns a list of file paths that were actually transferred.

    The host fields may include a username, as with rsync syntax: eg. "jm@rhe-jm-dev01.dev.lan"

    :type source_path: str
    :type destination_path: str
    :return: list of files transferred
    :rtype: list of str
    """

    def format_path(host, path):
        """Format a (possibly remote) path for rsync"""
        return '%s:%s' % (host, path) if host else path

    cmd = [
        'rsync', '-e', 'ssh -c arcfour', '-aL', '--out-format=%n',
        format_path(source_host, source_path),
        format_path(destination_host, destination_path)
    ]
    _log.info('Running %r', cmd)

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate()
    transferred = []
    if out:
        _log.info("rsync'd files: %r", out)
        transferred = out.splitlines()
    if err:
        _log.warn('rsync stderr: %r', err)
    if proc.returncode:
        raise IOError('Error %d returned from rsync: (%r, %r)' % (proc.returncode, out, err))

    return [to_absolute(filename, destination_path) for filename in transferred]


def to_absolute(filename, base_dir):
    """
    Convert a filename to absolute if needed, using the given base directory.
    :param filename:
    :param base_dir:
    :return:

    >>> to_absolute('1234', '/tmp')
    '/tmp/1234'
    >>> to_absolute('/tmp/other/1234', '/tmp')
    '/tmp/other/1234'
    >>> to_absolute('../1234', '/tmp/other')
    '/tmp/1234'
    """
    if os.path.isabs(filename):
        return filename

    return os.path.normpath(os.path.join(base_dir, filename))
