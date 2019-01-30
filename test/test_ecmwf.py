import os
import tempfile
import shutil
import json
import datetime

import pytest
import mock

from fetch.load import Config
from fetch._core import RemoteFetchException
from fetch.ecmwf import (
    EcmwfApiSource, ECMWFDataServer, APIException, URLError,
    HTTPException
)


@mock.patch.dict(os.environ, {'HOME':tempfile.gettempdir()})
@mock.patch('tempfile.mktemp', return_value='/path/to/fetch/dir-fetch')
def test_ecmwf_retrieve_serialisation(mktemp_patch):
    """
    Tests the transformation from config to ecmwf retrieve calls
    """
    try:
        tmp_dir = tempfile.mkdtemp()
        _write_config(tmp_dir)
        reporter = mock.MagicMock()
        raw_cfg = _make_ecmwf_config()
        cfg = Config.from_dict(raw_cfg)

        with mock.patch.dict(os.environ,{'HOME':tmp_dir}):
            for item in cfg.rules:
                with mock.patch('fetch.ecmwf.ECMWFDataServer') as MockServer:
                    mock_server = MockServer.return_value
                    src = item.module
                    src.trigger(reporter=reporter)

                    expected_args = {
                        'target': '/path/to/fetch/dir-fetch',
                        'class': getattr(raw_cfg['rules'][item.name]['source'], 'cls'),
                        'type': getattr(raw_cfg['rules'][item.name]['source'], 'typ')
                    }
                    expected_params = ['stream', 'area', 'levtype', 'expver', 'step',
                                       'dataset', 'grid', 'param', 'time', 'date']

                    for ep in expected_params:
                        if hasattr(raw_cfg['rules'][item.name]['source'], ep):
                            expected_args[ep] = getattr(raw_cfg['rules'][item.name]['source'], ep)

                    mock_server.retrieve.assert_called_once_with(expected_args)
    finally:
        shutil.rmtree(tmp_dir)


def test_ecmwf_credentials_read():
    """
    Tests resolving credentials from the HOME folder
    """

    tmp_dir = tempfile.mkdtemp()
    api_src = EcmwfApiSource()
    try:
        with mock.patch.dict(os.environ,{'HOME':tmp_dir}):
            with pytest.raises(RemoteFetchException):
                api_src.get_uri()  # no file configured

            _write_config(tmp_dir)
            assert api_src.get_uri() == 'ecmwfapi://example.com/tests?', "Validate cfg with rc file"
    finally:
        shutil.rmtree(tmp_dir)


class Test_EcmwfApiSourceErrors(object):

    @classmethod
    def setup_class(cls):
        cls.tmp_dir = tempfile.mkdtemp()
        _write_config(cls.tmp_dir)

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'tmp_dir') and cls.tmp_dir:
            shutil.rmtree(cls.tmp_dir)
        cls.tmp_dir = None

    def test_raises_remote_fetch_exception(self):
        reporter = mock.MagicMock()
        raw_cfg = _make_ecmwf_config(self.tmp_dir)

        cfg = Config.from_dict(raw_cfg)
        data_source = cfg.rules[0].module
        with mock.patch.dict(os.environ, {'HOME':self.tmp_dir}):
            with mock.patch('fetch.ecmwf.ECMWFDataServer') as MockServer:
                mock_server = MockServer.return_value

                test_exceptions = [
                    URLError(reason='test exception'),
                    APIException(),
                    HTTPException()
                ]

                for _exc in test_exceptions:
                    mock_server.retrieve.side_effect = _exc
                    with pytest.raises(RemoteFetchException):
                        data_source.trigger(reporter=reporter)

                    # Check no files remain:
                    for dirpath, dirnames, files in os.walk(self.tmp_dir):
                        if files and files != ['.ecmwfapirc']:
                            assert False, "File artifacts left in download directory"

    @mock.patch('os.path.getsize', return_value=500)
    def test_size(self, getsize_mock):
        raw_cfg = _make_ecmwf_config(self.tmp_dir)
        cfg = Config.from_dict(raw_cfg)
        data_source = cfg.rules[0].module

        # Mock the ECMWFDataServer
        server = mock.Mock()
        server.retrieve.return_value = {'size': 600}
        with mock.patch.dict(os.environ, {'HOME':self.tmp_dir}):
            data_source._fetch_file(server, mock.MagicMock(), False)
            # Assert getsize only called once; inside the do_fetch function
            # defined in fetch/ecmwf.py
            # This is side effect observable if do_fetch returns False
            #    signifying the file did not download correctly
            getsize_mock.assert_called_once()

        for dirpath, dirnames, files in os.walk(self.tmp_dir):
            if files and files != ['.ecmwfapirc']:
                assert False, "File artifacts left in download directory"


def _write_config(tmp_dir):
    """
    Writes a minimal config at the provided directory
    Can only be read if the environment HOME variable is changed to the same directory
    """
    with open(tmp_dir + '/.ecmwfapirc', 'w') as fd:
        fd.write(json.dumps({"url": "ecmwfapi://example.com/tests"}))


def _make_ecmwf_config(ancillary_data_root='/tmp/anc'):
    """
    Load a config dict (for ecmwf)
    """
    schedule = {
        'directory': '/tmp/anc-fetch',
        'notify': {
            'email': ['test@ga.gov.au']
        },
        'log': {
            'fetch': 'DEBUG'
        },
        'rules': {
            'Temperature': {
                'schedule':'0 17 26 * *',
                'source': EcmwfApiSource(
                    cls='ei',
                    dataset='interim',
                    date='2002-05-05/to/2002-05-05',
                    area='0/100/-50/160',
                    expver='1',
                    grid='0.125/0.125',
                    levtype='sfc',
                    param='130.128',
                    stream='oper',
                    time='10:00:00',
                    step='0',
                    typ='an',
                    target=ancillary_data_root + '/ecmwf_data/temperature_20020505.grib'
                )
            }
        }
    }

    return schedule
