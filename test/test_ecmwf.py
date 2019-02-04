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

@pytest.fixture(scope="module")
def ecmwf_config_dir(tmpdir_factory):
    """
    Writes a minimal config at the provided directory
    Can only be read if the environment HOME variable is changed to the same directory
    """
    base_dir = str(tmpdir_factory.mktemp("test-cfg"))

    with open(base_dir + '/.ecmwfapirc', 'w') as fd:
        fd.write(json.dumps({"url": "ecmwfapi://example.com/tests"}))

    return base_dir


def test_ecmwf_retrieve_serialisation(ecmwf_config_dir):
    """
    Tests the transformation from config to ecmwf retrieve calls
    """
    reporter = mock.MagicMock()
    raw_cfg = _make_ecmwf_config()
    cfg = Config.from_dict(raw_cfg)

    with mock.patch.dict(os.environ, {'HOME':ecmwf_config_dir}):
        with mock.patch('tempfile.mktemp', return_value='/path/to/fetch/dir-fetch'):
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


def test_ecmwf_credentials_read(tmpdir):
    """
    Tests resolving credentials from the HOME folder
    """

    api_src = EcmwfApiSource()
    with mock.patch.dict(os.environ, {'HOME':str(tmpdir)}):
        with pytest.raises(RemoteFetchException):
            api_src.get_uri()  # no file configured

        with open(str(tmpdir) + '/.ecmwfapirc', 'w') as fd:
            fd.write(json.dumps({"url": "ecmwfapi://example.com/tests"}))

        assert api_src.get_uri() == 'ecmwfapi://example.com/tests?', "Validate cfg with rc file"


def test_raises_remote_fetch_exception(ecmwf_config_dir):
    """ Tests that RemoteFetchException is called where the remote 
        server returns an error
    """
    reporter = mock.MagicMock()
    raw_cfg = _make_ecmwf_config(ecmwf_config_dir)

    cfg = Config.from_dict(raw_cfg)
    data_source = cfg.rules[0].module
    with mock.patch.dict(os.environ, {'HOME':ecmwf_config_dir}):
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
    for dirpath, dirnames, files in os.walk(ecmwf_config_dir):
        if files and files != ['.ecmwfapirc']:
            assert False, "File artifacts left in download directory"


@mock.patch('os.path.getsize', return_value=500)
def test_size(getsize_mock, ecmwf_config_dir):
    """ Tests that files are removed if the size returned 
        during sync is inconsistent
    """
    raw_cfg = _make_ecmwf_config(ecmwf_config_dir)
    cfg = Config.from_dict(raw_cfg)
    data_source = cfg.rules[0].module

    # Mock the ECMWFDataServer
    server = mock.Mock()
    server.retrieve.return_value = {'size': 600}
    with mock.patch.dict(os.environ, {'HOME':ecmwf_config_dir}):
        data_source._fetch_file(server, mock.MagicMock(), False)
        # Assert getsize only called once; inside the do_fetch function
        # defined in fetch/ecmwf.py
        # This is side effect observable if do_fetch returns False
        #    signifying the file did not download correctly
        getsize_mock.assert_called_once()

    for dirpath, dirnames, files in os.walk(ecmwf_config_dir):
        if files and files != ['.ecmwfapirc']:
            assert False, "File artifacts left in download directory"


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
