import os
import mock
import tempfile
import shutil
import json
from mock import MagicMock

from ecmwfapi import ECMWFDataServer

from fetch.load import Config
from fetch.ecmwf import EcmwfApiSource


@mock.patch.dict(os.environ,{'HOME':tempfile.gettempdir()})
@mock.patch('tempfile.mktemp', return_value='/path/to/fetch/dir-fetch')
def test_ecmwf_retrieve_serialisation(mktemp_patch):
    """
    Tests the transformation from config to ecmwf retrieve calls
    """
    reporter = MagicMock()
    raw_cfg = _make_ecmwf_config()
    cfg = Config.from_dict(raw_cfg)

    for item in cfg.rules:
        with mock.patch('ecmwfapi.ECMWFDataServer') as MockServer:
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


def test_ecmwf_credentials_read():
    """
    Tests resolving credentials from the HOME folder
    """

    tmp_dir = tempfile.mkdtemp()
    api_src = EcmwfApiSource()
    try:
        with mock.patch.dict(os.environ,{'HOME':tmp_dir}):
            assert api_src.get_uri() == 'ecmwfapi://UnknownHost?', "Validate cfg without rc file"

            with open(tmp_dir + '/.ecmwfapirc', 'w') as fd:
                fd.write(json.dumps({"url": "ecmwfapi://example.com/tests"}))
            assert api_src.get_uri() == 'ecmwfapi://example.com/tests?', "Validate cfg with rc file"
    finally:
        shutil.rmtree(tmp_dir)


def _make_ecmwf_config():
    """
    Load a config dict (for ecmwf)
    """
    anc_data = '/tmp/anc'
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
                    target=anc_data + '/ecmwf_data/temperature_20020505.grib'
                )
            }
        }
    }

    return schedule
