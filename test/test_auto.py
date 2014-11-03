import unittest

__author__ = 'u63606'

from onreceipt.fetch.auto import _filter_finished_children


class TestAuto(unittest.TestCase):
    def test_filter_children(self):
        class MockProcess:
            def __init__(self, name='p', exitcode=None, pid=None):
                self.exitcode = exitcode
                self.name = name
                self.pid = pid
                self.log_file = '/tmp/test.log'

        running_proc = MockProcess(exitcode=None)
        failed_proc = MockProcess(exitcode=1)
        succeeded_proc = MockProcess(exitcode=0)

        self.assertEqual(
            set([running_proc]),
            _filter_finished_children([running_proc], [])
        )
        self.assertEqual(
            set(),
            _filter_finished_children([failed_proc], [])
        )
        self.assertEqual(
            set([running_proc]),
            _filter_finished_children([running_proc, failed_proc, succeeded_proc], [])
        )


