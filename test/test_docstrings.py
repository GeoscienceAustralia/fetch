from __future__ import absolute_import

import doctest
import fetch
import fetch.auto
import fetch.load


def load_tests(loader=None, tests=None, ignore=None):
    tests.addTests(doctest.DocTestSuite(fetch))
    tests.addTests(doctest.DocTestSuite(fetch.auto))
    tests.addTests(doctest.DocTestSuite(fetch.load))
    return tests

