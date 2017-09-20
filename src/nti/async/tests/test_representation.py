#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904


from hamcrest import assert_that

import unittest

from nti.async.representation import WithRepr


@WithRepr
class Sample(object):

    def __init__(self):
        self.a = 1


class TestRepresentation(unittest.TestCase):

    def test_with_repr(self):

        sample = Sample()
        assert_that(repr(sample),
                    "Sample().__dict__.update( {'a': 1} )")
