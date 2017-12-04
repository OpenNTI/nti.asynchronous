#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import none
from hamcrest import is_not
from hamcrest import assert_that
from hamcrest import has_property

import unittest

from zope.dottedname import resolve as dottedname


class TestInterfaces(unittest.TestCase):

    def test_import_interfaces(self):
        dottedname.resolve('nti.asynchronous.interfaces')

    def test_events(self):
        from nti.asynchronous.interfaces import ReactorEvent
        event = ReactorEvent(object())
        assert_that(event, has_property('reactor', is_not(none())))

        from nti.asynchronous.interfaces import JobAbortedEvent
        event = JobAbortedEvent(object())
        assert_that(event, has_property('job', is_not(none())))
