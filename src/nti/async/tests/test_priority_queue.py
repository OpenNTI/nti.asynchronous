#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import is_
from hamcrest import has_length
from hamcrest import assert_that
from hamcrest import has_property

import unittest

from nti.async.job import create_job

from nti.async.redis_queue import PriorityQueue

from nti.async.tests import AsyncTestCase

from nti.dataserver.tests.mock_redis import InMemoryMockRedis


def _redis():
    return InMemoryMockRedis()


def mock_work():
    return 42


@unittest.SkipTest
class TestPriorityQueue(AsyncTestCase):

    def _makeOne(self):
        queue = PriorityQueue(redis=_redis())
        return queue

    def test_operations(self):
        queue = self._makeOne()
        job = create_job(mock_work, jobid='ichigo')
        queue.put(job, use_transactions=False)
        assert_that(queue, has_length(1))

        job = create_job(mock_work, jobid='aizen')
        queue.put(job, use_transactions=False)
        assert_that(queue, has_length(2))

        assert_that(queue, has_property('_first', is_('ichigo')))
        assert_that(list(queue), has_length(2))

        data = queue.keys()
        assert_that(sorted(data), is_(['aizen', 'ichigo']))
