#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import is_
from hamcrest import none
from hamcrest import is_in
from hamcrest import is_not
from hamcrest import has_length
from hamcrest import assert_that
from hamcrest import has_property

from nti.testing.matchers import validly_provides
from nti.testing.matchers import verifiably_provides

from nti.async.interfaces import IRedisQueue

from nti.async.job import create_job

from nti.async.redis_queue import PriorityQueue

from nti.async.tests import AsyncTestCase

from nti.dataserver.tests.mock_redis import InMemoryMockRedis


def _redis():
    return InMemoryMockRedis(db=100)


def mock_work():
    return 42


class TestPriorityQueue(AsyncTestCase):

    def _makeOne(self):
        queue = PriorityQueue(redis=_redis())
        return queue

    def test_model(self):
        queue = self._makeOne()
        assert_that(queue, validly_provides(IRedisQueue))
        assert_that(queue, verifiably_provides(IRedisQueue))

    def test_operations(self):
        queue = self._makeOne()
        ichigo = create_job(mock_work, jobid='ichigo')
        queue.put(ichigo, use_transactions=False)
        assert_that(queue, has_length(1))

        aizen = create_job(mock_work, jobid='aizen')
        queue.put(aizen, use_transactions=False)
        assert_that(queue, has_length(2))

        assert_that(queue, has_property('_first', is_('ichigo')))
        assert_that(list(queue), has_length(2))

        data = queue.keys()
        assert_that(sorted(data), is_(['aizen', 'ichigo']))

        job = queue.claim()
        assert_that(job, is_(ichigo))
        assert_that('ichigo', is_not(is_in(queue)))

        data = queue.keys()
        assert_that(sorted(data), is_(['aizen']))
        assert_that(queue['aizen'], is_(aizen))

        queue.empty()
        assert_that(queue, has_length(0))
        assert_that(queue.claim(), is_(none()))
        assert_that(list(queue.keys()), is_([]))

        queue.put(aizen, use_transactions=False)
        queue.put(ichigo, use_transactions=False)
        rukia = create_job(mock_work, jobid='rukia')
        queue.put(rukia, use_transactions=False)
        assert_that(queue, has_length(3))
        del queue['rukia']
        assert_that('rukia', is_not(is_in(queue)))
        data = queue.keys()
        assert_that(sorted(data), is_(['aizen', 'ichigo']))
