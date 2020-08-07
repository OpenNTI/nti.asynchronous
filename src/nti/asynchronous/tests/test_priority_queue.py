#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import is_
from hamcrest import none
from hamcrest import is_in
from hamcrest import is_not
from hamcrest import has_length
from hamcrest import assert_that
from hamcrest import starts_with
from hamcrest import contains_inanyorder

from nti.fakestatsd import FakeStatsDClient

from nti.fakestatsd.matchers import is_counter
from nti.fakestatsd.matchers import is_gauge
from nti.fakestatsd.matchers import is_timer

from nti.testing.matchers import validly_provides
from nti.testing.matchers import verifiably_provides

import fakeredis

from perfmetrics import statsd_client_stack

import transaction

from nti.asynchronous.interfaces import IJob
from nti.asynchronous.interfaces import IRedisQueue

from nti.asynchronous.job import create_job

from nti.asynchronous.redis_queue import PriorityQueue

from nti.asynchronous.tests import AsyncTestCase


def _redis():
    return fakeredis.FakeStrictRedis()


def mock_work():
    return 42


def foo_work():
    return 'foo'


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
        assert_that(list(queue), has_length(2))

        data = queue.keys()
        assert_that(sorted(data), is_([b'aizen', b'ichigo']))

        job = queue.claim()
        job()
        assert_that(job, is_(ichigo))
        assert_that('ichigo', is_not(is_in(queue)))

        data = queue.keys()
        assert_that(sorted(data), is_([b'aizen']))
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
        assert_that(sorted(data), is_([b'aizen', b'ichigo']))

        queue.remove(aizen)
        data = queue.keys()
        assert_that(sorted(data), is_([b'ichigo']))

        # empty
        queue.empty()
        assert_that(queue, has_length(0))

        # again for coverage
        queue.empty()
        assert_that(queue, has_length(0))

        # tail
        job3 = queue.put(create_job(foo_work), False, False)
        job4 = queue.put(create_job(mock_work), False, False)
        assert_that(queue.claim(), is_(job4))
        job4()
        assert_that(queue.claim(), is_(job3))
        job3() # coverage

        # unpickled
        queue.put(create_job(foo_work), False)
        jobs = queue.all(False)
        assert_that(jobs, has_length(1))
        assert_that(IJob.providedBy(queue._unpickle(jobs[0])),
                    is_(True))

        with self.assertRaises(KeyError):
            queue['key']  # pylint: disable=pointless-statement

class TestRedisQueueMetrics(AsyncTestCase):

    def setUp(self):
        self.statsd = FakeStatsDClient()
        statsd_client_stack.push(self.statsd)

    def tearDown(self):
        self.statsd = None
        statsd_client_stack.pop()

    def test_priority_queue(self):
        queue = PriorityQueue(redis=_redis())
        job = create_job(foo_work)
        queue.put(job)
        transaction.commit()

        # We expect to have pushed a couple of metrics to statsd as
        # part of executing the job.  We expect perfmetrics Metric around
        # the push, and we expect a gauge with the queue length
        metrics = self.statsd.metrics
        assert_that(metrics, contains_inanyorder(is_counter(starts_with('ntiasync.nti/async/jobs.put')),
                                                 is_timer(starts_with('ntiasync.nti/async/jobs.put')),
                                                 is_gauge('ntiasync.nti/async/jobs.length', '1')))

        self.statsd.clear()
        queue.claim()
        metrics = self.statsd.metrics
        assert_that(metrics, contains_inanyorder(is_gauge('ntiasync.nti/async/jobs.length', '0'),
                                                 is_counter(starts_with('ntiasync.nti/async/jobs.claim')),
                                                 is_timer(starts_with('ntiasync.nti/async/jobs.claim'))))
