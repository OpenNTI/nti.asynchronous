#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import contains_inanyorder
from hamcrest import is_
from hamcrest import none
from hamcrest import is_in
from hamcrest import is_not
from hamcrest import not_none
from hamcrest import equal_to
from hamcrest import has_length
from hamcrest import assert_that
from hamcrest import starts_with

from nti.fakestatsd import FakeStatsDClient

from nti.fakestatsd.matchers import is_counter
from nti.fakestatsd.matchers import is_gauge
from nti.fakestatsd.matchers import is_timer

from nti.testing.matchers import validly_provides
from nti.testing.matchers import verifiably_provides

import fakeredis

from perfmetrics import statsd_client_stack

import transaction

from nti.asynchronous.interfaces import IRedisQueue

from nti.asynchronous.job import create_job

from nti.asynchronous import redis_queue

from nti.asynchronous.tests import AsyncTestCase


def _redis(db=200):
    return fakeredis.FakeStrictRedis(db=db)


def mock_work():
    return 42


def multiply(a, b):
    return a * b


class TestRedisQueue(AsyncTestCase):

    def test_mixin(self):
        queue = redis_queue.QueueMixin(None, None)
        assert_that(queue.failed(), is_(()))
        assert_that(repr(queue), is_not(none()))

    def test_model(self):
        queue = redis_queue.RedisQueue(redis=_redis(100))
        assert_that(queue, validly_provides(IRedisQueue))
        assert_that(queue, verifiably_provides(IRedisQueue))

    def test_empty(self):
        queue = redis_queue.RedisQueue(redis=_redis(200))
        assert_that(queue, has_length(0))
        assert_that(list(queue), is_([]))

    def test_mockwork(self):
        old_value = redis_queue.LONG_PUSH_DURATION_IN_SECS
        try:
            redis_queue.LONG_PUSH_DURATION_IN_SECS = 0
            queue = redis_queue.RedisQueue(redis=_redis())

            job = queue.put(create_job(mock_work))
            transaction.commit()
            assert_that(queue, has_length(1))
            assert_that(list(queue), is_([job]))
            assert_that(queue[0], is_(job))
            assert_that(bool(queue), is_(True))

            claimed = queue.claim()
            assert_that(claimed, equal_to(job))
            assert_that(queue, has_length(0))
            assert_that(list(queue), is_([]))
            claimed()
            transaction.commit()
        finally:
            redis_queue.LONG_PUSH_DURATION_IN_SECS = old_value

    def test_operator(self):
        queue = redis_queue.RedisQueue(redis=_redis(300))
        assert_that(queue, has_length(0))

        job2 = queue.put(create_job(multiply, jargs=(7, 6)))
        transaction.commit()

        assert_that(queue, has_length(1))

        job3 = queue.put(create_job(multiply, jargs=(14, 3)))
        job4 = queue.put(create_job(multiply, jargs=(21, 2)))
        job5 = queue.put(create_job(multiply, jargs=(42, 1)))
        transaction.commit()

        assert_that(queue, has_length(4))
        assert_that(list(queue), is_([job2, job3, job4, job5]))

        claimed = queue.claim()
        assert_that(claimed, equal_to(job2))

        pulled = queue.claim()
        assert_that(pulled, equal_to(job3))
        assert_that(list(queue), is_([job4, job5]))
        assert_that(queue, has_length(2))

        try:
            queue.remove(job4)
        except NotImplementedError:
            pass

        queue.put(create_job(multiply, jargs=(34, 1)))
        job7 = queue.put(create_job(multiply, jargs=(34, 1)))
        transaction.commit()
        assert_that(queue, has_length(4))
        assert_that(queue.keys(), has_length(4))
        assert_that(job7.id, is_in(queue))
        job7()

        queue.empty()
        assert_that(queue, has_length(0))
        assert_that(queue.keys(), has_length(0))

        queue.put(job4)
        queue.put(job5)
        transaction.commit()
        assert_that(list(queue), is_([job4, job5]))
        assert_that(queue.keys(), has_length(2))

        first = queue[0]
        assert_that(first, is_(not_none()))

        last = queue[-1]
        assert_that(last, is_(not_none()))

        data = queue.all(unpickle=False)
        assert_that(data, has_length(2))

        data = queue.keys()
        assert_that(data, has_length(2))

        data = queue.all(unpickle=True)
        assert_that(data, has_length(2))

        data = queue.failed(unpickle=True)
        assert_that(data, has_length(0))

        # reset
        queue.empty()
        assert_that(queue, has_length(0))

        # and again for coverage
        queue.empty()
        assert_that(queue, has_length(0))

        queue.put(job4, False, False)
        queue.put(job5, False, False)
        assert_that(list(queue), is_([job5, job4]))

        with self.assertRaises(IndexError):
            queue[3]  # pylint: disable=pointless-statement

        queue.empty()
        assert_that(queue, has_length(0))


class TestRedisQueueMetrics(AsyncTestCase):

    def setUp(self):
        self.statsd = FakeStatsDClient()
        statsd_client_stack.push(self.statsd)

    def tearDown(self):
        self.statsd = None
        statsd_client_stack.pop()

    def test_redis_queue(self):
        queue = redis_queue.RedisQueue(redis=_redis(300))
        queue.put(create_job(multiply, jargs=(7, 6)))
        transaction.commit()

        # We expect to have pushed a couple of metrics to statsd as
        # part of executing the job.  We expect perfmetrics Metric around
        # the push, and we expect a gauge with the queue length
        metrics = self.statsd.metrics

        assert_that(metrics, contains_inanyorder(is_counter(starts_with('ntiasync.nti/async/jobs.put')),
                                                 is_timer(starts_with('ntiasync.nti/async/jobs.put')),
                                                 is_gauge('ntiasync.nti/async/jobs.length', '1')))

