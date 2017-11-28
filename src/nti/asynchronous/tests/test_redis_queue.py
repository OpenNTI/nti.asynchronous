#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import is_
from hamcrest import is_in
from hamcrest import not_none
from hamcrest import equal_to
from hamcrest import has_length
from hamcrest import assert_that

from nti.testing.matchers import validly_provides
from nti.testing.matchers import verifiably_provides

import fakeredis

import transaction

from nti.asynchronous.interfaces import IRedisQueue

from nti.asynchronous.job import create_job

from nti.asynchronous.redis_queue import RedisQueue

from nti.asynchronous.tests import AsyncTestCase


def _redis():
    return fakeredis.FakeStrictRedis(db=200)


def mock_work():
    return 42


def multiply(a, b):
    return a*b


class TestRedisQueue(AsyncTestCase):

    def test_model(self):
        queue = RedisQueue(redis=_redis())
        assert_that(queue, validly_provides(IRedisQueue))
        assert_that(queue, verifiably_provides(IRedisQueue))

    def test_empty(self):
        queue = RedisQueue(redis=_redis())
        assert_that(queue, has_length(0))
        assert_that(list(queue), is_([]))

    def test_mockwork(self):
        queue = RedisQueue(redis=_redis())

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
        transaction.commit()

    def test_operator(self):
        queue = RedisQueue(redis=_redis())
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
            self.fail()
        except NotImplementedError:
            pass

        queue.put(create_job(multiply, jargs=(34, 1)))
        job7 = queue.put(create_job(multiply, jargs=(34, 1)))
        transaction.commit()
        assert_that(queue, has_length(4))
        assert_that(queue.keys(), has_length(4))
        assert_that(job7.id, is_in(queue))

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

        queue.empty()
        assert_that(queue, has_length(0))