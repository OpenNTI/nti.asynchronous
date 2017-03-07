#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import is_
from hamcrest import is_in
from hamcrest import not_none
from hamcrest import equal_to
from hamcrest import has_length
from hamcrest import assert_that

import operator
import transaction

from nti.async.job import create_job

from nti.async.redis_queue import RedisQueue

from nti.transactions import transactions

from nti.async.tests import AsyncTestCase

from nti.dataserver.tests import mock_dataserver
from nti.dataserver.tests.mock_redis import InMemoryMockRedis


def _redis():
    return InMemoryMockRedis()


def mock_work():
    return 42


class TesRedistQueue(AsyncTestCase):

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

        job2 = queue.put(create_job(operator.mul, jargs=(7, 6)))
        transaction.commit()

        assert_that(queue, has_length(1))

        job3 = queue.put(create_job(operator.mul, jargs=(14, 3)))
        job4 = queue.put(create_job(operator.mul, jargs=(21, 2)))
        job5 = queue.put(create_job(operator.mul, jargs=(42, 1)))
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

        queue.put(create_job(operator.mul, jargs=(34, 1)))
        job7 = queue.put(create_job(operator.mul, jargs=(34, 1)))
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


def _pass_job():
    pass


def _fail_job():
    raise Exception()


class TestJobs(mock_dataserver.DataserverLayerTest):

    @mock_dataserver.WithMockDS
    def test_transactions(self):

        queue = RedisQueue(redis=_redis())
        assert_that(queue, has_length(0))

        # Fill it up
        with mock_dataserver.mock_db_trans(self.ds):
            queue.put(create_job(_pass_job))
            queue.put(create_job(_fail_job))
            queue.put(create_job(_pass_job))

        assert_that(queue, has_length(3))

        # Simple job
        with mock_dataserver.mock_db_trans(self.ds):
            claimed = queue.claim()
            assert_that(claimed, not_none())
            claimed()

        assert_that(queue, has_length(2))

        # Normal erring job
        with mock_dataserver.mock_db_trans(self.ds):
            claimed = queue.claim()
            assert_that(claimed, not_none())
            claimed()

        assert_that(queue, has_length(1))

        def _fail():
            raise Exception()

        # Job that fails during commit
        try:
            with mock_dataserver.mock_db_trans(self.ds):
                claimed = queue.claim()
                assert_that(claimed, not_none())
                claimed()

                transactions.do(target=self,
                                call=_fail,
                                args=None)
        except:
            pass

        # Job failed and is back on queue
        assert_that(queue, has_length(1))
