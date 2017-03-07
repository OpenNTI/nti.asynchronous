#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import not_none
from hamcrest import has_length
from hamcrest import assert_that

from nti.async.job import create_job

from nti.async.redis_queue import RedisQueue

from nti.transactions import transactions

from nti.async.tests import AsyncTestCase

from nti.dataserver.tests import mock_dataserver
from nti.dataserver.tests.mock_redis import InMemoryMockRedis


def _redis():
    return InMemoryMockRedis()


def _pass_job():
    pass


def _fail_job():
    raise Exception()


class TestJobs(AsyncTestCase):

    @mock_dataserver.WithMockDSTrans
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
        except Exception:
            pass

        # Job failed and is back on queue
        assert_that(queue, has_length(1))
