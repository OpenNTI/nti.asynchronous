#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import not_none
from hamcrest import has_length
from hamcrest import assert_that

import fakeredis

import transaction

from nti.async.job import create_job

from nti.async.redis_queue import RedisQueue

from nti.transactions import transactions

from nti.async.tests import AsyncTestCase


def _redis():
    return fakeredis.FakeStrictRedis(db=300)


def _pass_job():
    pass


def _fail_job():
    raise Exception()


class mock_db_trans(object):
    """
    A context manager that returns a connection. Use
    inside a function decorated with :class:`WithMockDSTrans`
    or similar.
    """

    def __enter__(self):
        transaction.begin()

    def __exit__(self, t, unused_v, unused_tb):
        # if this raises we're in trouble
        body_raised = t is not None
        try:
            try:
                if not transaction.isDoomed():
                    transaction.commit()
                else:
                    transaction.abort()
            except Exception:
                transaction.abort()
                raise
        except Exception:
            # Don't let our exception override the original exception
            if not body_raised:
                raise


class TestJobs(AsyncTestCase):

    def test_transactions(self):

        queue = RedisQueue(redis=_redis())
        assert_that(queue, has_length(0))

        # Fill it up
        with mock_db_trans():
            queue.put(create_job(_pass_job))
            queue.put(create_job(_fail_job))
            queue.put(create_job(_pass_job))

        assert_that(queue, has_length(3))

        # Simple job
        with mock_db_trans():
            claimed = queue.claim()
            assert_that(claimed, not_none())
            claimed()

        assert_that(queue, has_length(2))

        # Normal erring job
        with mock_db_trans():
            claimed = queue.claim()
            assert_that(claimed, not_none())
            claimed()

        assert_that(queue, has_length(1))

        def _fail():
            raise Exception()

        # Job that fails during commit are not put back in the queue
        try:
            with mock_db_trans():
                claimed = queue.claim()
                assert_that(claimed, not_none())
                claimed()

                transactions.do(target=self,
                                call=_fail,
                                args=None)
        except Exception:
            pass

        # Job failed and is back on queue
        assert_that(queue, has_length(0))
