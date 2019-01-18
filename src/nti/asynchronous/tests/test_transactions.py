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

from zope.component import eventtesting

from nti.asynchronous.interfaces import IJobAbortedEvent

from nti.asynchronous.job import create_job

from nti.asynchronous.redis_queue import RedisQueue
from nti.asynchronous.redis_queue import PriorityQueue

from nti.asynchronous.tests import AsyncTestCase

from nti.asynchronous.threadlocal import manager
from nti.asynchronous.threadlocal import get_current_callable

from nti.transactions import transactions


def _redis(db=300):
    return fakeredis.FakeStrictRedis(db=db, singleton=False)


def _pass_job():
    pass


def _fail_job():
    raise Exception()


def _doom_job():
    transaction.doom()


def _manager_job():
    assert_that(get_current_callable(), not_none())


def _fail():
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
        except Exception:  # pylint: disable=broad-except
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
            queue.put(create_job(_doom_job))

        assert_that(queue, has_length(4))

        # Simple job
        with mock_db_trans():
            claimed = queue.claim()
            assert_that(claimed, not_none())
            claimed()

        assert_that(queue, has_length(3))

        # Normal erring job
        with mock_db_trans():
            claimed = queue.claim()
            assert_that(claimed, not_none())
            claimed()

        assert_that(queue, has_length(2))

        eventtesting.clearEvents()
        # Job that fails during commit are not put back in the queue
        try:
            with mock_db_trans():
                claimed = queue.claim()
                assert_that(claimed, not_none())
                claimed()

                transactions.do_near_end(call=_fail)
        except Exception:  # pylint: disable=broad-except
            pass
        assert_that(eventtesting.getEvents(IJobAbortedEvent),
                    has_length(1))

        # Job failed and is back on queue
        assert_that(queue, has_length(1))

        with mock_db_trans():
            claimed = queue.claim()
            claimed()

        with mock_db_trans():
            queue.put(create_job(_manager_job))

        with mock_db_trans():
            claimed = queue.claim()
            claimed()

        assert_that(queue, has_length(0))
        manager.clear()

    def test_priority_queue(self):
        queue = PriorityQueue(redis=_redis(222))
        assert_that(queue, has_length(0))
        with mock_db_trans():
            queue.put(create_job(_pass_job))

        eventtesting.clearEvents()
        try:
            with mock_db_trans():
                queue.claim()
                transactions.do_near_end(call=_fail)
        except Exception:  # pylint: disable=broad-except
            pass
        assert_that(eventtesting.getEvents(IJobAbortedEvent),
                    has_length(1))
