#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import is_
from hamcrest import none
from hamcrest import is_not
from hamcrest import not_none
from hamcrest import has_length
from hamcrest import assert_that
from hamcrest import has_property

import operator

from zope import component
from zope import interface

from nti.asynchronous.interfaces import IQueue

from nti.asynchronous.job import create_job

from nti.asynchronous.queue import Queue

from nti.asynchronous.reactor import AsyncReactor

from nti.asynchronous.tests import AsyncTestCase

from nti.site.interfaces import ISiteTransactionRunner

from nti.zodb.interfaces import UnableToAcquireCommitLock


def _failed():
    raise Exception()


def _mock_job():
    return


@interface.implementer(ISiteTransactionRunner)
class MockTrxRunner(object):

    def __call__(self, func, *unused_args, **unused_kwars):
        return func()


@interface.implementer(ISiteTransactionRunner)
class CommitLockTrxRunner(object):

    def __call__(self, *unused_args, **unused_kwars):
        raise UnableToAcquireCommitLock()


@interface.implementer(ISiteTransactionRunner)
class ExceptionTrxRunner(object):

    def __call__(self, *unused_args, **unused_kwars):
        raise Exception()


class TestAsyncReactor(AsyncTestCase):

    def setUp(self):
        self.reactor = AsyncReactor(trx_sleep=0.1, trx_retries=1,
                                    max_sleep_time=1, max_range_uniform=1)
        self.reactor.queues = ()

    def test_empty(self):
        q1 = Queue()
        q2 = Queue()
        q3 = Queue()
        self.reactor.queues = [q1, q2, q3]
        job = self.reactor._get_job()
        assert_that(self.reactor.queues, has_length(3))
        assert_that(job, none())

    def test_basic(self):
        q1 = Queue()
        q2 = Queue()
        q3 = Queue()

        job1 = q3.put(create_job(operator.mul, (7, 6), jobid="1"))
        job2 = q3.put(create_job(operator.mul, (14, 3), jobid="2"))
        self.reactor.queues = [q1, q2, q3]

        job = self.reactor._get_job()
        assert_that(job, not_none())
        assert_that(job, is_(job1))

        self.reactor.current_job = None
        job = self.reactor._get_job()
        assert_that(job, not_none())
        assert_that(job, is_(job2))

        val = self.reactor.perform_job(job)
        assert_that(val, is_(True))
        assert_that(job.has_failed(), is_(False))
        assert_that(job.is_running(), is_(False))
        assert_that(job.has_completed(), is_(True))

        self.reactor.current_job = None
        job = self.reactor._get_job()
        assert_that(job, is_(none()))

        assert_that(self.reactor.uniform(),
                    is_not(none()))

        assert_that(self.reactor.execute_job(), is_(False))

        self.reactor.perform_job(create_job(_failed), q1)

    def test_boundary(self):
        q1 = Queue()
        q2 = Queue()
        q3 = Queue()
        job1 = q3.put(create_job(operator.mul, (7, 6)))
        job2 = q3.put(create_job(operator.mul, (14, 3)))
        self.reactor.queues = [q1, q2, q3]

        job = self.reactor._get_job()
        assert_that(job, not_none())
        assert_that(job, is_(job1))

        # Job in first queue processed first
        job3 = q1.put(create_job(operator.mul, (7, 6)))
        job4 = q2.put(create_job(operator.mul, (14, 3)))

        self.reactor.current_job = None
        job = self.reactor._get_job()
        assert_that(job, not_none())
        assert_that(job, is_(job3))

        self.reactor.current_job = None
        job = self.reactor._get_job()
        assert_that(job, not_none())
        assert_that(job, is_(job4))

        self.reactor.current_job = None
        job = self.reactor._get_job()
        assert_that(job, not_none())
        assert_that(job, is_(job2))

        # Empty again
        self.reactor.current_job = None
        job = self.reactor._get_job()
        assert_that(job, none())

        # And again
        self.reactor.current_job = None
        job5 = q3.put(create_job(operator.mul, (7, 6)))
        job6 = q1.put(create_job(operator.mul, (14, 3)))
        job = self.reactor._get_job()
        assert_that(job, not_none())
        assert_that(job, is_(job6))

        self.reactor.current_job = None
        job = self.reactor._get_job()
        assert_that(job, not_none())
        assert_that(job, is_(job5))

    def test_coverage_run(self):
        queue = Queue()
        reactor = AsyncReactor(trx_sleep=0.1, poll_interval=0.1)
        reactor.queues = [queue]
        queue.put(create_job(_mock_job))
        reactor.run()
        reactor.current_job = create_job(_mock_job)
        reactor.execute_job()

    def test_pause_resume(self):
        reactor = AsyncReactor()
        reactor.pause()
        assert_that(reactor.is_paused(), is_(True))
        reactor.resume()
        assert_that(reactor.is_paused(), is_(False))

    def test_process_job(self):
        gsm = component.getGlobalSiteManager()

        queue = Queue()
        reactor = AsyncReactor(trx_sleep=0.1, poll_interval=0.1)
        reactor.queues = [queue]
        queue.put(create_job(_mock_job))
        queue.put(create_job(_mock_job))

        # no trx runner
        assert_that(reactor.process_job(), is_(False))
        assert_that(reactor, has_property('poll_interval', is_(0.1)))

        trx_runner = MockTrxRunner()
        gsm.registerUtility(trx_runner, ISiteTransactionRunner)
        assert_that(reactor.process_job(), is_(True))
        assert_that(reactor, has_property('poll_interval', is_(0)))

        assert_that(reactor.process_job(), is_(True))
        assert_that(queue, has_length(0))
        gsm.unregisterUtility(trx_runner, ISiteTransactionRunner)

        trx_runner = CommitLockTrxRunner()
        gsm.registerUtility(trx_runner, ISiteTransactionRunner)

        queue.put(create_job(_mock_job))
        assert_that(reactor.process_job(), is_(True))
        gsm.unregisterUtility(trx_runner, ISiteTransactionRunner)

        trx_runner = ExceptionTrxRunner()
        gsm.registerUtility(trx_runner, ISiteTransactionRunner)
        assert_that(reactor.process_job(), is_(False))
        gsm.unregisterUtility(trx_runner, ISiteTransactionRunner)

    def test_queues(self):
        q1 = Queue()
        q2 = Queue()
        gsm = component.getGlobalSiteManager()
        gsm.registerUtility(q1, IQueue, 'q1')
        gsm.registerUtility(q2, IQueue, 'q2')

        reactor = AsyncReactor(queue_names=('q1',))
        assert_that(reactor,
                    has_property('queues', has_length(1)))

        reactor.add_queues('q2')
        assert_that(reactor,
                    has_property('queues', has_length(2)))

        for _ in range(2):
            reactor.remove_queues('q1')
            assert_that(reactor,
                        has_property('queues', has_length(1)))

        gsm.unregisterUtility(q1, IQueue, 'q1')
        gsm.unregisterUtility(q2, IQueue, 'q2')
