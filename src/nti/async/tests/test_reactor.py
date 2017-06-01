#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import is_
from hamcrest import none
from hamcrest import not_none
from hamcrest import has_length
from hamcrest import assert_that

import operator

from nti.async.job import create_job

from nti.async.queue import Queue

from nti.async.reactor import AsyncReactor

from nti.async.tests import AsyncTestCase


def mock_work():
    return 42


class TestReactor(AsyncTestCase):

    def setUp(self):
        self.reactor = AsyncReactor()
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
        assert_that(job, none())

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
