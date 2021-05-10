#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=protected-access,too-many-public-methods

from hamcrest import is_
from hamcrest import calling
from hamcrest import raises
from hamcrest import not_none
from hamcrest import assert_that
from hamcrest import instance_of
from hamcrest import same_instance

from nti.asynchronous.job import Job

from nti.asynchronous.tests import AsyncTestCase

from nti.asynchronous.scheduled import ImmediateQueueRunner

from nti.asynchronous.scheduled.interfaces import IScheduledQueueFactory
from nti.asynchronous.scheduled.interfaces import IScheduledExecutorQueueFactory

from nti.asynchronous.scheduled.job import ScheduledJob

from nti.asynchronous.scheduled.utils import get_scheduled_factory
from nti.asynchronous.scheduled.utils import get_executor_factory
from nti.asynchronous.scheduled.utils import add_scheduled_job


class TestScheduled(AsyncTestCase):

    def test_queues(self):
        factory = get_scheduled_factory()
        assert_that(factory, not_none())
        assert_that(IScheduledQueueFactory.providedBy(factory), is_(True))
        assert_that(factory.get_queue(None), instance_of(ImmediateQueueRunner))

        factory = get_executor_factory()
        assert_that(factory, not_none())
        assert_that(IScheduledExecutorQueueFactory.providedBy(factory), is_(True))
        assert_that(factory.get_queue(None), instance_of(ImmediateQueueRunner))

    def test_add_scheduled_job(self):
        def _test_a():
            pass
        job = Job(_test_a)
        assert_that(calling(add_scheduled_job).with_args(job), raises(ValueError))

        job = ScheduledJob(5, _test_a)
        assert_that(add_scheduled_job(job), same_instance(job))
