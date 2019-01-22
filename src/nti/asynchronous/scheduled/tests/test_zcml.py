#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from hamcrest import is_
from hamcrest import is_not
from hamcrest import not_none
from hamcrest import assert_that
from hamcrest import instance_of

from zope import component
from zope.component.hooks import site

from nti.appserver.policies.sites import BASECOPPA as BASESITE

from nti.asynchronous.redis_queue import PriorityQueue

from nti.asynchronous.scheduled import SCHEDULED_JOB_QUEUE_NAME
from nti.asynchronous.scheduled import SCHEDULED_JOB_EXECUTOR_QUEUE_NAME

from nti.asynchronous.scheduled.interfaces import IScheduledQueueFactory
from nti.asynchronous.scheduled.interfaces import IScheduledExecutorQueueFactory

from nti.asynchronous.scheduled.redis_queue import ScheduledQueue

from nti.asynchronous.scheduled.utils import get_scheduled_factory
from nti.asynchronous.scheduled.utils import get_executor_factory

from nti.site.transient import TrivialSite as _TrivialSite

from nti.testing.base import ConfiguringTestBase

ZCML_STRING = """
        <configure xmlns="http://namespaces.zope.org/zope"
            xmlns:zcml="http://namespaces.zope.org/zcml"
            xmlns:scheduled="http://nextthought.com/ntp/asynchronous/scheduled">

            <include package="z3c.baseregistry" file="meta.zcml" />

            <include package="nti.asynchronous.scheduled" file="meta.zcml" />

            <registerIn registry="nti.asynchronous.scheduled.tests.test_zcml._TEST_SITE">
                <scheduled:registerScheduledQueue />
                <scheduled:registerExecutorQueue />
            </registerIn>

        </configure>
        """

from z3c.baseregistry.baseregistry import BaseComponents
_TEST_SITE = BaseComponents(BASESITE, name='test.components', bases=(BASESITE,))


class TestZcml(ConfiguringTestBase):

    def test_zcml(self):
        self.configure_string(ZCML_STRING)
        with site(_TrivialSite(_TEST_SITE)):
            factory = get_scheduled_factory()
            assert_that(IScheduledQueueFactory.providedBy(factory), is_(True))
            queue = factory.get_queue(SCHEDULED_JOB_QUEUE_NAME)
            assert_that(queue, instance_of(ScheduledQueue))

            factory = get_executor_factory()
            assert_that(IScheduledExecutorQueueFactory.providedBy(factory), is_(True))
            queue = factory.get_queue(SCHEDULED_JOB_EXECUTOR_QUEUE_NAME)
            assert_that(queue, instance_of(PriorityQueue))
