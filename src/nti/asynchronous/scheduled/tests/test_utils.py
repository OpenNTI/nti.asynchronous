#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=protected-access,too-many-public-methods

from hamcrest import is_
from hamcrest import not_none
from hamcrest import assert_that
from hamcrest import instance_of
from hamcrest import has_length

from zope import component

from nti.asynchronous.tests import AsyncTestCase

from nti.asynchronous.scheduled.interfaces import IScheduledQueueFactory
from nti.asynchronous.scheduled.interfaces import INotificationQueueFactory

from nti.asynchronous.scheduled.utils import get_scheduled_factory
from nti.asynchronous.scheduled.utils import get_notification_factory

from nti.asynchronous.scheduled.zcml import ImmediateQueueRunner


class TestScheduled(AsyncTestCase):

    def test_queues(self):
        factory = get_scheduled_factory()
        assert_that(factory, not_none())
        assert_that(IScheduledQueueFactory.providedBy(factory), is_(True))
        assert_that(factory.get_queue(None), instance_of(ImmediateQueueRunner))

        factory = get_notification_factory()
        assert_that(factory, not_none())
        assert_that(INotificationQueueFactory.providedBy(factory), is_(True))
        assert_that(factory.get_queue(None), instance_of(ImmediateQueueRunner))