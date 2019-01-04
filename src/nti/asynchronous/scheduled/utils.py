#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from zope import component
from zope import interface

from nti.asynchronous import get_job_queue as async_queue

from nti.asynchronous.scheduled import SCHEDULED_QUEUE_NAME
from nti.asynchronous.scheduled import NOTIFICATION_QUEUE_NAME

from nti.asynchronous.scheduled.interfaces import IScheduledJob
from nti.asynchronous.scheduled.interfaces import IScheduledQueueFactory
from nti.asynchronous.scheduled.interfaces import INotificationQueueFactory

logger = __import__('logging').getLogger(__name__)


def get_scheduled_factory():
    return component.getUtility(IScheduledQueueFactory)


def get_scheduled_queue(name=SCHEDULED_QUEUE_NAME):
    factory = get_scheduled_factory()
    return factory.get_queue(name)


def get_notification_factory():
    return component.getUtility(INotificationQueueFactory)


def get_notification_queue(name=NOTIFICATION_QUEUE_NAME):
    factory = get_notification_factory()
    return factory.get_queue(name)


def add_scheduled_job(job):
    if not IScheduledJob.providedBy(job):
        logger.debug("%s is not a sheduled job.", job)
        return None

    queue = get_scheduled_queue(name=SCHEDULED_QUEUE_NAME)
    queue.put(job)
    return job
