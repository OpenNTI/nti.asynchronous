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

from nti.asynchronous.scheduled import SCHEDULED_JOB_QUEUE_NAME
from nti.asynchronous.scheduled import SCHEDULED_JOB_EXECUTOR_QUEUE_NAME

from nti.asynchronous.scheduled.interfaces import IScheduledJob
from nti.asynchronous.scheduled.interfaces import IScheduledQueueFactory
from nti.asynchronous.scheduled.interfaces import IScheduledExecutorQueueFactory

logger = __import__('logging').getLogger(__name__)


def get_scheduled_factory():
    return component.getUtility(IScheduledQueueFactory)


def get_scheduled_queue(name=SCHEDULED_JOB_QUEUE_NAME):
    factory = get_scheduled_factory()
    return factory.get_queue(name)


def get_executor_factory():
    return component.getUtility(IScheduledExecutorQueueFactory)


def get_executor_queue(name=SCHEDULED_JOB_EXECUTOR_QUEUE_NAME):
    factory = get_executor_factory()
    return factory.get_queue(name)


def add_scheduled_job(job):
    if not IScheduledJob.providedBy(job):
        raise ValueError("%s is not a sheduled job." % job)

    queue = get_scheduled_queue()
    queue.put(job)
    return job
