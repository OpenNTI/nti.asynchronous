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

from zope.component.zcml import utility

from nti.asynchronous import get_job_queue as async_queue

from nti.asynchronous.interfaces import IRedisQueue

from nti.asynchronous.redis_queue import PriorityQueue as RedisQueue

from nti.asynchronous.scheduled import SCHEDULED_JOB_QUEUE_NAMES
from nti.asynchronous.scheduled import SCHEDULED_JOB_EXECUTOR_QUEUE_NAMES

from nti.asynchronous.scheduled import ImmediateQueueRunner

from nti.asynchronous.scheduled.interfaces import IScheduledQueueFactory
from nti.asynchronous.scheduled.interfaces import IScheduledExecutorQueueFactory

from nti.asynchronous.scheduled.redis_queue import ScheduledQueue

from nti.coremetadata.interfaces import IRedisClient

logger = __import__('logging').getLogger(__name__)


class _AbstractQueueFactory(object):

    queue_interface = None

    def get_queue(self, name):
        queue = async_queue(name, self.queue_interface)
        if queue is None:
            msg = "No queue exists for scheduled/executor job queue (%s)." % name
            raise ValueError(msg)
        return queue

    def _redis(self):
        return component.getUtility(IRedisClient)


# scheduled queue


@interface.implementer(IScheduledQueueFactory)
class _ScheduledQueueFactory(_AbstractQueueFactory):

    queue_interface = IRedisQueue

    def __init__(self, _context):
        for name in SCHEDULED_JOB_QUEUE_NAMES:
            queue = ScheduledQueue(self._redis, name)
            utility(_context, provides=IRedisQueue, component=queue, name=name)


@interface.implementer(IScheduledQueueFactory)
class _ImmediateScheduledQueueFactory(object):

    def get_queue(self, unused_name):
        return ImmediateQueueRunner()


def registerScheduledQueue(_context):
    logger.info("Registering scheduled redis queue")
    factory = _ScheduledQueueFactory(_context)
    utility(_context, provides=IScheduledQueueFactory, component=factory)


def registerImmediateScheduledQueue(_context):
    logger.info("Registering immediate scheduled queue")
    factory = _ImmediateScheduledQueueFactory()
    utility(_context, provides=IScheduledQueueFactory, component=factory)


# executor queue


@interface.implementer(IScheduledExecutorQueueFactory)
class _ExecutorQueueFactory(_AbstractQueueFactory):

    queue_interface = IRedisQueue

    def __init__(self, _context):
        for name in SCHEDULED_JOB_EXECUTOR_QUEUE_NAMES:
            queue = RedisQueue(self._redis, name)
            utility(_context, provides=IRedisQueue, component=queue, name=name)


@interface.implementer(IScheduledExecutorQueueFactory)
class _ImmediateExecutorQueueFactory(object):

    def get_queue(self, unused_name):
        return ImmediateQueueRunner()


def registerExecutorQueue(_context):
    logger.info("Registering scheduled executor redis queue")
    factory = _ExecutorQueueFactory(_context)
    utility(_context, provides=IScheduledExecutorQueueFactory, component=factory)


def registerImmediateExecutorQueue(_context):
    logger.info("Registering immediate scheduled executor queue")
    factory = _ImmediateExecutorQueueFactory()
    utility(_context, provides=IScheduledExecutorQueueFactory, component=factory)
