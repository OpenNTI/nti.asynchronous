#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=inherit-non-class

from zope import interface

from nti.asynchronous.interfaces import IJob

from nti.schema.field import Number


class IScheduledJob(IJob):

    timestamp = Number(title=u"The executing time",
                       description=u'The utc timestamp when this job should be executed.',
                       required=True)


class IScheduledQueueFactory(interface.Interface):
    """
    A factory for scheduled job queues.
    """


class IScheduledExecutorQueueFactory(interface.Interface):
    """
    A factory for scheduled job executor queues.
    """
