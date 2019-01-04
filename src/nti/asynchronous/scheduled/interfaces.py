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


class IScheduledJob(IJob):

    score = interface.Attribute("The score value of this job.")


class IScheduledQueueFactory(interface.Interface):
    """
    A factory for job scheduled queues.
    """


class INotificationQueueFactory(interface.Interface):
    """
    A factory for job notifications queues.
    """
