#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import uuid

from collections import Mapping

from zope import interface

from nti.asynchronous._compat import text_

from nti.asynchronous.job import Job

from nti.asynchronous.scheduled.interfaces import IScheduledJob


@interface.implementer(IScheduledJob)
class ScheduledJob(Job):

    def __init__(self, timestamp, *args, **kwargs):
        self.timestamp = timestamp
        Job.__init__(self, *args, **kwargs)


def create_scheduled_job(call, timestamp, jargs=None, jkwargs=None, jobid=None, cls=ScheduledJob):
    assert jargs is None or isinstance(jargs, (tuple, list))
    assert jkwargs is None or isinstance(jkwargs, Mapping)
    jkwargs = jkwargs or {}
    jargs = [call] + list(jargs or ())
    result = cls(timestamp, *jargs, **jkwargs)
    result.id = text_(jobid or uuid.uuid4())
    return result
