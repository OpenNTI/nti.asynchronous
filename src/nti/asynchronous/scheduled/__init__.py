#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import


SCHEDULED_JOB_QUEUE_NAME = '++etc++nti++asynchronous++queue++scheduled'
SCHEDULED_JOB_QUEUE_NAMES = (SCHEDULED_JOB_QUEUE_NAME, )

SCHEDULED_JOB_EXECUTOR_QUEUE_NAME = '++etc++nti++asynchronous++queue++scheduled++executor'
SCHEDULED_JOB_EXECUTOR_QUEUE_NAMES = (SCHEDULED_JOB_EXECUTOR_QUEUE_NAME, )


class NonRaisingImmediateQueueRunner(object):
    """
    A queue that immediately runs the given job and ignores any exceptions.
    This is generally desired for test or dev mode.
    """
    def put(self, job):
        job()


class ImmediateQueueRunner(object):
    """
    A queue that immediately runs the given job. This may be used in
    live environments to inline jobs.

    We *must* reraise in such scenarios to avoid mangling transaction handling.
    """
    def put(self, job):
        job()
        job.reraise()
