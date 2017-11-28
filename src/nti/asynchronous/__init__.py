#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from zope import component

from nti.asynchronous.interfaces import IJob
from nti.asynchronous.interfaces import IQueue

from nti.asynchronous.job import create_job as job_creator


def get_job_queue(name='', queue_interface=IQueue):
    result = component.queryUtility(queue_interface, name=name)
    return result


def create_job(func, *args, **kwargs):
    result = job_creator(func, jargs=args, jkwargs=kwargs)
    return result
