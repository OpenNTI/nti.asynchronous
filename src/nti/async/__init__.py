#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import print_function, absolute_import, division
__docformat__ = "restructuredtext en"

from zope import component

from nti.async.interfaces import IJob
from nti.async.interfaces import IQueue

from nti.async.job import create_job as job_creator

logger = __import__('logging').getLogger(__name__)


def get_job_queue(name='', queue_interface=IQueue):
    result = component.queryUtility(queue_interface, name=name)
    return result


def create_job(func, *args, **kwargs):
    result = job_creator(func, jargs=args, jkwargs=kwargs)
    return result
