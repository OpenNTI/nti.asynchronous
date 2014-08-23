#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from zope import component

from .job import create_job as job_creator

from .interfaces import IQueue

def get_job_queue(name=u'', queue_interface=IQueue):
    result = component.queryUtility(queue_interface, name=name)
    return result

def create_job(func, *args, **kwargs):
    result = job_creator(func, jargs=args, jkwargs=kwargs)
    return result
