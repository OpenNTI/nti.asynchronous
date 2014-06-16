#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from zope import component

from . import job
from . import interfaces as async_interfaces

def get_job_queue():
    result = component.queryUtility(async_interfaces.IQueue)
    return result

def create_job(func, *args, **kwargs):
    all_args = [func] + list(args)
    result = job.Job(*all_args, **kwargs)
    return result
