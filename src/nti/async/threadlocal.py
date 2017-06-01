#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import print_function, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import threading


def defaults():
    return {'job': None, 'callable':None}


class ThreadLocalManager(threading.local):

    def __init__(self, default=None):
        self.stack = []
        self.default = default

    def push(self, info):
        self.stack.append(info)
    set = push

    def pop(self):
        if self.stack:
            return self.stack.pop()

    def get(self):
        try:
            return self.stack[-1]
        except IndexError:
            return self.default()

    def clear(self):
        self.stack[:] = []


manager = ThreadLocalManager(default=defaults)


def get_current_job():
    """
    Return the currently active job or ``None`` if no job
    is currently active.
    """
    return manager.get()['job']


def get_current_callable():
    """
    Return the currently callable or ``None`` if no job
    is currently active.
    """
    return manager.get()['callable']
