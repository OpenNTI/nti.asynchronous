#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import


class TestImmediateQueueRunner(object):
    """
    A queue that immediately runs the given job. This is generally
    desired for test or dev mode.
    """
    def put(self, job):
        job()
