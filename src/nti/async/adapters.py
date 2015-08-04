#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from zope import interface

from zope.exceptions.exceptionformatter import format_exception

from .job import Error

from .interfaces import IError

@interface.implementer(IError)
def _default_error_adapter(e):
	return Error(e.message)

@interface.implementer(IError)
def _default_exc_info(exc_info):
	t, v, tb = exc_info
	lines = format_exception(t, v, tb, with_filenames=True)
	if isinstance(str(''), bytes):
		lines = [l.encode('utf-8', 'replace') if isinstance(l, unicode) else l for l in lines]
	else:
		lines = [l.decode('utf-8', 'replace') if isinstance(l, bytes) else l for l in lines]
	message = str('').join(lines)
	return Error(message)
