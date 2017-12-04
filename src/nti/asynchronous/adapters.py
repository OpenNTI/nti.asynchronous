#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from zope import component
from zope import interface

from zope.exceptions.exceptionformatter import format_exception

from nti.asynchronous._compat import text_

from nti.asynchronous.interfaces import IError
from nti.asynchronous.interfaces import IException

from nti.asynchronous.job import Error

logger = __import__('logging').getLogger(__name__)


@interface.implementer(IError)
def _default_error_adapter(e):
    return Error(str(e))


@component.adapter(IException)
@interface.implementer(IError)
def _default_exc_info(exc_info):
    t, v, tb = exc_info
    lines = format_exception(t, v, tb, with_filenames=True) or ()
    lines = [text_(l) for l in lines]
    message = ''.join(lines)
    result = Error(message)
    return result

interface.classImplements(Exception, IException)
