#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import six

from zope import component
from zope import interface

from zope.exceptions.exceptionformatter import format_exception

from nti.async.interfaces import IError
from nti.async.interfaces import IException

from nti.async.job import Error

logger = __import__('logging').getLogger(__name__)


def _encode(l):
    return l.encode('utf-8', 'replace') if isinstance(l, six.text_type) else l


def _decode(l):
    return l.decode('utf-8', 'replace') if isinstance(l, bytes) else l


@interface.implementer(IError)
def _default_error_adapter(e):
    return Error(str(e))


@component.adapter(IException)
@interface.implementer(IError)
def _default_exc_info(exc_info):
    t, v, tb = exc_info
    lines = format_exception(t, v, tb, with_filenames=True) or ()
    if isinstance('', bytes):
        lines = [_encode(l) for l in lines]
    else:
        lines = [_decode(l) for l in lines]
    message = ''.join(lines)
    result = Error(message)
    return result

interface.classImplements(Exception, IException)
