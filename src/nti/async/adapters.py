#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import print_function, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import six

from zope import interface

from zope.exceptions.exceptionformatter import format_exception

from nti.async.interfaces import IError

from nti.async.job import Error


def _encode(l):
    return l.encode('utf-8', 'replace') if isinstance(l, six.text_type) else l


def _decode(l):
    return l.decode('utf-8', 'replace') if isinstance(l, bytes) else l


@interface.implementer(IError)
def _default_error_adapter(e):
    return Error(e.message)


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
