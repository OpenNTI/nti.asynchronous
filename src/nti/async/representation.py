#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import print_function, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from ZODB.POSException import ConnectionStateError


def make_repr(default=None):
    if default is None:
        def default(self):
            return "%s().__dict__.update( %s )" % (self.__class__.__name__, self.__dict__)

    def __repr__(self):
        try:
            return default(self)
        except ConnectionStateError:
            return '%s(Ghost)' % self.__class__.__name__
        except (ValueError, LookupError) as e:
            return '%s(%s)' % (self.__class__.__name__, e)
    return __repr__


def WithRepr(default=object()):
    """
    A class decorator factory to give a __repr__ to
    the object. Useful for persistent objects.

    :keyword default: A callable to be used for the default value.
    """

    # If we get one argument that is a type, we were
    # called bare (@WithRepr), so decorate the type
    if isinstance(default, type):
        default.__repr__ = make_repr()
        return default

    # If we got None or anything else, we were called as a factory,
    # so return a decorator
    def d(cls):
        cls.__repr__ = make_repr()
        return cls
    return d
