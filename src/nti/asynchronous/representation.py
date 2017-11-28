#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from ZODB.POSException import POSError

logger = __import__('logging').getLogger(__name__)


def _type_name(self):
    t = type(self)
    type_name = t.__module__ + '.' + t.__name__
    return type_name


def _default_repr(self):
    # When we're executing, even if we're wrapped in a proxy when called,
    # we get an unwrapped self.
    return "<%s at %x %s>" % (_type_name(self), id(self), self.__dict__)


def make_repr(default=_default_repr):
    default = default if callable(default) else _default_repr

    def __repr__(self):
        try:
            return default(self)
        except POSError as cse:
            return '<%s(Ghost, %r)>' % (_type_name(self), cse)
        except (ValueError, LookupError, AttributeError) as e:
            # Things like invalid NTIID, missing registrations for the first two.
            # The final would be a  weird database-related issue.
            return '<%s(%r)>' % (_type_name(self), e)

    return __repr__


def WithRepr(default=object()):
    """
    A class decorator factory to give a ``__repr__`` to
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
        cls.__repr__ = make_repr(default)
        return cls
    return d
