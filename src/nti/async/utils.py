#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import types

import ZODB

import persistent

def custom_repr(obj):
    if (isinstance(obj, persistent.Persistent) or
        persistent.interfaces.IPersistent.providedBy(obj)):
        dbname = "?"
        if obj._p_jar is not None:
            dbname = getattr(obj._p_jar.db(), 'database_name', "?")
            if dbname != '?':
                dbname = repr(dbname)
        if obj._p_oid is not None:
            oid = ZODB.utils.u64(obj._p_oid)
        else:
            oid = '?'
        return '%s.%s (oid %s, db %s)' % (
                obj.__class__.__module__,
                obj.__class__.__name__,
                oid,
                dbname)
    if isinstance(obj, (types.FunctionType, types.BuiltinFunctionType)):
        return '%s.%s' % (obj.__module__, obj.__name__)
    else:
        return repr(obj)
