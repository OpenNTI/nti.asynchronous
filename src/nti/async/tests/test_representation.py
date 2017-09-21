#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import is_
from hamcrest import assert_that
from hamcrest import contains_string

import unittest

from nti.async.representation import WithRepr


class TestRepresentation(unittest.TestCase):

    def test_default(self):

        @WithRepr
        class Foo(object):
            pass

        r = repr(Foo())
        assert_that(r, contains_string('<nti.async.tests.test_representation.Foo'))
        assert_that(r, contains_string('{}>'))

    def test_proxied(self):
        from zope.security.checker import ProxyFactory
        @WithRepr
        class Foo(object):
            pass

        r = repr(ProxyFactory(Foo()))
        assert_that(r, contains_string('<nti.async.tests.test_representation.Foo at'))
        assert_that(r, contains_string('{}'))


    def test_with_default_callable(self):
        @WithRepr(lambda s: "<HI>")
        class Foo(object):
            pass

        r = repr(Foo())
        assert_that(r, is_("<HI>"))

    def test_raises_POSError(self):
        def raise_(self):
            from ZODB.POSException import ConnectionStateError
            raise ConnectionStateError("CSE")

        @WithRepr(raise_)
        class Foo(object):
            pass

        r = repr(Foo())
        assert_that(r,
                    is_("<nti.async.tests.test_representation.Foo(Ghost, "
                        "ConnectionStateError('CSE',))>"))

    def test_raises_attribute_error(self):
        def raise_(self):
            raise AttributeError("an attr")

        @WithRepr(raise_)
        class Foo(object):
            pass

        r = repr(Foo())
        assert_that(r,
                    is_("<nti.async.tests.test_representation.Foo("
                        "AttributeError('an attr',))>"))
