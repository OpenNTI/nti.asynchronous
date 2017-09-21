#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import is_
from hamcrest import none
from hamcrest import is_not
from hamcrest import has_length
from hamcrest import assert_that
from hamcrest import has_property
from hamcrest import greater_than

from nti.testing.matchers import validly_provides
from nti.testing.matchers import verifiably_provides

import sys
from io import BytesIO

try:
    import cPickle as pickle
except ImportError:
    import pickle

from nti.async.interfaces import IJob
from nti.async.interfaces import IError

from nti.async.job import Job

from nti.async.threadlocal import get_current_job

from nti.async.tests import AsyncTestCase


def call():
    return u'my result'


class Demo(object):
    counter = 0

    def increase(self, value=1):
        self.counter += value


def call_args(*args):
    res = 1
    for a in args:
        res *= a
    return res


def multiply(first, second, third=None):
    res = first * second
    if third is not None:
        res *= third
    return res


def current_job():
    job = get_current_job()
    assert_that(job, is_not(none()))


class TestJob(AsyncTestCase):

    def test_job(self):
        job = Job(call)
        assert_that(job, validly_provides(IJob))
        assert_that(job, verifiably_provides(IJob))

    def test_call(self):
        job = Job(call)
        result = job()
        assert_that(result, is_('my result'))

    def test_adapter(self):
        job = IJob(call, None)
        assert_that(job, is_not(none()))

    def test_demo(self):
        demo = Demo()
        assert_that(demo, has_property('counter', is_(0)))
        j = Job(demo.increase)
        j()
        assert_that(demo, has_property('counter', is_(1)))

    def test_call_args(self):
        job = Job(call_args, 2, 3)
        job(4)
        assert_that(job, has_property('result', is_(24)))

    def test_multiply(self):
        job = Job(multiply, 5, 3)
        job()
        assert_that(job, has_property('result', is_(15)))

        job = Job(multiply, 5, None)
        job()
        assert_that(job.has_failed(), is_(True))

    def test_error(self):
        e = Exception('error')
        error = IError(e)
        assert_that(error, is_not(none()))
        assert_that(error, has_property('message', 'error'))

        try:
            raise Exception()
        except:
            error = IError(sys.exc_info())
        assert_that(error, is_not(none()))
        assert_that(error,
                    has_property('message', has_length(greater_than(1))))

    def test_pickle(self):
        job = Job(multiply, 5, 3)
        bio = BytesIO()
        pickle.dump(job, bio)

        bio.seek(0)
        unpickled = pickle.load(bio)
        assert_that(unpickled.is_new(), is_(True))
        unpickled()
        assert_that(unpickled, has_property('result', is_(15)))

    def test_current_job(self):
        assert_that(get_current_job(), is_(none()))
        job = Job(current_job)
        job()
        job = get_current_job()
        assert_that(job, is_(none()))
