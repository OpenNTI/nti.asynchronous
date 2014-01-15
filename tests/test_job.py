#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import is_
from hamcrest import assert_that
from hamcrest import has_property

import persistent

from nti.graphdb.async.job import Job

from nti.graphdb.async.tests import ConfiguringTestBase

def call():
	return 'my result'

class Demo(persistent.Persistent):
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

class TestJob(ConfiguringTestBase):

	def test_call(self):
		job = Job(call)
		result = job()
		assert_that(result, is_('my result'))
		
	def test_demo(self):
		demo = Demo()
		assert_that(demo, has_property('counter', is_(0)))
		j = Job(demo.increase)
		j()
		assert_that(demo, has_property('counter', is_(1)))

	def test_call_args(self):
		job = Job(call_args, 2,3)
		job(4)
		assert_that(job, has_property('result', is_(24)))

	def test_multiply(self):
		job = Job(multiply, 5, 3)
		job()
		assert_that(job, has_property('result', is_(15)))

		job = Job(multiply, 5, None)
		job()
		assert_that(job, has_property('has_failed', is_(True)))
