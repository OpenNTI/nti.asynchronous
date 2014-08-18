#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import is_
from hamcrest import assert_that
from hamcrest import none
from hamcrest import not_none

import operator

from nti.async.job import Job
from nti.async.queue import Queue
from nti.async.reactor import AsyncReactor

from nti.async.tests import AsyncTestCase

def mock_work():
	return 42

class TestReactor(AsyncTestCase):

	def setUp(self):
		self.reactor = AsyncReactor()
		self.reactor.queues = ()

	def test_empty(self):
		q1 = Queue()
		q2 = Queue()
		q3 = Queue()
		self.reactor.queues = [q1,q2,q3]
		job = self.reactor._get_job()
		assert_that( self.reactor.queues, has_length( 3 ))
		assert_that( job, none() )

	def test_basic(self):
		q1 = Queue()
		q2 = Queue()
		q3 = Queue()
		job1 = q3.put(Job(operator.mul, 7, 6))
		job2 = q3.put(Job(operator.mul, 14, 3))
		self.reactor.queues = [q1,q2,q3]

		job = self.reactor._get_job()
		assert_that( job, not_none() )
		assert_that( job, is_( job1 ))

		job = self.reactor._get_job()
		assert_that( job, not_none() )
		assert_that( job, is_( job2 ))

		job = self.reactor._get_job()
		assert_that( job, none() )

	def test_boundary(self):
		q1 = Queue()
		q2 = Queue()
		q3 = Queue()
		job1 = q3.put(Job(operator.mul, 7, 6))
		job2 = q3.put(Job(operator.mul, 14, 3))
		self.reactor.queues = [q1,q2,q3]

		job = self.reactor._get_job()
		assert_that( job, not_none() )
		assert_that( job, is_( job1 ))

		# New jobs up front
		job3 = q1.put(Job(operator.mul, 7, 6))
		job4 = q2.put(Job(operator.mul, 14, 3))

		job = self.reactor._get_job()
		assert_that( job, not_none() )
		assert_that( job, is_( job2 ))

		job = self.reactor._get_job()
		assert_that( job, not_none() )
		assert_that( job, is_( job3 ))

		job = self.reactor._get_job()
		assert_that( job, not_none() )
		assert_that( job, is_( job4 ))

		# Empty again
		job = self.reactor._get_job()
		assert_that( job, none() )

		# And again
		job5 = q3.put(Job(operator.mul, 7, 6))
		job6 = q1.put(Job(operator.mul, 14, 3))
		job = self.reactor._get_job()
		assert_that( job, not_none() )
		assert_that( job, is_( job5 ))

		job = self.reactor._get_job()
		assert_that( job, not_none() )
		assert_that( job, is_( job6 ))
