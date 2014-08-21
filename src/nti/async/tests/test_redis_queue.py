#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import is_
from hamcrest import equal_to
from hamcrest import has_length
from hamcrest import assert_that
from hamcrest import has_property

import unittest
import operator

from nti.async.job import Job
from nti.async.redis_queue import RedisQueue

from nti.async.tests import AsyncTestCase

from nti.dataserver.tests.mock_redis import InMemoryMockRedis

def redis():
	return InMemoryMockRedis()

def mock_work():
	return 42

@unittest.SkipTest
class TesRedistQueue(AsyncTestCase):

	def test_empty(self):
		queue = RedisQueue(redis=redis())
		assert_that(queue, has_length(0))
		assert_that(list(queue), is_([]))

	def test_mockwork(self):
		from IPython.core.debugger import Tracer; Tracer()()
		queue = RedisQueue(redis=redis())
		job = queue.put(mock_work)
		assert_that(queue, has_length(1))
		assert_that(list(queue), is_([job]))
		assert_that(queue[0], is_(job))
		assert_that(bool(queue), is_(True))
		assert_that(job, has_property('__parent__', queue))
		claimed = queue.claim()
		assert_that(claimed, equal_to(job))
		assert_that(queue, has_length(0))
		assert_that(list(queue), is_([]))

	def test_operator(self):
		from IPython.core.debugger import Tracer; Tracer()()
		queue = RedisQueue(redis=redis())
		job2 = queue.put(Job(operator.mul, 7, 6))
		assert_that(queue, has_length(1))
		job3 = queue.put(Job(operator.mul, 14, 3))
		job4 = queue.put(Job(operator.mul, 21, 2))
		job5 = queue.put(Job(operator.mul, 42, 1))
		assert_that(queue, has_length(4))
		assert_that(list(queue), is_([job2, job3, job4, job5]))
		claimed = queue.claim()
		assert_that(claimed, equal_to(job2))

		pulled = queue.pull()
		assert_that(pulled, equal_to(job3))
		assert_that(list(queue), is_([job4, job5]))

		queue.remove(job4)
		assert_that(list(queue), is_([job5]))

		queue.remove(job5)
		assert_that(list(queue), is_([]))

		queue.put(job4)
		queue.put(job5)
		assert_that(list(queue), is_([job4, job5]))

		first = queue[0]
		assert_that(queue.pull(0), equal_to(first))

		last = queue[-1]
		assert_that(queue.pull(-1), equal_to(last))

		assert_that(queue.put(first), equal_to(first))
		assert_that(queue.put(last), equal_to(last))
		
		assert_that( queue, has_length( 2 ) )
		emptied = queue.empty()
		assert_that( queue, has_length( 0 ) )
		assert_that( emptied, is_( 2 ) )
