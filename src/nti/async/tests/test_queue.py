#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import is_
from hamcrest import is_in
from hamcrest import equal_to
from hamcrest import has_length
from hamcrest import assert_that
from hamcrest import has_property

from nti.testing.matchers import validly_provides
from nti.testing.matchers import verifiably_provides

import operator

from nti.async.interfaces import IQueue

from nti.async.job import create_job

from nti.async.queue import Queue

from nti.async.tests import AsyncTestCase


def mock_work():
    return 42


class TestQueue(AsyncTestCase):

    def test_queue(self):
        queue = Queue()
        assert_that(queue, validly_provides(IQueue))
        assert_that(queue, verifiably_provides(IQueue))

    def test_empty(self):
        queue = Queue()
        assert_that(queue, has_length(0))
        assert_that(list(queue), is_([]))

    def test_mockwork(self):
        queue = Queue()
        job = queue.put(create_job(mock_work))
        assert_that(queue, has_length(1))
        assert_that(list(queue), is_([job]))
        assert_that(queue[0], is_(job))
        assert_that(bool(queue), is_(True))
        assert_that(job.id, is_in(queue))
        assert_that(queue.keys(), has_length(1))
        assert_that(job, has_property('__parent__', queue))
        claimed = queue.claim()
        assert_that(claimed, equal_to(job))
        assert_that(queue, has_length(0))
        assert_that(list(queue), is_([]))
        assert_that(queue.keys(), has_length(0))

    def test_operator(self):
        queue = Queue()
        job2 = queue.put(create_job(operator.mul, (7, 6)))
        assert_that(queue, has_length(1))
        job3 = queue.put(create_job(operator.mul, (14, 3)))
        job4 = queue.put(create_job(operator.mul, (21, 2)))
        job5 = queue.put(create_job(operator.mul, (42, 1)))
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

        try:
            queue.remove(job4)
            self.fail()
        except LookupError:
            pass

        queue.put(job4)
        queue.put(job5)
        assert_that(list(queue), is_([job4, job5]))

        data = queue.all()
        assert_that(data, has_length(2))
        assert_that(queue.keys(), has_length(2))

        data = queue.failed()
        assert_that(data, has_length(0))

        first = queue[0]
        assert_that(queue.pull(0), equal_to(first))

        last = queue[-1]
        assert_that(queue.pull(-1), equal_to(last))

        assert_that(queue.put(first), equal_to(first))
        assert_that(queue.put(last), equal_to(last))

        assert_that(queue, has_length(2))
        emptied = queue.empty()
        assert_that(queue, has_length(0))
        assert_that(emptied, is_(2))
