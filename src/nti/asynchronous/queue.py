#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from zope import interface

from zope.location.interfaces import IContained

from zc.queue import CompositeQueue

import BTrees

from persistent import Persistent

from persistent.list import PersistentList

from nti.asynchronous.interfaces import IJob
from nti.asynchronous.interfaces import IQueue

logger = __import__('logging').getLogger(__name__)


@interface.implementer(IQueue, IContained)
class Queue(Persistent):

    __name__ = None
    __parent__ = None

    family = BTrees.family64

    _queue = _length = _failed_jobs = None

    def __init__(self, compositeSize=15):
        self._reset(compositeSize)

    def _reset(self, compositeSize=15):
        self._jobs = self.family.OO.OOSet()
        self._length = BTrees.Length.Length(0)
        self._queue = CompositeQueue(compositeSize=compositeSize)

    def put(self, item, *unused_args, **unused_kwargs):
        item = IJob(item)
        self._queue.put(item)
        self._jobs.add(item.id)
        item.__parent__ = self
        self._length.change(1)
        return item

    def _iter(self):
        queue = self._queue
        q = enumerate(queue)
        q_pop = queue.pull

        def get_next(i):
            try:
                next_ = next(i)
            except StopIteration:
                active = False
                next_ = (None, None)
            else:
                active = True
            return active, next_

        q_active, (q_index, q_next) = get_next(q)
        if q_active:
            yield q_pop, q_index, q_next
            for (q_index, q_next) in q:
                yield q_pop, q_index, q_next

    def _discard(self, key):
        try:
            self._jobs.remove(key)
        except KeyError:
            pass

    def pull(self, index=0):
        length = len(self)
        if index < 0:
            index += length
            if index < 0:
                raise IndexError(index + length)
        if index >= length:
            raise IndexError(index)
        for i, (pop, ix, job) in enumerate(self._iter()):
            if i == index:
                tmp = pop(ix)
                assert tmp is job
                self._discard(job.id)
                self._length.change(-1)
                return job
        assert False, 'programmer error: the length appears to be incorrect.'

    def remove(self, item):
        for pop, ix, job in self._iter():
            if job is item:
                assert pop(ix) is job
                self._discard(job.id)
                self._length.change(-1)
                break
        else:
            raise LookupError('item not in queue', item)

    def claim(self, default=None):
        if not self._length():
            return default
        for pop, ix, job in self._iter():
            tmp = pop(ix)
            assert tmp is job
            self._discard(job.id)
            self._length.change(-1)
            return job
        return default

    def all(self):
        result = []
        for _, _, job in list(self._iter()):  # snapshot
            result.append(job)
        return result
    values = all

    def empty(self):
        result = self._length()
        if result:
            self._reset()
        return result
    reset = empty

    def put_failed(self, item):
        if self._failed_jobs is None:
            self._failed_jobs = PersistentList()
        item = IJob(item)
        self._failed_jobs.append(item)
    putFailed = put_failed

    def failed(self):
        return list(self._failed_jobs) if self._failed_jobs else ()

    def __len__(self):
        return self._length()

    def __iter__(self):
        return (next_ for pop, ix, next_ in self._iter())

    def __nonzero__(self):
        return bool(self._length())

    def __getitem__(self, index):
        length = len(self)
        if index < 0:
            index += length
            if index < 0:
                raise IndexError(index + length)
        if index >= length:
            raise IndexError(index)
        for i, (_, _, job) in enumerate(self._iter()):
            if i == index:
                return job
        assert False, 'programmer error: the length appears to be incorrect.'

    def keys(self):
        return list(self._jobs)

    def __contains__(self, key):
        return key and key in self._jobs
