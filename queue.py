#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import BTrees

from zope import interface
from zope.container import contained as zcontained

from zc.queue import CompositeQueue

from persistent import Persistent

from . import interfaces as async_interfaces

@interface.implementer(async_interfaces.IQueue)
class Queue(Persistent, zcontained.Contained):

	def __init__(self):
		self._queue = CompositeQueue()
		self._length = BTrees.Length.Length(0)

	def put(self, item):
		item = async_interfaces.IJob(item)
		self._queue.put(item)
		item.__parent__ = self
		self._length.change(1)
		return item

	def _iter(self):
		queue = self._queue
		q = enumerate(queue)
		q_pop = queue.pull
		def get_next(i):
			try:
				next_ = i.next()
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
				self._length.change(-1)
				return job
		assert False, 'programmer error: the length appears to be incorrect.'

	def remove(self, item):
		for pop, ix, job in self._iter():
			if job is item:
				assert pop(ix) is job
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
			self._length.change(-1)
			return job
		return default

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
