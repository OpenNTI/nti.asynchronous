#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import zlib
import pickle
import transaction
from io import BytesIO

from zope import interface

from nti.utils.property import Lazy

from .interfaces import IJob
from .interfaces import IRedisQueue

from nti.utils import transactions

DEFAULT_QUEUE_NAME = 'nti/async/jobs'

@interface.implementer(IRedisQueue)
class RedisQueue(object):

	_queue = _length = _failed_jobs = None

	def __init__(self, redis, job_queue_name=None, failed_queue_name=None):
		self.__redis = redis
		self._name = job_queue_name or DEFAULT_QUEUE_NAME
		self._failed = failed_queue_name or self._name + "/failed"
		transactions.add_abort_hooks()

	@Lazy
	def _redis(self):
		return self.__redis() if callable(self.__redis) else self.__redis

	def _pickle(self, job):
		bio = BytesIO()
		pickle.dump(job, bio)
		bio.seek(0)
		result = zlib.compress(bio.read())
		return result

	def _put_job(self,data):
		logger.debug( 'Placing job %s', id( data ))
		self._redis.pipeline().rpush(self._name, data).execute()

	def put(self, item):
		logger.debug( 'Placing item %s', item)
		item = IJob(item)
		data = self._pickle(item)

		transactions.do(target=self,
						call=self._put_job,
						args=(data,) )

		return item

	def _unpickle(self, data):
		data = zlib.decompress(data)
		bio = BytesIO(data)
		bio.seek(0)
		result = pickle.load(bio)
		assert IJob.providedBy(result)
		return result

	def removeAt(self, index, unpickle=True):
		length = len(self)
		if index < 0:
			index += length
			if index < 0:
				raise IndexError(index + length)
		if index >= length:
			raise IndexError(index)

		data = self._redis.pipeline().\
						   lrange(self._name, 0, index-1).\
						   lindex(self._name, index).\
						   lrange(self._name, index+1,-1).\
						   execute()

		result = data[1] if data and len(data) >= 2 and data[1] else None
		if not result:
			raise ValueError("Invalid data at index %s" % index)

		new_items = []
		def _append(items):
			new_items.extend(i for i in items or () if i is not None)
		_append(data[0] if data and len(data) >= 1 else ())
		_append(data[2] if data and len(data) >= 3 else ())
		if new_items:
			self._redis.pipeline().\
						delete(self._name).\
						rpush(self._name, *new_items).\
						execute()

		result = self._unpickle(result) if unpickle else result
		return result

	def pull(self, index=0):
		data = None
		if index == 0:
			data = self._redis.pipeline().lpop(self._name).execute()
			data = data[0] if data and data[0] else None
		elif index == -1:
			data = self._redis.pipeline().rpop(self._name).execute()
			data = data[0] if data and data[0] else None
		else:
			data = self.removeAt(index, result=False)

		if data is None:
			raise IndexError(index)
		job = self._unpickle(data)

		return job

	def remove(self, item):
		# jobs are pickled
		raise NotImplementedError()

	def claim(self, default=None):
		data = self._redis.pipeline().lpop(self._name).execute()

		def after_commit_or_abort( success=False ):
			if success:
				return
			logger.info( "Pushing message back onto queue on abort (%s) (%s)", self._name, id( data ) )
			# We do not want to claim any jobs on transaction abort.
			# Add our job back to the front of the queue.
			if data and data[0]:
				self._redis.pipeline().lpush(self._name, data[0]).execute()
		transaction.get().addAfterCommitHook( after_commit_or_abort )
		transaction.get().addAfterAbortHook( after_commit_or_abort )

		if data and data[0]:
			job = self._unpickle(data[0])
			return job
		return default

	def empty(self):
		self._redis.pipeline().delete(self._name).execute()

	def putFailed(self, item):
		item = IJob(item)
		data = self._pickle(item)
		self._redis.pipeline().rpush(self._failed, data).execute()
	put_failed = putFailed

	def __len__(self):
		result = self._redis.pipeline().llen(self._name).execute()
		return result[0] if result and result[0] is not None else 0

	def __iter__(self):
		all_jobs = self._redis.pipeline().lrange(self._name, 0, -1).execute()
		all_jobs = all_jobs[0] if all_jobs else ()
		for data in all_jobs or ():
			if data is not None:
				job = self._unpickle(data)
				yield job

	def __nonzero__(self):
		return bool(len(self))

	def __getitem__(self, index):
		data = self._redis.pipeline().lindex(self._name, index).execute()
		data = data[0] if data and data[0] else None
		if data is None:
			raise IndexError(index)
		job = self._unpickle(data)
		return job

Queue = RedisQueue # alias

