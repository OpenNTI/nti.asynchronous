#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import print_function, unicode_literals, absolute_import, division
from feedgenerator.django.utils.functional import empty
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import zlib
import pickle
from io import BytesIO

import transaction

from zope import interface

from nti.async.interfaces import IJob
from nti.async.interfaces import IRedisQueue

from nti.property.property import Lazy

from nti.transactions import transactions

DEFAULT_QUEUE_NAME = 'nti/async/jobs'

@interface.implementer(IRedisQueue)
class RedisQueue(object):

	_queue = _length = _failed_jobs = None

	def __init__(self, redis, job_queue_name=None, failed_queue_name=None,
				 create_failed_queue=True):
		self.__redis = redis
		self._name = job_queue_name or DEFAULT_QUEUE_NAME
		self._hash = self._name + '/hash'
		if create_failed_queue:
			failed_queue_name = failed_queue_name or self._name + "/failed"
			self._failed = RedisQueue(self.__redis,
									  job_queue_name=failed_queue_name,
									  create_failed_queue=False)
		else:
			self._failed = self

	@Lazy
	def _redis(self):
		return self.__redis() if callable(self.__redis) else self.__redis

	def _pickle(self, job):
		bio = BytesIO()
		pickle.dump(job, bio)
		bio.seek(0)
		result = zlib.compress(bio.read())
		return result

	def _put_job(self, pipe, data, tail=True, jid=None):
		if tail:
			pipe.rpush(self._name, data)
		else:
			pipe.lpush(self._name, data)
		if jid is not None:
			pipe.hset(self._hash, jid, '1')
		pipe.execute()

	def put(self, item, use_transactions=True, tail=True):
		item = IJob(item)
		data = self._pickle(item)
		pipe = self._redis.pipeline()
		logger.debug('Placing job (%s) in [%s]', item.id, self._name)
		if use_transactions:
			# Only place the job once the transaction has been committed.
			transactions.do(target=self,
							call=self._put_job,
							args=(pipe, data, tail, item.id))
		else:
			self._put_job(pipe, data, tail, item.id)
		return item

	def _unpickle(self, data):
		data = zlib.decompress(data)
		bio = BytesIO(data)
		bio.seek(0)
		result = pickle.load(bio)
		assert IJob.providedBy(result)
		return result

	def _hdel(self, job):
		self._redis.pipeline().hdel(self._hash, job.id).execute()

	def removeAt(self, index, unpickle=True):
		length = len(self)
		if index < 0:
			index += length
			if index < 0:
				raise IndexError(index + length)
		if index >= length:
			raise IndexError(index)

		data = self._redis.pipeline().\
						   lrange(self._name, 0, index - 1).\
						   lindex(self._name, index).\
						   lrange(self._name, index + 1, -1).\
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
		self._hdel(result) # unset
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
		self._hdel(job) # unset
		return job

	def all(self, unpickle=True):
		data = self._redis.pipeline().lrange(self._name, 0, -1).execute()
		data = data[0] if data and data[0] else None
		if unpickle:
			result = [self._unpickle(x) for x in data or ()]
		else:
			result = data or ()
		return result
	values = all

	def remove(self, item):
		# jobs are pickled
		raise NotImplementedError()

	def claim(self, default=None):
		# once we get the job from redis, it's remove from it
		data = self._redis.pipeline().lpop(self._name).execute()
		if not data or not data[0]:
			return default

		job = self._unpickle(data[0])
		self._hdel(job) # unset
		logger.debug("Job (%s) claimed", job.id)

		# make sure we put the job back if the transaction fails
		def after_commit_or_abort(success=False):
			if not success:
				logger.warn("Pushing job back onto queue on abort [%s] (%s)",
							self._name, job.id)
				# We do not want to claim any jobs on transaction abort.
				# Add our job back to the front of the queue.
				self._redis.pipeline().lpush(self._name, data[0]) \
									  .hset(self._hash, job.id, '1').execute()
		transaction.get().addAfterCommitHook(after_commit_or_abort)
		return job

	def empty(self):
		keys = self._redis.pipeline().delete(self._name).hkeys(self._hash).execute()
		if keys and keys[1]:
			self._redis.pipeline().hdel(self._hash, *keys[1]).execute()
			return keys[1]
		return ()
	reset = empty

	def putFailed(self, item):
		self._failed.put(item)
	put_failed = putFailed

	def get_failed_queue(self):
		return self._failed

	def failed(self, unpickle=True):
		if self._failed is not self:
			return self._failed.all(unpickle)
		return ()

	def __str__(self):
		return self._name

	def __repr__(self):
		return "%s(%s,%s)" % (self.__class__.__name__, self._name, self._failed._name)

	def __len__(self):
		result = self._redis.pipeline().llen(self._name).execute()
		return result[0] if result and result[0] is not None else 0

	def keys(self):
		result = self._redis.pipeline().hkeys(self._hash).execute()
		return result[0] if result else ()

	def __contains__(self, key):
		result = self._redis.pipeline().hexists(self._hash, key).execute()
		return bool(result[0]) if result else False

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

Queue = RedisQueue  # alias
