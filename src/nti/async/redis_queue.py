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
from io import BytesIO

from zope import interface

from .interfaces import IJob
from .interfaces import IRedisQueue

DEFAULT_QUEUE_NAME = 'nti/async/jobs'

@interface.implementer(IRedisQueue)
class RedisQueue(object):

	_queue = _length = _failed_jobs = None

	def __init__(self, redis, job_queue_name=None, failed_queue_name=None):
		self._redis = redis
		self._name = job_queue_name or DEFAULT_QUEUE_NAME
		self._failed = failed_queue_name or self._name + "/failed"
		
	def _pickle(self, job):
		bio = BytesIO()
		pickle.dump(job, bio)
		bio.seek(0)
		result = zlib.compress(bio.read())
		return result
		
	def put(self, item):
		item = IJob(item)
		data = self._pickle(item)
		self._redis.pipeline().rpush(self._name, data).execute()
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

		result = data[1] if data and len(data) >= 2 else None
		if not result:
			raise ValueError("Invalid data at index %s" % index)
		
		new_items = []
		new_items.extend(data[0] if data and len(data) >= 1 else ())
		new_items.extend(data[2] if data and len(data) >= 3 else ())
		if new_items:
			self._redis.pipeline().\
						delete(self._name).\
						rpush(self._name, *new_items).\
						execute()
					
		result = self._unpickle(result) if unpickle else result
		return result
	
	def pull(self, index=0):
		if index < 0 and index != -1:
			raise IndexError(index)
		
		data = None
		if index == 0:
			data = self._redis.pipeline().lpop(self._name).execute()
			data = data[0] if data else None
		elif index == -1:
			data = self._redis.pipeline().rpop(self._name).execute()
			data = data[0] if data else None
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
		if data:
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
		return result[0] if result else 0

	def __iter__(self):
		all_jobs = self._redis.pipeline().lrange(self._name, 0, -1).execute()
		all_jobs = all_jobs[0] if all_jobs else ()
		for data in all_jobs or ():
			job = self._unpickle(data)
			yield job

	def __nonzero__(self):
		return bool(len(self))

	def __getitem__(self, index):
		data = self._redis.pipeline().lindex(self._name, index).execute()
		data = data[0] if data else None
		if data is None:
			raise IndexError(index)
		job = self._unpickle(data)
		return job

Queue = RedisQueue # alias

