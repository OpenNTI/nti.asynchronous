#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import gevent
import random
from functools import partial

from zope import component
from zope import interface

from zope.component import ComponentLookupError

from zope.event import notify

from ZODB.POSException import ConflictError

from nti.async.interfaces import IQueue
from nti.async.interfaces import IAsyncReactor
from nti.async.interfaces import ReactorStarted
from nti.async.interfaces import ReactorStopped

from nti.property.property import CachedProperty

from nti.site.interfaces import ISiteTransactionRunner

from nti.zodb.interfaces import UnableToAcquireCommitLock
from nti.zodb.interfaces import ZODBUnableToAcquireCommitLock

@interface.implementer(IAsyncReactor)
class AsyncReactor(object):

	stop = False
	processor = None
	current_job = None
	poll_interval = None
	current_queue = None

	def __init__(self, queue_names=(), poll_interval=2, exitOnError=True,
				 queue_interface=IQueue, site_names=()):
		self.site_names = site_names
		self.exitOnError = exitOnError
		self.generator = random.Random()
		self.poll_interval = poll_interval
		self.queue_interface = queue_interface
		self.queue_names = list(queue_names or ())

	@CachedProperty('queue_names')
	def queues(self):
		queues = [component.getUtility(self.queue_interface, name=x)
				  for x in self.queue_names]
		return queues

	def add_queues(self, *queues):
		registered = []
		for x in queues:
			if 		x not in self.queue_names \
				and component.queryUtility(self.queue_interface, name=x) != None:
				registered.append(x)
		self.queue_names.extend(registered)
		return registered
	addQueues = add_queues
	
	def remove_queues(self, *queues):
		for x in queues:
			try:
				self.queue_names.remove(x)
			except ValueError:
				pass
	removeQueues = remove_queues
	
	def halt(self):
		self.stop = True

	@property
	def isRunning(self):
		return not self.stop and self.processor != None
	is_running = isRunning
	
	def start(self):
		if self.processor is None:
			self.processor = self._spawn_job_processor()

	def _get_job(self):
		# These are basically priority queues.
		# We should start at the beginning each time.
		job = None
		for queue in self.queues:
			job = queue.claim()
			if job is not None:
				self.current_job = job
				self.current_queue = queue
				break
		return job

	def execute_job(self):
		job = self._get_job()
		if job is None:
			return False

		logger.debug("[%s] Executing job (%s)", self.current_queue, job)
		job()
		if job.hasFailed:
			logger.error("[%s] Job %s failed", self.current_queue, job.id)
			self.current_queue.putFailed(job)
		logger.debug("[%s] Job %s has been executed", self.current_queue, job.id)

		return True
	executeJob = execute_job
	
	def process_job(self):
		result = True

		transaction_runner = component.getUtility(ISiteTransactionRunner)
		if self.site_names:
			transaction_runner = partial(transaction_runner, site_names=self.site_names)

		try:
			if transaction_runner(self.execute_job, retries=2, sleep=1):
				# Do not sleep if we have work to do, especially since
				# we may be reading from multiple queues.
				self.poll_interval = 0
			else:
				self.poll_interval += self.generator.uniform(1, 5)
				self.poll_interval = min(self.poll_interval, 60)
		except (ComponentLookupError, AttributeError, TypeError, StandardError) as e:
			logger.error('Error while processing job. Queue=[%s], error=%s',
						 self.current_queue, e)
			result = False
		except (ConflictError, UnableToAcquireCommitLock, ZODBUnableToAcquireCommitLock) as e:
			logger.error('ConflictError while pulling job from Queue=[%s], error=%s',
						 self.current_queue, e)
		except:
			logger.exception('Cannot execute job. Queue=[%s]', self.current_queue)
			result = not self.exitOnError
		return result
	processJob = process_job
	
	def run(self, sleep=gevent.sleep):
		notify(ReactorStarted(self))
		self.generator.seed()
		self.stop = False
		try:
			logger.info('Starting reactor for queues=(%s)', self.queue_names)
			while not self.stop:
				try:
					sleep(self.poll_interval)
					if not self.stop and not self.process_job():
						self.stop = True
				except KeyboardInterrupt:
					break
		finally:
			notify(ReactorStopped(self))
			logger.warn('Exiting reactor. Queues=(%s)', self.queue_names)
			self.processor = None

	__call__ = run

	def _spawn_job_processor(self):
		result = gevent.spawn(self.run)
		return result

class AsyncFailedReactor(AsyncReactor):
	"""
	Knows how to process jobs off of the failure queue.  We'll try
	each job once and then return.
	"""

	def __init__(self, queue_names=(), queue_interface=IQueue, site_names=()):
		self.site_names = site_names
		self.queue_names = queue_names
		self.queue_interface = queue_interface

	@CachedProperty('queue_names')
	def queues(self):
		queues = [component.getUtility(self.queue_interface, name=x).get_failed_queue()
				  for x in self.queue_names]
		return queues

	def __iter__(self):
		queue = self.current_queue
		job = original_job = queue.claim()
		logger.info('Processing queue [%s]', queue._name)
		while job is not None:
			yield job
			job = queue.claim()
			if job == original_job:
				# Stop when we reach the start
				break

	def execute_job(self):
		count = 0
		# We do all jobs for a queue inside a single (hopefully manageable) transaction.
		for job in self:
			logger.debug("[%s] Executing job (%s)", self.current_queue, job)
			job()
			if job.hasFailed:
				logger.error("[%s] Job (%s) failed", self.current_queue, job.id)
				self.current_queue.putFailed(job)
			else:
				count += 1
			logger.debug("[%s] Job (%s) has been executed", self.current_queue, job.id)

		return count

	def process_job(self):
		transaction_runner = component.getUtility(ISiteTransactionRunner)
		if self.site_names:
			transaction_runner = partial(transaction_runner, site_names=self.site_names)
		for queue in self.queues:
			self.current_queue = queue
			count = transaction_runner(self.execute_job, retries=2, sleep=1)
			logger.info('Finished processing queue [%s] [count=%s]', queue._name, count)

	def run(self):
		try:
			logger.info('Starting reactor for failed jobs in queues=(%s)',
						self.queue_names)
			self.process_job()
		finally:
			logger.warn('Exiting reactor. queues=(%s)', self.queue_names)
			self.processor = None

	__call__ = run
