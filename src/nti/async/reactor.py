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
import functools

from zope import component
from zope import interface
from zope.component import ComponentLookupError

from ZODB.POSException import ConflictError

from nti.zodb.interfaces import UnableToAcquireCommitLock

from nti.dataserver.interfaces import IDataserverTransactionRunner

from nti.utils.property import Lazy

from .interfaces import IQueue
from .interfaces import IAsyncReactor

@interface.implementer(IAsyncReactor)
class AsyncReactor(object):

	stop = False
	processor = None
	poll_interval = None
	current_queue = None
	current_queue_index = 0

	def __init__(self, queue_names=(), poll_interval=2, exitOnError=True, 
				 queue_interface=IQueue, site_names=()):
		self.site_names = site_names
		self.queue_names = queue_names
		self.exitOnError = exitOnError
		self.generator = random.Random()
		self.poll_interval = poll_interval
		self.queue_interface = queue_interface

	@Lazy
	def queues(self):
		queues = [component.getUtility(self.queue_interface, name=x) for x in self.queue_names]
		return queues

	def halt(self):
		self.stop = True

	@property
	def isRunning(self):
		return not self.stop and self.processor != None

	def start(self):
		if self.processor is None:
			self.processor = self._spawn_job_processor()

	def _get_job(self):
		job = None
		# Prefer our current queue first
		if self.current_queue is not None:
			job = self.current_queue.claim()

		# Ok, look at other queues
		if job is None:
			idx = self.current_queue_index
			while True:
				queue = self.queues[idx]
				job = queue.claim()
				if job is not None:
					self.current_queue_index = idx
					self.current_queue = queue
					break

				idx += 1
				if idx >= len( self.queues ):
					idx = 0

				if idx == self.current_queue_index:
					break
		return job

	def execute_job(self):
		job = self._get_job()

		if job is None:
			return False

		logger.debug("[%s] Executing job (%s)", self.current_queue, job)
		job()
		if job.hasFailed:
			logger.error("[%s] Job %r failed", self.current_queue, job)
			self.current_queue.putFailed(job)
		logger.debug("[%s] Job %r has been executed", self.current_queue, job)

		return True

	def process_job(self):
		result = True

		transaction_runner = component.getUtility(IDataserverTransactionRunner)
		if self.site_names:
			transaction_runner = functools.partial(transaction_runner,
												   site_names=self.site_names)
		# TODO We need to have the site draped off of the events, and then run
		# within that site in the transaction_runner.
		try:
			if transaction_runner(self.execute_job, retries=2, sleep=1):
				# Do not sleep if we have work to do, especially since
				# we may be reading from multiple queues.
				self.poll_interval = 0
			else:
				self.poll_interval += self.generator.uniform(1, 5)
				self.poll_interval = min(self.poll_interval, 60)
		except (ComponentLookupError, AttributeError, TypeError, StandardError) as e:
			logger.error('Error while processing job. Queue=(%s), error=%s', self.current_queue, e)
			result = False
		except (ConflictError,UnableToAcquireCommitLock) as e:
			logger.error('ConflictError while pulling job from Queue=(%s), error=%s', self.current_queue, e)
		except:
			logger.exception('Cannot execute job. Queue=(%s)', self.current_queue)
			result = not self.exitOnError
		return result

	def run(self, sleep=gevent.sleep):
		self.generator.seed()
		self.stop = False
		try:
			logger.info('Starting reactor for queues=(%s)', self.queue_names )
			while not self.stop:
				try:
					sleep(self.poll_interval)
					if not self.stop and not self.process_job():
						self.stop = True
				except KeyboardInterrupt:
					break
		finally:
			logger.warn('Exiting reactor. Queue=(%s)', self.queue_names)
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

	def __init__(self, queue_names=(), queue_interface=IQueue,
				 site_names=()):
		self.site_names = site_names
		self.queue_names = queue_names
		self.queue_interface = queue_interface

	@Lazy
	def queues(self):
		queues = [	component.getUtility(self.queue_interface, name=x).get_failed_queue()
					for x in self.queue_names]
		return queues

	def __iter__(self):
		for queue in self.queues:
			self.current_queue = queue
			job = original_job = queue.claim()
			logger.info( 'Processing queue (%s)', queue._name )
			while job is not None:
				yield job
				job = queue.claim()
				if job == original_job:
					# Stop when we reach the start
					break

	def execute_job(self):
		# We do all jobs inside a single (hopefully manageable) transaction.
		for job in self:
			logger.debug("[%s] Executing job (%s)", self.current_queue, job)
			job()
			if job.hasFailed:
				logger.error("[%s] Job %r failed", self.current_queue, job)
				self.current_queue.putFailed(job)
			logger.debug("[%s] Job %r has been executed", self.current_queue, job)

		return True

	def process_job(self):
		transaction_runner = component.getUtility(IDataserverTransactionRunner)
		if self.site_names:
			transaction_runner = functools.partial(transaction_runner,
												   site_names=self.site_names)
		# TODO We need to have the site draped off of the events, and then run
		# within that site in the transaction_runner.
		transaction_runner(self.execute_job, retries=2, sleep=1)

	def run(self):
		try:
			logger.info('Starting reactor for failed jobs in queues=(%s)', self.queue_names )
			self.process_job()
		finally:
			logger.warn('Exiting reactor. queues=(%s)', self.queue_names)
			self.processor = None

	__call__ = run
