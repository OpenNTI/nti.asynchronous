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

from zope import component
from zope import interface
from zope.component import ComponentLookupError

from ZODB.POSException import ConflictError

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

	def __init__(self, queue_names=(), poll_interval=2, exitOnError=True):
		self.queue_names = queue_names
		self.exitOnError = exitOnError
		self.poll_interval = poll_interval

	@Lazy
	def queues(self):
		queues = [component.getUtility(IQueue, name=x) for x in self.queue_names]
		return queues

	def halt(self):
		self.stop = True

	@property
	def isRunning(self):
		return not self.stop and self.processor != None

	def start(self):
		if self.processor is None:
			self.processor = self._spawn_job_processor()

	def execute_job(self):
		current_queue = None
		for current_queue in self.queues:
			job = current_queue.claim()
			if job is not None:
				self.current_queue = current_queue
				break

		if job is None:
			return False

		logger.debug("[%s] Executing job (%s)", current_queue, job)
		job()
		if job.hasFailed:
			logger.error("[%s] Job %r failed", current_queue, job)
			current_queue.putFailed(job)
		logger.debug("[%s] Job %r has been executed", current_queue, job)

		return True

	def process_job(self):
		transaction_runner = component.getUtility(IDataserverTransactionRunner)
		result = True

		try:
			if transaction_runner(self.execute_job, retries=2, sleep=1):
				# TODO Maybe we should not sleep if we have work to do
				# Especially since we may be reading from multiple queues.
				self.poll_interval = random.random() * 1.5
			else:
				self.poll_interval += random.uniform(1, 5)
				self.poll_interval = min(self.poll_interval, 60)
		except (ComponentLookupError, AttributeError, TypeError, StandardError), e:
			logger.error('Error while processing job. Queue=(%s), error=%s', self.current_queue, e)
			result = False
		except ConflictError:
			logger.error('ConflictError while pulling job from Queue=(%s)', self.current_queue)
		except:
			logger.exception('Cannot execute job. Queue=(%s)', self.current_queue)
			result = not self.exitOnError
		return result

	def run(self, sleep=gevent.sleep):
		random.seed()
		self.stop = False
		try:
			logger.info('Starting reactor for Queue=(%s)', self.queue_names )
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
