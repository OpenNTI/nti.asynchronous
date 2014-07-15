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
	currentJob = None

	def __init__(self, name=u'', to_sleep=True, poll_inteval=2, exitOnError=True):
		self.name = name
		self.to_sleep = to_sleep
		self.exitOnError = exitOnError
		self.poll_inteval = poll_inteval

	@Lazy
	def queue(self):
		queue = component.getUtility(IQueue, name=self.name)
		return queue

	def halt(self):
		self.stop = True

	@property
	def isRunning(self):
		return not self.stop and self.processor != None

	def start(self):
		if self.processor is None:
			self.processor = self._spawn_job_processor()

	def execute_job(self):
		self.currentJob = job = self.queue.claim()
		if job is None:
			return False

		logger.debug("Executing job (%s)", job)
		job()
		if job.hasFailed:
			logger.error("job %r failed", job)
			self.queue.putFailed(job)
		logger.debug("job %r has been executed", job)

		return True

	def process_job(self):
		transaction_runner = component.getUtility(IDataserverTransactionRunner)
		result = True
		try:
			if transaction_runner(self.execute_job, retries=2, sleep=1):
				self.poll_inteval = random.random() * 1.5
			else:
				self.poll_inteval += random.uniform(1, 5)
				self.poll_inteval = min(self.poll_inteval, 60)
		except (ComponentLookupError, AttributeError, TypeError, StandardError), e:
			logger.error('Error while processing job. Queue=(%s), error=%s', self.name, e)
			result = False
		except ConflictError:
			logger.error('ConflictError while pulling job from Queue=(%s)', self.name)
		except:
			logger.exception('Cannot execute job. Queue=(%s)', self.name)
			result = not self.exitOnError
		return result

	def run(self, sleep=gevent.sleep):
		random.seed()
		self.stop = False
		try:
			logger.info('Starting reactor for Queue=(%s) (sleeping=%s)',
						self.name, self.to_sleep )
			while not self.stop:
				try:
					if self.to_sleep:
						logger.info( 'Sleeping for %s seconds', self.poll_inteval )
						sleep(self.poll_inteval)
					if not self.stop and not self.process_job():
						self.stop = True
				except KeyboardInterrupt:
					break
		finally:
			logger.warn('Exiting reactor. Queue=(%s)', self.name)
			self.processor = None

	__call__ = run

	def _spawn_job_processor(self):
		result = gevent.spawn(self.run)
		return result
