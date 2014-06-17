#!/usr/bin/env python
# -*- coding: utf-8 -*
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

from ZODB.POSException import ConflictError

from nti.dataserver import interfaces as nti_interfaces

from nti.utils.property import Lazy

from . import interfaces as async_interfaces

@interface.implementer(async_interfaces.IAsyncReactor)
class AsyncReactor(object):

	stop = False
	processor = None
	currentJob = None

	def __init__(self, name=u'', poll_inteval=2, exitOnError=False):
		self.name = name
		self.exitOnError = exitOnError
		self.poll_inteval = poll_inteval

	@Lazy
	def queue(self):
		queue = component.getUtility(async_interfaces.IQueue, name=self.name)
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

		job()
		if job.hasFailed:
			logger.error("job %r failed", job)
			self.queue.putFailed(job)
		logger.debug("job %r has been executed", job)

		return True
	
	def process_job(self):
		transaction_runner = \
				component.getUtility(nti_interfaces.IDataserverTransactionRunner)

		result = True
		try:
			if transaction_runner(self.execute_job, retries=2, sleep=1):
				self.poll_inteval = random.random() * 1.5
			else:
				self.poll_inteval += random.uniform(1, 5)
				self.poll_inteval = min(self.poll_inteval, 60)
		except (component.ComponentLookupError, AttributeError), e:
			logger.error('Error while processing job. Queue=(%s), error=%s', self.name, e)
			result = False
		except ConflictError:
			logger.error('ConflictError while pulling job from Queue=(%s)', self.name)
		except Exception:
			logger.exception('Cannot execute job. Queue=(%s)', self.name)
			result = not self.exitOnError
		return result

	def run(self, sleep=gevent.sleep):
		random.seed()
		self.stop = False
		try:
			logger.info('Starting reactor for Queue=(%s)', self.name)
			while not self.stop:
				try:
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
