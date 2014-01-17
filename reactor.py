#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import os
import gevent
import random

from zope import component
from zope import interface

from ZODB import loglevels
from ZODB.POSException import ConflictError

from nti.dataserver import interfaces as nti_interfaces

from . import interfaces as async_interfaces

@interface.implementer(async_interfaces.IJobReactor)
class JobReactor(object):

	stop = False
	inline = True
	processor = None

	def __init__(self, poll_inteval=2):
		self.poll_inteval = poll_inteval

	def __repr__(self):
		return "(%s)" % self.pid

	@classmethod
	def queue(self):
		queue = component.getUtility(async_interfaces.IQueue)
		return queue

	def halt(self):
		self.stop = True

	def start(self):
		if self.processor is None:
			self.processor = self._spawn_job_processor()

	def execute_job(self):
		job = self.queue().claim()
		if job is None:
			return False

		logger.log(loglevels.TRACE, "%s executing job %r", self.pid, job)
		job()
		if job.has_failed:
			logger.error("job %r failed in process %s", job, self.pid)
		logger.log(loglevels.TRACE, "%s executed job %r", self.pid, job)

		return True

	def process_job(self, pid):
		transaction_runner = \
				component.getUtility(nti_interfaces.IDataserverTransactionRunner)
		try:
			result = transaction_runner(self.execute_job, retries=3)
			if result:
				self.poll_inteval = random.random() * 3
			else:
				self.poll_inteval += 5
				self.poll_inteval = min(self.poll_inteval, 60)
		except (component.ComponentLookupError, AttributeError), e:
			logger.error('Error while processing queued job. pid=(%s), error=%s', pid, e)
			return False
		except ConflictError:
			logger.error('ConflictError while pulling job from queue. pid=(%s)', pid)
		except Exception:
			logger.exception('Cannot execute job. pid=(%s)', pid)
		return True

	def run(self, sleep=gevent.sleep):
		random.seed()
		self.stop = False
		self.pid = os.getpid()
		try:
			sleep(random.randint(3, 10))
			while not self.stop:
				try:
					sleep(self.poll_inteval)
					if not self.stop and not self.process_job(self.pid):
						self.stop = True
				except KeyboardInterrupt:
					break
		finally:
			logger.warn('Exiting reactor. pid=(%s)', self.pid)
			self.processor = None

	__call__ = run

	def _spawn_job_processor(self):
		result = gevent.spawn(self.run)
		return result

from zope.processlifetime import IDatabaseOpenedWithRoot

@component.adapter(IDatabaseOpenedWithRoot)
def _start_reactor(database_event):
	reactor = JobReactor()
	component.provideUtility(reactor, async_interfaces.IJobReactor)
	reactor.start()
