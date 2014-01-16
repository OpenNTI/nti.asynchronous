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

from nti.dataserver import interfaces as nti_interfaces

from . import interfaces as async_interfaces

@interface.implementer(async_interfaces.IJobReactor)
class JobReactor(object):

	stop = False
	inline = True
	processor = None

	def __init__(self, poll_inteval=2):
		self.pid = os.getpid()
		self.poll_inteval = poll_inteval
		self.processor = self._spawn_job_processor()

	@classmethod
	def queue(self):
		queue = component.getUtility(async_interfaces.IQueue)
		return queue

	def execute_job(self):
		job = self.queue().claim()
		if job is None:
			return False

		pid = self.pid
		logger.log(loglevels.TRACE, "%s executing job %r", pid, job)
		job()
		if job.has_failed:
			logger.error("job %r failed in process %s", job, pid)
		logger.log(loglevels.TRACE, "%s executing job %r", pid, job)
		return True

	def halt(self):
		self.stop = True

	def start(self):
		if self.processor is None:
			self.processor = self._spawn_job_processor()

	def _spawn_job_processor(self):
		random.seed()
		def process():
			# XXX not as efficient, but wait some time before start checking
			# the job queue
			secs = random.randint(25, 35)
			gevent.sleep(seconds=secs)
			while not self.stop:
				gevent.sleep(seconds=self.poll_inteval)
				if not self.stop:
					try:
						self.process_job(self.pid)
					except:
						break
			self.processor = None

		result = gevent.spawn(process)
		return result

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
		except component.ComponentLookupError:
			raise
		except AttributeError:
			logger.error('Cannot pull job from queue (%s)', pid)
			raise
		except Exception:
			logger.exception('Cannot pull job from queue (%s)', pid)


