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
import functools

from zope import component
from zope import interface

from nti.dataserver import interfaces as nti_interfaces

from . import interfaces as async_interfaces

def _execute_job(job):
	logger.debug("executing job %r", job)
	job()
	if job.has_failed:
		logger.error("job %r failed", job)
	logger.debug("job %r completed", job)

def _pull_job(inline=False):
	queue = component.getUtility(async_interfaces.IQueue)
	job = queue.pull()
	if job is None:
		return

	if inline:
		_execute_job(job)
	else:
		# XXX Execute in a seperate transaction
		def _executor():
			transaction_runner = \
				component.getUtility(nti_interfaces.IDataserverTransactionRunner)
			func = functools.partial(_execute_job, job=job)
			transaction_runner(func)
		gevent.spawn(_executor)

@interface.implementer(async_interfaces.IJobReactor)
class JobReactor(object):

	stop = False
	inline = False

	def __init__(self, poll_inteval=5):
		self.poll_inteval = poll_inteval
		self.processor = self._spawn_job_processor()

	def halt(self):
		self.stop = True

	def _spawn_job_processor(self):
		def process():
			pid = os.getpid()
			while not self.stop:
				gevent.sleep(seconds=self.poll_inteval)
				if not self.stop:
					try:
						self.process_job()
					except component.ComponentLookupError:
						logger.error("process %s could not get component", pid)
						break

		result = gevent.spawn(process)
		return result

	def process_job(self):
		transaction_runner = \
				component.getUtility(nti_interfaces.IDataserverTransactionRunner)
		try:
			func = functools.partial(_pull_job, inline=self.inline)
			transaction_runner(func, retries=3)
		except Exception:
			logger.exception('Cannot process job messages')

