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

    _stop = None
    _paused = False

    site_names = ()
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

    def stop(self):
        self._stop = True
        notify(ReactorStopped(self))
    halt = stop

    def is_running(self):
        return self._stop is not None and not self._stop
    isRunning = is_running

    def start(self):
        self._stop = False
        self.generator.seed()
        notify(ReactorStarted(self))

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

    def perform_job(self, job):
        logger.debug("[%s] Executing job (%s)", self.current_queue, job)
        job.run()
        if job.has_failed():
            logger.error("[%s] Job %s failed", self.current_queue, job.id)
            self.current_queue.put_failed(job)
        logger.debug(
            "[%s] Job %s has been executed", self.current_queue, job.id)
        return True
    
    def execute_job(self, job=None):
        job = self._get_job() if job is None else job
        if job is None:
            return False
        return self.perform_job(job)
    executeJob = execute_job

    def transaction_runner(self):
        result = component.getUtility(ISiteTransactionRunner)
        if self.site_names:
            result = partial(result, site_names=self.site_names)
        return result

    def process_job(self):
        result = True
        try:
            if self.transaction_runner()(self.execute_job, retries=2, sleep=1):
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
            logger.exception('Cannot execute job.')
            result = not self.exitOnError
        return result
    processJob = process_job

    def run(self, sleep=gevent.sleep):
        self.start()
        try:
            logger.info('Starting reactor for queues=(%s)', self.queue_names)
            while self.is_running():
                try:
                    sleep(self.poll_interval)
                    if self.is_running():
                        if not self.is_paused() and not self.process_job():
                            break
                except KeyboardInterrupt:
                    break
        finally:
            self.stop()
            logger.warn('Exiting reactor. Queues=(%s)', self.queue_names)

    __call__ = run

    def pause(self):
        self._paused = True
    
    def resume(self):
        self._paused = False
        
    def is_paused(self):
        return self._paused

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

    def execute_job(self, job=None):
        count = 0
        # We do all jobs for a queue inside a single (hopefully manageable)
        # transaction.
        for job in self:
            logger.debug("[%s] Executing job (%s)", self.current_queue, job)
            job.run()
            if job.has_failed():
                logger.error("[%s] Job (%s) failed",
                             self.current_queue, job.id)
                self.current_queue.putFailed(job)
            else:
                count += 1
            logger.debug("[%s] Job (%s) has been executed",
                          self.current_queue, job.id)

        return count

    def process_job(self):
        for queue in self.queues:
            self.current_queue = queue
            count = self.transaction_runner()(
                self.execute_job, retries=2, sleep=1)
            logger.info(
                'Finished processing queue [%s] [count=%s]', queue._name, count)

    def run(self):
        try:
            logger.info('Starting reactor for failed jobs in queues=(%s)',
                        self.queue_names)
            self.process_job()
        finally:
            logger.warn('Exiting reactor. queues=(%s)', self.queue_names)
            self.processor = None

    __call__ = run
