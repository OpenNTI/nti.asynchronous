#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import print_function, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import time
import random
import logging
from threading import Thread
from functools import partial

import gevent

from zope import component
from zope import interface

from zope.cachedescriptors.property import Lazy
from zope.cachedescriptors.property import CachedProperty

from zope.component import ComponentLookupError

from zope.event import notify

from ZODB.POSException import ConflictError

from nti.async.interfaces import IQueue
from nti.async.interfaces import IAsyncReactor
from nti.async.interfaces import ReactorStarted
from nti.async.interfaces import ReactorStopped

from nti.site.interfaces import ISiteTransactionRunner

from nti.zodb.interfaces import UnableToAcquireCommitLock
from nti.zodb.interfaces import ZODBUnableToAcquireCommitLock

DEFAULT_TRX_SLEEP = 1
DEFAULT_TRX_RETRIES = 3

DEFAULT_MAX_UNIFORM = 5
DEFAULT_MAX_SLEEP_TIME = 60


class RunnerMixin(object):

    site_names = ()
    current_job = None
    current_queue = None

    trx_sleep = DEFAULT_TRX_SLEEP
    trx_retries = DEFAULT_TRX_RETRIES

    max_sleep_time = DEFAULT_MAX_SLEEP_TIME
    max_range_uniform = DEFAULT_MAX_UNIFORM

    def __init__(self, site_names=(),
                 max_sleep_time=None,
                 max_range_uniform=None,
                 trx_sleep=None,
                 trx_retries=None):
        self.generator = random.Random()
        self.site_names = site_names or ()
        # check db transaction runner params
        if trx_sleep is not None:
            self.trx_sleep = trx_sleep
        if trx_retries is not None:
            self.trx_retries = trx_retries
        assert self.trx_retries > 0
        assert self.trx_retries > 0
        # check reactor params
        if max_sleep_time is not None:
            self.max_sleep_time = max_sleep_time
        if max_range_uniform is not None:
            self.max_range_uniform = max_range_uniform
        assert self.max_sleep_time > 0
        assert self.max_range_uniform > 0

    def uniform(self):
        return self.generator.uniform(1, self.max_range_uniform)

    def perform_job(self, job, queue=None):
        queue = self.current_queue if queue is None else queue
        logger.debug("[%s] Executing job (%s)", queue, job)
        job.run()
        if job.has_failed():
            logger.error("[%s] Job %s failed", queue, job.id)
            queue.put_failed(job)
        logger.info("[%s] Job %s has been executed (%s).",
                     queue, job.id, job.status)
        return True

    @Lazy
    def transaction_runner(self):
        result = component.getUtility(ISiteTransactionRunner)
        if self.site_names:
            result = partial(result, site_names=self.site_names)
        return result


class ReactorMixin(object):

    _stop = None
    _paused = False

    def start(self):
        self._stop = False

    def stop(self):
        self._stop = True
    halt = stop

    def is_running(self):
        return self._stop is not None and not self._stop
    isRunning = is_running

    def pause(self):
        self._paused = True

    def resume(self):
        self._paused = False

    def is_paused(self):
        return self._paused


class QueuesMixin(object):

    def __init__(self, queue_names=(), queue_interface=IQueue):
        self.queue_interface = queue_interface
        self.queue_names = list(queue_names or ())

    @CachedProperty('queue_names')
    def queues(self):
        queues = tuple(component.getUtility(self.queue_interface, name=x)
                       for x in set(self.queue_names))
        return queues

    def add_queues(self, *queues):
        registered = []
        for x in queues:
            if      x not in self.queue_names \
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


@interface.implementer(IAsyncReactor)
class AsyncReactor(RunnerMixin, ReactorMixin, QueuesMixin):

    processor = None
    poll_interval = None

    def __init__(self, queue_names=(), poll_interval=2, exitOnError=True,
                 queue_interface=IQueue, site_names=(), **kwargs):
        RunnerMixin.__init__(self, site_names, **kwargs)
        QueuesMixin.__init__(self, queue_names, queue_interface)
        self.exitOnError = exitOnError
        self.poll_interval = poll_interval

    def start(self):
        super(AsyncReactor, self).start()
        self.generator.seed()
        notify(ReactorStarted(self))

    def stop(self):
        super(AsyncReactor, self).stop()
        notify(ReactorStopped(self))
    halt = stop

    def _get_job(self):
        # if current_job is active then it means
        # we are in a retry from the transaction runner
        if self.current_job is not None:
            logger.warn("Retrying %s", self.current_job.id)
            return self.current_job
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

    def execute_job(self, job=None):
        job = self._get_job() if job is None else job
        if job is None:
            return False
        return self.perform_job(job)
    executeJob = execute_job

    def process_job(self):
        result = True
        try:
            # XXX: Should we pull jobs outside of transaction to
            # avoid race conditions?
            if self.transaction_runner(self.execute_job,
                                       sleep=self.trx_sleep,
                                       retries=self.trx_retries):
                # Do not sleep if we have work to do, especially since
                # we may be reading from multiple queues.
                self.poll_interval = 0
            else:
                self.poll_interval += self.uniform()
                self.poll_interval = min(self.poll_interval,
                                         self.max_sleep_time)
        except (ComponentLookupError, AttributeError, TypeError, StandardError) as e:
            logger.error('Error while processing job. Queue=[%s], error=%s',
                         self.current_queue, e)
            result = False
        except (ConflictError,
                UnableToAcquireCommitLock,
                ZODBUnableToAcquireCommitLock) as e:
            logger.error('ConflictError while pulling job from Queue=[%s], error=%s',
                         self.current_queue, e)
        except:
            logger.exception('Cannot execute job.')
            result = not self.exitOnError
        finally:
            # XXX: Signal we need to get a new job
            self.current_queue = self.current_job = None
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


class AsyncFailedReactor(AsyncReactor):
    """
    Knows how to process jobs off of the failure queue.  We'll try
    each job once and then return.
    """

    current_jobs = None

    def __init__(self, queue_names=(), queue_interface=IQueue,
                 site_names=(), **kwargs):
        RunnerMixin.__init__(self, site_names, **kwargs)
        QueuesMixin.__init__(self, queue_names, queue_interface)

    @CachedProperty('queue_names')
    def queues(self):
        queues = [component.getUtility(self.queue_interface, name=x).get_failed_queue()
                  for x in set(self.queue_names)]
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
        # if current jobs has anythig it means we are in a
        # retry from the transaction manager
        if self.current_jobs:
            jobs_to_process = self.current_jobs
        else:
            jobs_to_process = list(self)
        count = 0
        # We do all jobs for a queue inside a single (hopefully manageable)
        # transaction.
        for job in jobs_to_process:
            self.current_job = job
            logger.debug("[%s] Executing job (%s)",
                         self.current_queue, job)
            job.run()
            if job.has_failed():
                logger.error("[%s] Job (%s) failed",
                             self.current_queue,
                             job.id)
                self.current_queue.putFailed(job)
            else:
                count += 1
            logger.debug("[%s] Job (%s) has been executed",
                         self.current_queue, job.id)
            self.current_job = None

        return count

    def process_job(self):
        for queue in self.queues:
            try:
                self.current_queue = queue  # set proper queue
                count = self.transaction_runner(self.execute_job,
                                                retries=3,
                                                sleep=1)
                logger.info('Finished processing queue [%s] [count=%s]',
                            queue._name, count)
            finally:
                # XXX: Signal jobs for next que need to be processed
                self.current_job = self.current_queue = self.current_jobs = None

    def run(self):
        self.start()
        try:
            logger.info('Starting reactor for failed jobs in queues=(%s)',
                        set(self.queue_names))
            self.process_job()
        finally:
            self.stop()
            logger.warn('Exiting reactor. queues=(%s)', 
                        set(self.queue_names))
            self.processor = None
    __call__ = run


class SingleQueueReactor(RunnerMixin, ReactorMixin):

    def __init__(self, queue_name, queue_interface=IQueue,
                 site_names=(), poll_interval=3, **kwargs):
        RunnerMixin.__init__(self, site_names, **kwargs)
        self.queue_name = queue_name
        self.poll_interval = poll_interval
        self.queue_interface = queue_interface

    @Lazy
    def queue(self):
        return component.getUtility(self.queue_interface, name=self.queue_name)
    current_queue = queue

    def execute_job(self):
        # if current_job is active then it means
        # we are in a retry from the transaction runner
        if self.current_job is not None:
            return self.current_job
        job = self.queue.claim()
        if job is not None:
            return self.perform_job(job, self.queue)
        return False

    def run(self, sleep=time.sleep):
        self.start()
        try:
            sleep_time = self.poll_interval
            logger.info('Starting reactor queue=(%s)', self.queue_name)
            while self.is_running():
                try:
                    try:
                        runner = self.transaction_runner
                        if not runner(self.execute_job,
                                      sleep=self.trx_sleep,
                                      retries=self.trx_retries):
                            # sleep some random time
                            sleep_time += self.uniform()
                            sleep_time = min(sleep_time,
                                             self.max_sleep_time)
                            sleep(sleep_time)
                        else:
                            sleep_time = self.poll_interval
                    except (ComponentLookupError,
                            AttributeError,
                            TypeError,
                            StandardError) as e:
                        logger.error('Error while processing job. Queue=[%s], error=%s',
                                     self.queue, e)
                    except (ConflictError,
                            UnableToAcquireCommitLock,
                            ZODBUnableToAcquireCommitLock) as e:
                        logger.error('ConflictError while pulling job from Queue=[%s], error=%s',
                                     self.queue, e)
                    except:
                        logger.exception('Cannot execute job.')
                    finally:
                        # XXX: Signal we need to get a new job
                        self.current_job = None
                except KeyboardInterrupt:
                    break
        finally:
            self.stop()
            logger.info('Exiting reactor. queue=(%s)', self.queue_name)
    __call__ = run


@interface.implementer(IAsyncReactor)
class ThreadedReactor(RunnerMixin, ReactorMixin, QueuesMixin):

    def __init__(self, queue_names=(), queue_interface=IQueue,
                 site_names=(), poll_interval=3, **kwargs):
        RunnerMixin.__init__(self, site_names, **kwargs)
        QueuesMixin.__init__(self, queue_names, queue_interface)
        self.poll_interval = poll_interval

    def start(self):
        super(ThreadedReactor, self).start()
        notify(ReactorStarted(self))

    def stop(self):
        super(ThreadedReactor, self).stop()
        notify(ReactorStopped(self))
    halt = stop

    def run(self, sleep=time.sleep):
        self.start()
        threads = []
        try:
            logger.info('Starting threaded reactor queues=(%s)',
                        set(self.queue_names))
            # create threads
            for name in self.queue_names:
                target = SingleQueueReactor(name,
                                            self.queue_interface,
                                            self.site_names,
                                            self.poll_interval,
                                            trx_sleep=self.trx_sleep,
                                            trx_retries=self.trx_retries,
                                            max_sleep_time=self.max_sleep_time,
                                            max_range_uniform=self.max_range_uniform)
                target.__parent__ = self
                thread = Thread(target=target, name=name)
                thread.daemon = True
                thread.start()
                threads.append(thread)
            # wait till signal
            while self.is_running():
                try:
                    sleep(0.2)
                except KeyboardInterrupt:
                    break
        finally:
            self.stop()
            logger.warn('Exiting reactor. queue=(%s)', 
                        set(self.queue_names))
    __call__ = run

# Reduce verbosity of activity logger
activity_logger = __import__('logging').getLogger("nti.zodb.activitylog")
activity_logger.setLevel(logging.ERROR)
