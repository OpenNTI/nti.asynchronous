#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import functools

import time

import zlib

from ZODB._compat import HIGHEST_PROTOCOL

from ZODB._compat import dumps
from ZODB._compat import loads

from datetime import datetime

from perfmetrics import statsd_client
from perfmetrics import Metric

import transaction

from zope import interface

from zope.cachedescriptors.property import Lazy

from zope.event import notify

from nti.asynchronous.interfaces import IJob
from nti.asynchronous.interfaces import IRedisQueue
from nti.asynchronous.interfaces import JobAbortedEvent

from nti.property.property import alias

from nti.transactions import transactions

DEFAULT_QUEUE_NAME = 'nti/async/jobs'

LONG_PUSH_DURATION_IN_SECS = 1

logger = __import__('logging').getLogger(__name__)


class QueueMixin(object):

    __parent__ = None
    __name__ = alias('_name')

    def __init__(self, redis, job_queue_name=None):
        self.__redis = redis
        self._name = job_queue_name or DEFAULT_QUEUE_NAME
        self._hash = self._name + '/hash'
        self._failed = self

    @property
    def __name__(self):
        return unicode(self._name)

    @Lazy
    def _put_metric_name(self):
        return 'ntiasync.' + self._name + '.put'

    @Lazy
    def _queue_length_metric_name(self):
        return 'ntiasync.' + self._name + '.length'

    @Lazy
    def _claim_metric_name(self):
        return 'ntiasync.' + self._name + '.claim'

    @Lazy
    def _redis(self):
        return self.__redis() if callable(self.__redis) else self.__redis

    def _pickle(self, job):
        return zlib.compress(dumps(job, HIGHEST_PROTOCOL))

    def _update_length_stat(self, size=None):
        client = statsd_client()
        if client is not None:
            size = len(self) if size is None else size
            client.gauge(self._queue_length_metric_name, size)

    def _do_put_job(self, pipe, data, tail=True, jid=None):
        """Implementations should implement this method to push the data to
        the provided redis pipe. If efficient, this function may
        choose to return the length of the queue after adding the data.
        """
        raise NotImplementedError()

    def _put_job(self, *args, **kwargs):
        _do_put = functools.partial(self._do_put_job, *args, **kwargs)
        queue_size, duration = _timing(_do_put, self._put_metric_name)
        self._update_length_stat(queue_size)
        if duration > LONG_PUSH_DURATION_IN_SECS:
            logger.warning("Slow running redis push (%s)", duration)

    def put(self, item, use_transactions=True, tail=True):
        item = IJob(item)
        data = self._pickle(item)
        # pylint: disable=no-member
        pipe = self._redis.pipeline()
        logger.debug('Placing job (%s) in [%s]', item.id, self._name)
        if use_transactions:
            # We must attempt to execute after the rest of the transaction in
            # case our job is picked up before the database state is updated.
            transactions.do_near_end(target=self,
                                     call=self._put_job,
                                     args=(pipe, data, tail, item.id))
        else:
            self._put_job(pipe, data, tail, item.id)
        return item

    def _do_claim(self):
        """
        Implementations should implement this to claim data.
        """
        raise NotImplementedError()

    def claim(self, default=None):
        try:
            with Metric(self._claim_metric_name):
                return self._do_claim(default=default)
        finally:
            self._update_length_stat()

    def _unpickle(self, data):
        data = zlib.decompress(data)
        result = loads(data)
        assert IJob.providedBy(result)
        return result

    def put_failed(self, item, *args, **kwargs):
        self._failed.put(item, *args, **kwargs)
    putFailed = put_failed

    def get_failed_queue(self):
        return self._failed

    def failed(self, unpickle=True):
        if self._failed is not self:
            return self._failed.all(unpickle)
        return ()

    def __str__(self):
        return self._name

    def __repr__(self):
        # pylint: disable=protected-access
        return "%s(%s,%s)" % (self.__class__.__name__,
                              self._name,
                              self._failed._name)

    def keys(self):
        # pylint: disable=no-member
        result = self._redis.hkeys(self._hash)
        return result or ()

    def __len__(self):
        """
        Length of the list.
        """
        # pylint: disable=no-member
        result = self._redis.llen(self._name)
        return result or 0

    def __contains__(self, key):
        # pylint: disable=no-member
        result = self._redis.hexists(self._hash, key)
        return bool(result)

    def __nonzero__(self):
        return bool(len(self))
    __bool__ = __nonzero__


@interface.implementer(IRedisQueue)
class RedisQueue(QueueMixin):

    _queue = _length = _failed_jobs = None

    def __init__(self, redis, job_queue_name=None, failed_queue_name=None,
                 create_failed_queue=True, dedupe=True):
        super(RedisQueue, self).__init__(redis, job_queue_name)
        self.dedupe = dedupe
        if create_failed_queue:
            failed_queue_name = failed_queue_name or self._name + "/failed"
            self._failed = RedisQueue(redis,
                                      job_queue_name=failed_queue_name,
                                      create_failed_queue=False)

    def _do_put_job_pipe(self, pipe, data, tail, jid):
        length_response_idx = 1
        if tail:
            pipe.rpush(self._name, data)
        else:
            pipe.lpush(self._name, data)
        if jid is not None:
            length_response_idx += 1
            pipe.hset(self._hash, jid, '1')
        results = pipe.execute()
        return results[-1*length_response_idx]

    def _do_put_job(self, pipe, data, tail=True, jid=None):
        # Check for dedupe before putting job
        # The last clause checks if jid is in `_hash`.
        # This should be transactionally safe: we should put at the end of a
        # tx. Any job that is already in the queue would not have been
        # popped yet.
        if     self.dedupe \
            and (jid is None or jid not in self):
            return self._do_put_job_pipe(pipe, data, tail, jid)

    def all(self, unpickle=True):
        # pylint: disable=no-member
        data = self._redis.lrange(self._name, 0, -1)
        if unpickle:
            result = [self._unpickle(x) for x in data or ()]
        else:
            result = data or ()
        return result
    values = all

    def remove(self, item):
        # jobs are pickled
        raise NotImplementedError()

    def _do_claim_client(self):
        # pylint: disable=no-member
        result = self._redis.lpop(self._name)
        return result

    def _do_claim(self, default=None):
        # pylint: disable=no-member
        # once we get the job from redis, it's remove from it
        data = self._do_claim_client()
        if data is None:
            return default

        job = self._unpickle(data)
        self._redis.hdel(self._hash, job.id)  # unset
        logger.debug("Job (%s) claimed", job.id)

        # notify if the transaction aborts
        def after_commit_or_abort(success=False):
            if not success and not job.is_side_effect_free:
                logger.warning("Transaction abort for [%s] (%s)",
                           self._name, job.id)
                notify(JobAbortedEvent(job))
                transaction.get().addAfterCommitHook(after_commit_or_abort)
        return job

    def empty(self):
        # pylint: disable=no-member
        keys = self._redis.pipeline().delete(self._name) \
                          .hkeys(self._hash).execute()
        if keys and keys[1]:
            self._redis.hdel(self._hash, *keys[1])
            return keys[1]
        return ()
    reset = empty

    def __iter__(self):
        return iter(self.all(True))

    def __getitem__(self, index):
        # pylint: disable=no-member
        data = self._redis.lindex(self._name, index)
        if data is None:
            raise IndexError(index)
        job = self._unpickle(data)
        return job

    def __delitem__(self, index):
        raise NotImplementedError()


Queue = RedisQueue  # alias

MAX_TIMESTAMP = time.mktime(datetime.max.timetuple())


class ScoredQueueMixin(QueueMixin):

    def __init__(self, redis, job_queue_name=None,
                 failed_queue_name=None, create_failed_queue=True):
        super(ScoredQueueMixin, self).__init__(redis, job_queue_name)
        if create_failed_queue:
            failed_queue_name = failed_queue_name or self._name + "/failed"
            self._failed = RedisQueue(redis,
                                      job_queue_name=failed_queue_name,
                                      create_failed_queue=False)

    # pylint: disable=arguments-differ
    def _do_put_job(self, pipe, data, tail=True, jid=None, score=None):
        assert jid, 'must provide a job id'
        if score is None:
            if tail:
                score = MAX_TIMESTAMP - time.time()
            else:
                score = time.time()
        pipe.zadd(self._name, {jid: score})
        pipe.hset(self._hash, jid, data)
        pipe.hlen(self._hash)
        results = pipe.execute()
        return results[-1]

    def _do_claim_client(self):
        raise NotImplementedError()

    def _do_claim(self, default=None):
        jid = self._do_claim_client()
        if jid is None:
            return default
        # pylint: disable=no-member
        # We managed to pop the item from the queue. remove job
        data = self._redis.pipeline().hget(self._hash, jid) \
                          .hdel(self._hash, jid).execute()[0]
        job = self._unpickle(data)

        # make sure we put the job back if the transaction fails
        def after_commit_or_abort(success=False):
            if not success and not job.is_side_effect_free:
                logger.warning("Transaction abort for [%s] (%s)",
                               self._name, job.id)
                notify(JobAbortedEvent(job))
        transaction.get().addAfterCommitHook(after_commit_or_abort)
        return job

    def empty(self):
        # pylint: disable=no-member
        keys = self._redis.pipeline().zremrangebyscore(self._name, 0, MAX_TIMESTAMP) \
                          .hkeys(self._hash).execute()
        if keys and keys[1]:
            self._redis.hdel(self._hash, *keys[1])
            return keys[1]
        return ()
    reset = empty

    def remove(self, item):
        if IJob.providedBy(item):
            item = item.id
        del self[item]

    def all(self, unpickle=True):
        # pylint: disable=no-member
        all_jobs = self._redis.hgetall(self._hash) or {}
        if not unpickle:
            result = [x for x in all_jobs.values() if x is not None]
        else:
            result = []
            for data in all_jobs.values():
                if data is not None:
                    result.append(self._unpickle(data))
        return result or ()
    values = all

    def __getitem__(self, key):
        # pylint: disable=no-member
        data = self._redis.hget(self._hash, key)
        if data is not None:
            return self._unpickle(data)
        raise KeyError(key)

    def __delitem__(self, key):
        # pylint: disable=no-member
        if self._redis.zrem(self._name, key):
            self._redis.hdel(self._hash, key)

    def __iter__(self):
        return iter(self.all(True))

    def __len__(self):
        """
        Length of the set.
        """
        # pylint: disable=no-member
        result = self._redis.hlen(self._hash)
        return result or 0


@interface.implementer(IRedisQueue)
class PriorityQueue(ScoredQueueMixin):

    def _do_claim_client(self):
        # pylint: disable=no-member
        try:
            jid = self._redis.zrevrange(self._name, 0, 0)[0]
            while self._redis.zrem(self._name, jid) == 0:
                # Somebody else also got the same item and removed before us
                # Try again.
                jid = self._redis.zrevrange(self._name, 0, 0)[0]  # pragma: no cover
            return jid
        except IndexError:
            # Queue is empty
            pass


def _timing(operation, name):
    """
    Run the `operation` callable returning a tuple
    of the return value of the operation, and the timing information
    """
    now = time.time()
    with Metric(name):
        result = operation()
    done = time.time()
    return (result, (done - now),)
