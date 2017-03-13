#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import time
import zlib
import pickle
from io import BytesIO
from hashlib import sha1
from datetime import datetime

import transaction

from zope import interface

from nti.async.interfaces import IJob
from nti.async.interfaces import IRedisQueue

from nti.property.property import Lazy
from nti.property.property import alias

from nti.transactions import transactions

DEFAULT_QUEUE_NAME = 'nti/async/jobs'

try:
    from redis.exceptions import NoScriptError
except ImportError:
    NoScriptError = Exception


class QueueMixin(object):

    __parent__ = None
    __name__ = alias('_name')

    def __init__(self, redis, job_queue_name=None):
        self.__redis = redis
        self._name = job_queue_name or DEFAULT_QUEUE_NAME
        self._hash = self._name + '/hash'
        self._failed = self

    @Lazy
    def _redis(self):
        return self.__redis() if callable(self.__redis) else self.__redis

    def _pickle(self, job):
        bio = BytesIO()
        pickle.dump(job, bio)
        bio.seek(0)
        result = zlib.compress(bio.read())
        return result

    def _put_job(self, pipe, data, tail=True, jid=None):
        raise NotImplementedError()

    def put(self, item, use_transactions=True, tail=True):
        item = IJob(item)
        data = self._pickle(item)
        pipe = self._redis.pipeline()
        logger.debug('Placing job (%s) in [%s]', item.id, self._name)
        if use_transactions:
            # Only place the job once the transaction has been committed.
            transactions.do(target=self,
                            call=self._put_job,
                            args=(pipe, data, tail, item.id))
        else:
            self._put_job(pipe, data, tail, item.id)
        return item

    def _unpickle(self, data):
        data = zlib.decompress(data)
        bio = BytesIO(data)
        bio.seek(0)
        result = pickle.load(bio)
        assert IJob.providedBy(result)
        return result

    def put_failed(self, item):
        self._failed.put(item)
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
        return "%s(%s,%s)" % (self.__class__.__name__,
                              self._name,
                              self._failed._name)

    def keys(self):
        result = self._redis.hkeys(self._hash)
        return result or ()

    def __len__(self):
        result = self._redis.llen(self._name)
        return result or 0

    def __contains__(self, key):
        result = self._redis.hexists(self._hash, key)
        return bool(result)

    def __nonzero__(self):
        return bool(len(self))


@interface.implementer(IRedisQueue)
class RedisQueue(QueueMixin):

    _queue = _length = _failed_jobs = None

    def __init__(self, redis, job_queue_name=None, failed_queue_name=None,
                 create_failed_queue=True):
        super(RedisQueue, self).__init__(redis, job_queue_name)
        if create_failed_queue:
            failed_queue_name = failed_queue_name or self._name + "/failed"
            self._failed = RedisQueue(redis,
                                      job_queue_name=failed_queue_name,
                                      create_failed_queue=False)

    def _put_job(self, pipe, data, tail=True, jid=None):
        if tail:
            pipe.rpush(self._name, data)
        else:
            pipe.lpush(self._name, data)
        if jid is not None:
            pipe.hset(self._hash, jid, '1')
        pipe.execute()

    def all(self, unpickle=True):
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

    def claim(self, default=None):
        # once we get the job from redis, it's remove from it
        data = self._redis.lpop(self._name)
        if not data:
            return default

        job = self._unpickle(data)
        self._redis.hdel(self._hash, job.id)  # unset
        logger.debug("Job (%s) claimed", job.id)

        # make sure we put the job back if the transaction fails
        def after_commit_or_abort(success=False):
            if not success:
                logger.warn("Pushing job back onto queue on abort [%s] (%s)",
                            self._name, job.id)
                # We do not want to claim any jobs on transaction abort.
                # Add our job back to the front of the queue.
                pipe = self._redis.pipeline()
                self._put_job(pipe, data, tail=False, jid=job.id)
        transaction.get().addAfterCommitHook(after_commit_or_abort)
        return job

    def empty(self):
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
        data = self._redis.lindex(self._name, index)
        if data is None:
            raise IndexError(index)
        job = self._unpickle(data)
        return job

    def __delitem__(self, index):
        raise NotImplementedError()
Queue = RedisQueue  # alias


MAX_TIMESTAMP = time.mktime(datetime.max.timetuple())

CLAIM_SCRIPT = b"""
    local x = redis.call("zrevrange", KEYS[1], 0, 0)
    if x[1] == nil then
        return nil
    else
        redis.call("zrem", KEYS[1], x[1])
        return x[1]
    end
"""
CLAIM_SCRIPT_HASH = sha1(CLAIM_SCRIPT).hexdigest()


@interface.implementer(IRedisQueue)
class PriorityQueue(QueueMixin):

    def __init__(self, redis, job_queue_name=None,
                 failed_queue_name=None, create_failed_queue=True):
        super(PriorityQueue, self).__init__(redis, job_queue_name)
        if create_failed_queue:
            failed_queue_name = failed_queue_name or self._name + "/failed"
            self._failed = PriorityQueue(redis,
                                         job_queue_name=failed_queue_name,
                                         create_failed_queue=False)

    def _put_job(self, pipe, data, tail=True, jid=None, score=None):
        assert jid, 'must provide a job id'
        if score is None:
            if tail:
                score = MAX_TIMESTAMP - time.time()
            else:
                score = time.time()
        pipe.zadd(self._name, score, jid)
        pipe.hset(self._hash, jid, data)
        pipe.execute()

    def __claim(self, default=None):
        try:
            jid = self._redis.zrevrange(self._name, 0, 0)[0]
            while self._redis.zrem(self._name, jid) == 0:
                # Somebody else also got the same item and removed before us
                # Try again.
                jid = self._redis.zrevrange(self._name, 0, 0)[0]
            return jid
        except IndexError:
            # Queue is empty
            pass
        return default

    def _do_claim(self, default=None):
        try:
            result = self._redis.evalsha(CLAIM_SCRIPT_HASH, 1, self._name)
        except NoScriptError:
            result = self._redis.eval(CLAIM_SCRIPT, 1, self._name)
        except AttributeError:
            result = self.__claim(default)
        return default if result is None else result

    def claim(self, default=None):
        jid = self._do_claim(default)
        if jid is default:
            return default
        # We managed to pop the item from the queue. remove job
        data = self._redis.pipeline().hget(self._hash, jid) \
                          .hdel(self._hash, jid).execute()[0]
        job = self._unpickle(data)

        # make sure we put the job back if the transaction fails
        def after_commit_or_abort(success=False):
            if not success:
                logger.warn("Pushing job back onto queue on abort [%s] (%s)",
                            self._name, job.id)
                # We do not want to claim any jobs on transaction abort.
                # Add our job back to the front of the queue.
                pipe = self._redis.pipeline()
                self._put_job(pipe, data, score=MAX_TIMESTAMP, jid=jid)
        transaction.get().addAfterCommitHook(after_commit_or_abort)
        return job

    def empty(self):
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
        data = self._redis.hget(self._hash, key)
        if data is not None:
            return self._unpickle(data)
        raise KeyError(key)

    def __delitem__(self, key):
        if self._redis.zrem(self._name, key):
            self._redis.hdel(self._hash, key)

    def __iter__(self):
        return iter(self.all(True))
