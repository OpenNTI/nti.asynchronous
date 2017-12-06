#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import time
import zlib
from io import BytesIO
from hashlib import sha1
from datetime import datetime
from six.moves import cPickle as pickle

from redis.exceptions import NoScriptError  # pylint: disable=import-error,no-name-in-module

import transaction

from zope import interface

from zope.cachedescriptors.property import Lazy

from zope.event import notify

from nti.asynchronous.interfaces import IJob
from nti.asynchronous.interfaces import IRedisQueue
from nti.asynchronous.interfaces import JobAbortedEvent

from nti.property.property import alias

from nti.transactions import transactions

DEFAULT_QUEUE_NAME = u'nti/async/jobs'

USE_LUA = False
LONG_PUSH_DURATION_IN_SECS = 5

logger = __import__('logging').getLogger(__name__)


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

    def _put_job(self, *args, **kwargs):
        t0 = time.time()
        self._do_put_job(*args, **kwargs)
        duration = time.time() - t0
        if duration > LONG_PUSH_DURATION_IN_SECS:
            logger.warn("Slow running redis push (%ss)", duration)

    def _do_put_job(self, pipe, data, tail=True, jid=None):
        raise NotImplementedError()

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

    def _unpickle(self, data):
        data = zlib.decompress(data)
        bio = BytesIO(data)
        bio.seek(0)
        result = pickle.load(bio)
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
        # pylint: disable=no-member
        result = self._redis.hlen(self._hash)
        return result or 0

    def __contains__(self, key):
        # pylint: disable=no-member
        result = self._redis.hexists(self._hash, key)
        return bool(result)

    def __nonzero__(self):
        return bool(len(self))


TAIL_PUT_SCRIPT = b"""
    redis.call("rpush", KEYS[1], ARGV[1])
    redis.call("hset", KEYS[2], ARGV[2], '1')
"""
TAIL_PUT_SCRIPT_HASH = sha1(TAIL_PUT_SCRIPT).hexdigest()

HEAD_PUT_SCRIPT = b"""
    redis.call("lpush", KEYS[1], ARGV[1])
    redis.call("hset", KEYS[2], ARGV[2], '1')
"""
HEAD_PUT_SCRIPT_HASH = sha1(HEAD_PUT_SCRIPT).hexdigest()

LPOP_SCRIPT = b"""
    return redis.call("lpop", KEYS[1])
"""
LPOP_SCRIPT_HASH = sha1(LPOP_SCRIPT).hexdigest()


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

    def _do_put_job_lua(self, data, tail, jid):
        # pylint: disable=no-member
        try:
            script = TAIL_PUT_SCRIPT if tail else HEAD_PUT_SCRIPT
            hash_script = TAIL_PUT_SCRIPT_HASH if tail else HEAD_PUT_SCRIPT_HASH
            self._redis.evalsha(hash_script, 2, self._name,
                                self._hash, data, jid)
        except NoScriptError:
            logger.warn("script not cached.")
            self._redis.eval(script, 2, self._name, self._hash, data, jid)

    def _do_put_job_pipe(self, pipe, data, tail, jid):
        if tail:
            pipe.rpush(self._name, data)
        else:
            pipe.lpush(self._name, data)
        if jid is not None:
            pipe.hset(self._hash, jid, '1')
        pipe.execute()

    def _do_put_job(self, pipe, data, tail=True, jid=None):
        if USE_LUA:
            try:
                self._do_put_job_lua(data, tail, jid)
            except AttributeError:
                self._do_put_job_pipe(pipe, data, tail, jid)
        else:
            self._do_put_job_pipe(pipe, data, tail, jid)

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

    def _do_claim_lua(self):
        # pylint: disable=no-member
        try:
            result = self._redis.evalsha(LPOP_SCRIPT_HASH, 1, self._name)
        except NoScriptError:
            result = self._redis.eval(LPOP_SCRIPT, 1, self._name)
        return result

    def _do_claim_client(self):
        # pylint: disable=no-member
        result = self._redis.lpop(self._name)
        return result

    def _do_claim(self):
        if USE_LUA:
            try:
                return self._do_claim_lua()
            except AttributeError:
                return self._do_claim_client()
        else:
            return self._do_claim_client()

    def claim(self, default=None):
        # pylint: disable=no-member
        # once we get the job from redis, it's remove from it
        data = self._do_claim()
        if data is None:
            return default

        job = self._unpickle(data)
        self._redis.hdel(self._hash, job.id)  # unset
        logger.debug("Job (%s) claimed", job.id)

        # notify if the transaction aborts
        def after_commit_or_abort(success=False):
            if not success and not job.is_side_effect_free:
                logger.warn("Transaction abort for [%s] (%s)",
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

MAX_TIMESTAMP = time.mktime(datetime.max.timetuple())


@interface.implementer(IRedisQueue)
class PriorityQueue(QueueMixin):

    def __init__(self, redis, job_queue_name=None,
                 failed_queue_name=None, create_failed_queue=True):
        super(PriorityQueue, self).__init__(redis, job_queue_name)
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
        pipe.zadd(self._name, score, jid)
        pipe.hset(self._hash, jid, data)
        pipe.execute()

    def _do_claim_lua(self):
        # pylint: disable=no-member
        try:
            result = self._redis.evalsha(CLAIM_SCRIPT_HASH, 1, self._name)
        except NoScriptError:
            result = self._redis.eval(CLAIM_SCRIPT, 1, self._name)
        return result

    def _do_claim_client(self):
        # pylint: disable=no-member
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

    def _do_claim(self):
        if USE_LUA:
            try:
                return self._do_claim_lua()
            except AttributeError:
                return self._do_claim_client()
        else:
            return self._do_claim_client()

    def claim(self, default=None):
        jid = self._do_claim()
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
                logger.warn("Transaction abort for [%s] (%s)",
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
