#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import time

from zope import interface

from nti.asynchronous.redis_queue import ScoredQueueMixin

from nti.asynchronous.interfaces import IRedisQueue

from nti.asynchronous.scheduled.interfaces import IScheduledJob

from nti.transactions import transactions

logger = __import__('logging').getLogger(__name__)


@interface.implementer(IRedisQueue)
class ScheduledQueue(ScoredQueueMixin):

    def put(self, item, use_transactions=True, tail=True):
        item = IScheduledJob(item, None)
        if item is None:
            # only store scheduled jobs.
            return None

        data = self._pickle(item)
        # pylint: disable=no-member
        pipe = self._redis.pipeline()
        logger.debug('Placing job (%s) in [%s]', item.id, self._name)
        if use_transactions:
            # We must attempt to execute after the rest of the transaction in
            # case our job is picked up before the database state is updated.
            transactions.do_near_end(target=self,
                                     call=self._put_job,
                                     args=(pipe, data, tail, item.id, item.score))
        else:
            self._put_job(pipe, data, tail, item.id, item.score)
        return item

    def _do_claim_client(self):
        # pylint: disable=no-member
        try:
            max_score = time.time()
            jid = self._redis.zrevrangebyscore(self._name, max_score, 0, 0, 1)[0]
            while self._redis.zrem(self._name, jid) == 0:
                # Somebody else also got the same item and removed before us
                # Try again.
                max_score = time.time()
                jid = self._redis.zrevrangebyscore(self._name, max_score, 0, 0, 1)[0]  # pragma: no cover
            return jid
        except IndexError:
            # Queue is empty
            pass
