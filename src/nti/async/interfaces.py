#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from zope import interface

from zope.annotation.interfaces import IAttributeAnnotatable

from zope.interface.interfaces import ObjectEvent
from zope.interface.interfaces import IObjectEvent

from zope.location.interfaces import IContained

#: New job code
NEW = u'New'

#: Failed job code
FAILED = u'Failed'

#: Active Job code
ACTIVE = u'Active'

#: Completed job code
COMPLETED = u'Completed'


class IException(interface.Interface):
    """
    marker interface for Exception objects
    """


class IError(interface.Interface):
    message = interface.Attribute("Error message")


class IBaseQueue(IContained):

    def put(item, *kwargs):
        """
        Put an IJob adapted from item into the queue.  Returns IJob.
        """

    def all():
        """
        Return all elements in this queue
        """

    def empty():
        """
        Empty all items in this queue
        """

    def remove(item):
        """
        Removes item from queue or raises LookupError if not found.
        """

    def claim():
        """
        Returns first due job that is available
        removing it from the queue as appropriate; or None, if none are
        available.
        """

    def put_failed(item):
        """
        Stores a failed job for review
        """
    putFailed = put_failed

    def failed():
        """
        Return all failed jobs in this queue
        """

    def keys():
        """
        return all keys in this queue
        """

    def __contains__(key):
        """
        Check if the specified key is in this queue
        """


class IQueue(IBaseQueue, IAttributeAnnotatable):
    pass


class IRedisQueue(IBaseQueue):

    def put(item, use_transactions=True, tail=True):
        pass

    def all(unpickle=True):
        pass

    def failed(unpickle=True):
        pass


class IJob(IAttributeAnnotatable, IContained):

    id = interface.Attribute("job identifier.")

    error = interface.Attribute("Any job execution error.")

    result = interface.Attribute("The result of the call.")

    callable = interface.Attribute(
        """The callable object that should be called with *IJob.args and
        **IJob.kwargs when the IJob is called.  Mutable.""")

    args = interface.Attribute(
        """a peristent list of the args that should be applied to self.call.
        May include persistent objects (though note that, if passing a method
        is desired, it will typicall need to be wrapped in an IJob).""")

    kwargs = interface.Attribute(
        """a persistent mapping of the kwargs that should be applied to
        self.call.  May include persistent objects (though note that, if
        passing a method is desired, it will typicall need to be wrapped
        in an IJob).""")

    is_side_effect_free = interface.Attribute(
        """the job does not change the underlying storage""")

    def __call__(*args, **kwargs):
        """
        call the callable.  Any given args are effectively appended to
        self.args for the call, and any kwargs effectively update self.kwargs
        for the call.
        """
    run = __call__

    def is_new():
        """
        Check if the job is new
        """
    isNew = is_new

    def has_completed():
        """
        Check if the job has completed
        """
    isSuccess = is_success = has_completed

    def is_running():
        """
        Check if the job is running
        """
    isRunning = is_running

    def has_failed():
        """
        check if job has failed
        """
    hasFailed = has_failed


class IAsyncReactor(interface.Interface):
    """
    marker interface for a reactor
    """

    queue_names = interface.Attribute("Queue names.")

    def stop():
        """
        Stop the processor
        """

    def pause():
        """
        Pause this processor
        """

    def resume():
        """
        Resume this processor
        """

    def is_running():
        """
        Return if this processor is running
        """

    def is_paused():
        """
        Return if this processor has been paused
        """


class IReactorEvent(IObjectEvent):
    pass


class IReactorStarted(IReactorEvent):
    pass


class IReactorStopped(IReactorEvent):
    pass


@interface.implementer(IReactorEvent)
class ReactorEvent(ObjectEvent):

    @property
    def reactor(self):
        return self.object


@interface.implementer(IReactorStarted)
class ReactorStarted(ReactorEvent):
    pass


@interface.implementer(IReactorStopped)
class ReactorStopped(ReactorEvent):
    pass


class IJobEvent(IObjectEvent):
    job = interface.Attribute("Job")


class IJobAbortedEvent(IJobEvent):
    pass


@interface.implementer(IJobAbortedEvent)
class JobAbortedEvent(ObjectEvent):

    @property
    def job(self):
        return self.object
