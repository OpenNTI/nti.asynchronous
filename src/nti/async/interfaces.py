#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

from zope import interface

from zope.annotation.interfaces import IAttributeAnnotatable

from zope.interface.interfaces import ObjectEvent
from zope.interface.interfaces import IObjectEvent

from zope.location.interfaces import IContained

NEW = u'New'
FAILED = u'Failed'
ACTIVE = u'Active'
COMPLETED = u'Completed'

class IError(interface.Interface):
	message = interface.Attribute("""Error message""")

class IBaseQueue(IContained):

	def put(item, *kwargs):
		"""
		Put an IJob adapted from item into the queue.  Returns IJob.
		"""

	def pull(index=0):
		"""
		Remove and return a job, by default from the front of the queue.

		Raise IndexError if index does not exist.
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

	def putFailed(item):
		"""
		Stores a failed job for review
		"""

class IQueue(IBaseQueue, IAttributeAnnotatable):
	pass

class IRedisQueue(IBaseQueue):

	def put(item, use_transactions=True, tail=True):
		pass

class IJob(IAttributeAnnotatable, IContained):

	id = interface.Attribute("""job identifier.""")

	error = interface.Attribute("""Any job execution error.""")

	result = interface.Attribute("""The result of the call. """)

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

	def __call__(*args, **kwargs):
		"""
		call the callable.  Any given args are effectively appended to
		self.args for the call, and any kwargs effectively update self.kwargs
		for the call.
		"""

class IAsyncReactor(interface.Interface):
	"""
	marker interface for a reactor
	"""

	queue_names = interface.Attribute("""Queue names.""")

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
