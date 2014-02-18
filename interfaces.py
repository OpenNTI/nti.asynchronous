#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

from zope import interface
from zope.location import interfaces as loc_interfaces
from zope.annotation import interfaces as an_interfaces

NEW = u'New'
FAILED = u'Failed'
ACTIVE = u'Active'
COMPLETED = u'Completed'

class IQueue(an_interfaces.IAttributeAnnotatable, loc_interfaces.IContained):

	def put(item):
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

class IJob(an_interfaces.IAttributeAnnotatable, loc_interfaces.IContained):

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

class IJobReactor(interface.Interface):
	"""
	marker interface for a reactor
	"""
