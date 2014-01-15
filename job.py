#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import pytz
import types
import datetime

from zope import interface
from zope.container import contained as zcontained

from zc.blist import BList

from persistent import Persistent
from persistent.mapping import PersistentMapping

from .utils import custom_repr
from . import interfaces as async_interfaces

@interface.implementer(async_interfaces.IJob)
class Job(Persistent, zcontained.Contained):

	_callable_name = _callable_root = _result = None

	def __init__(self, *args, **kwargs):
		self.args = BList(args)
		self.callable = self.args.pop(0)
		self.kwargs = PersistentMapping(kwargs)

	@property
	def queue(self):
		return self.__parent__

	@property
	def result(self):
		return self._result

	def _get_callable(self):
		if self._callable_name is None:
			return self._callable_root
		else:
			return getattr(self._callable_root, self._callable_name)
	
	def _set_callable(self, value):
		if isinstance(value, types.MethodType):
			self._callable_root = value.im_self
			self._callable_name = value.__name__
		elif (isinstance(value, types.BuiltinMethodType) and
			  getattr(value, '__self__', None) is not None):
			self._callable_root = value.__self__
			self._callable_name = value.__name__
		else:
			self._callable_root, self._callable_name = value, None

		if (async_interfaces.IJob.providedBy(self._callable_root) and
			self._callable_root.__parent__ is None):
			self._callable_root.__parent__ = self

	callable = property (_get_callable, _set_callable)

	def __call__(self, *args, **kwargs):
		self._active_start = datetime.datetime.now(pytz.UTC)
		effective_args = list(args)
		effective_args[0:0] = self.args
		effective_kwargs = dict(self.kwargs)
		effective_kwargs.update(kwargs)
		__traceback_info__ = self._callable_root, self._callable_name, \
							 effective_args, effective_kwargs
		try:
			result = self.callable(*effective_args, **effective_kwargs)
			self._result = result
			return result
		except Exception, e:
			self._result = e
			logger.exception("Job execution failed")
		finally:
			self._active_end = datetime.datetime.now(pytz.UTC)
			
	def __repr__(self):
		try:
			call = custom_repr(self._callable_root)
			if self._callable_name is not None:
				call += ' :' + self._callable_name
			args = ', '.join(custom_repr(a) for a in self.args)
			kwargs = ', '.join(
				k + "=" + custom_repr(v)
				for k, v in self.kwargs.items())
			if args:
				if kwargs:
					args += ", " + kwargs
			else:
				args = kwargs
			return '<%s ``%s(%s)``>' % (custom_repr(self), call, args)
		except (TypeError, ValueError, AttributeError):
			# broken reprs are a bad idea; they obscure problems
			return super(Job, self).__repr__()
