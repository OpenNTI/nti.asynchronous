#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import sys
import uuid
import types
import datetime
from collections import Mapping

from zope import interface

from zope.container.contained import Contained

from nti.common.property import alias
from nti.common.representation import WithRepr

from nti.schema.schema import EqHash

from .interfaces import NEW
from .interfaces import ACTIVE
from .interfaces import FAILED
from .interfaces import COMPLETED

from .interfaces import IJob
from .interfaces import IError

NEW_ID = 0
ACTIVE_ID = 1
FAILED_ID = 3
COMPLETED_ID = 2

_status_mapping = {
	NEW_ID : NEW,
	ACTIVE_ID: ACTIVE,
	FAILED_ID: FAILED,
	COMPLETED_ID: COMPLETED}

@WithRepr
@interface.implementer(IJob)
class Job(Contained):

	_id = None
	_error = _active_start = _active_end = None
	_status_id = _callable_name = _callable_root = _result = None

	error_adapter = IError

	id = alias('_id')

	def __init__(self, *args, **kwargs):
		self._status_id = NEW_ID
		self._reset(*args, **kwargs)

	def _reset(self, *args, **kwargs):
		_tmpargs = list(args)
		self.kwargs = dict(kwargs)
		self.callable = _tmpargs.pop(0)
		self.args = tuple(_tmpargs)

	@property
	def queue(self):
		return self.__parent__

	@property
	def result(self):
		return self._result

	@property
	def status(self):
		return _status_mapping[self._status_id]

	@property
	def error(self):
		return self._error

	@property
	def has_failed(self):
		return self._status_id == FAILED_ID
	hasFailed = has_failed

	@property
	def is_success(self):
		return self._status_id == COMPLETED_ID

	@property
	def is_new(self):
		return self._status_id == NEW_ID

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

		if (IJob.providedBy(self._callable_root) and
			self._callable_root.__parent__ is None):
			self._callable_root.__parent__ = self

	callable = property (_get_callable, _set_callable)

	def __call__(self, *args, **kwargs):
		self._active_start = datetime.datetime.utcnow()
		effective_args = list(args)
		effective_args[0:0] = self.args
		effective_kwargs = dict(self.kwargs)
		effective_kwargs.update(kwargs)
		__traceback_info__ = self._callable_root, self._callable_name, \
							 effective_args, effective_kwargs
		try:
			self._status_id = ACTIVE_ID
			result = self.callable(*effective_args, **effective_kwargs)
			self._status_id = COMPLETED_ID
			self._result = result
			return result
		except Exception as e:
			self._status_id = FAILED_ID
			self._error = self.error_adapter(sys.exc_info(), None) or \
						  self.error_adapter(e, None)
			logger.exception("Job (%s) execution failed", self)
		finally:
			self._active_end = datetime.datetime.utcnow()

	def __eq__(self, other):
		try:
			return self is other or (self._id == other._id and self._id)
		except AttributeError:
			return NotImplemented

	def __hash__(self):
		xhash = 47
		xhash ^= hash(self._id) if self._id else int(id(self) / 16)
		return xhash

def create_job(call, jargs=None, jkwargs=None, jobid=None, cls=Job):
	assert jargs is None or isinstance(jargs, (tuple, list))
	assert jkwargs is None or isinstance(jkwargs, Mapping)
	jkwargs = jkwargs or {}
	jargs = [call] + list(jargs or ())
	result = cls(*jargs, **jkwargs)
	result.id = unicode(jobid or uuid.uuid4())
	return result

@WithRepr
@EqHash('message')
@interface.implementer(IError)
class Error(Contained):

	def __init__(self, message=u''):
		self.message = message

	def __str__(self):
		return self.message
